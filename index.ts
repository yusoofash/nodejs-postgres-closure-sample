import Sequelize, { QueryTypes } from "sequelize";

const sequelize = new Sequelize.Sequelize({
  host: "localhost",
  username: "postgres",
  password: "root",
  database: "usr_mgmt",
  dialect: "postgres",
  port: 5432,
  pool: {
    max: 5,
    min: 0,
    acquire: 30000,
    idle: 10000,
  },
});

const initDB = async () => {
  await sequelize.query(`
    DROP TABLE IF EXISTS orders;
    `);
  await sequelize.query(`
    DROP TABLE IF EXISTS users_closure;
    `);
  await sequelize.query(`
    DROP TABLE IF EXISTS users;
    `);

  await sequelize.query(`
    CREATE TABLE users (
        id SERIAL PRIMARY KEY NOT NULL,
        parent_id INTEGER REFERENCES users(id),
        name VARCHAR NOT NULL,
        is_deleted BOOLEAN NOT NULL
    );
    `);

  await sequelize.query(`
    CREATE TABLE users_closure (
        ancestor_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
        descendant_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
        depth INTEGER NOT NULL,
        UNIQUE(ancestor_id, descendant_id)
    );
    `);

  await sequelize.query(`
    CREATE TABLE orders (
        id SERIAL PRIMARY KEY NOT NULL,
        total DECIMAL NOT NULL,
        user_id INTEGER REFERENCES users(id) NOT NULL
    );
    `);

  await sequelize.query(`
  CREATE OR REPLACE FUNCTION insert_user_closure_function()
    RETURNS trigger AS
    $BODY$
    BEGIN    
        IF (TG_OP = 'INSERT') THEN
            INSERT INTO users_closure (ancestor_id, descendant_id, depth)
            VALUES(NEW.id, NEW.id, 0);

            INSERT INTO users_closure (ancestor_id, descendant_id, depth)
                SELECT p.ancestor_id, c.descendant_id, p.depth+c.depth+1
                FROM users_closure p, users_closure c
                WHERE p.descendant_id = NEW.parent_id AND c.ancestor_id = NEW.id;
            RETURN NEW;
        END IF;
    
        RETURN null;
    END;
    $BODY$
    LANGUAGE plpgsql
  `);

  await sequelize.query(`
  CREATE TRIGGER insert_user_closure_trigger
  AFTER INSERT ON users
  FOR EACH ROW
  EXECUTE PROCEDURE insert_user_closure_function();
  `);

  await sequelize.query(`
  CREATE OR REPLACE FUNCTION delete_user_closure_function()
    RETURNS trigger AS
    $BODY$
    BEGIN    
        IF NEW.is_deleted != OLD.is_deleted THEN
          DELETE
          FROM
              users_closure O
          WHERE
              EXISTS (
                  SELECT
                      1
                  FROM
                      users_closure p,
                      users_closure c
                  WHERE
                      p.ancestor_id = O.ancestor_id
                      AND c.descendant_id = O.descendant_id
                      AND p.descendant_id = OLD.parent_id
                      AND c.ancestor_id = OLD.id
              );
            RETURN NEW;
        END IF;
    
        RETURN null;
    END;
    $BODY$
    LANGUAGE plpgsql
  `);

  await sequelize.query(`
  CREATE TRIGGER delete_user_closure_trigger
  AFTER UPDATE ON users
  FOR EACH ROW
  EXECUTE PROCEDURE delete_user_closure_function();
  `);

  console.log("Successfully created the tables");
};

const insertUser = async (
  name: string,
  isDeleted: boolean,
  parentId: number | null
) => {
  const [res] = await sequelize.query(
    `INSERT INTO users (name, parent_id, is_deleted) VALUES (:name, :parentId, :isDeleted) RETURNING id`,
    {
      type: QueryTypes.INSERT,
      replacements: { name, parentId, isDeleted },
    }
  );

  return (res as any)[0].id;
};

const deleteUser = async (userId: number) => {
  await sequelize.query(`UPDATE users SET is_deleted=true where id= :userId`, {
    type: QueryTypes.UPDATE,
    replacements: { userId },
  });
};

const updateParent = async (userId: number, parentId: number) => {
  // update parent id
  await sequelize.query(
    `UPDATE users SET parent_id= :parentId where parent_id= :userId`,
    {
      type: QueryTypes.UPDATE,
      replacements: { parentId, userId },
    }
  );

  // delete the subtree
  await sequelize.query(
    `DELETE FROM users_closure
    WHERE descendant_id IN (SELECT descendant_id FROM users_closure WHERE ancestor_id = :userId)
    AND ancestor_id IN (SELECT ancestor_id FROM users_closure WHERE descendant_id = :userId AND ancestor_id != descendant_id);
    `,
    {
      type: QueryTypes.UPDATE,
      replacements: { userId },
    }
  );

  // insert the subtree
  await sequelize.query(
    `INSERT INTO users_closure (ancestor_id, descendant_id, depth)
    SELECT supertree.ancestor_id, subtree.descendant_id, supertree.depth + subtree.depth + 1
    FROM users_closure AS supertree
    CROSS JOIN users_closure AS subtree
    WHERE supertree.descendant_id = :parentId
    AND subtree.ancestor_id = :userId;
    `,
    {
      type: QueryTypes.UPDATE,
      replacements: { parentId, userId },
    }
  );
};

const queryTree = async (userId: number) => {
  const users = await sequelize.query(
    `
    SELECT u.*
        FROM users u 
        JOIN users_closure uc 
        ON u.id = uc.descendant_id 
    WHERE ancestor_id = :userId AND is_deleted = false AND depth <> 0
    ORDER BY depth ASC;`,
    {
      type: QueryTypes.SELECT,
      logging: false,
      replacements: { userId },
    }
  );

  console.log(`\n`);
  console.table(users, ["id", "name", "parent_id"]);
  console.log(`\n`);
};

const queryDirectChildren = async (userId: number) => {
  const users = await sequelize.query(
    `
  SELECT
      subusers.*
  FROM
      users u
      INNER JOIN users subusers ON u.id = subusers.parent_id
  WHERE
      u.id = :userId;`,
    {
      type: QueryTypes.SELECT,
      logging: false,
      replacements: { userId },
    }
  );

  console.log(`\n`);
  console.table(users, ["id", "name", "parent_id"]);
  console.log(`\n`);
};

const queryAncestors = async (userId: number) => {
  const users = await sequelize.query(
    `
    SELECT *
    FROM   users u
           JOIN users_closure uc
             ON u.id = uc.ancestor_id
    WHERE  u.is_deleted = false
           AND uc.descendant_id = :userId
           AND depth <> 0;  
  `,
    {
      type: QueryTypes.SELECT,
      logging: false,
      replacements: { userId },
    }
  );

  console.log(`\n`);
  console.table(users, ["id", "name", "parent_id"]);
  console.log(`\n`);
};

const main = async () => {
  try {
    await initDB();
    const owner = await insertUser("owner 1", false, null);
    const manager1 = await insertUser("manager 1", false, owner);
    const staff1_1 = await insertUser("staff 1.1", false, manager1);

    const owner2 = await insertUser("owner 2", false, null);
    const manager2_1 = await insertUser("manager 2.1", false, owner2);
    const staff2_1 = await insertUser("staff 2.1", false, manager2_1);

    console.log("\nQuerying for owner 1");
    await queryTree(owner);

    // console.log("Querying for manager1");
    // await queryTree(manager1);

    console.log("Before Deleting staff1_1\n");
    await deleteUser(staff1_1);

    console.log("\nQuerying for owner 1 after staff1_1 deletion");
    await queryTree(owner);

    console.log("Query parents of staff2_1");
    await queryAncestors(staff2_1);

    console.log("Make manager1 the parent user of staff2_1\n");
    await updateParent(staff2_1, manager1);
    console.log("\nQuerying for owner1");
    await queryTree(owner);

    console.log("Querying direct children of manager1");
    await queryDirectChildren(manager1);

    process.exit(0);
  } catch (err) {
    console.error(err);
  }
};

main();
