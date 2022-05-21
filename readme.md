o2sql for pg is a ready-to-use tool to have [o2sql](https://www.npmjs.com/package/o2sql) working with [pg (node-postgres)](https://www.npmjs.com/package/pg).

# Install

```
npm install o2sql-pg
```

# Usage

## o2sql query

**Command.execute():Object|Array|Number|null**

**Command.execute(client: Client):Object|Array|Number|null**

```javascript
const o2sqlPg = require('o2sql-pg');
const o2sql = o2sqlPg({
  user: '** user **',
  host: '** host **',
  database: '** dbname **',
  password: '** pass **',
  port: 5432,
});

let list = await o2sql.select(['id', 'name']).from('user').execute();
```

For information about O2sql, please refer to [o2sql](https://www.npmjs.com/package/o2sql).

## group columns

```javascript
o2sql
  .select([
    'id',
    'name',
    {
      table: 'dept',
      fields: ['id', 'name'],
      prefix: 'dept',
      group: true,
    },
  ])
  .from('user')
  .innerJoin('dept', ['deptId', 'dept.id'])
  .default('user')
  .where({
    orgId: 3,
  })
  .orderby(['deptName']);

[
  {
    id: 1,
    name: 'a',
    dept: {
      id: 9,
      name: 'dept sample
    }
  },
  ...
]
```

## transaction

**O2sqlPg.transaction(queries: function, client: Client):Any**

Example:

```javascript
let data = await o2sql.transaction(async client => {
  let list1 = await o2sql.select(['id', 'name']).from('user').execute(client);
  let list2 = await o2sql.select(['id', 'name']).from('group').execute(client);
  return [list1, list2];
});
```

## pg query

**O2sqlPg.query(text: String, values: Array, client: Client)**

```javascript
// query
o2sql.query('select id, name from "user" where id = $1', [1]);

// transaction
let data = await o2sql.transaction(async client => {
  let { rows: list1 } = o2sql.query(
    'select id, name from "user" where id = $1',
    [1],
    client
  );
  let list2 = await o2sql.select(['id', 'name']).from('group').execute(client);
  return [list1, list2];
});
```
