'use strict';
const mysql = require('mysql');
const { mysql: O2sql } = require('o2sql');

function wrapCommand(self, command) {
  command.execute = async function (client) {
    return await self.onExecuteHandler(this, client);
  };
  return command;
}

function queryFormat(query, values) {
  if (!values || values.length === 0) return query;

  const sql = query.replace(/\$(\d+)/g, function (txt, key) {
    const index = (~~key) - 1;
    if (values.length > index) {
      let value = values[index];
      if (value && typeof value === 'object') {
        value = JSON.stringify(value);
      }
      return this.escape(value);
    }
    return txt;
  }.bind(this));
  return sql;
}

let seq = 0;

class O2sqlMysql extends O2sql {
  constructor(config) {
    super();

    this.pool = mysql.createPool({
      ...config,
      queryFormat,
      typeCast: function (field, next) {
        if (field.type === 'JSON') {
          return JSON.parse(field.string());
        } else if (field.type === 'TINY' && field.length === 1) {
          return (field.string() === '1');
        }
        return next();
      }
    });
    this.id = `${process.pid}:${seq++}`;
    this.debug = !!config.debug;
    this.query('select now() as "currentTime", $1 as db', [config.database]).then(({ rows }) => {
      console.log(
        `[o2sql-mysql:${this.id}]: init connection to ${rows[0].db} succesfully. remote time ${rows[0].currentTime}`
      );
    });
  }
  _log(info) {
    this.debug && console.log(`[o2sql-mysql:${this.id}]:${info}`);
  }
  _dir(data) {
    this.debug && (console.log(`[o2sql-mysql:${this.id}]`) || console.dir(data));
  }
  _error(msg, error) {
    this.debug &&
      (console.error(`[o2sql-mysql:${this.id}]:${msg}`) || console.dir(error));
  }

  select() {
    return wrapCommand(this, super.select(...arguments));
  }
  get() {
    return wrapCommand(this, super.get(...arguments));
  }
  count() {
    return wrapCommand(this, super.count(...arguments));
  }
  update() {
    return wrapCommand(this, super.update(...arguments));
  }
  delete() {
    return wrapCommand(this, super.delete(...arguments));
  }
  insert() {
    return wrapCommand(this, super.insert(...arguments));
  }
  insertInto() {
    return wrapCommand(this, super.insertInto(...arguments));
  }

  async onExecuteHandler(command, client) {
    const { sql: text, values } = command.toParams();
    this._dir({
      text,
      values,
    });

    const { rows, fields } = await this.query(text, values, client);
    const rowCount = rows.length;

    let result;
    if (command instanceof O2sql.command.Count) {
      result = rows[0].count;
    } else {
      if (rows.length > 0) {
        let columns;
        if (command instanceof O2sql.command.Select) {
          columns = command.data.columns;
        } else if (
          command instanceof O2sql.command.Insert ||
          command instanceof O2sql.command.Update ||
          command instanceof O2sql.command.Delete
        ) {
          columns = command.data.returning;
        }

        if (command.data.columnGroups && command.data.columnGroups.length > 0) {
          rows.forEach(r => {
            command.data.columnGroups.forEach(g => {
              r[g.name] = {};
              g.columns.forEach(f => {
                r[g.name][f[1]] = r[f[0]];
                delete r[f[0]];
              });
            });
          });
        }
      }

      if (command instanceof O2sql.command.Insert) {
        if (rowCount === 0) {
          return null;
        } else if (command.data.values.length === 1) {
          result = rows.length > 0 ? rows[0] : {};
        } else {
          result = rows;
        }
      } else if (
        command instanceof O2sql.command.Update ||
        command instanceof O2sql.command.Delete
      ) {
        if (rowCount === 0) {
          return null;
        } else {
          result = rows;
        }
      } else if (command instanceof O2sql.command.Get) {
        result = rows.length > 0 ? rows[0] : null;
      } else if (command instanceof O2sql.command.Select) {
        result = rows;
      }
    }

    return result;
  }

  query(text, values, client) {
    this._dir({ text, values });
    return new Promise((resolve, reject) => {
      (client || this.pool).query(text, values, (error, results, fields) => {
        if (error) {
          reject(error);
        } else {
          resolve({ rows: results, fields });
        }
      });
    });
  }

  getConnection() {
    return new Promise((resolve, reject) => {
      this.pool.getConnection((err, connection) => {
        if (err) {
          reject(err);
        } else {
          resolve(connection)
        }
      })
    });
  }

  async transaction(queries, client) {
    const transitionClient = client || (await this.getConnection());

    return new Promise((resolve, reject) => {
      transitionClient.beginTransaction(err => {
        this._log('TRANSACTION BEGINS.............');
        if (err) {
          return reject(err);
        }
        queries(transitionClient).then((result) => {
          transitionClient.commit(err => {
            if (err) {
              transitionClient.rollback(() => {
                this._log('TRANSACTION ROLLBACK.............');
              });
              return reject(err);
            }
            this._log('TRANSACTION COMMITTED.............');
            resolve(result);
          })
          resolve(result);
        }).catch(err => {
          transitionClient.rollback(() => {
            this._log('TRANSACTION ROLLBACK.............');
          });
          reject(err);
        })
      })
    });
    // try {
    //   await transitionClient.query('BEGIN');
    //   this._log('TRANSACTION BEGINS.............');

    //   result = await queries(transitionClient);

    //   await transitionClient.query('COMMIT');
    //   this._log('TRANSACTION COMMITTED.............');
    // } catch (e) {
    //   this._error(`TRANSACTION ERROR`, e);
    //   error = e;
    //   await transitionClient.query('ROLLBACK');
    //   this._log('TRANSACTION ROLLBACK.............');
    // } finally {
    //   if (!client) {
    //     transitionClient.release();
    //   }
    // }
    // if (error) {
    //   throw error;
    // } else {
    //   return result;
    // }
  }
}

module.exports = function (config) {
  return new O2sqlMysql(config);
};