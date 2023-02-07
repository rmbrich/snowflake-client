//const _ = require('lodash')
const snowflake = require('snowflake-sdk')

module.exports = class SnowflakeClient {  
   #conn
   #options

   /**
    * 
    * @param {Object} options
    * @param {string} options.account - Snowflake account ID (i.e. ab13241.us-east-2.aws)
    * @param {string} options.username - Snowflake username
    * @param {string} options.password - Snowflake password
    * @param {string} [options.role] - Role to use for queries
    * @param {string} [options.warehouse] - Warehouse to use for queries
    * @param {string} [options.database] - Database to use for queries
    * @param {string} [options.schema] - Schema to use for queries
    */
   constructor(options) {
      this.#conn = null
      this.#options = options
   }

   /**
    * Executes a select statement and returns an array of result objects or a Readable stream 
    * @param {string} statement - Select statement to execute
    * @param {boolean} [streamResults=false] Return a Readable stream instead of an array
    * @returns {Promise|ReadableStream}
    */
   async select(statement, streamResults = false) {
      try {
         if (!statement || !statement.toLowerCase().startsWith('select')) {
            throw new Error('Expected a SELECT statement')
         }
         if (!this.#conn || !Object(this.#conn).hasOwnProperty('isUp') || !this.#conn.isUp()) {
            this.#conn = await getConnection(this.#options)
         }
   
         console.info('Executing statement:', statement)
         let result       
         if (streamResults) {
            result = this.#conn.execute({
               sqlText: statement
            }).streamRows({
               objectMode: true
            })
         } else {
            result = new Promise((resolve, reject) => {
               this.#conn.execute({
                  sqlText: statement,
                  complete: function(err, stmt, rows) {
                     if (err) {
                        reject(err)
                     } else {
                        console.info('Select statement successful')
                        resolve(rows)
                     }
                  }
               })
            })
         }
         return result
      } catch (e) {
         console.error(e.message)
         throw e
      }
   }

   /**
    * Executes an insert statement given an array of records
    * @param {string} table - Table to insert into
    * @param {Object[]} records - Array of objects, each object containing one or more COLUMN: VALUE pairs
    */
   async insert(table, records = []) {
      try {
         if (!this.#conn || !Object(this.#conn).hasOwnProperty('isUp') || !this.#conn.isUp()) {
            this.#conn = await getConnection(this.#options)
         }

         let mapped = getKeysAndValues(records)
         let statement = `INSERT INTO ${table} (${mapped.keys.join()}) 
            VALUES (${mapped.keys.map(() => '?').join()})`
         console.info('Executing statement:', statement)
         return new Promise((resolve, reject) => {
            this.#conn.execute({
               sqlText: statement,
               binds: mapped.values,
               complete: function(err, stmt, rows) {
                  if (err) {
                     reject(err)
                  } else {
                     console.info('Insert statement successful')
                     resolve()
                  }
               }
            })
         })
      } catch (e) {
         console.error(e.message)
         throw e
      }
   }

   /**
    * Executes an update statement given an array of records
    * @param {string} table - Table to insert into
    * @param {Object} updates - Object containg COLUMN: VALUE pairs to update
    * @param {string} where - Restrict update to this where clause
    */
   async update(table, updates, where) {
      try {
         if (!this.#conn || !Object(this.#conn).hasOwnProperty('isUp') || !this.#conn.isUp()) {
            this.#conn = await getConnection(this.#options)
         }

         let cols = Object.keys(updates)
         let set = []
         let values = []
         for (let i=0; i<cols.length; i++) {
            set.push(`${cols[i]} = :${i+1}`)
            values.push(updates[cols[i]])
         }
         let statement = `UPDATE ${table} 
            SET ${set.join()} 
            WHERE ${where}`
         console.info('Executing statement:', statement)
         return new Promise((resolve, reject) => {
            this.#conn.execute({
               sqlText: statement,
               binds: values,
               complete: function(err, stmt, rows) {
                  if (err) {
                     reject(err)
                  } else {
                     console.info('Update statement successful')
                     resolve()
                  }
               }
            })
         })
      } catch(e) {
         console.error(e.message)
         throw e
      }
   }

   /**
    * Executes a delete statement with the given conditions
    * @param {string} table 
    * @param {string} where 
    */
   async delete(table, where) {
      try {
         if (!this.#conn || !Object(this.#conn).hasOwnProperty('isUp') || !this.#conn.isUp()) {
            this.#conn = await getConnection(this.#options)
         }

         let statement = `DELETE FROM ${table} WHERE ${where}`
         console.info('Executing statement:', statement)
         return new Promise((resolve, reject) => {
            this.#conn.execute({
               sqlText: statement,
               complete: function(err, stmt, rows) {
                  if (err) {
                     reject(err)
                  } else {
                     console.info('Delete statement successful')
                     resolve()
                  }
               }
            })
         })
      } catch (e) {
         console.error(e.message)
         throw e
      }
   }
}

async function getConnection (options) {
   const connection = snowflake.createConnection({...options})

   console.info(`Connecting to Snowflake ${options.account}`, {...options})
   return new Promise((resolve, reject) => {
      connection.connect(function(err, conn) {
         if (err) {            
            reject(err)
         } else {
            console.log(`Connected`)
            resolve(conn)
         }
      })
   })
}

// Get unique set of keys for an array of objects
function getUniqueKeys(arr) {
   let keys = {}
   for (o of arr) {
      for (k of Object.keys(o)) {
         keys[k] = 1
      }
   }
   return Object.keys(keys)
}

// Get array of arrays of values in order of unqiue keys
function getKeysAndValues(arr) {
   let values = []
   let keys = getUniqueKeys(arr)
   for (o of arr) {
      values.push(keys.map((k) => {
         return o[k] || null
      }))
   }
   return {keys, values}
}