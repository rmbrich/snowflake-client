require('dotenv').config()

const SnowflakeClient = require('../src/snowflake')
const client = new SnowflakeClient({
   account: process.env.SF_ACCOUNT,
   username: process.env.SF_USERNAME,
   password: process.env.SF_PASSWORD,
   warehouse: process.env.SF_WAREHOUSE,
   role: process.env.SF_ROLE,
   database: process.env.SF_DATABASE
})

const run1 = async () => {
   //let result = await client.select('select * from dbt_rob_brichler.temp')
   // let result = await client.select('select * from dbt_rob_brichler.temp', true)
   // result.on('data', (r) => {
   //    console.log(r)
   // })
   
   // let result = await client.insert('dbt_rob_brichler.temp', [
   //    { loan_id: '101', system: 'test', first_name: 'test1' },
   //    { loan_id: '102', system: 'test', first_name: 'test2' }
   // ])

   // let result = await client.update('dbt_rob_brichler.temp', {
   //    first_name: 'updated',
   //    autopay_amount: 1.2
   // }, `loan_id=101`)

   let result = await client.delete('dbt_rob_brichler.temp', `first_name='updated'`)

   console.log(result)
}
run1()