--role to use - accountadmin is just for demo purposes
use role accountadmin; 
--The database (and schema) where the stored procedure is saved
create or replace database python_procedure;
--Where is the output database were the duplicated data should be saved
create or replace database dataout;

--set context 
use warehouse adhoc;
use database dataout;  
use schema public;


--Describe the share. We cannot pull share information in the stored procedure - we will do it outside
--Find the name of the share you want to pull from
show shares;

--Run a describe command. Make sure to put your share name in below
describe share AAAAA.AAA11111.DBNAME;

--Set the session variable query_id to the id of the previously run describe statement
set query_id=last_query_id();  

--Create a view that has the table of all the names of tables to be moved.
--This list will feed into our Python Stored procedure
create or replace temporary table python_procedure.public.table_list as  
select $2 as name
from table(result_scan($query_id))
where $1='TABLE';

--Create a python stored procedure
--Define the procedure, language, packages and lanugage versions
CREATE OR REPLACE PROCEDURE replicate_tables(input_tables STRING, output_db_schema STRING)
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.8'
  PACKAGES = ('snowflake-snowpark-python')
  HANDLER = 'run'
AS
$$
def run(session, input_tables, output_db_schema):

  #Create a datafframe w/ the tables to be copied 
  desc_share=session.table(input_tables)
  
  ##Collect returns reslts a list of row objects - the row has to be parsed out
  table_list = [row.NAME for row in desc_share.collect()] 
  
  #Iterate through the tables and issue a CTAS command
  for table in table_list:
    session.sql("create or replace table " + output_db_schema +"."+table.split('.')[2]+" as select * from "+ table+";").collect()
    
  return 'SUCCESS'
$$;


--Call the function - feed the input list and the output database.schema
CALL replicate_tables('python_procedure.public.table_list', 'dataout.public');
