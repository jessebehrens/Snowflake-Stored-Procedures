use role accountadmin; -- please use the correct role for your security policies
create or replace database dataout; --this is where new tables will go

--See what our shares are called
show shares; 

-- source share
describe share GBCILHP.RBB23199.CMS_DATA_SHARE; 

--Set the last query
set query_id=last_query_id();

create or replace temporary table outputdatatables as
select $2 as name
from table(result_scan($query_id))
where $1='TABLE';


create or replace procedure copy_share(input_tables STRING, output_db_schema STRING)
returns string
language sql
as
declare
    tbl_name string;
    c1 cursor for
        select name as tbl_name
        from outputdatatables;
    sql string;
    
begin
    for record in c1 do
        tbl_name := record.tbl_name;
        
        sql := 
        $$ create table $$||:output_db_schema||$$.$$||split_part(:tbl_name,'.',3)||$$ as 
           select *
           from $$||:tbl_name||$$;
        $$
        ;
        execute immediate (:sql);
    end for;
    return 'Success';
end;

CALL copy_share('outputdatatables', 'dataout.public');
