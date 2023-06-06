--switch to account admin role 
Use role accountadmin;

-- Starting with creating a warehouse
create or replace warehouse Project_wh with
warehouse_size = 'X-SMALL'
min_cluster_count = 1
max_cluster_count = 3 
auto_suspend = 180
auto_resume = true
initially_suspended = true
comment = 'creating the warehouse for project and will be using this WH as default';

show warehouses;

--creating a database
create or replace database project_db;

--creating schema for tables
create or replace schema schema_table;

--creating schema for stage objects
create or replace schema schema_stage;

--creating schema for file format objects
create or replace schema schema_file_formats;

--creating a resource monitor to notify when certain percent of credit quota is been used
create or replace resource monitor monitor_1 with credit_quota = 50
triggers on 80 percent do notify
         on 95 percent do suspend;

alter warehouse project_wh set resource_monitor = monitor_1;

show warehouses;

--using stage schema to create stage object for 3 types of data such as csv, json and parquet
use schema schema_stage;

--creating integration object to connect with AWS to create a pipe that inserts data into snowflake database whenever any file uploaded into S3 bucket which we have created

create or replace storage integration AWS_Integration
type = external_stage
storage_provider = s3
enabled = true
storage_aws_role_arn = 'arn:aws:iam::756894550914:role/snowflake_role_policy'
storage_allowed_locations = ('s3://snowflakebucketintegration/');

--describing integration properties to fetch external id to paste it in AWS
desc integration aws_integration;

//creating file format objects. 
use schema schema_file_formats;

//creating csv format file_format
CREATE OR REPLACE file format csv_fileformat
    type = csv
    field_delimiter = ','
    skip_header = 1
    null_if = ('NULL','null')
    empty_field_as_null = TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY  = '"';
desc file format csv_fileformat;
--drop file format csv_fileformat;


//creating json format file_format 
CREATE OR REPLACE file format json_fileformat
type = json;

//creating parquet format file_format
CREATE OR REPLACE file format parquet_fileformat
type = parquet;

show file formats;

--creating stage objects
use schema schema_stage;

--creating stage object for csv format
create or replace stage csv_stage_aws
    URL = 's3://snowflakebucketintegration/CSV/'
    STORAGE_INTEGRATION = aws_integration
    FILE_FORMAT = project_db.schema_file_formats.csv_fileformat;

//listing the files in the respective folder
list @csv_stage_aws;

--creating stage object for json format
create or replace stage json_stage_aws
    URL = 's3://snowflakebucketintegration/JSON/'
    STORAGE_INTEGRATION = aws_integration
    FILE_FORMAT = project_db.schema_file_formats.json_fileformat;

//listing the files in the respective folder
list @json_stage_aws;

--creating stage object for parquet format
create or replace stage parquet_stage_aws
    URL = 's3://snowflakebucketintegration/Parquet/'
    STORAGE_INTEGRATION = aws_integration
    FILE_FORMAT = project_db.schema_file_formats.parquet_fileformat;

//listing the files in the respective folder
list @parquet_stage_aws;

show stages;

use schema schema_table; 
-- creating table for csv format file
create or replace table Netflix_csv(
    show_id	varchar,
    type	varchar,
    title	varchar,
    director varchar,
    casts	 varchar,
    country	 varchar,
    date_added	varchar,
    release_year integer,	
    rating	varchar,
    duration varchar,
    listed_in varchar,	
    description varchar
);

select * from Netflix_csv;

--creating table for json format file
create or replace table instruments_json(
    asin varchar,
    helpful varchar,
    overall float,
    reviewText varchar,
    reviewTime date,
    reviewerID varchar,
    reviewerName varchar,      
    summary varchar,
    unixReviewTime date
);

--creating table for parquet format
CREATE OR REPLACE TABLE sales_parquet (
    ROW_NUMBER int,
    index_level int,
    cat_id VARCHAR,
    date date,
    dept_id VARCHAR,
    id VARCHAR,
    item_id VARCHAR,
    state_id VARCHAR,
    store_id VARCHAR,
    value int,
    Load_date timestamp default TO_TIMESTAMP_NTZ(current_timestamp));


--validating if any errors while copying into table
COPY INTO Netflix_csv
    FROM @project_db.schema_stage.csv_stage_aws
    VALIDATION_MODE = RETURN_ERRORS;

--validating the copy command to return 5 rows if no error occurs
COPY INTO Netflix_csv
    FROM @project_db.schema_stage.csv_stage_aws
    VALIDATION_MODE = RETURN_5_rows;

--Loading the csv forma data
copy into Netflix_csv
from
@project_db.schema_stage.csv_stage_aws;

select * from Netflix_csv;
--truncate table netflix_csv;

//Validating the json stage
select * from @project_db.schema_stage.json_stage_aws;

//fetching them into individual columns
select $1:asin::varchar as Asin, 
       $1:helpful as Helpful,
       $1:overall::integer as Overall_Rating,
       $1:reviewText::varchar as Review,
       date_from_parts(right($1:reviewTime::varchar,4), left($1:reviewTime::varchar,2), 
       case when substr($1:reviewTime::varchar,5,1) = ',' 
            then substr($1:reviewTime::varchar,4,1) 
            else substr($1:reviewTime::varchar,4,2) end) as Review_date,
       $1:reviewerID::varchar as Reviewer_ID,
       $1:reviewerName::varchar as Reviewer_Name,
       $1:summary::varchar as Summary,
       date($1:unixReviewTime::int) as date
       from @project_db.schema_stage.json_stage_aws;

//Loading Json format data
copy into instruments_json
from (select $1:asin::varchar as Asin, 
       $1:helpful as Helpful,
       $1:overall::integer as Overall_Rating,
       $1:reviewText::varchar as Review,
       date_from_parts(right($1:reviewTime::varchar,4), left($1:reviewTime::varchar,2), 
       case when substr($1:reviewTime::varchar,5,1) = ',' 
            then substr($1:reviewTime::varchar,4,1) 
            else substr($1:reviewTime::varchar,4,2) end) as Review_date,
       $1:reviewerID::varchar as Reviewer_ID,
       $1:reviewerName::varchar as Reviewer_Name,
       $1:summary::varchar as Summary,
       date($1:unixReviewTime::int) as date
       from @project_db.schema_stage.json_stage_aws);

select * from instruments_json;
--truncate table instruments_json;


   // Load the parquet format data
   
COPY INTO sales_parquet
    FROM (SELECT 
            METADATA$FILE_ROW_NUMBER,
            $1:__index_level_0__::int,
            $1:cat_id::VARCHAR,
            DATE($1:date::int ),
            $1:"dept_id"::VARCHAR,
            $1:"id"::VARCHAR,
            $1:"item_id"::VARCHAR,
            $1:"state_id"::VARCHAR,
            $1:"store_id"::VARCHAR,
            $1:"value"::int,
            TO_TIMESTAMP_NTZ(current_timestamp)
        FROM @project_db.schema_stage.parquet_stage_aws);
        
    
SELECT * FROM sales_parquet;
--truncate table sales_parquet;

//Creating Snowpipe to auto insert the data into snowflake database whenever an file was uploaded iin S3 bucket.
//Creating table in snowflake to ingest the data
CREATE OR REPLACE TABLE employees_pipe (
  id INT,
  first_name STRING,
  last_name STRING,
  email STRING,
  location STRING,
  department STRING
  );

  // Create file format object
use schema schema_file_formats;

CREATE OR REPLACE file format csv_fileformat_pipe
    type = csv
    field_delimiter = ','
    skip_header = 1
    null_if = ('NULL','null')
    empty_field_as_null = TRUE;

 // Create stage object with integration object & file format object
 use schema schema_stage;
 
CREATE OR REPLACE stage stage_pipe
    URL = 's3://snowflakebucketintegration/CSV/Snowpipe/'
    STORAGE_INTEGRATION = aws_integration
    FILE_FORMAT = project_db.SCHEMA_FILE_FORMATS.csv_fileformat_pipe;

list@stage_pipe;
//Creating a schema for pipes
create or replace schema schema_pipe;

//Creating pipe to auto ingest the data into snowflake database whenever a file gets uploaded in S3 bucket
Create or replace pipe Snowflake_pipe
auto_ingest = true
as
copy into project_db.schema_table.employees_pipe
from @project_db.schema_stage.stage_pipe;


//describing the pipe to fetch notification channel
desc pipe Snowflake_pipe;

//Created event notification channel for AWS S3 bucket

//validating the table before insertion of files in S3 bucket
select * from employees_pipe;

//Now inserting one csv file with the name employee_data_1

//checking the status of pipe 
 alter pipe project_db.schema_pipe.snowflake_pipe refresh;

 //Validations
 select system$pipe_status('snowflake_pipe');

 select * from table(validate_pipe_load(
    pipe_name => 'project_db.schema_pipe.snowflake_pipe',
    start_time => dateadd(hour,-1,current_timestamp())));

// COPY command history from table to see error massage

select * from table (INFORMATION_SCHEMA.COPY_HISTORY(
   table_name  =>  'project_db.schema_table.employees_pipe',
   start_time =>dateadd(hour,-1,current_timestamp())));

 //after insertion of csv file
select * from employees_pipe;

//checking the status of pipe 
 alter pipe project_db.schema_pipe.snowflake_pipe refresh;

//inserted 3 more files into S3 bucket
   select * from table (INFORMATION_SCHEMA.COPY_HISTORY(
   table_name  =>  'project_db.schema_table.employees_pipe',
   start_time =>dateadd(hour,-1,current_timestamp())));
   
select * from project_db.schema_table.employees_pipe;

//To work with Time Travel, checking the above table before insertion of 3 files

//Fetching the query history to get the Query ID
select * from table(information_schema.query_history());

select * from employees_pipe at (statement => '01aca7f6-0000-5bf3-0004-eed6000731c6');

//Working with streams to capture change which is also called as Change Data Capture (CDC)

//Creating schema for stream object
create or replace schema schema_stream;


//TO capture the inserted data we are creating a stream object on table employee_pipe 

Create or replace stream Project_stream on table project_db.schema_table.employees_pipe;

show streams;

insert into project_db.schema_table.employees_pipe
values
(401,'Bharani','Prasanth','name@gmail.com','Hyderabad', 'IT'),
(402,'SHiva','Kumar','name@yahoo.com','Kolkata','IT');

Select * from project_stream;

//We can see the Metadata$action as insert and Metadata$update as false because the data has been inserted.
//If we consume the data the stream object will become empty. Consuming the data meant to be, for example, when a stream object is used in join operation with other table, the data will be consumed by the final table.

//Validating stream object by updating 1 row

update project_db.schema_table.employees_pipe
set location = 'Beizing'
where first_name = 'Monah' and last_name = 'MacKniely';

--drop stream project_stream;
--select * from project_db.schema_table.employees_pipe;
Select * from project_stream;
//As we can see here two rows were being reflected although we had just updated 1 row. This is because whenever a row gets updated it undergoes two operations, beginning with deletion and ending with insertion into the table. So, in this stream object we can see Metadata$action as delete and insert and with Metadata$update as true.



//Validating stream object by deleting rows
delete from project_db.schema_table.employees_pipe
where id = 2;

select * from project_stream;

//Here 4 rows have been deleted where id = 2 and we can see the Metadata$action as delete with Metadata$update as false. We can also see that for every insert, update and delete of data from table, the metadata$row_id as different.



//Creating a reader account for non-Snowflake user
//To create a share to non_snowflake user we need to create shared account for them and for that we need account admin role
use role accountadmin;

create managed account non_snowflake_account
admin_name = Bharani,
admin_password = 'Password@123',
type = reader;

//We get the URL to access the account from the query result
show managed accounts;
//Creating share object to add the managed account we cjust created

create or replace share Project_share;

//Granting usage and select on database, schema and table respectively to share object
Grant usage on database project_db to share project_share;
Grant usage on schema schema_table to share project_share;
grant select on table employees_pipe to share project_share;

show grants to share project_share;

//adding the non_snowflake user account to the share object
alter share project_share add account = ke94078;

//Log in into the reader account and create a database from share to query the table inside it.



/*Concepts/Utilities used in this Project
Loading/Copying structured and unstructured data from AWS S3 bucket,
Snowpipe - Automating ingestion of data,
Time Travel,
Streams - Change Data Capture(CDC),
Data Sharing 
*/