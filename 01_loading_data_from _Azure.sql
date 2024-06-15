-- Create a storage integration object
CREATE STORAGE INTEGRATION snow_azure_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = AZURE
  ENABLED = TRUE
  AZURE_TENANT_ID = 'a472b9b0-5551-460c-9094-6970671a46d4'
  STORAGE_ALLOWED_LOCATIONS = ('azure://snowazureintgshri.blob.core.windows.net/snowflake-azure-practice-file');

-- Describe integration object
DESC STORAGE INTEGRATION snow_azure_int;


  
  // Create database and schema
CREATE DATABASE IF NOT EXISTS MYDB;
CREATE SCHEMA IF NOT EXISTS MYDB.file_formats;
CREATE SCHEMA IF NOT EXISTS MYDB.external_stages;

// Create file format object
CREATE OR REPLACE file format mydb.file_formats.csv_fileformat
    type = csv
    field_delimiter = '|'
    skip_header = 1
    empty_field_as_null = TRUE;    
    
// Create stage object with integration object & file format object
CREATE OR REPLACE STAGE mydb.external_stages.stg_azure_cont
    URL = 'azure://snowazureintgshri.blob.core.windows.net/snowflake-azure-practice-file'
    STORAGE_INTEGRATION = snow_azure_int
    FILE_FORMAT = mydb.file_formats.csv_fileformat ;



//Listing files under your azure containers
list @mydb.external_stages.stg_azure_cont;


// Create a table first
CREATE OR REPLACE TABLE mydb.public.customer_data 
(
   customerid NUMBER,
   custname STRING,
   email STRING,
   city STRING,
   state STRING,
   DOB DATE
); 


// Use Copy command to load the files
COPY INTO mydb.public.customer_data
    FROM @mydb.external_stages.stg_azure_cont
    PATTERN = '.*customer.*';


//Validate the data
SELECT * FROM mydb.public.customer_data;
