// Create storage integration object
create or replace storage integration s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE 
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::620078953357:role/aws_s3-snowflake_intg'
  STORAGE_ALLOWED_LOCATIONS = ('s3://aws-s3-shri-dev/csv/', 's3://aws-s3-shri-dev/json_data/')
  COMMENT = 'Integration with aws s3 buckets' ;


// description about storage integration

  DESC integration s3_int;

  // Create database and schema
CREATE DATABASE IF NOT EXISTS MYDB;

CREATE SCHEMA IF NOT EXISTS MYDB.customer_data;

// Create file format object
CREATE OR REPLACE file format mydb.customer_data.csv_fileformat
    type = csv
    field_delimiter = '|'
    skip_header = 1
    empty_field_as_null = TRUE; 





// Create stage object with integration object & file format object
CREATE OR REPLACE STAGE mydb.external_stages.aws_s3_csv
    URL = 's3://aws-s3-shri-dev/csv/'
    STORAGE_INTEGRATION = s3_int
    FILE_FORMAT = mydb.file_formats.csv_fileformat ;



//Listing files under your s3 buckets
list @mydb.external_stages.aws_s3_csv;


// Create a table first
CREATE OR REPLACE TABLE mydb.customer_data.customer_tbl (
customerid NUMBER,
custname STRING,
email STRING,
city STRING,
state STRING,
DOB DATE
);


// Use Copy command to load the files
COPY INTO mydb.customer_data.customer_tbl
    FROM @mydb.external_stages.aws_s3_csv
    PATTERN = '.*customer.*'; 

    
	
//Validate the data
SELECT * FROM mydb.customer_data.customer_tbl;

   