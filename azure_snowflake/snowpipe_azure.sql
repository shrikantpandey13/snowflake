// create database
create database bloomberg;


// create schema 
create schema bloomberg.bloomberg_schema;


// Create storage integration object
create or replace storage integration snowpipe_az_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = AZURE
  ENABLED = TRUE 
  AZURE_TENANT_ID = '16ffbeea-3794-4b52-82a5-08d333d3d699'
  STORAGE_ALLOWED_LOCATIONS = ('azure://snowflakedatasetsa.blob.core.windows.net/customer-data')
  COMMENT = 'Integration with Azure blob ' ;



desc storage integration snowpipe_az_integration;


// create the file format object
create or replace file format bloomberg.bloomberg_schema.csv_snowpipe
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1;

    
// Create stage object with integration object & file format object
CREATE OR REPLACE STAGE csv_az_stage
    URL = 'azure://snowflakedatasetsa.blob.core.windows.net/customer-data'
    STORAGE_INTEGRATION = snowpipe_az_integration
    FILE_FORMAT = bloomberg.bloomberg_schema.csv_snowpipe;
    
-- list the files 
list @csv_az_stage;



// create notification 
CREATE OR REPLACE NOTIFICATION INTEGRATION SNOWPIPE_EVENT
  ENABLED = true
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = AZURE_STORAGE_QUEUE
  AZURE_STORAGE_QUEUE_PRIMARY_URI = 'https://snowflakedatasetsa.queue.core.windows.net/snowpipeque'
  AZURE_TENANT_ID = '16ffbeea-3794-4b52-82a5-08d333d3d699';

DESC NOTIFICATION INTEGRATION SNOWPIPE_EVENT;


CREATE OR REPLACE TABLE bloomberg.bloomberg_schema.orders (
ORDER_ID VARCHAR(30),
AMOUNT VARCHAR(30),
PROFIT INT,
QUANTITY INT,
CATEGORY VARCHAR(30),
SUBCATEGORY VARCHAR(30)
);


SELECT * FROM bloomberg.bloomberg_schema.orders;

COPY INTO bloomberg.bloomberg_schema.orders FROM @csv_az_stage;


-- create pipe

create or replace pipe orders_pipe
auto_ingest = true
integration = 'SNOWPIPE_EVENT'
AS
COPY INTO bloomberg.bloomberg_schema.orders FROM @csv_az_stage;

