
// Alter storage integration to add xml files location
alter storage integration s3_int
set STORAGE_ALLOWED_LOCATIONS = ('s3://aws-s3-shri-dev/json_data/', 's3://aws-s3-shri-dev/csv', 's3://aws-s3-shri-dev/xml_data/');

DESC integration s3_int;

// Create required datbase and schemas
create database if not exists my_db;
create schema if not exists my_db.file_formats;
create schema if not exists my_db.external_stages;
create schema if not exists my_db.stage_tbls;
create schema if not exists my_db.intg_tbls;



// Create file format object for xml files
CREATE OR REPLACE file format my_db.file_formats.xml_fileformat
    type = xml;


// Create stage on external s3 location
CREATE OR REPLACE STAGE my_db.external_stages.aws_s3_xml
    URL = 's3://aws-s3-shri-dev/xml_data/'
    STORAGE_INTEGRATION = s3_int
    FILE_FORMAT = my_db.file_formats.xml_fileformat;


// Listing files under your s3 xml bucket
list @my_db.external_stages.aws_s3_xml;


// View data from xml file
select * from @my_db.external_stages.aws_s3_xml/books_20230304.xml;



// Create variant table to load xml file
CREATE OR REPLACE TABLE my_db.stage_tbls.STG_BOOKS(xml_data variant);

select * from my_db.stage_tbls.STG_BOOKS;

// Load xml file to variant table
copy into my_db.stage_tbls.STG_BOOKS
from @my_db.external_stages.aws_s3_xml
files=('books_20230304.xml')
force=TRUE;


// Query stage table
select * from my_db.stage_tbls.STG_BOOKS;


// Create Target table to load final data 
CREATE OR REPLACE TABLE intg_tbls.BOOKS
( 
	book_id varchar(20) not null,
	author varchar(50),
	title varchar(50),
	genre varchar(20),
	price number(10,2),
	publish_date date,
	description varchar(255),
	PRIMARY KEY(book_id)
);


// To get the root element name
select xml_data:"@" from my_db.stage_tbls.STG_BOOKS;


// To get root element value
select xml_data:"$" from my_db.stage_tbls.STG_BOOKS;


// Get the content using xmlget function, index position
select xmlget(xml_data,'book',0):"$" from my_db.stage_tbls.STG_BOOKS;
select xmlget(xml_data,'book',1):"$" from my_db.stage_tbls.STG_BOOKS;


// Fetch actual data from file
SELECT 
XMLGET(bk.value, 'id' ):"$" as "book_id",
XMLGET(bk.value, 'author' ):"$" as "author"
FROM my_db.stage_tbls.STG_BOOKS,
LATERAL FLATTEN(to_array(STG_BOOKS.xml_data:"$" )) bk;



// Fetch data and assign datatypes
SELECT 
XMLGET(bk.value, 'id' ):"$" :: varchar as "book_id",
XMLGET(bk.value, 'author' ):"$" :: varchar as "author",
XMLGET(bk.value, 'title' ):"$" :: varchar as "title",
XMLGET(bk.value, 'genre' ):"$" :: varchar as "genre",
XMLGET(bk.value, 'price' ):"$" :: number(10,2) as "price",
XMLGET(bk.value, 'publish_date' ):"$" :: date as "publish_date",
XMLGET(bk.value, 'description' ):"$" :: varchar as "description"

FROM my_db.stage_tbls.STG_BOOKS,
LATERAL FLATTEN(to_array(STG_BOOKS.xml_data:"$" )) bk;



// Insert data from stage table to final target table
INSERT INTO intg_tbls.BOOKS
SELECT 
XMLGET(bk.value, 'id' ):"$" :: varchar as "book_id",
XMLGET(bk.value, 'author' ):"$" :: varchar as "author",
XMLGET(bk.value, 'title' ):"$" :: varchar as "title",
XMLGET(bk.value, 'genre' ):"$" :: varchar as "genre",
XMLGET(bk.value, 'price' ):"$" :: number(10,2) as "price",
XMLGET(bk.value, 'publish_date' ):"$" :: date as "publish_date",
XMLGET(bk.value, 'description' ):"$" :: varchar as "description"
FROM my_db.stage_tbls.STG_BOOKS,
LATERAL FLATTEN(to_array(STG_BOOKS.xml_data:"$" )) bk;


// View final data
SELECT * FROM intg_tbls.BOOKS;


/*

XML Operators and Functions:

$ : The dollar sign operator returns the contents, as a VARIANT, of the value it operates on. In other words, The contents of the VARIANT field.

@: The name of the “root” element of the XML contained in the VARIANT field.

XMLGET : Extracts an XML element object (often referred to as simply a “tag”) from a content of outer XML element object by the name of the tag and its instance number (counting from 0):

If any argument of XMLGET is NULL, the result is NULL.
If the tag instance is not found, the result is similarly NULL



Load XML
Devise a query to extract the Root element as well as root content from the XML file.

Will use :

@ : to extract root element

$ : to extract root content

We will also use LATERAL FLATTEN takes an XML input and explode the list of sub-elements into its own (denormalized) row.

*/
