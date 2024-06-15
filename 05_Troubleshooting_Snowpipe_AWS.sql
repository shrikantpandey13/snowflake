
//Trouble shooting Pipes


//Change the delimeter from ',' to '|'
CREATE OR REPLACE file format mydb.file_formats.csv_fileformat
    type = csv
    field_delimiter = '|'
    skip_header = 1
    empty_field_as_null = TRUE;



// Test the copy command
COPY INTO mydb.public.emp_data
FROM @mydb.external_stages.stage_aws_pipes
pattern = '.*employee.*';



// Step1: Validate pipe is actually working or not
SELECT SYSTEM$PIPE_STATUS('employee_pipe');



// Step2: Check the Copy History
SELECT * FROM TABLE( INFORMATION_SCHEMA.COPY_HISTORY
	(TABLE_NAME  =>  'mydb.public.emp_data',
	 START_TIME => DATEADD(HOUR, -10 ,CURRENT_TIMESTAMP()))
);


// Correct the delimiter to ','
CREATE OR REPLACE file format mydb.file_formats.csv_fileformat
    type = csv
    field_delimiter = ','
    skip_header = 1
    empty_field_as_null = TRUE;

// Step3: Validate the pipe load
SELECT * FROM TABLE(INFORMATION_SCHEMA.VALIDATE_PIPE_LOAD
	(PIPE_NAME => 'mydb.pipes.employee_pipe',
     START_TIME => DATEADD(HOUR,-2,CURRENT_TIMESTAMP()))
);


// Load the history files by running Copy command
COPY INTO mydb.public.emp_data
FROM @mydb.external_stages.stage_aws_pipes
FILES = ('sp_employee_3.csv');

// Validate the data in the table
SELECT * FROM mydb.public.emp_data;


// Managing Pipes


// How to see pipes?
DESC PIPE employee_pipe;

SHOW PIPES;

SHOW PIPES like '%employee%';

SHOW PIPES in database mydb;

SHOW PIPES in schema mydb.pipes;

// How to pause a pipe
ALTER PIPE mydb.pipes.employee_pipe SET PIPE_EXECUTION_PAUSED = true;

// How to resume the pipe
ALTER PIPE mydb.pipes.employee_pipe SET PIPE_EXECUTION_PAUSED = false;