-- register the piggybank.jar for CSVExcelStorage function
-- the PIG_HOME is an externalized property which is looked up from task_2.properties
REGISTER '$PIG_HOME/lib/piggybank.jar';

-- load the data set using CSVExcelStorage function
-- the INPUT_PATH is an externalized property which is looked up from task_2.properties
psngr_data = LOAD '$INPUT_PATH' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX','SKIP_INPUT_HEADER') AS (psngr_id:chararray, survived: int, class:int, name: chararray, sex:chararray, age:int, sibsp: chararray, parch:chararray, tkt: chararray, fare: float, cabin:chararray, embarked:chararray);

/*
project only the class, embarked and survived fields because we have to calculate the 
number of survived people embarked at Southampton for each class. Other fields are not 
needed
*/
psngr_data_projected = FOREACH psngr_data GENERATE class,embarked,survived;

-- filter by survived and embarked to limit the data in the 1st step itself
psngr_data_filtered = FILTER psngr_data_projected BY survived == 1 AND embarked == 'S';

-- now group the filtered data by class because we need the count per class
psngr_data_grouped = GROUP psngr_data_filtered by class;

-- calculate the count per class using the COUNT function
psngr_count = FOREACH psngr_data_grouped GENERATE group AS class, COUNT(psngr_data_filtered) AS count;

-- store the output in local/hadoop file system
-- the OUTPUT_PATH is an externalized property which is looked up from task_2.properties
STORE psngr_count INTO '$OUTPUT_PATH/task_2' USING PigStorage(',');
