-- register the piggybank.jar for CSVExcelStorage function
-- the PIG_HOME is an externalized property which is looked up from task_3.properties
REGISTER '$PIG_HOME/lib/piggybank.jar';

-- load the data set using CSVExcelStorage function
-- the INPUT_PATH is an externalized property which is looked up from task_3.properties
psngr_data = LOAD '$INPUT_PATH' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX','SKIP_INPUT_HEADER') AS (psngr_id:chararray, survived: int, class:int, name: chararray, sex:chararray, age:int, sibsp: chararray, parch:chararray, tkt: chararray, fare: float, cabin:chararray, embarked:chararray);

/*
project only the class, sex and survived fields because we have to calculate the number of males and females for each class. Other fields are not needed
*/
psngr_data_projected = FOREACH psngr_data GENERATE class,sex,survived;

-- filter by survived = 0 because we want count of dead people only
psngr_data_filtered = FILTER psngr_data_projected BY survived == 0;

-- now group the filtered data by class and sex because we need the number of dead male AND female for each class
psngr_data_grouped = GROUP psngr_data_filtered by (class,sex);

-- calculate the count per (class,sex) pair using the COUNT function
psngr_count = FOREACH psngr_data_grouped GENERATE group.class as class, group.sex as sex, COUNT(psngr_data_filtered) AS count;

-- store the output in local/hadoop file system
-- the OUTPUT_PATH is an externalized property which is looked up from task_3.properties
STORE psngr_count INTO '$OUTPUT_PATH/task_3' USING PigStorage(',');
