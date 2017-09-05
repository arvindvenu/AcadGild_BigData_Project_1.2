-- register the piggybank.jar for CSVExcelStorage function
-- the PIG_HOME is an externalized property which is looked up from task_1.properties
REGISTER '$PIG_HOME/lib/piggybank.jar';

-- load the data set using CSVExcelStorage function
-- the INPUT_PATH is an externalized property which is looked up from task_1.properties
psngr_data = LOAD '$INPUT_PATH' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX','SKIP_INPUT_HEADER') AS (psngr_id:chararray, survived: int, class:int, name: chararray, sex:chararray, age:int, sibsp: chararray, parch:chararray, tkt: chararray, fare: float, cabin:chararray, embarked:chararray);

-- project only the class and fare because we have to calculate the average fare for each class
-- other fields are not required
psngr_data_projected = FOREACH psngr_data GENERATE class,fare;

-- group by class because we have to apply aggregate function on each class
psngr_data_grouped = GROUP psngr_data_projected BY class;

-- calculate the average fare for each group(i.e. class) by using the AVG function
psngr_data_calc = FOREACH psngr_data_grouped GENERATE group as class, AVG(psngr_data_projected.fare) AS avg_fare;

-- store the output in local/hadoop file system
-- the OUTPUT_PATH is an externalized property which is looked up from task_1.properties
STORE psngr_data_calc INTO '$OUTPUT_PATH/task_1' USING PigStorage(',');
