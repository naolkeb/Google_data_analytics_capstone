# Create database for data cleaning
CREATE DATABASE Fitabase;
USE Fitabase;
set session group_concat_max_len = 1000000;

# Create tables for fitabase data
# Some ID values required BIGINT because they did not fit into they default int range.
CREATE TABLE IF NOT EXISTS daily_activity(
	Id BIGINT,
    Dt VARCHAR(255),
    TotalSteps INT,
    TotalDistance FLOAT,
    TrackerDistance FLOAT,
    LoggedActivitiesDistance FLOAT,
    VeryActiveDistance FLOAT,
    ModeratelyActiveDistance FLOAT,
    LightActiveDistance FLOAT,
    SedentaryActiveDistance FLOAT,
    VeryActiveMinutes INT,
    FairlyActiveMinutes INT,
    LightlyActiveMinutes INT,
    SedentaryMinutes INT,
    Calories INT);
CREATE TABLE IF NOT EXISTS heart_rate(
	Id INT,
    Dt VARCHAR(300),
    Val INT);
CREATE TABLE IF NOT EXISTS hourly_calories(
	Id BIGINT,
    Dt VARCHAR(300),
    Calories INT);
CREATE TABLE IF NOT EXISTS hourly_intensities(
	Id BIGINT,
    Dt VARCHAR(300),
    TotalIntensity INT,
    AverageIntensity FLOAT);
CREATE TABLE IF NOT EXISTS hourly_steps(
	Id BIGINT,
    Dt VARCHAR(300),
    Steps INT);
CREATE TABLE IF NOT EXISTS minute_calories(
	Id BIGINT,
    Dt VARCHAR(300),
    Calories INT);
CREATE TABLE IF NOT EXISTS minute_intensities(
	Id BIGINT,
    Dt VARCHAR(300),
    Intensity INT);
CREATE TABLE IF NOT EXISTS minute_steps(
	Id BIGINT,
    Dt VARCHAR(300),
    Steps INT);
CREATE TABLE IF NOT EXISTS minute_MET(
	Id BIGINT,
    Dt VARCHAR(300),
    METs INT);
CREATE TABLE IF NOT EXISTS minute_sleep(
	Id BIGINT,
    Dt VARCHAR(300),
    val INT,
    LogId BIGINT);
CREATE TABLE IF NOT EXISTS sleep_day(
	Id BIGINT,
    Dt VARCHAR(300),
    TotalSleepRecords INT,
    TotalMinutesAsleep INT,
    TotalTimeInBed INT);
CREATE TABLE IF NOT EXISTS weight_log(
	Id INT,
    Dt VARCHAR(300),
    WeightKg FLOAT,
    Weightlbs FLOAT,
    BMI FLOAT,
    IsManualReport BOOLEAN,
    LogId INT);

# This statement was use to generate SQL commands that affect all tables
# It made the cleaning process faster since I did not need to manually create each command
SELECT t.query
FROM
(
    SELECT CONCAT('TABLE ', a.table_name,
                  ' INTO OUTFILE \'C:/Users/Mrwin/Documents/datascience/Google_data_analytics_capstone/Fitabase Data 4.12.16-5.12.16/', a.table_name,'.csv\'
                  FIELDS TERMINATED BY \',\' OPTIONALLY ENCLOSED BY \'"\' ESCAPED BY \'\' LINES TERMINATED BY \'n\';') AS query,
        '1' AS id
    FROM information_schema.tables a
    WHERE a.table_schema = 'Fitabase'
) t;
SET GLOBAL local_infile=1;

# The dates need to be updated to dashes for the datetime conversion to work correctly
UPDATE daily_activity SET Dt = REPLACE(Dt, "/", "-");
UPDATE hourly_calories SET Dt = REPLACE(Dt, "/", "-");
UPDATE hourly_intensities SET Dt = REPLACE(Dt, "/", "-");
UPDATE hourly_steps SET Dt = REPLACE(Dt, "/", "-");
UPDATE minute_calories SET Dt = REPLACE(Dt, "/", "-");
UPDATE minute_intensities SET Dt = REPLACE(Dt, "/", "-");
UPDATE minute_met SET Dt = REPLACE(Dt, "/", "-");
UPDATE minute_sleep SET Dt = REPLACE(Dt, "/", "-");
UPDATE minute_steps SET Dt = REPLACE(Dt, "/", "-");
UPDATE sleep_day SET Dt = REPLACE(Dt, "/", "-");
UPDATE weight_log SET Dt = REPLACE(Dt, "/", "-");
UPDATE heart_rate SET Dt = REPLACE(Dt, "/", "-");

/* Because the values for all tables except daily_activity are 12 hour format datetimes,
the standard alter table command to modify columns to the datetime type does not work.
Here, it is best to create a new column and specify the format through the str_to_date() function.
*/
ALTER TABLE daily_activity ADD Datet DATETIME;
ALTER TABLE hourly_calories ADD Datet DATETIME;
ALTER TABLE hourly_intensities ADD Datet DATETIME;
ALTER TABLE hourly_steps ADD Datet DATETIME;
ALTER TABLE minute_calories ADD Datet DATETIME;
ALTER TABLE minute_intensities ADD Datet DATETIME;
ALTER TABLE minute_met ADD Datet DATETIME;
ALTER TABLE minute_sleep ADD Datet DATETIME;
ALTER TABLE minute_steps ADD Datet DATETIME;
ALTER TABLE sleep_day ADD Datet DATETIME;
ALTER TABLE weight_log ADD Datet DATETIME;
ALTER TABLE heart_rate ADD Datet DATETIME;

# Data from Dt column is converted into datetime datatype
UPDATE daily_activity SET Datet = str_to_date(Dt, '%m-%d-%Y');
UPDATE hourly_calories SET Datet = str_to_date(Dt, '%m-%d-%Y %r');
UPDATE hourly_intensities SET Datet = str_to_date(Dt, '%m-%d-%Y %r');
UPDATE hourly_steps SET Datet = str_to_date(Dt, '%m-%d-%Y %r');
UPDATE minute_calories SET Datet = str_to_date(Dt, '%m-%d-%Y %r');
UPDATE minute_intensities SET Datet = str_to_date(Dt, '%m-%d-%Y %r');
UPDATE minute_met SET Datet = str_to_date(Dt, '%m-%d-%Y %r');
UPDATE minute_sleep SET Datet = str_to_date(Dt, '%m-%d-%Y %r');
UPDATE minute_steps SET Datet = str_to_date(Dt, '%m-%d-%Y %r');
UPDATE sleep_day SET Datet = str_to_date(Dt, '%m-%d-%Y %r');
UPDATE weight_log SET Datet = str_to_date(Dt, '%m-%d-%Y %r');
UPDATE heart_rate SET Datet = str_to_date(Dt, '%m-%d-%Y %r');


# Loading data from csv files
LOAD DATA LOCAL INFILE 'C:/Users/Mrwin/Documents/datascience/Google_data_analytics_capstone/Fitabase Data 4.12.16-5.12.16/dailyActivity_merged.csv'
INTO TABLE daily_activity
FIELDS TERMINATED BY ','
ENCLOSED BY ''
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
LOAD DATA LOCAL INFILE 'C:/Users/Mrwin/Documents/datascience/Google_data_analytics_capstone/Fitabase Data 4.12.16-5.12.16/hourlyCalories_merged.csv'
INTO TABLE hourly_calories
FIELDS TERMINATED BY ','
ENCLOSED BY ''
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
LOAD DATA LOCAL INFILE 'C:/Users/Mrwin/Documents/datascience/Google_data_analytics_capstone/Fitabase Data 4.12.16-5.12.16/heartrate_seconds_merged.csv'
INTO TABLE heart_rate
FIELDS TERMINATED BY ','
ENCLOSED BY ''
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
LOAD DATA LOCAL INFILE 'C:/Users/Mrwin/Documents/datascience/Google_data_analytics_capstone/Fitabase Data 4.12.16-5.12.16/hourlyIntensities_merged.csv'
INTO TABLE hourly_intensities
FIELDS TERMINATED BY ','
ENCLOSED BY ''
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
LOAD DATA LOCAL INFILE 'C:/Users/Mrwin/Documents/datascience/Google_data_analytics_capstone/Fitabase Data 4.12.16-5.12.16/hourlySteps_merged.csv'
INTO TABLE hourly_steps
FIELDS TERMINATED BY ','
ENCLOSED BY ''
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
LOAD DATA LOCAL INFILE 'C:/Users/Mrwin/Documents/datascience/Google_data_analytics_capstone/Fitabase Data 4.12.16-5.12.16/minuteCaloriesNarrow_merged.csv'
INTO TABLE minute_calories
FIELDS TERMINATED BY ','
ENCLOSED BY ''
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
LOAD DATA LOCAL INFILE 'C:/Users/Mrwin/Documents/datascience/Google_data_analytics_capstone/Fitabase Data 4.12.16-5.12.16/minuteIntensitiesNarrow_merged.csv'
INTO TABLE minute_intensities
FIELDS TERMINATED BY ','
ENCLOSED BY ''
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
LOAD DATA LOCAL INFILE 'C:/Users/Mrwin/Documents/datascience/Google_data_analytics_capstone/Fitabase Data 4.12.16-5.12.16/minuteMETsNarrow_merged.csv'
INTO TABLE minute_MET
FIELDS TERMINATED BY ','
ENCLOSED BY ''
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
LOAD DATA LOCAL INFILE 'C:/Users/Mrwin/Documents/datascience/Google_data_analytics_capstone/Fitabase Data 4.12.16-5.12.16/minuteSleep_merged.csv'
INTO TABLE minute_sleep
FIELDS TERMINATED BY ','
ENCLOSED BY ''
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
LOAD DATA LOCAL INFILE 'C:/Users/Mrwin/Documents/datascience/Google_data_analytics_capstone/Fitabase Data 4.12.16-5.12.16/minuteStepsNarrow_merged.csv'
INTO TABLE minute_steps
FIELDS TERMINATED BY ','
ENCLOSED BY ''
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
LOAD DATA LOCAL INFILE 'C:/Users/Mrwin/Documents/datascience/Google_data_analytics_capstone/Fitabase Data 4.12.16-5.12.16/sleepDay_merged.csv'
INTO TABLE sleep_day
FIELDS TERMINATED BY ','
ENCLOSED BY ''
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
LOAD DATA LOCAL INFILE 'C:/Users/Mrwin/Documents/datascience/Google_data_analytics_capstone/Fitabase Data 4.12.16-5.12.16/weightLogInfo_merged.csv'
INTO TABLE weight_log
FIELDS TERMINATED BY ','
ENCLOSED BY ''
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

select GROUP_CONCAT(CONCAT("'",COLUMN_NAME,"'"))
from INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'weight_log'
AND TABLE_SCHEMA = 'Fitabase'
order BY ORDINAL_POSITION;

# Data is put back into csv files to be handled in R for statistical analysis and processing
SELECT 'Id','Dt','TotalSteps','TotalDistance','TrackerDistance','LoggedActivitiesDistance','VeryActiveDistance','ModeratelyActiveDistance','LightActiveDistance','SedentaryActiveDistance','VeryActiveMinutes','FairlyActiveMinutes','LightlyActiveMinutes','SedentaryMinutes','Calories','Datet'
UNION ALL
SELECT Id,Dt,TotalSteps,TotalDistance,TrackerDistance,LoggedActivitiesDistance,VeryActiveDistance,ModeratelyActiveDistance,LightActiveDistance,SedentaryActiveDistance,VeryActiveMinutes,FairlyActiveMinutes,LightlyActiveMinutes,SedentaryMinutes,Calories,Datet
FROM daily_activity INTO OUTFILE 'C:/Users/Mrwin/Documents/datascience/Google_data_analytics_capstone/Fitabase Data 4.12.16-5.12.16/daily_activity.csv'
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '' LINES TERMINATED BY '\n';

SELECT 'Id','Dt','Calories','Datet'
UNION ALL
SELECT Id, Dt, Calories, Datet
FROM hourly_calories INTO OUTFILE 'C:/Users/Mrwin/Documents/datascience/Google_data_analytics_capstone/Fitabase Data 4.12.16-5.12.16/hourly_calories.csv'
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '' LINES TERMINATED BY '\n';

SELECT 'Id','Dt','TotalIntensity','AverageIntensity','Datet'
UNION ALL
SELECT Id, Dt, TotalIntensity, AverageIntensity, Datet 
FROM hourly_intensities INTO OUTFILE 'C:/Users/Mrwin/Documents/datascience/Google_data_analytics_capstone/Fitabase Data 4.12.16-5.12.16/hourly_intensities.csv'
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '' LINES TERMINATED BY '\n';

SELECT 'Id','Dt','Steps','Datet'
UNION ALL
SELECT Id,Dt,Steps,Datet
FROM hourly_steps INTO OUTFILE 'C:/Users/Mrwin/Documents/datascience/Google_data_analytics_capstone/Fitabase Data 4.12.16-5.12.16/hourly_steps.csv'
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '' LINES TERMINATED BY '\n';

SELECT 'Id','Dt','Calories','Datet'
UNION ALL
SELECT Id, Dt, Calories, Datet
FROM minute_calories INTO OUTFILE 'C:/Users/Mrwin/Documents/datascience/Google_data_analytics_capstone/Fitabase Data 4.12.16-5.12.16/minute_calories.csv'
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '' LINES TERMINATED BY '\n';

SELECT 'Id','Dt','Intensity','Datet'
UNION ALL
SELECT Id, Dt, Intensity, Datet
FROM minute_intensities INTO OUTFILE 'C:/Users/Mrwin/Documents/datascience/Google_data_analytics_capstone/Fitabase Data 4.12.16-5.12.16/minute_intensities.csv'
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '' LINES TERMINATED BY '\n';

SELECT 'Id','Dt','METs','Datet'
UNION ALL
SELECT Id, Dt, METs, Datet
FROM minute_met INTO OUTFILE 'C:/Users/Mrwin/Documents/datascience/Google_data_analytics_capstone/Fitabase Data 4.12.16-5.12.16/minute_met.csv'
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '' LINES TERMINATED BY '\n';

SELECT 'Id','Dt','val','LogId','Datet'
UNION ALL
SELECT Id, Dt, val, LogId, Datet
FROM minute_sleep INTO OUTFILE 'C:/Users/Mrwin/Documents/datascience/Google_data_analytics_capstone/Fitabase Data 4.12.16-5.12.16/minute_sleep.csv'
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '' LINES TERMINATED BY '\n';

SELECT 'Id','Dt','Steps','Datet'
UNION ALL
SELECT Id, Dt, Steps, Datet
FROM minute_steps INTO OUTFILE 'C:/Users/Mrwin/Documents/datascience/Google_data_analytics_capstone/Fitabase Data 4.12.16-5.12.16/minute_steps.csv'
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '' LINES TERMINATED BY '\n';

SELECT 'Id','Dt','TotalSleepRecords','TotalMinutesAsleep','TotalTimeInBed','Datet'
UNION ALL
SELECT Id,Dt,TotalSleepRecords,TotalMinutesAsleep,TotalTimeInBed,Datet
FROM sleep_day INTO OUTFILE 'C:/Users/Mrwin/Documents/datascience/Google_data_analytics_capstone/Fitabase Data 4.12.16-5.12.16/sleep_day.csv'
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '' LINES TERMINATED BY '\n';

SELECT 'Id','Dt','WeightKg','Weightlbs','BMI','IsManualReport','LogId','Datet'
UNION ALL
SELECT Id,Dt,WeightKg,Weightlbs,BMI,IsManualReport,LogId,Datet
FROM weight_log INTO OUTFILE 'C:/Users/Mrwin/Documents/datascience/Google_data_analytics_capstone/Fitabase Data 4.12.16-5.12.16/weight_log.csv'
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '' LINES TERMINATED BY '\n';

SELECT 'Id','Dt','Val','Datet'
UNION ALL
SELECT Id, Dt, Val, Datet
FROM heart_rate INTO OUTFILE 'C:/Users/Mrwin/Documents/datascience/Google_data_analytics_capstone/Fitabase Data 4.12.16-5.12.16/heart_rate.csv'
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '' LINES TERMINATED BY '\n';

DROP DATABASE Fitabase;