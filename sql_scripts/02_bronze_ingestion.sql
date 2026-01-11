/********************************************************************************
 * LAYER: BRONZE (Raw Data Ingestion)
 * ------------------------------------------------------------------------------
 * PURPOSE: 
 * Load raw CSV data from Azure Blob Storage into Snowflake tables.
 * This layer handles schema mapping and fixes source data issues (like NULL neighborhoods)
 * by selecting the correct columns during the COPY process.
 ********************************************************************************/

-- 1. Set Context
USE DATABASE PARIS_AIRBNB_DB;
USE SCHEMA BRONZE;

-- 2. Create File Format
-- Defines how Snowflake interprets the CSV files (handling commas, headers, and quotes)
CREATE OR REPLACE FILE FORMAT BRONZE_CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- 3. Create External Stage
-- Connects Snowflake to the Azure Blob Storage container
-- IMPORTANT: Replace with your actual Azure SAS token before running
CREATE OR REPLACE STAGE PARIS_DATA_STAGE
    URL = 'azure://parisairbnbraw.blob.core.windows.net/paris-data'
    CREDENTIALS = (AZURE_SAS_TOKEN = '<YOUR_AZURE_SAS_TOKEN>')
    FILE_FORMAT = BRONZE_CSV_FORMAT;


/********************************************************************************
 * TABLE 1: RAW_NEIGHBOURHOODS (Dimension Source)
 * Reference data containing the 20 official districts of Paris.
 ********************************************************************************/
CREATE OR REPLACE TABLE RAW_NEIGHBOURHOODS (
    neighbourhood_group STRING,
    neighbourhood STRING
);

-- Load data
COPY INTO RAW_NEIGHBOURHOODS
FROM @PARIS_DATA_STAGE/neighbourhoods.csv
FILE_FORMAT = BRONZE_CSV_FORMAT
ON_ERROR = 'CONTINUE';


/********************************************************************************
 * TABLE 2: RAW_LISTINGS (Fact Source)
 * Main inventory table. 
 * MAPPING STRATEGY: 
 * We map column $29 (neighbourhood_cleansed) instead of the raw neighbourhood column
 * to avoid 53,000+ NULL values. We also capture room_type and accommodates for pricing logic.
 ********************************************************************************/
 

-- 1. Re-create RAW_LISTINGS with safe data types (STRING)
CREATE OR REPLACE TABLE RAW_LISTINGS (
    id STRING,
    name STRING,
    neighbourhood STRING,      -- Mapped from $29
    property_type STRING,      -- Mapped from $33
    room_type STRING,          -- Mapped from $34
    accommodates STRING,       -- Mapped from $35
    price_raw STRING           -- Mapped from $41
);

-- 2. Load data using the CORRECTED column mapping
COPY INTO RAW_LISTINGS
FROM (
    SELECT 
        $1,   -- id
        $6,   -- name
        $29,  -- neighbourhood_cleansed
        $33,  -- property_type
        $34,  -- room_type
        $35,  -- accommodates (Correction: was 34)
        $41   -- price (Correction: was 40)
    FROM @PARIS_DATA_STAGE/listings.csv.gz
)
FILE_FORMAT = BRONZE_CSV_FORMAT
ON_ERROR = 'CONTINUE';

 


/********************************************************************************
 * TABLE 3: RAW_REVIEWS (Fact Source)
 * High-volume transactional data linking users to listings.
 ********************************************************************************/
CREATE OR REPLACE TABLE RAW_REVIEWS (
    listing_id NUMBER,
    review_id NUMBER,
    date DATE,
    reviewer_name STRING,
    comments STRING
);

-- Load data
COPY INTO RAW_REVIEWS
FROM (
    SELECT 
        $1, -- listing_id
        $2, -- id
        $3, -- date
        $5, -- reviewer_name
        $6  -- comments
    FROM @PARIS_DATA_STAGE/reviews.csv.gz
)
FILE_FORMAT = BRONZE_CSV_FORMAT
ON_ERROR = 'CONTINUE';

-- Final Validation: Check Row Counts
SELECT 'Listings' AS TABLE_NAME, COUNT(*) AS CNT FROM RAW_LISTINGS
UNION ALL
SELECT 'Neighbourhoods', COUNT(*) FROM RAW_NEIGHBOURHOODS
UNION ALL
SELECT 'Reviews', COUNT(*) FROM RAW_REVIEWS;





 