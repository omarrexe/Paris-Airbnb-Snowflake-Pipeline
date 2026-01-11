 /********************************************************************************
 * LAYER: SILVER (Transformation & Modeling)
 * ------------------------------------------------------------------------------
 * PURPOSE: 
 * - Clean raw data (Type casting, NULL handling).
 * - Implement Star Schema (Facts & Dimensions).
 * - Apply Business Logic: Calculate 'Estimated Price' due to missing source data.
 ********************************************************************************/

USE DATABASE PARIS_AIRBNB_DB;
CREATE SCHEMA IF NOT EXISTS SILVER;
USE SCHEMA SILVER;


/********************************************************************************
 * 1. DIM_NEIGHBOURHOODS (Dimension Table)
 * Source: RAW_NEIGHBOURHOODS
 * Logic: Standardize IDs to Uppercase for reliable joining.
 ********************************************************************************/
CREATE OR REPLACE TABLE DIM_NEIGHBOURHOODS AS
SELECT DISTINCT
    UPPER(TRIM(neighbourhood)) AS neighbourhood_id, -- Primary Key
    neighbourhood AS neighbourhood_name,
    neighbourhood_group
FROM PARIS_AIRBNB_DB.BRONZE.RAW_NEIGHBOURHOODS
WHERE neighbourhood IS NOT NULL;


/********************************************************************************
 * 2. FACT_LISTINGS (Fact Table)
 * Source: RAW_LISTINGS
 * Logic: 
 * - Convert string IDs to Numbers.
 * - Calculate 'estimated_price' based on room_type and capacity logic.
 ********************************************************************************/
CREATE OR REPLACE TABLE FACT_LISTINGS AS
SELECT 
    TRY_CAST(id AS INTEGER) AS listing_id,          -- PK
    UPPER(TRIM(neighbourhood)) AS neighbourhood_id, -- FK to DIM_NEIGHBOURHOODS
    
    name AS listing_name,
    property_type,
    room_type,
    
    -- Handle capacity: Default to 1 if NULL or 0 to avoid calculation errors
    COALESCE(TRY_CAST(accommodates AS INTEGER), 1) AS accommodates,
    
    -- -------------------------------------------------------
    -- BUSINESS LOGIC: PRICE ESTIMATION MODEL
    -- Since raw price is missing, we estimate daily rate:
    -- Base Rate + (Per Person Rate * Capacity)
    -- -------------------------------------------------------
    CASE 
        WHEN room_type = 'Entire home/apt' 
            THEN (80 + (COALESCE(TRY_CAST(accommodates AS INTEGER), 1) * 20))
            
        WHEN room_type = 'Private room' 
            THEN (40 + (COALESCE(TRY_CAST(accommodates AS INTEGER), 1) * 15))
            
        WHEN room_type = 'Shared room' 
            THEN (25 + (COALESCE(TRY_CAST(accommodates AS INTEGER), 1) * 10))
            
        WHEN room_type = 'Hotel room' 
            THEN (100 + (COALESCE(TRY_CAST(accommodates AS INTEGER), 1) * 25))
            
        ELSE (50 + (COALESCE(TRY_CAST(accommodates AS INTEGER), 1) * 15))
    END AS estimated_price
    
FROM PARIS_AIRBNB_DB.BRONZE.RAW_LISTINGS
WHERE id IS NOT NULL;


/********************************************************************************
 * 3. FACT_REVIEWS (Fact Table)
 * Source: RAW_REVIEWS
 * Logic: Convert dates and link to listings.
 ********************************************************************************/
CREATE OR REPLACE TABLE FACT_REVIEWS AS
SELECT 
    TRY_CAST(review_id AS INTEGER) AS review_id,    -- PK
    TRY_CAST(listing_id AS INTEGER) AS listing_id,  -- FK to FACT_LISTINGS
    TRY_CAST(date AS DATE) AS review_date,
    reviewer_name,
    comments
FROM PARIS_AIRBNB_DB.BRONZE.RAW_REVIEWS
WHERE listing_id IS NOT NULL;


 