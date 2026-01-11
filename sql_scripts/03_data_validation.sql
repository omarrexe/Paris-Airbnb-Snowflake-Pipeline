 /********************************************************************************
 * FILE: 03_data_validation.sql
 * PURPOSE: Quality check for the Bronze layer.
 ********************************************************************************/

SELECT 'Listings' as table_name, COUNT(*) as row_count FROM PARIS_AIRBNB_DB.BRONZE.RAW_LISTINGS
UNION ALL
SELECT 'Reviews', COUNT(*) FROM PARIS_AIRBNB_DB.BRONZE.RAW_REVIEWS
UNION ALL
SELECT 'Neighbourhoods', COUNT(*) FROM PARIS_AIRBNB_DB.BRONZE.RAW_NEIGHBOURHOODS;