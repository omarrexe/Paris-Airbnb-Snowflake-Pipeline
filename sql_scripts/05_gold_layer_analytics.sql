 /********************************************************************************
 * LAYER: GOLD (Reporting & Analytics)
 * ------------------------------------------------------------------------------
 * PURPOSE: 
 * Create aggregated views (Data Marts) that directly answer business questions.
 * Optimized for BI tools (PowerBI, Tableau) or direct executive reporting.
 ********************************************************************************/

-- Set the warehouse for computation
USE WAREHOUSE PARIS_WH;

USE DATABASE PARIS_AIRBNB_DB;
CREATE SCHEMA IF NOT EXISTS GOLD;
USE SCHEMA GOLD;


/********************************************************************************
 * REPORT 1: PRICING STRATEGY  
 * Question: Which neighborhoods are the most expensive?
 * Logic: Join Listings with Neighborhoods, Aggregate Average Price.
 ********************************************************************************/
CREATE OR REPLACE TABLE DM_NEIGHBOURHOOD_METRICS AS
SELECT 
    n.neighbourhood_name,
    COUNT(l.listing_id) AS total_listings,
    ROUND(AVG(l.estimated_price), 2) AS avg_price_per_night,
    MIN(l.estimated_price) AS min_price,
    MAX(l.estimated_price) AS max_price,
    ROUND(SUM(l.accommodates), 0) AS total_capacity_guests
FROM PARIS_AIRBNB_DB.SILVER.FACT_LISTINGS l
JOIN PARIS_AIRBNB_DB.SILVER.DIM_NEIGHBOURHOODS n 
    ON l.neighbourhood_id = n.neighbourhood_id
GROUP BY n.neighbourhood_name
ORDER BY avg_price_per_night DESC;


/********************************************************************************
 * REPORT 2: MARKET DEMAND  
 * Question: Which areas have the most tourist traffic (Reviews)?
 * Logic: Join Reviews -> Listings -> Neighborhoods.
 ********************************************************************************/
CREATE OR REPLACE TABLE DM_TOURISM_TRAFFIC AS
SELECT 
    n.neighbourhood_name,
    COUNT(r.review_id) AS total_reviews,
    COUNT(DISTINCT r.listing_id) AS reviewed_listings_count,
    -- Calculate "Reviews per Listing" to measure density/popularity
    ROUND(COUNT(r.review_id) / NULLIF(COUNT(DISTINCT r.listing_id), 0), 1) AS avg_reviews_per_listing
FROM PARIS_AIRBNB_DB.SILVER.FACT_REVIEWS r
JOIN PARIS_AIRBNB_DB.SILVER.FACT_LISTINGS l 
    ON r.listing_id = l.listing_id
JOIN PARIS_AIRBNB_DB.SILVER.DIM_NEIGHBOURHOODS n 
    ON l.neighbourhood_id = n.neighbourhood_id
GROUP BY n.neighbourhood_name
ORDER BY total_reviews DESC;


/********************************************************************************
 * REPORT 3: INVENTORY TYPE  
 * Question: Is Paris mostly Apartments or Private Rooms?
 ********************************************************************************/
CREATE OR REPLACE TABLE DM_ROOM_TYPE_DISTRIBUTION AS
SELECT 
    n.neighbourhood_name,
    l.room_type,
    COUNT(l.listing_id) AS listing_count,
    ROUND(AVG(l.estimated_price), 2) AS avg_price
FROM PARIS_AIRBNB_DB.SILVER.FACT_LISTINGS l
JOIN PARIS_AIRBNB_DB.SILVER.DIM_NEIGHBOURHOODS n 
    ON l.neighbourhood_id = n.neighbourhood_id
GROUP BY n.neighbourhood_name, l.room_type
ORDER BY n.neighbourhood_name, listing_count DESC;


/********************************************************************************
 * REPORT 4: PRICE vs DEMAND ANALYSIS (Sweet Spot Strategy)
 * Question: Which price range attracts the most customers? What's the optimal pricing?
 * Logic: 
 * - Segment listings into 4 price categories (Budget, Mid-Range, Premium, Luxury).
 * - Count total reviews as a proxy for demand/bookings.
 * - Calculate "Reviews per Listing" to measure popularity within each price tier.
 * - Identify the "sweet spot" where price and demand are balanced.
 ********************************************************************************/
CREATE OR REPLACE TABLE DM_PRICE_DEMAND_ANALYSIS AS
SELECT 
    CASE 
        WHEN l.estimated_price < 50 THEN 'Budget (<€50)'
        WHEN l.estimated_price BETWEEN 50 AND 100 THEN 'Mid-Range (€50-€100)'
        WHEN l.estimated_price BETWEEN 100 AND 200 THEN 'Premium (€100-€200)'
        ELSE 'Luxury (>€200)'
    END AS price_category,
    COUNT(l.listing_id) AS total_listings,
    COUNT(r.review_id) AS total_reviews,
    ROUND(COUNT(r.review_id) / COUNT(DISTINCT l.listing_id), 1) AS avg_reviews_per_listing,
    ROUND(AVG(l.estimated_price), 2) AS avg_price
FROM PARIS_AIRBNB_DB.SILVER.FACT_LISTINGS l
LEFT JOIN PARIS_AIRBNB_DB.SILVER.FACT_REVIEWS r ON l.listing_id = r.listing_id
GROUP BY price_category
ORDER BY avg_price;



/********************************************************************************
 * REPORT 5: REVENUE POTENTIAL ANALYSIS
 * Question: How much revenue can each neighborhood generate annually?
 * Logic: 
 * - Calculate potential annual revenue per listing (Price × Occupancy × 365 days).
 * - Assume 60% occupancy rate (industry average for Airbnb).
 * - Aggregate total market size by neighborhood.
 ********************************************************************************/
CREATE OR REPLACE TABLE DM_REVENUE_POTENTIAL AS
SELECT 
    n.neighbourhood_name,
    l.room_type,
    COUNT(l.listing_id) AS total_listings,
    ROUND(AVG(l.estimated_price), 2) AS avg_nightly_rate,
    ROUND(AVG(l.estimated_price) * 0.60 * 365, 0) AS estimated_annual_revenue_per_listing,
    ROUND(SUM(l.estimated_price) * 0.60 * 365, 0) AS total_market_revenue_potential
FROM PARIS_AIRBNB_DB.SILVER.FACT_LISTINGS l
JOIN PARIS_AIRBNB_DB.SILVER.DIM_NEIGHBOURHOODS n 
    ON l.neighbourhood_id = n.neighbourhood_id
GROUP BY n.neighbourhood_name, l.room_type
ORDER BY total_market_revenue_potential DESC;

-- View the results
SELECT * FROM DM_TOURISM_TRAFFIC ;
SELECT * FROM DM_NEIGHBOURHOOD_METRICS;
SELECT * FROM DM_ROOM_TYPE_DISTRIBUTION ;
SELECT * FROM DM_PRICE_DEMAND_ANALYSIS ;
SELECT * FROM DM_REVENUE_POTENTIAL ;
 





  


 









 