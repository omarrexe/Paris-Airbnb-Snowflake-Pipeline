/********************************************************************************
 * FILE: 01_infrastructure_setup.sql
 * PURPOSE: Setup Snowflake compute and storage resources.
 ********************************************************************************/

-- Create a Virtual Warehouse for compute power
CREATE OR REPLACE WAREHOUSE PARIS_WH
WITH WAREHOUSE_SIZE = 'SMALL'
AUTO_SUSPEND = 60
AUTO_RESUME = TRUE; 

USE WAREHOUSE PARIS_WH; 

-- Create the main Database
CREATE DATABASE IF NOT EXISTS PARIS_AIRBNB_DB; 
USE DATABASE PARIS_AIRBNB_DB;

-- Create Schemas for Medallion Architecture
CREATE OR REPLACE SCHEMA BRONZE;
CREATE OR REPLACE SCHEMA SILVER;
CREATE OR REPLACE SCHEMA GOLD;

  