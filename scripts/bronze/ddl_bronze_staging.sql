/*
===============================================================================
Stored Procedure: Create Staging Tables for Incremental Loads with Date Handling as Text the way Source Provides (CSV Files).
===============================================================================
*/

-- ============================================================================
-- STEP 1: Create ALL Staging Tables (Run Once)
-- ============================================================================

PRINT '================================================';
PRINT 'Creating Staging Tables';
PRINT '================================================';

-- ===========================
-- CRM Staging Tables
-- ===========================

-- Staging table for crm_cust_info (all columns as NVARCHAR)
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'crm_cust_info_staging' AND schema_id = SCHEMA_ID('bronze'))
BEGIN
    CREATE TABLE bronze.crm_cust_info_staging (
        cst_id NVARCHAR(255),
        cst_key NVARCHAR(255),
        cst_firstname NVARCHAR(255),
        cst_lastname NVARCHAR(255),
        cst_marital_status NVARCHAR(255),
        cst_gndr NVARCHAR(255),
        cst_create_date NVARCHAR(255)  -- Text, not DATE!
    );
    PRINT '✓ Created staging table: bronze.crm_cust_info_staging';
END
ELSE
    PRINT '• Staging table already exists: bronze.crm_cust_info_staging';
GO

-- Staging table for crm_prd_info
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'crm_prd_info_staging' AND schema_id = SCHEMA_ID('bronze'))
BEGIN
    CREATE TABLE bronze.crm_prd_info_staging (
        prd_id NVARCHAR(255),
        prd_key NVARCHAR(255),
        prd_nm NVARCHAR(255),
        prd_cost NVARCHAR(255),
        prd_line NVARCHAR(255),
        prd_start_dt NVARCHAR(255)  -- Text, not DATE!
    );
    PRINT '✓ Created staging table: bronze.crm_prd_info_staging';
END
ELSE
    PRINT '• Staging table already exists: bronze.crm_prd_info_staging';
GO

-- Staging table for crm_sales_details
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'crm_sales_details_staging' AND schema_id = SCHEMA_ID('bronze'))
BEGIN
    CREATE TABLE bronze.crm_sales_details_staging (
        sls_ord_num NVARCHAR(255),
        sls_prd_key NVARCHAR(255),
        sls_cust_id NVARCHAR(255),
        sls_order_dt NVARCHAR(255),  -- Text!
        sls_ship_dt NVARCHAR(255),   -- Text!
        sls_due_dt NVARCHAR(255),    -- Text!
        sls_sales NVARCHAR(255),
        sls_quantity NVARCHAR(255),
        sls_price NVARCHAR(255)
    );
    PRINT '✓ Created staging table: bronze.crm_sales_details_staging';
END
ELSE
    PRINT '• Staging table already exists: bronze.crm_sales_details_staging';
GO

-- ===========================
-- ERP Staging Tables
-- ===========================

-- Staging table for erp_loc_a101
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'erp_loc_a101_staging' AND schema_id = SCHEMA_ID('bronze'))
BEGIN
    CREATE TABLE bronze.erp_loc_a101_staging (
        cid NVARCHAR(255),
        cntry NVARCHAR(255)
    );
    PRINT '✓ Created staging table: bronze.erp_loc_a101_staging';
END
ELSE
    PRINT '• Staging table already exists: bronze.erp_loc_a101_staging';
GO

-- Staging table for erp_cust_az12
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'erp_cust_az12_staging' AND schema_id = SCHEMA_ID('bronze'))
BEGIN
    CREATE TABLE bronze.erp_cust_az12_staging (
        cid NVARCHAR(255),
        bdate NVARCHAR(255),  -- Text, not DATE!
        gen NVARCHAR(255)
    );
    PRINT '✓ Created staging table: bronze.erp_cust_az12_staging';
END
ELSE
    PRINT '• Staging table already exists: bronze.erp_cust_az12_staging';
GO

-- Staging table for erp_px_cat_g1v2
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'erp_px_cat_g1v2_staging' AND schema_id = SCHEMA_ID('bronze'))
BEGIN
    CREATE TABLE bronze.erp_px_cat_g1v2_staging (
        id NVARCHAR(255),
        cat NVARCHAR(255),
        subcat NVARCHAR(255),
        maintenance NVARCHAR(255)  -- Text, not DECIMAL!
    );
    PRINT '✓ Created staging table: bronze.erp_px_cat_g1v2_staging';
END
ELSE
    PRINT '• Staging table already exists: bronze.erp_px_cat_g1v2_staging';
GO

PRINT '================================================';
PRINT 'Staging Tables Setup Complete';
PRINT '================================================';
GO