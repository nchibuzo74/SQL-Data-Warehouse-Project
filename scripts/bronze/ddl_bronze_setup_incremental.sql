/*
===============================================================================
Setup Script: Prepare Bronze Tables for Incremental Loading
===============================================================================
Script Purpose:
    This script prepares your bronze layer for incremental loading by:
    Adding load_timestamp column to track when records were loaded
    
Instructions:
    If tables already have load_timestamp or staging tables exist, those 
    sections will be skipped (no errors will occur).
    
Note: 
    Modify the staging table structures if your bronze tables have been 
    customized beyond the original design.
===============================================================================
*/

USE DataWarehouse;
GO

-- ============================================================================
-- STEP 1: Add load_timestamp column to existing bronze tables
-- ============================================================================

PRINT '';
PRINT '------------------------------------------------';
PRINT 'Step 1: Adding load_timestamp columns';
PRINT '------------------------------------------------';

-- Add to bronze.crm_cust_info
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'bronze' 
    AND TABLE_NAME = 'crm_cust_info' 
    AND COLUMN_NAME = 'load_timestamp'
)
BEGIN
    ALTER TABLE bronze.crm_cust_info 
    ADD load_timestamp DATETIME DEFAULT GETDATE();
    PRINT '✓ Added load_timestamp to bronze.crm_cust_info';
END
ELSE
    PRINT '• load_timestamp already exists in bronze.crm_cust_info';

-- Add to bronze.crm_prd_info
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'bronze' 
    AND TABLE_NAME = 'crm_prd_info' 
    AND COLUMN_NAME = 'load_timestamp'
)
BEGIN
    ALTER TABLE bronze.crm_prd_info 
    ADD load_timestamp DATETIME DEFAULT GETDATE();
    PRINT '✓ Added load_timestamp to bronze.crm_prd_info';
END
ELSE
    PRINT '• load_timestamp already exists in bronze.crm_prd_info';

-- Add to bronze.crm_sales_details
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'bronze' 
    AND TABLE_NAME = 'crm_sales_details' 
    AND COLUMN_NAME = 'load_timestamp'
)
BEGIN
    ALTER TABLE bronze.crm_sales_details 
    ADD load_timestamp DATETIME DEFAULT GETDATE();
    PRINT '✓ Added load_timestamp to bronze.crm_sales_details';
END
ELSE
    PRINT '• load_timestamp already exists in bronze.crm_sales_details';

-- Add to bronze.erp_cust_az12
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'bronze' 
    AND TABLE_NAME = 'erp_cust_az12' 
    AND COLUMN_NAME = 'load_timestamp'
)
BEGIN
    ALTER TABLE bronze.erp_cust_az12 
    ADD load_timestamp DATETIME DEFAULT GETDATE();
    PRINT '✓ Added load_timestamp to bronze.erp_cust_az12';
END
ELSE
    PRINT '• load_timestamp already exists in bronze.erp_cust_az12';

-- Add to bronze.erp_loc_a101
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'bronze' 
    AND TABLE_NAME = 'erp_loc_a101' 
    AND COLUMN_NAME = 'load_timestamp'
)
BEGIN
    ALTER TABLE bronze.erp_loc_a101 
    ADD load_timestamp DATETIME DEFAULT GETDATE();
    PRINT '✓ Added load_timestamp to bronze.erp_loc_a101';
END
ELSE
    PRINT '• load_timestamp already exists in bronze.erp_loc_a101';

-- Add to bronze.erp_px_cat_g1v2
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'bronze' 
    AND TABLE_NAME = 'erp_px_cat_g1v2' 
    AND COLUMN_NAME = 'load_timestamp'
)
BEGIN
    ALTER TABLE bronze.erp_px_cat_g1v2 
    ADD load_timestamp DATETIME DEFAULT GETDATE();
    PRINT '✓ Added load_timestamp to bronze.erp_px_cat_g1v2';
END
ELSE
    PRINT '• load_timestamp already exists in bronze.erp_px_cat_g1v2';