/*
===============================================================================
Stored Procedure: Load Bronze Layer from Single File with Date Handling
===============================================================================
Script Purpose:
    This procedure handles date conversion issues by:
    1. Loading data into a staging table (all NVARCHAR columns)
    2. Converting dates properly with TRY_CONVERT
    3. Inserting cleaned data into target table

    Parameters:
    @FilePath NVARCHAR(500)     - Full path to the CSV file to load
    @TableName NVARCHAR(100)    - Name of the bronze table (without schema prefix)
    @AppendMode BIT             - 1 = Append data (default), 0 = Truncate first
    @FirstRow INT               - Row number where data starts (default = 2)
    @FieldTerminator CHAR(1)    - Field delimiter (default = ',')

Usage Examples:
    -- Append new data to existing table (customer table)
    EXEC bronze.load_bronze_file 
        @FilePath = 'C:\Chibz\VS Code Project\SQL Data Warehouse Project\datasets\source_crm\cust_info_one.csv',
        @TableName = 'crm_cust_info',
        @AppendMode = 1;

    -- Append new data to existing table (product table)
    EXEC bronze.load_bronze_file 
        @FilePath = 'C:\Chibz\VS Code Project\SQL Data Warehouse Project\datasets\source_crm\prd_info_one.csv',
        @TableName = 'crm_prd_info',
        @AppendMode = 1;

    -- Append new data to existing table (sales table)
    EXEC bronze.load_bronze_file 
        @FilePath = 'C:\Chibz\VS Code Project\SQL Data Warehouse Project\datasets\source_crm\sales_details_one.csv',
        @TableName = 'crm_sales_details',
        @AppendMode = 1;  

    -- Append new data to existing table (ERP location table)
    EXEC bronze.load_bronze_file 
        @FilePath = 'C:\Chibz\VS Code Project\SQL Data Warehouse Project\datasets\source_erp\LOC_A101_one.csv',
        @TableName = 'erp_loc_a101',
        @AppendMode = 1;

    -- Append new data to existing table (ERP customer birth table)
    EXEC bronze.load_bronze_file 
        @FilePath = 'C:\Chibz\VS Code Project\SQL Data Warehouse Project\datasets\source_erp\CUST_AZ12_one.csv',
        @TableName = 'erp_cust_az12',
        @AppendMode = 1;  

    -- Append new data to existing table (ERP category table)
    EXEC bronze.load_bronze_file 
        @FilePath = 'C:\Chibz\VS Code Project\SQL Data Warehouse Project\datasets\source_erp\PX_CAT_G1V2_one.csv',
        @TableName = 'erp_px_cat_g1v2',
        @AppendMode = 1;              

    -- Replace all data in table (full refresh example)
    EXEC bronze.load_bronze_file 
        @FilePath = 'C:\Chibz\VS Code Project\SQL Data Warehouse Project\datasets\source_crm\cust_info.csv',
        @TableName = 'crm_cust_info',
        @AppendMode = 0;

===============================================================================
*/

---- drop procedure bronze.load_bronze_file;

CREATE OR ALTER PROCEDURE bronze.load_bronze_file
    @FilePath NVARCHAR(500),
    @TableName NVARCHAR(100),
    @AppendMode BIT = 1,
    @FirstRow INT = 2,
    @FieldTerminator CHAR(1) = ','
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRANSACTION;   ---- Start Transaction
    
    DECLARE @start_time DATETIME, @end_time DATETIME, @row_count INT, @inserted_count INT;
    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @full_table_name NVARCHAR(150);
    DECLARE @staging_table NVARCHAR(150);
    
    BEGIN TRY    --- Try Block Starts Here
        SET @start_time = GETDATE();
        SET @full_table_name = 'bronze.' + @TableName;
        SET @staging_table = 'bronze.' + @TableName + '_staging';
        
        PRINT '================================================';
        PRINT 'Loading File with Data Type Handling';
        PRINT '================================================';
        PRINT 'File Path: ' + @FilePath;
        PRINT 'Target Table: ' + @full_table_name;
        PRINT 'Staging Table: ' + @staging_table;
        PRINT 'Mode: ' + CASE WHEN @AppendMode = 1 THEN 'APPEND' ELSE 'REPLACE' END;
        PRINT '------------------------------------------------';
        
        -- Validate tables exist
        IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES 
                      WHERE TABLE_SCHEMA = 'bronze' AND TABLE_NAME = @TableName)
        BEGIN
            RAISERROR('Target table %s does not exist', 16, 1, @full_table_name);
        END
        
        IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES 
                      WHERE TABLE_SCHEMA = 'bronze' AND TABLE_NAME = @TableName + '_staging')
        BEGIN
            RAISERROR('Staging table %s does not exist. Please create it first (see script header).', 16, 1, @staging_table);
        END
        
        -- STEP 1: Clear staging table
        PRINT '>> Step 1: Clearing staging table';
        SET @SQL = 'TRUNCATE TABLE ' + @staging_table;
        EXEC sp_executesql @SQL;
        
        -- STEP 2: Load to staging (all NVARCHAR, no conversion errors!)
        PRINT '>> Step 2: Loading data to staging (as text)';
        SET @SQL = N'
        BULK INSERT ' + @staging_table + N'
        FROM ''' + @FilePath + N'''
        WITH (
            FIRSTROW = ' + CAST(@FirstRow AS NVARCHAR) + N',
            FIELDTERMINATOR = ''' + @FieldTerminator + N''',
            TABLOCK
        )';
        
        EXEC sp_executesql @SQL;
        
        SET @SQL = N'SELECT @count = COUNT(*) FROM ' + @staging_table;
        EXEC sp_executesql @SQL, N'@count INT OUTPUT', @count = @row_count OUTPUT;
        PRINT '   - Loaded ' + CAST(@row_count AS NVARCHAR) + ' rows to staging';
        
        -- STEP 3: Truncate target if replace mode
        IF @AppendMode = 0
        BEGIN
            PRINT '>> Step 3: Truncating target table (REPLACE mode)';
            SET @SQL = 'TRUNCATE TABLE ' + @full_table_name;
            EXEC sp_executesql @SQL;
        END
        ELSE
        BEGIN
            PRINT '>> Step 3: Skipping truncate (APPEND mode)';
        END
        
        -- STEP 4: Insert with proper type conversion
        PRINT '>> Step 4: Converting data types and inserting to target table';
        
        -- ===========================
        -- CRM Tables
        -- ===========================
        IF @TableName = 'crm_cust_info'
        BEGIN
            INSERT INTO bronze.crm_cust_info (
                cst_id, cst_key, cst_firstname, cst_lastname, 
                cst_marital_status, cst_gndr, cst_create_date
            )
            SELECT 
                TRY_CAST(cst_id AS INT),
                cst_key,
                cst_firstname,
                cst_lastname,
                cst_marital_status,
                cst_gndr,
                -- Convert to DATE (if column is DATE type)
                COALESCE(
                    TRY_CONVERT(DATE, cst_create_date, 101),  -- MM/DD/YYYY
                    TRY_CONVERT(DATE, cst_create_date, 103),  -- DD/MM/YYYY
                    TRY_CONVERT(DATE, cst_create_date, 120),  -- YYYY-MM-DD
                    NULL
                ) AS cst_create_date
            FROM bronze.crm_cust_info_staging
            WHERE LTRIM(RTRIM(ISNULL(cst_id, ''))) != '';
            
            SET @inserted_count = @@ROWCOUNT;
        END
        ELSE IF @TableName = 'crm_prd_info'
        BEGIN
            INSERT INTO bronze.crm_prd_info (
                prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt
            )
            SELECT 
                TRY_CAST(prd_id AS INT),
                prd_key,
                prd_nm,
                TRY_CAST(prd_cost AS DECIMAL(19,2)),
                prd_line,
                -- Convert to DATE (if column is DATE type)
                COALESCE(
                    TRY_CONVERT(DATE, prd_start_dt, 101),
                    TRY_CONVERT(DATE, prd_start_dt, 103),
                    TRY_CONVERT(DATE, prd_start_dt, 120),
                    NULL
                ) AS prd_start_dt
            FROM bronze.crm_prd_info_staging
            WHERE LTRIM(RTRIM(ISNULL(prd_id, ''))) != '';
            
            SET @inserted_count = @@ROWCOUNT;
        END
        ELSE IF @TableName = 'crm_sales_details'
        BEGIN
            -- CRITICAL FIX: Keep dates as INT (not DATE) for bronze layer
            INSERT INTO bronze.crm_sales_details (
                sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, 
                sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price
            )
            SELECT 
                sls_ord_num,
                sls_prd_key,
                TRY_CAST(sls_cust_id AS INT),
                -- Keep as INT (YYYYMMDD format) - convert to DATE in Silver layer
                TRY_CAST(sls_order_dt AS INT) AS sls_order_dt,
                TRY_CAST(sls_ship_dt AS INT) AS sls_ship_dt,
                TRY_CAST(sls_due_dt AS INT) AS sls_due_dt,
                TRY_CAST(sls_sales AS DECIMAL(19,2)),
                TRY_CAST(sls_quantity AS INT),
                TRY_CAST(sls_price AS DECIMAL(19,2))
            FROM bronze.crm_sales_details_staging
            WHERE LTRIM(RTRIM(ISNULL(sls_ord_num, ''))) != '';
            
            SET @inserted_count = @@ROWCOUNT;
        END
        
        -- ===========================
        -- ERP Tables
        -- ===========================
        ELSE IF @TableName = 'erp_loc_a101'
        BEGIN
            INSERT INTO bronze.erp_loc_a101 (cid, cntry)
            SELECT 
                cid,
                cntry
            FROM bronze.erp_loc_a101_staging
            WHERE LTRIM(RTRIM(ISNULL(cid, ''))) != '';
            
            SET @inserted_count = @@ROWCOUNT;
        END
        ELSE IF @TableName = 'erp_cust_az12'
        BEGIN
            INSERT INTO bronze.erp_cust_az12 (cid, bdate, gen)
            SELECT 
                cid,
                -- Convert birthdate to DATE (if column is DATE type)
                COALESCE(
                    TRY_CONVERT(DATE, bdate, 101),
                    TRY_CONVERT(DATE, bdate, 103),
                    TRY_CONVERT(DATE, bdate, 120),
                    NULL
                ) AS bdate,
                gen
            FROM bronze.erp_cust_az12_staging
            WHERE LTRIM(RTRIM(ISNULL(cid, ''))) != '';
            
            SET @inserted_count = @@ROWCOUNT;
        END
        ELSE IF @TableName = 'erp_px_cat_g1v2'
        BEGIN
            INSERT INTO bronze.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
            SELECT 
                id,
                cat,
                subcat,
                TRY_CAST(maintenance AS DECIMAL(19,2))
            FROM bronze.erp_px_cat_g1v2_staging
            WHERE LTRIM(RTRIM(ISNULL(id, ''))) != '';
            
            SET @inserted_count = @@ROWCOUNT;
        END
        ELSE
        BEGIN
            RAISERROR('Table "%s" is not supported. Supported tables: crm_cust_info, crm_prd_info, crm_sales_details, erp_loc_a101, erp_cust_az12, erp_px_cat_g1v2', 16, 1, @TableName);
        END
        
        PRINT '   - Inserted ' + CAST(@inserted_count AS NVARCHAR) + ' rows to target table';
        
        -- STEP 5: Clear staging
        PRINT '>> Step 5: Clearing staging table';
        SET @SQL = 'TRUNCATE TABLE ' + @staging_table;
        EXEC sp_executesql @SQL;
        
        SET @end_time = GETDATE();
        
        PRINT '>> Total Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '================================================';
        PRINT 'File Load Completed Successfully';
        PRINT '================================================';
        
        COMMIT TRANSACTION;
        
    END TRY     ---- Try Block Ends Here
    BEGIN CATCH   --- Catching Errors Starts Here
        PRINT '================================================';
        PRINT 'ERROR OCCURRED DURING FILE LOAD';
        PRINT '------------------------------------------------';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error Line: ' + CAST(ERROR_LINE() AS NVARCHAR);
        PRINT '================================================';
        
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;  ---- Rollback Transaction
        THROW;
    END CATCH   ---- Error Handling End
END   --- End of Procedure
GO