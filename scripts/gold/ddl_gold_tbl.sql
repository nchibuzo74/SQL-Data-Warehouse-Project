/*
===============================================================================
DDL Script: Create Gold Tables
===============================================================================
Script Purpose:
    This script creates tables for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each table performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These tables can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.customers
-- =============================================================================
IF OBJECT_ID('gold.customers', 'U') IS NOT NULL
    DROP table gold.customers;
GO

SELECT
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, -- Surrogate key
    ci.cst_id                          AS customer_id,
    ci.cst_key                         AS customer_number,
    ci.cst_firstname                   AS first_name,
    ci.cst_lastname                    AS last_name,
    la.cntry                           AS country,
    ci.cst_marital_status              AS marital_status,
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the primary source for gender
        ELSE COALESCE(ca.gen, 'n/a')  			   -- Fallback to ERP data
    END                                AS gender,
    ca.bdate                           AS birthdate,
    ci.cst_create_date                 AS create_date
INTO gold.customers
FROM silver.crm_cust_info as ci
LEFT JOIN silver.erp_cust_az12 as ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 as la
    ON ci.cst_key = la.cid;
GO

-- =============================================================================
-- Create Dimension: gold.products
-- =============================================================================
IF OBJECT_ID('gold.products', 'U') IS NOT NULL
    DROP TABLE gold.products;
GO

SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- Surrogate key
    pn.prd_id       AS product_id,
    pn.prd_key      AS product_number,
    pn.prd_nm       AS product_name,
    pn.cat_id       AS category_id,
    pc.cat          AS category,
    pc.subcat       AS subcategory,
    pc.maintenance  AS maintenance,
    pn.prd_cost     AS cost,
    pn.prd_line     AS product_line,
    pn.prd_start_dt AS start_date
INTO gold.products
FROM silver.crm_prd_info as pn
LEFT JOIN silver.erp_px_cat_g1v2 as pc
    ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL; -- Filter out all historical data
GO

-- =============================================================================
-- Create Fact Table: gold.sales
-- =============================================================================
IF OBJECT_ID('gold.sales', 'U') IS NOT NULL
    DROP TABLE gold.sales;
GO

SELECT
    sd.sls_ord_num  AS order_number,
    pr.product_key  AS product_key,
    cu.customer_key AS customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price
into gold.sales
FROM silver.crm_sales_details as sd
LEFT JOIN gold.dim_products as pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers as cu
    ON sd.sls_cust_id = cu.customer_id;
GO