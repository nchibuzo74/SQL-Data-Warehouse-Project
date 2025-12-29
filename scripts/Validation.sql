--------------------------------------------------------------------------------------------------------------------
---------------------------bronze_staging---------------------------------------------------------------------------------------
drop table bronze.crm_sales_details_staging;
drop table bronze.crm_prd_info_staging;
drop table bronze.crm_cust_info_staging;
drop table bronze.erp_loc_a101_staging;
drop table bronze.erp_cust_az12_staging;
drop table bronze.erp_px_cat_g1v2_staging;

----------------------------silver layer------------------------------------------------------------------------------------------
drop table silver.crm_sales_details;
drop table silver.crm_prd_info;
drop table silver.crm_cust_info;
drop table silver.erp_loc_a101;
drop table silver.erp_cust_az12;
drop table silver.erp_px_cat_g1v2;
--------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------


----------------------------Retrieval of data----------------------------------------------------------------------
SELECT count(*) as total_record 
from gold.dim_customers
where country is null;

SELECT * 
from gold.dim_customers
where country is null;

SELECT * FROM silver.erp_loc_a101;

SELECT * FROM bronze.erp_loc_a101;

select * from bronze.crm_cust_info;
select * from bronze.crm_prd_info;
select * from bronze.crm_sales_details;

select * from silver.crm_cust_info;
select * from silver.crm_prd_info;
select * from silver.crm_sales_details;

select * from gold.customers;
select * from gold.products;
select * from gold.sales;


select * from silver.erp_loc_a101;