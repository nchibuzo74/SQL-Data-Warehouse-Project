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