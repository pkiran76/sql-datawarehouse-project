/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

create view gold.dim_customers as
select
	row_number() over(order by cst_id) as customer_key,
	ci.cst_id as customer_id,
	ci.cst_key as customer_number,
	ci.cst_firstname as first_name,
	ci.cst_lastname as last_name,
	la.cntry as country,
	ci.cst_marital_status as marital_status,
	case
	   when ci.cst_gndr!='n/a' then cst_gndr
	   else coalesce(ca.gen,'n/a')---to take care of the NULL in the gen column
	   end as gender,
	ca.bdate as birth_date,
	ci.cst_create_date as create_date
from silver.crm_cust_info as ci
left join silver.erp_cust_AZ12 ca on ci.cst_key=ca.cid
left join silver.erp_loc_A101 la on ci.cst_key=la.cid;
GO
  
-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO
create view gold.dim_products as
select
row_number() over(order by cpi.prd_start_dt,cpi.prd_key) as product_key,
cpi.prd_id as product_id,
cpi.prd_key as product_number,
cpi.prd_nm as product_name,
cpi.cat_id as category_id,
epc.cat as category_type,
epc.subcat as subcategory_type,
epc.maintenance as maintenance_needed,
cpi.prd_cost as product_cost,
cpi.prd_line as product_line,
cpi.prd_start_dt as start_date
from silver.crm_prd_info as cpi
left join silver.erp_PX_CAT_G1V2 as epc on cpi.cat_id=epc.id
where cpi.prd_end_dt is null;
GO

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

create view gold.fact_sales as  
select
sls_ord_num as order_number,
pr.product_key as product_key,
cu.customer_key as customer_key,
sls_order_dt as order_date,
sls_ship_dt as shipping_date,
sls_due_dt as due_date,
sls_sales as sales,
sls_quantity as quantity,
sls_price as price
from silver.crm_sales_details sd
left join gold.dim_products pr on sd.sls_prd_key=pr.product_number
left join gold.dim_customers cu on sd.sls_cust_id=cu.customer_id;
GO
