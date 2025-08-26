/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

create or alter procedure silver.load_silver as
begin
    declare @start_time datetime,@end_time datetime,@start_time_batch datetime,@end_time_batch datetime ---declare the variables to calculate the duration for each table insert execution
	begin try           ---try and catch is used to capture any errors during the execution
		set @start_time_batch=GETDATE();  ---to compute the entier batch run time

		print '====================================================================================='; 
		print 'Loading Silver Layer';
		print '=====================================================================================' ;

		print'--------------------------------------------------------------------------------------';
		print'Loading CRM Tables';
		print'--------------------------------------------------------------------------------------';

		set @start_time=GETDATE();
		print'>> Truncating table:silver.crm_cust_info';
		truncate table silver.crm_cust_info;
		print'>>Inserting Data Into the table: silver.crm_cust_info';
		insert into silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
		)
		select
		cst_id,
		cst_key,
		trim(cst_firstname),  ---remove Unwanted spaces
		trim(cst_lastname),
		case                  ---data normalisation/standardisation-maps coded values to meaningful,user-friendly descriptions
		  when upper(trim(cst_marital_status))='M' then 'Married'
		  when upper(trim(cst_marital_status))='S' then 'Single'
		  else 'n/a'          ---handled missing values, by filling the blanks with a default value
		  end as cst_marital_status,
		case 
		  when upper(trim(cst_gndr))='F' then 'Female'--to capture any mixed case issues
		  when upper(trim(cst_gndr))='M' then 'Male'
		  else 'n/a'
		  end as cst_gndr,
		cst_create_date
		from
		(select * ,           ---removed duplicates, by ensuring only 1 record per entity by identifying and retaining the most relevant row
		row_number()over(partition by cst_id order by cst_create_date desc) as rank_creation
		from bronze.crm_cust_info where cst_id is not null) as x
		where rank_creation=1; ---date filtering
		set @end_time=GETDATE();
		print'>>Load Duration:: '+cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds';
		print'>>------------------------------';

		set @start_time=GETDATE();
		print'>> Truncating table:silver.crm_prd_info';
		truncate table silver.crm_prd_info;
		print'>>Inserting Data Into the table: silver.crm_prd_info';
		insert into silver.crm_prd_info(
		 prd_id,
		 cat_id,
		 prd_key,
		 prd_nm,
		 prd_cost,
		 prd_line,
		 prd_start_dt,
		 prd_end_dt
		 )
		select
		prd_id,
		replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
		SUBSTRING(prd_key,7,len(prd_key)) as prd_key,
		prd_nm,
		coalesce(prd_cost,0) as prd_cost,
		case                  ---data normalisation/standardisation-maps coded values to meaningful,user-friendly descriptions
		  when upper(trim(prd_line))='M' then 'Mountain'
		  when upper(trim(prd_line))='S' then 'Other Sales'
		  when upper(trim(prd_line))='R' then 'Road'
		  when upper(trim(prd_line))='T' then 'Touring'
		  else 'n/a'          ---handled missing values, by filling the blanks with a default value
		end as prd_line,
		cast(prd_start_dt as date) as prd_start_dt,
		cast(dateadd(day,-1,lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)) as date) as prd_end_dt---calculate end date as one day before the next start date, just tomaintain easy roll-overs from day to day
		from
		bronze.crm_prd_info;
		set @end_time=GETDATE();
		print'>>Load Duration:: '+cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds';
		print'>>------------------------------';

		set @start_time=GETDATE();
		print'>> Truncating table:silver.crm_sales_details';
		truncate table silver.crm_sales_details;
		print'>>Inserting Data Into the table: silver.crm_sales_details';
		insert into silver.crm_sales_details(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
		)
		select 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		case
		   when sls_order_dt=0 or len(sls_order_dt)!=8 then null
		   else cast(cast(sls_order_dt as varchar) as date)  ---only in sqlserver, as we cannot convert an integer to date directly
		   end as sls_order_dt,
		case
		   when sls_ship_dt=0 or len(sls_ship_dt)!=8 then null---this is done as a precaution for any unseen issues even though the present column has no issues
		   else cast(cast(sls_ship_dt as varchar) as date)  ---only in sqlserver, as we cannot convert an integer to date directly
		   end as sls_ship_dt,
		case
		   when sls_due_dt=0 or len(sls_due_dt)!=8 then null---this is done as a precaution for any unseen issues even though the present column has no issues
		   else cast(cast(sls_due_dt as varchar) as date)  ---only in sqlserver, as we cannot convert an integer to date directly
		   end as sls_due_dt,
		case
		   when sls_sales<0 or sls_sales is null or sls_sales!=sls_quantity*abs(sls_price) then sls_quantity*abs(sls_price)--if the sales are -ve or 0 or null, then derive it using quantity and price
		   else sls_sales
		   end as sls_sales,
		sls_quantity,
		case
		   when sls_price<=0 or sls_price is null then abs(sls_sales)/nullif(sls_quantity,0)--if the price is<=0 or null, then derive it using sales and quantity, we use nullif to cover the scenario of quantity being 0, which will then be taken as null
		   else sls_price
		   end as sls_price
		from bronze.crm_sales_details;
		set @end_time=GETDATE();
		print'>>Load Duration:: '+cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds';
		print'>>------------------------------';

		print'--------------------------------------------------------------------------------------';
		print'Loading ERP Tables';
		print'--------------------------------------------------------------------------------------';

		set @start_time=GETDATE();
		print'>> Truncating table:silver.erp_CUST_AZ12';
		truncate table silver.erp_CUST_AZ12;
		print'>>Inserting Data Into the table: silver.erp_CUST_AZ12';
		insert into silver.erp_CUST_AZ12(
		CID,
		BDATE,
		GEN
		)
		select 
		case
		   when left(CID,3)='NAS' then SUBSTRING(CID,4,len(CID))
		   else CID
		   end as CID,
		case
		   when BDATE>getdate() then null
		   else BDATE
		   end as BDATE,
		case 
		   when upper(trim(gen)) in ('F','FEMALE') then 'Female'---this covers the case snesitivity and also the presence of any whitespaces
		   when upper(trim(gen)) in ('M','MALE') then 'Male'
		   else 'n/a'
		   end as GEN
		from bronze.erp_CUST_AZ12;
		set @end_time=GETDATE();
		print'>>Load Duration:: '+cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds';
		print'>>------------------------------';


		set @start_time=GETDATE();
		print'>> Truncating table:silver.erp_LOC_A101';
		truncate table silver.erp_LOC_A101;
		print'>>Inserting Data Into the table: silver.erp_LOC_A101';
		insert into silver.erp_LOC_A101(CID,CNTRY)
		select
		REPLACE(CID,'-','') as CID,
		case 
			when trim(upper(cntry)) in('US','USA') then 'United States'
			when trim(upper(cntry)) like 'DE' then 'Germany'
			when trim(upper(cntry)) is null or trim(upper(cntry))='' then 'n/a'
			else trim(cntry)
			end as CNTRY	
		from bronze.erp_LOC_A101;
		set @end_time=GETDATE();
		print'>>Load Duration:: '+cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds';
		print'>>------------------------------';

		set @start_time=GETDATE();
		print'>> Truncating table:silver.erp_PX_CAT_G1V2';
		truncate table silver.erp_PX_CAT_G1V2;
		print'>>Inserting Data Into the table: silver.erp_PX_CAT_G1V2';
		insert into silver.erp_PX_CAT_G1V2(
		ID,
		CAT,
		SUBCAT,
		MAINTENANCE
		)
		select *
		from bronze.erp_PX_CAT_G1V2;
		set @end_time=GETDATE();
		print'>>Load Duration:: '+cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds';
		print'>>------------------------------';

		set @end_time_batch=GETDATE();
		print'>>Load Duration of the Silver layer is:: '+cast(datediff(second,@start_time_batch,@end_time_batch) as nvarchar) + 'seconds';
		print'>>------------------------------';

    end try
	begin catch
	  print '=============================================================================='
	  print'ERROR OCCURRED DURING LOADING THE SILVER LAYER'
	  PRINT'Error Message' + error_message();
	  PRINT'Error Message' + cast(error_number() as nvarchar);
	  PRINT'Error Message' + cast(error_state() as nvarchar);
	  print '=============================================================================='

	end catch

end;


exec silver.load_silver;
