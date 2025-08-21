/*
==============================================================================================================================
  DDL Script: To create the Bronze Layer tables
==============================================================================================================================
Script Purpose:
    This script is used to create the tables in the 'bronze' schema, dropping exisitng tables, if they already exist.
    Run this script to re-define the structure of the bronze tables
==============================================================================================================================
*/
GO
if OBJECT_ID('bronze.crm_cost_info','U')is not null --this is only for sql server, in other tools, we can directly check
	drop table bronze.crm_cost_info;
create table bronze.crm_cost_info(
			cst_id   int,
			cst_key  nvarchar(50),
			cst_firstname nvarchar(50),
			cst_lastname nvarchar(50),
			cst_marital_status nvarchar(50),
			cst_gndr nvarchar(50),
			cst_create_date date
 );
 GO

if OBJECT_ID('bronze.crm_prd_info','U')is not null --this is only for sql server, in other tools, we can directly check
	drop table bronze.crm_prd_info;
 create table bronze.crm_prd_info(
 prd_id int,
 prd_key nvarchar(50),
 prd_nm nvarchar(50),
 prd_cost int,
 prd_line nvarchar(50),
 prd_start_dt date,
 prd_end_dt date
  );
GO
if OBJECT_ID('bronze.crm_sales_details','U')is not null --this is only for sql server, in other tools, we can directly check
	drop table bronze.crm_sales_details;
create table bronze.crm_sales_details(
sls_ord_num nvarchar(50),
sls_prd_key nvarchar(50),
sls_cust_id	int,
sls_order_dt int,
sls_ship_dt int,
sls_due_dt int,
sls_sales int,
sls_quantity int,
sls_price int

);
GO
if OBJECT_ID('bronze.erp_CUST_AZ12','U')is not null --this is only for sql server, in other tools, we can directly check
	drop table bronze.erp_CUST_AZ12;
create table bronze.erp_CUST_AZ12(
CID	 nvarchar(50),
BDATE date,
GEN nvarchar(50)
);
GO
if OBJECT_ID('bronze.erp_LOC_A101','U')is not null --this is only for sql server, in other tools, we can directly check
	drop table bronze.erp_LOC_A101;
create table bronze.erp_LOC_A101(
CID	 nvarchar(50),
CNTRY nvarchar(50)
);
GO
if OBJECT_ID('bronze.erp_PX_CAT_G1V2','U')is not null --this is only for sql server, in other tools, we can directly check
	drop table bronze.erp_PX_CAT_G1V2;
create table bronze.erp_PX_CAT_G1V2(
ID nvarchar(50),
CAT	nvarchar(50),
SUBCAT nvarchar(50),
MAINTENANCE nvarchar(50)
);
GO


--Develop SQL Load Scripts
--Use the Bulk Insert method--here all the data is inserted in one go from the source to the destination unlike the normal Insert
--which does it row by row
truncate table bronze.crm_cust_info;--just to ensure that no duplicates exist whcih may happen if the bulk insert is inadertently executed more than once
bulk insert bronze.crm_cust_info ---18493 rows
from 'C:\Users\phani\Downloads\MS_Excel\Alex_DS_Course\SQL\Data_With_Baraa\sql-ultimate-course\sql-ultimate-course\datasets\mysql\Advanced_Level\Projects\DataWarehouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
with (
 firstrow=2, ---to indicate that the actual data starts from the 2nd row and the first row,having headers, must not be considered
 fieldterminator=',',---to tell sql the delimiter used in the csv file
 tablock ---to lock the table when the data is being loaded
);
--QC
select * from bronze.crm_cust_info;
select count(*) from bronze.crm_cust_info;;--18493

truncate table bronze.crm_prd_info;--just to ensure that no duplicates exist whcih may happen if the bulk insert is inadertently executed more than once
bulk insert bronze.crm_prd_info ---18493 rows
from 'C:\Users\phani\Downloads\MS_Excel\Alex_DS_Course\SQL\Data_With_Baraa\sql-ultimate-course\sql-ultimate-course\datasets\mysql\Advanced_Level\Projects\DataWarehouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
with (
 firstrow=2, ---to indicate that the actual data starts from the 2nd row and the first row,having headers, must not be considered
 fieldterminator=',',---to tell sql the delimiter used in the csv file
 tablock ---to lock the table when the data is being loaded
);
--QC
select * from bronze.crm_prd_info;
select count(*) from bronze.crm_prd_info;;--397


truncate table bronze.crm_sales_details;--just to ensure that no duplicates exist whcih may happen if the bulk insert is inadertently executed more than once
bulk insert bronze.crm_sales_details ---18493 rows
from 'C:\Users\phani\Downloads\MS_Excel\Alex_DS_Course\SQL\Data_With_Baraa\sql-ultimate-course\sql-ultimate-course\datasets\mysql\Advanced_Level\Projects\DataWarehouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
with (
 firstrow=2, ---to indicate that the actual data starts from the 2nd row and the first row,having headers, must not be considered
 fieldterminator=',',---to tell sql the delimiter used in the csv file
 tablock ---to lock the table when the data is being loaded
);
--QC
select * from bronze.crm_sales_details;
select count(*) from bronze.crm_sales_details;;--60398

truncate table bronze.erp_CUST_AZ12;--just to ensure that no duplicates exist whcih may happen if the bulk insert is inadertently executed more than once
bulk insert bronze.erp_CUST_AZ12 ---18493 rows
from 'C:\Users\phani\Downloads\MS_Excel\Alex_DS_Course\SQL\Data_With_Baraa\sql-ultimate-course\sql-ultimate-course\datasets\mysql\Advanced_Level\Projects\DataWarehouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
with (
 firstrow=2, ---to indicate that the actual data starts from the 2nd row and the first row,having headers, must not be considered
 fieldterminator=',',---to tell sql the delimiter used in the csv file
 tablock ---to lock the table when the data is being loaded
);
--QC
select * from bronze.erp_CUST_AZ12;
select count(*) from bronze.erp_CUST_AZ12;;--18483

truncate table bronze.erp_LOC_A101;--just to ensure that no duplicates exist whcih may happen if the bulk insert is inadertently executed more than once
bulk insert bronze.erp_LOC_A101 ---18493 rows
from 'C:\Users\phani\Downloads\MS_Excel\Alex_DS_Course\SQL\Data_With_Baraa\sql-ultimate-course\sql-ultimate-course\datasets\mysql\Advanced_Level\Projects\DataWarehouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
with (
 firstrow=2, ---to indicate that the actual data starts from the 2nd row and the first row,having headers, must not be considered
 fieldterminator=',',---to tell sql the delimiter used in the csv file
 tablock ---to lock the table when the data is being loaded
);
--QC
select * from bronze.erp_LOC_A101;
select count(*) from bronze.erp_LOC_A101;;--18484

truncate table bronze.erp_PX_CAT_G1V2;--just to ensure that no duplicates exist whcih may happen if the bulk insert is inadertently executed more than once
bulk insert bronze.erp_PX_CAT_G1V2 ---18493 rows
from 'C:\Users\phani\Downloads\MS_Excel\Alex_DS_Course\SQL\Data_With_Baraa\sql-ultimate-course\sql-ultimate-course\datasets\mysql\Advanced_Level\Projects\DataWarehouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
with (
 firstrow=2, ---to indicate that the actual data starts from the 2nd row and the first row,having headers, must not be considered
 fieldterminator=',',---to tell sql the delimiter used in the csv file
 tablock ---to lock the table when the data is being loaded
);
--QC
select * from bronze.erp_PX_CAT_G1V2;
select count(*) from bronze.erp_PX_CAT_G1V2;;--37


--Create Stored Procedure as the above block of code will be used frequently to refresh the data in the tables by loading them frequently

create or alter procedure bronze.load_bronze as
begin
    declare @start_time datetime,@end_time datetime,@start_time_batch datetime,@end_time_batch datetime ---declare the variables to calculate the duration for each table insert execution
	begin try           ---try and catch is used to capture any errors during the execution
		set @start_time_batch=GETDATE();  ---to compute the entier batch run time

		print '====================================================================================='; 
		print 'Loading Bronze Layer';
		print '=====================================================================================' ;

		print'--------------------------------------------------------------------------------------';
		print'Loading CRM Tables';
		print'--------------------------------------------------------------------------------------';

		set @start_time=GETDATE();
		print'>> Truncating table:bronze.crm_cust_info';
		truncate table bronze.crm_cust_info;--just to ensure that no duplicates exist whcih may happen if the bulk insert is inadertently executed more than once
	
		print'>> Inserting Data into the table:bronze.crm_cust_info';
		bulk insert bronze.crm_cust_info ---18493 rows
		from 'C:\Users\phani\Downloads\MS_Excel\Alex_DS_Course\SQL\Data_With_Baraa\sql-ultimate-course\sql-ultimate-course\datasets\mysql\Advanced_Level\Projects\DataWarehouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
		 firstrow=2, ---to indicate that the actual data starts from the 2nd row and the first row,having headers, must not be considered
		 fieldterminator=',',---to tell sql the delimiter used in the csv file
		 tablock ---to lock the table when the data is being loaded
		);
		set @end_time=GETDATE();
		print'>>Load Duration:: '+cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds';
		print'>>------------------------------';

		set @start_time=GETDATE();
		print'>> Truncating table:bronze.crm_prd_info';
		truncate table bronze.crm_prd_info;--just to ensure that no duplicates exist whcih may happen if the bulk insert is inadertently executed more than once
	

		print'>> Inserting Data into the table:bronze.crm_prd_info';
		bulk insert bronze.crm_prd_info ---18493 rows
		from 'C:\Users\phani\Downloads\MS_Excel\Alex_DS_Course\SQL\Data_With_Baraa\sql-ultimate-course\sql-ultimate-course\datasets\mysql\Advanced_Level\Projects\DataWarehouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
		 firstrow=2, ---to indicate that the actual data starts from the 2nd row and the first row,having headers, must not be considered
		 fieldterminator=',',---to tell sql the delimiter used in the csv file
		 tablock ---to lock the table when the data is being loaded
		);
		set @end_time=GETDATE();
		print'>>Load Duration:: '+cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds';
		print'>>------------------------------';

		set @start_time=GETDATE();
		print'>> Truncating table:bronze.crm_sales_details';
		truncate table bronze.crm_sales_details;--just to ensure that no duplicates exist whcih may happen if the bulk insert is inadertently executed more than once

		print'>> Inserting Data into the table:bronze.crm_sales_details';
		bulk insert bronze.crm_sales_details ---18493 rows
		from 'C:\Users\phani\Downloads\MS_Excel\Alex_DS_Course\SQL\Data_With_Baraa\sql-ultimate-course\sql-ultimate-course\datasets\mysql\Advanced_Level\Projects\DataWarehouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
		 firstrow=2, ---to indicate that the actual data starts from the 2nd row and the first row,having headers, must not be considered
		 fieldterminator=',',---to tell sql the delimiter used in the csv file
		 tablock ---to lock the table when the data is being loaded
		);
		set @end_time=GETDATE();
		print'>>Load Duration:: '+cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds';
		print'>>------------------------------';

		print'--------------------------------------------------------------------------------------';
		print'Loading ERP Tables';
		print'--------------------------------------------------------------------------------------';

		set @start_time=GETDATE();
		print'>> Truncating table:bronze.erp_CUST_AZ12';
		truncate table bronze.erp_CUST_AZ12;--just to ensure that no duplicates exist whcih may happen if the bulk insert is inadertently executed more than once
	
		print'>> Inserting Data into the table:bronze.erp_CUST_AZ12';
		bulk insert bronze.erp_CUST_AZ12 ---18493 rows
		from 'C:\Users\phani\Downloads\MS_Excel\Alex_DS_Course\SQL\Data_With_Baraa\sql-ultimate-course\sql-ultimate-course\datasets\mysql\Advanced_Level\Projects\DataWarehouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with (
		 firstrow=2, ---to indicate that the actual data starts from the 2nd row and the first row,having headers, must not be considered
		 fieldterminator=',',---to tell sql the delimiter used in the csv file
		 tablock ---to lock the table when the data is being loaded
		);
		set @end_time=GETDATE();
		print'>>Load Duration:: '+cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds';
		print'>>------------------------------';


		set @start_time=GETDATE();
		print'>> Truncating table:bronze.erp_LOC_A101';
		truncate table bronze.erp_LOC_A101;--just to ensure that no duplicates exist whcih may happen if the bulk insert is inadertently executed more than once

		print'>> Inserting Data into the table:bronze.erp_LOC_A101';
		bulk insert bronze.erp_LOC_A101 ---18493 rows
		from 'C:\Users\phani\Downloads\MS_Excel\Alex_DS_Course\SQL\Data_With_Baraa\sql-ultimate-course\sql-ultimate-course\datasets\mysql\Advanced_Level\Projects\DataWarehouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with (
		 firstrow=2, ---to indicate that the actual data starts from the 2nd row and the first row,having headers, must not be considered
		 fieldterminator=',',---to tell sql the delimiter used in the csv file
		 tablock ---to lock the table when the data is being loaded
		);
		set @end_time=GETDATE();
		print'>>Load Duration:: '+cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds';
		print'>>------------------------------';

		set @start_time=GETDATE();
		print'>> Truncating table:bronze.erp_PX_CAT_G1V2';
		truncate table bronze.erp_PX_CAT_G1V2;--just to ensure that no duplicates exist whcih may happen if the bulk insert is inadertently executed more than once
	
		print'>> Inserting Data into the table:bronze.erp_PX_CAT_G1V2';
		bulk insert bronze.erp_PX_CAT_G1V2 ---18493 rows
		from 'C:\Users\phani\Downloads\MS_Excel\Alex_DS_Course\SQL\Data_With_Baraa\sql-ultimate-course\sql-ultimate-course\datasets\mysql\Advanced_Level\Projects\DataWarehouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with (
		 firstrow=2, ---to indicate that the actual data starts from the 2nd row and the first row,having headers, must not be considered
		 fieldterminator=',',---to tell sql the delimiter used in the csv file
		 tablock ---to lock the table when the data is being loaded
		);
		set @end_time=GETDATE();
		print'>>Load Duration:: '+cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds';
		print'>>------------------------------';

		set @end_time_batch=GETDATE();
		print'>>Load Duration of the Bronze layer is:: '+cast(datediff(second,@start_time_batch,@end_time_batch) as nvarchar) + 'seconds';
		print'>>------------------------------';

    end try
	begin catch
	  print '=============================================================================='
	  print'ERROR OCCURRED DURING LOADING THE BRONZE LAYER'
	  PRINT'Error Message' + error_message();
	  PRINT'Error Message' + cast(error_number() as nvarchar);
	  PRINT'Error Message' + cast(error_state() as nvarchar);
	  print '=============================================================================='

	end catch
end

--To execute the SP----
execute bronze.load_bronze;
