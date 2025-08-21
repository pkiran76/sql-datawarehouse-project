/*
===================================================================================================================================
Stored procedure: Load bronze layer
===================================================================================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
=================================================================================================================================
*/

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
