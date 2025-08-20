=========================================================================================================================================
/*Script Purpose: 
    This script creates a new DB named 'DataWarehouse' after checking if it already exists. If the DB exists, it is dropped and recreated.
    Additionally,the script sets up 3 schemas-bronze,silver,gold

/*WARNING:
    THE BELOW COMMAND WILL DELETE THE EXISTING DB DataWarehouse...PEASE CHECK FOR ITS CONTENTS AND IMPORTANCE BEFORE PROCEEDING. PLEASE
    ENSURE YOU HAVE THE NECESSARY BACKUP OF THE EXISTING FOLDER AND ITS CONTENTS BEFORE PROCEEDING......
========================================================================================================================================
--Create a DB DataWarehouse---
use master;---it is the master DB of sql server under which other DBs can be created
GO

--Drop and re-create the DataWarehouse DB
if exists(select 1 from sys.databases where name='DataWarehouse')
begin
  alter database DataWarehouse set single_user with rollback immediate;
  drop database DataWarehouse;
end;
GO
create database DataWarehouse;
GO
use DataWarehouse;
GO
---create the 3 schemas/folder--bronze,silver,gold

create schema bronze;
GO
create schema silver;
Go
create schema gold;
GO

---use GO in b/n multiple sql statements to enable simulataneous execution
