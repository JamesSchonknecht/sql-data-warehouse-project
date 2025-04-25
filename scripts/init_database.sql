/*
==================================================================
Create Database and Schemas
==================================================================

Script purpose:
	Creates a new database called 'DataWarehouse' after checking if it already exists.
	If it already exists, it is dropped and recreated. Also, the script sets up three schemas
	in the database: bronze, silver, and gold.

WARNING: 
	Running this script will drop the entire 'DataWarehouse' database if it exists, and all data
	will be permanently deleted. Ensure the data is backed up before running this script.
*/

USE master;
GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS(SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE
	DROP DATABASE DataWarehouse
END;
GO

CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create the bronze, silver, and gold schemas
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
