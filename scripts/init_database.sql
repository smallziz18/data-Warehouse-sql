-- ================================================
-- Script pour la création du Data Warehouse
-- ================================================
-- Ce script permet de vérifier l'existence de la base de données 'DataWarehouse', 
-- et de créer les schémas Bronze, Silver et Gold dans cette base.
-- Les schémas sont utilisés pour organiser les données à différents stades :
--    - Schéma Bronze : Stockage brut des données en l'état.
--    - Schéma Silver : Stockage des données après nettoyage et transformation.
--    - Schéma Gold : Stockage des données optimisées pour l'analyse et le reporting.
-- Ce processus permet de structurer et d'optimiser les données pour des usages analytiques.
-- ================================================

-- Connexion à la base de données master avant la création du Data Warehouse
USE master;
GO

-- Vérification de l'existence de la base avant de la créer
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    CREATE DATABASE DataWarehouse;
    PRINT 'Base de données DataWarehouse créée avec succès.';
END
ELSE
    PRINT 'La base de données DataWarehouse existe déjà.';
GO

-- Sélection de la base de données DataWarehouse
USE DataWarehouse;
GO

-- Vérification et création du schéma Bronze (stockage brut des données)
IF NOT EXISTS (SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'bronze')
BEGIN
    EXEC('CREATE SCHEMA bronze');
    PRINT 'Schéma bronze créé avec succès.';
END
ELSE
    PRINT 'Le schéma bronze existe déjà.';
GO

-- Vérification et création du schéma Silver (stockage transformé et nettoyé)
IF NOT EXISTS (SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'silver')
BEGIN
    EXEC('CREATE SCHEMA silver');
    PRINT 'Schéma silver créé avec succès.';
END
ELSE
    PRINT 'Le schéma silver existe déjà.';
GO

-- Vérification et création du schéma Gold (stockage optimisé pour l'analyse et le reporting)
IF NOT EXISTS (SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'gold')
BEGIN
    EXEC('CREATE SCHEMA gold');
    PRINT 'Schéma gold créé avec succès.';
END
ELSE
    PRINT 'Le schéma gold existe déjà.';
GO
