-- ******************************************
-- Script d'Insertion en Masse des Données CSV
-- ******************************************
-- Objectif : Ce script permet de vider les tables existantes dans le schéma "bronze" 
--            et d'y insérer des données en masse à partir de fichiers CSV.
--
-- Les fichiers sources sont issus des répertoires :
-- - source_crm : contient des informations sur les clients, produits et ventes.
-- - source_erp : contient des informations sur les clients ERP, les localisations et les catégories produits.
--
-- Tables concernées :
-- 1. crm_customer_info
-- 2. crm_product_info
-- 3. crm_sales_details
-- 4. erp_customer_az12
-- 5. erp_loc_a101
-- 6. erp_px_cat_g1v2
--
-- Le processus est le suivant :
-- 1. Troncature (vidage) des tables existantes.
-- 2. Insertion des données depuis des fichiers CSV avec l'utilisation de BULK INSERT pour des performances optimales.
-- 3. Chaque fichier CSV est traité indépendamment et les en-têtes de chaque fichier sont ignorés (FIRSTROW = 2).
-- 4. La délimitation des colonnes dans les fichiers CSV est effectuée par des virgules (FIELDTERMINATOR = ',').
-- 5. La table est verrouillée (TABLOCK) pendant l'insertion en masse pour améliorer la performance.

-- ******************************************
-- Début du script
-- ******************************************

-- Utilisation de la base de données DataWarehouse
USE DataWarehouse;
GO

-- ****************************
-- Création ou modification de la procédure stockée
-- ****************************

CREATE OR ALTER PROCEDURE bronze.load_bronze 
AS
BEGIN
    BEGIN TRY
        -- Début de la transaction pour garantir que toutes les étapes sont cohérentes
        BEGIN TRANSACTION;

        -- ****************************
        -- Section 1 : Troncature des tables (Clear des données existantes)
        -- ****************************

        PRINT 'Vider la table crm_customer_info';
        TRUNCATE TABLE bronze.crm_customer_info;

        PRINT 'Vider la table crm_product_info';
        TRUNCATE TABLE bronze.crm_product_info;

        PRINT 'Vider la table crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT 'Vider la table erp_customer_az12';
        TRUNCATE TABLE bronze.erp_customer_az12;

        PRINT 'Vider la table erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT 'Vider la table erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        -- ****************************
        -- Section 2 : Chargement des données depuis des fichiers CSV
        -- ****************************

        PRINT 'Insertion des données dans crm_customer_info depuis le fichier CSV';
        BULK INSERT bronze.crm_customer_info
        FROM 'D:\data-engeneering\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,  -- Ignore la première ligne qui contient les noms de colonnes
            FIELDTERMINATOR = ',',  -- Délimiteur de champ
            TABLOCK  -- Verrouillage de la table pour améliorer les performances lors de l'insertion en masse
        );

        PRINT 'Insertion des données dans crm_product_info depuis le fichier CSV';
        BULK INSERT bronze.crm_product_info
        FROM 'D:\data-engeneering\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        PRINT 'Insertion des données dans crm_sales_details depuis le fichier CSV';
        BULK INSERT bronze.crm_sales_details
        FROM 'D:\data-engeneering\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        PRINT 'Insertion des données dans erp_customer_az12 depuis le fichier CSV';
        BULK INSERT bronze.erp_customer_az12
        FROM 'D:\data-engeneering\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        PRINT 'Insertion des données dans erp_loc_a101 depuis le fichier CSV';
        BULK INSERT bronze.erp_loc_a101
        FROM 'D:\data-engeneering\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        PRINT 'Insertion des données dans erp_px_cat_g1v2 depuis le fichier CSV';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'D:\data-engeneering\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        -- Validation de la transaction
        COMMIT TRANSACTION;
        PRINT 'Données chargées avec succès';

    END TRY
    BEGIN CATCH
        -- En cas d'erreur, annuler la transaction et afficher l'erreur
        ROLLBACK TRANSACTION;
        PRINT 'Erreur rencontrée: ' + ERROR_MESSAGE();
    END CATCH
END;

-- ******************************************
-- Fin du script
-- ******************************************

