-- ******************************************
-- Script d'Insertion en Masse des Données CSV
-- ******************************************
-- Objectif :
-- Ce script permet de vider les tables existantes dans le schéma "bronze"
-- et d'y insérer des données en masse à partir de fichiers CSV. Il inclut un suivi
-- du temps d'exécution de chaque opération pour chaque table et donne un temps total
-- d'exécution du script.

-- ******************************************

-- Utilisation de la base de données DataWarehouse
USE DataWarehouse;
GO

-- ******************************************
-- Création ou modification de la procédure stockée
-- ******************************************

CREATE OR ALTER PROCEDURE bronze.load_bronze 
AS
BEGIN
    -- Déclaration des variables de temps
    DECLARE 
        @starttime DATETIME,  -- Heure de début de chaque opération
        @endtime DATETIME,    -- Heure de fin de chaque opération
        @total_starttime DATETIME,  -- Heure de début du script
        @total_endtime DATETIME;    -- Heure de fin du script

    -- Heure de début du script
    SET @total_starttime = GETDATE();
    PRINT '======================================================================';
    PRINT 'Début du script - ' + CAST(@total_starttime AS VARCHAR(19));
    PRINT '======================================================================';

    BEGIN TRY
        -- Début de la transaction pour garantir que toutes les étapes sont cohérentes
        BEGIN TRANSACTION;

        -- ******************************************
        -- Section 1 : Troncature des tables (Clear des données existantes)
        -- ******************************************

        -- Troncature de la table crm_customer_info
        SET @starttime = GETDATE();
        PRINT 'Vider la table crm_customer_info...';
        TRUNCATE TABLE bronze.crm_customer_info;
        SET @endtime = GETDATE();
        PRINT 'Temps d exécution pour crm_customer_info TRUNCATE : ' + CAST(DATEDIFF(SECOND, @starttime, @endtime) AS VARCHAR(10)) + ' secondes';

        -- Troncature de la table crm_product_info
        SET @starttime = GETDATE();
        PRINT 'Vider la table crm_product_info...';
        TRUNCATE TABLE bronze.crm_product_info;
        SET @endtime = GETDATE();
        PRINT 'Temps d exécution pour crm_product_info TRUNCATE : ' + CAST(DATEDIFF(SECOND, @starttime, @endtime) AS VARCHAR(10)) + ' secondes';

        -- Troncature de la table crm_sales_details
        SET @starttime = GETDATE();
        PRINT 'Vider la table crm_sales_details...';
        TRUNCATE TABLE bronze.crm_sales_details;
        SET @endtime = GETDATE();
        PRINT 'Temps dexécution pour crm_sales_details TRUNCATE : ' + CAST(DATEDIFF(SECOND, @starttime, @endtime) AS VARCHAR(10)) + ' secondes';

        -- Troncature de la table erp_customer_az12
        SET @starttime = GETDATE();
        PRINT 'Vider la table erp_customer_az12...';
        TRUNCATE TABLE bronze.erp_customer_az12;
        SET @endtime = GETDATE();
        PRINT 'Temps d exécution pour erp_customer_az12 TRUNCATE : ' + CAST(DATEDIFF(SECOND, @starttime, @endtime) AS VARCHAR(10)) + ' secondes';

        -- Troncature de la table erp_loc_a101
        SET @starttime = GETDATE();
        PRINT 'Vider la table erp_loc_a101...';
        TRUNCATE TABLE bronze.erp_loc_a101;
        SET @endtime = GETDATE();
        PRINT 'Temps d exécution pour erp_loc_a101 TRUNCATE : ' + CAST(DATEDIFF(SECOND, @starttime, @endtime) AS VARCHAR(10)) + ' secondes';

        -- Troncature de la table erp_px_cat_g1v2
        SET @starttime = GETDATE();
        PRINT 'Vider la table erp_px_cat_g1v2...';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        SET @endtime = GETDATE();
        PRINT 'Temps d exécution pour erp_px_cat_g1v2 TRUNCATE : ' + CAST(DATEDIFF(SECOND, @starttime, @endtime) AS VARCHAR(10)) + ' secondes';

        -- ******************************************
        -- Section 2 : Chargement des données depuis des fichiers CSV
        -- ******************************************

        -- Insertion dans crm_customer_info
        SET @starttime = GETDATE();
        PRINT 'Insertion des données dans crm_customer_info...';
        BULK INSERT bronze.crm_customer_info
        FROM 'D:\data-engeneering\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @endtime = GETDATE();
        PRINT 'Temps d exécution pour crm_customer_info BULK INSERT : ' + CAST(DATEDIFF(SECOND, @starttime, @endtime) AS VARCHAR(10)) + ' secondes';

        -- Insertion dans crm_product_info
        SET @starttime = GETDATE();
        PRINT 'Insertion des données dans crm_product_info...';
        BULK INSERT bronze.crm_product_info
        FROM 'D:\data-engeneering\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @endtime = GETDATE();
        PRINT 'Temps d exécution pour crm_product_info BULK INSERT : ' + CAST(DATEDIFF(SECOND, @starttime, @endtime) AS VARCHAR(10)) + ' secondes';

        -- Insertion dans crm_sales_details
        SET @starttime = GETDATE();
        PRINT 'Insertion des données dans crm_sales_details...';
        BULK INSERT bronze.crm_sales_details
        FROM 'D:\data-engeneering\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @endtime = GETDATE();
        PRINT 'Temps d exécution pour crm_sales_details BULK INSERT : ' + CAST(DATEDIFF(SECOND, @starttime, @endtime) AS VARCHAR(10)) + ' secondes';

        -- Insertion dans erp_customer_az12
        SET @starttime = GETDATE();
        PRINT 'Insertion des données dans erp_customer_az12...';
        BULK INSERT bronze.erp_customer_az12
        FROM 'D:\data-engeneering\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @endtime = GETDATE();
        PRINT 'Temps d exécution pour erp_customer_az12 BULK INSERT : ' + CAST(DATEDIFF(SECOND, @starttime, @endtime) AS VARCHAR(10)) + ' secondes';

        -- Insertion dans erp_loc_a101
        SET @starttime = GETDATE();
        PRINT 'Insertion des données dans erp_loc_a101...';
        BULK INSERT bronze.erp_loc_a101
        FROM 'D:\data-engeneering\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @endtime = GETDATE();
        PRINT 'Temps d exécution pour erp_loc_a101 BULK INSERT : ' + CAST(DATEDIFF(SECOND, @starttime, @endtime) AS VARCHAR(10)) + ' secondes';

        -- Insertion dans erp_px_cat_g1v2
        SET @starttime = GETDATE();
        PRINT 'Insertion des données dans erp_px_cat_g1v2...';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'D:\data-engeneering\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @endtime = GETDATE();
        PRINT 'Temps d exécution pour erp_px_cat_g1v2 BULK INSERT : ' + CAST(DATEDIFF(SECOND, @starttime, @endtime) AS VARCHAR(10)) + ' secondes';

        -- ******************************************
        -- Fin du script
        -- ******************************************

        -- Validation de la transaction
        COMMIT TRANSACTION;

        -- Heure de fin du script
        SET @total_endtime = GETDATE();
        PRINT '======================================================================';
        PRINT 'Script terminé - ' + CAST(@total_endtime AS VARCHAR(19));
        PRINT 'Temps total d exécution : ' + CAST(DATEDIFF(SECOND, @total_starttime, @total_endtime) AS VARCHAR(10)) + ' secondes';
        PRINT '======================================================================';

    END TRY
    BEGIN CATCH
        -- Gestion des erreurs
        ROLLBACK TRANSACTION;

        -- Détails sur l'erreur
        PRINT '======================================================================';
        PRINT 'Erreur rencontrée lors de l exécution :';
        PRINT 'Code d erreur : ' + CAST(ERROR_NUMBER() AS VARCHAR(10));
        PRINT 'Message d erreur : ' + ERROR_MESSAGE();
        PRINT 'Procédure ou ligne où l erreur s est produite : ' + CAST(ERROR_LINE() AS VARCHAR(10));
        PRINT '======================================================================';
    END CATCH
END;
