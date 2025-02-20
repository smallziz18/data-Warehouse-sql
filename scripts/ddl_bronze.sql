-- =========================================================
--          SCRIPT DE CRÉATION DES TABLES BRONZE
-- =========================================================
-- 📌 Objectif :
-- Ce script permet de créer les tables de la couche Bronze du Data Warehouse.
-- La couche Bronze est destinée au stockage brut des données, telles qu'elles 
-- proviennent des systèmes sources (CRM, ERP, etc.), sans transformation ni nettoyage.
-- 
-- 📌 Pourquoi une couche Bronze ?
-- - Permet d'assurer la traçabilité et la conservation des données d'origine.
-- - Facilite les opérations d’ingestion de données.
-- - Sert de point de départ pour l’enrichissement et la transformation en Silver/Gold.
-- 
-- 📌 Améliorations et robustesse du script :
-- - Vérification préalable de l'existence de la base de données et du schéma.
-- - Vérification de l'existence des tables avant leur création pour éviter les erreurs.
-- - Ajout de messages informatifs (`PRINT`) pour le suivi de l’exécution.
-- - Séparation claire des structures CRM et ERP pour faciliter la maintenance.
-- 
-- =========================================================

-- 🔹 1️⃣ Sélection de la base de données et création si nécessaire
USE DataWarehouse;
GO





-- 🔹 3️⃣ Création des tables de la source CRM (Customer Relationship Management)
-- ------------------------------------------------------------

-- 📌 Table : crm_customer_info 
-- ➡️ Contient les informations clients extraites du CRM.
IF NOT EXISTS (SELECT * FROM information_schema.tables WHERE table_schema = 'bronze' AND table_name = 'crm_customer_info')
BEGIN
    CREATE TABLE bronze.crm_customer_info
    (
        customer_id INT,
        customer_key NVARCHAR(50),
        customer_first_name NVARCHAR(50),
        customer_last_name NVARCHAR(50),
        customer_material_status NVARCHAR(50),
        customer_gender NVARCHAR(50),
        customer_create_date NVARCHAR(50)
    );
    PRINT '✅ Table bronze.crm_customer_info créée avec succès.';
END
ELSE
    PRINT 'ℹ️ La table bronze.crm_customer_info existe déjà.';
GO

-- 📌 Table : crm_product_info 
-- ➡️ Contient les informations sur les produits provenant du CRM.
IF NOT EXISTS (SELECT * FROM information_schema.tables WHERE table_schema = 'bronze' AND table_name = 'crm_product_info')
BEGIN
    CREATE TABLE bronze.crm_product_info
    (
        product_id INT,
        product_key NVARCHAR(50),
        product_nm NVARCHAR(50),
        product_cost INT,
        product_line CHAR,
        product_start_date NVARCHAR(50),
        product_end_date NVARCHAR(50)
    );
    PRINT '✅ Table bronze.crm_product_info créée avec succès.';
END
ELSE
    PRINT 'ℹ️ La table bronze.crm_product_info existe déjà.';
GO

-- 📌 Table : crm_sales_details 
-- ➡️ Contient les détails des ventes issues du CRM.
IF NOT EXISTS (SELECT * FROM information_schema.tables WHERE table_schema = 'bronze' AND table_name = 'crm_sales_details')
BEGIN
    CREATE TABLE bronze.crm_sales_details
    (
        sales_order_num NVARCHAR(50),
        sales_order_product_key NVARCHAR(50),
        sales_order_customer_id INT,
        sales_date INT,
        sales_shipping_date INT,
        sls_sales INT,
        sales_quatity INT,
        sales_price INT,
        sales_due_date INT
    );
    PRINT '✅ Table bronze.crm_sales_details créée avec succès.';
END
ELSE
    PRINT 'ℹ️ La table bronze.crm_sales_details existe déjà.';
GO

-- 🔹 4️⃣ Création des tables de la source ERP (Enterprise Resource Planning)
-- ------------------------------------------------------------

-- 📌 Table : erp_customer_az12 
-- ➡️ Contient les informations clients issues de l’ERP.
IF NOT EXISTS (SELECT * FROM information_schema.tables WHERE table_schema = 'bronze' AND table_name = 'erp_customer_az12')
BEGIN
    CREATE TABLE bronze.erp_customer_az12
    (
        cid NVARCHAR(50),
        birthday NVARCHAR(50),
        gender NVARCHAR(50)
    );
    PRINT '✅ Table bronze.erp_customer_az12 créée avec succès.';
END
ELSE
    PRINT 'ℹ️ La table bronze.erp_customer_az12 existe déjà.';
GO

-- 📌 Table : erp_loc_A101 
-- ➡️ Contient les informations de localisation des clients de l’ERP.
IF NOT EXISTS (SELECT * FROM information_schema.tables WHERE table_schema = 'bronze' AND table_name = 'erp_loc_A101')
BEGIN
    CREATE TABLE bronze.erp_loc_A101
    (
        cid NVARCHAR(50),
        country NVARCHAR(50)
    );
    PRINT '✅ Table bronze.erp_loc_A101 créée avec succès.';
END
ELSE
    PRINT 'ℹ️ La table bronze.erp_loc_A101 existe déjà.';
GO

-- 📌 Table : erp_px_cat_g1v2 
-- ➡️ Contient les catégories et sous-catégories de produits de l’ERP.
IF NOT EXISTS (SELECT * FROM information_schema.tables WHERE table_schema = 'bronze' AND table_name = 'erp_px_cat_g1v2')
BEGIN
    CREATE TABLE bronze.erp_px_cat_g1v2
    (
        id NVARCHAR(50),
        category NVARCHAR(50),
        subcategory NVARCHAR(50),
        maintenance NVARCHAR(50)
    );
    PRINT '✅ Table bronze.erp_px_cat_g1v2 créée avec succès.';
END
ELSE
    PRINT 'ℹ️ La table bronze.erp_px_cat_g1v2 existe déjà.';
GO

-- ✅ Fin du script. Toutes les vérifications et créations de tables sont effectuées avec succès.
