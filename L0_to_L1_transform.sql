-------------------------------------------
-- L0_google_sheets
-------------------------------------------

-- 1. L1_branch
CREATE OR REPLACE VIEW `involuted-river-455616-u4.L1.L1_branch` AS
SELECT
  CAST(id_branch AS INT64) AS branch_id,
  branch_name,
  PARSE_DATE('%m/%d/%Y', date_update) AS product_status_update_date
FROM `involuted-river-455616-u4.L0_google_sheets.branch`
WHERE id_branch IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY id_branch) = 1;

-- 2. L1_product
CREATE OR REPLACE VIEW `involuted-river-455616-u4.L1.L1_product` AS
SELECT
  CAST(id_product AS INT64) AS product_id,
  LOWER(name) AS product_name,
  LOWER(type) AS product_type,
  LOWER(category) AS product_category
FROM `involuted-river-455616-u4.L0_google_sheets.product`
WHERE id_product IS NOT NULL AND name IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY id_product) = 1;

-- 3. L1_status
CREATE OR REPLACE VIEW `involuted-river-455616-u4.L1.L1_status` AS
SELECT
  CAST(id_status AS INT64) AS product_status_id,
  LOWER(status_name) AS product_status_name,
  PARSE_DATE('%m/%d/%Y', date_update) AS product_status_update_date
FROM `involuted-river-455616-u4.L0_google_sheets.status`
WHERE id_status IS NOT NULL AND status_name IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY id_status) = 1;

-------------------------------------------
-- L0_accounting_system
-------------------------------------------

-- 4. L1_invoice
CREATE OR REPLACE VIEW `involuted-river-455616-u4.L1.L1_invoice` AS
SELECT
  id_invoice AS invoice_id,
  id_invoice_old AS invoice_previous_id,
  invoice_id_contract AS contract_id,
  id_branch AS branch_id,
  status AS invoice_status_id,
  IF(status < 100, TRUE, FALSE) AS flag_invoice_issued,
  DATE(date, "Europe/Prague") AS date_issue,
  DATE(scadent, "Europe/Prague") AS due_date,
  DATE(date_paid, "Europe/Prague") AS paid_date,
  DATE(start_date, "Europe/Prague") AS start_date,
  DATE(end_date, "Europe/Prague") AS end_date,
  DATE(date_insert, "Europe/Prague") AS insert_date,
  DATE(date_update, "Europe/Prague") AS update_date,
  value AS amount_w_vat,
  payed AS amount_payed,
  flag_paid_currier,
  invoice_type AS invoice_type_id,
  CASE
    WHEN invoice_type = 1 THEN "invoice"
    WHEN invoice_type = 2 THEN "return"
    WHEN invoice_type = 3 THEN "credit_note"
    WHEN invoice_type = 4 THEN "other"
  END AS invoice_type,
  number AS invoice_number,
  value_storno AS return_w_vat
FROM `involuted-river-455616-u4.L0_accounting_system.invoice`
WHERE id_invoice IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY id_invoice) = 1;

-- 5. L1_invoice_load
CREATE OR REPLACE VIEW `involuted-river-455616-u4.L1.L1_invoice_load` AS
SELECT
  id_load AS invoice_load_id,
  id_contract AS contract_id,
  id_package AS package_id,
  id_invoice AS invoice_id,
  id_package_template AS product_id,
  notlei AS price_wo_vat_usd,
  tva AS vat_rate,
  value AS price_w_vat_usd,
  payed AS paid_w_vat_usd,
  CASE 
    WHEN um IN ('mesia', 'm?síce', 'm?si?1ce', 'měsíce', 'mesiace', 'mesice') THEN 'month'
    WHEN um = 'kus' THEN 'item'
    WHEN um = 'den' THEN 'day'
    WHEN um = '0' THEN NULL
    ELSE um
  END AS unit,
  quantity,
  DATE(TIMESTAMP(start_date), "Europe/Prague") AS start_date,
  DATE(TIMESTAMP(end_date), "Europe/Prague") AS end_date,
  DATE(TIMESTAMP(date_insert), "Europe/Prague") AS date_insert,
  DATE(TIMESTAMP(date_update), "Europe/Prague") AS date_update
FROM `involuted-river-455616-u4.L0_accounting_system.invoices_load`
WHERE id_load IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY id_load) = 1;

-------------------------------------------
-- L0_crm
-------------------------------------------

-- 6. L1_contract
CREATE OR REPLACE VIEW `involuted-river-455616-u4.L1.L1_contract` AS
SELECT
  id_contract AS contract_id,
  id_branch AS branch_id,
  DATE(TIMESTAMP(date_contract_valid_from), "Europe/Prague") AS contract_valid_from,
  DATE(TIMESTAMP(date_contract_valid_to), "Europe/Prague") AS contract_valid_to,
  DATE(TIMESTAMP(date_registered), "Europe/Prague") AS registred_date,
  DATE(TIMESTAMP(date_signed), "Europe/Prague") AS signed_date,
  DATE(TIMESTAMP(activation_process_date), "Europe/Prague") AS activation_process_date,
  DATE(TIMESTAMP(prolongation_date), "Europe/Prague") AS prolongation_date,
  registration_end_reason,
  flag_prolongation,
  CAST(flag_send_inv_email AS BOOL) AS flag_send_email,
  contract_status
FROM `involuted-river-455616-u4.L0_crm.contracts`
WHERE id_contract IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY id_contract) = 1;

-- 7. L1_product_purchase
CREATE OR REPLACE VIEW `involuted-river-455616-u4.L1.L1_product_purchase` AS
SELECT
  packages.id_package AS product_purchase_id,
  packages.id_contract AS contract_id,
  packages.id_package_template AS product_id,
  DATE(TIMESTAMP(packages.date_insert), "Europe/Prague") AS create_date,
  DATE(TIMESTAMP(packages.start_date), "Europe/Prague") AS product_valid_from,
  DATE(TIMESTAMP(packages.end_date), "Europe/Prague") AS product_valid_to,
  packages.fee AS price_wo_vat,
  DATE(TIMESTAMP(packages.date_update), "Europe/Prague") AS date_update,
  packages.package_status AS product_status_id,
  status.product_status_name AS product_status,
  product.product_name,
  product.product_type,
  product.product_category,
  CASE 
    WHEN packages.measure_unit IN ('mesia', 'm?síce', 'měsíce', 'mesiace', 'mesice') THEN 'month'
    WHEN packages.measure_unit = 'kus' THEN 'item'
    WHEN packages.measure_unit = 'den' THEN 'day'
    WHEN packages.measure_unit = '0' THEN NULL
    ELSE packages.measure_unit
  END AS unit
FROM `involuted-river-455616-u4.L0_crm.product_purchases` AS packages
LEFT JOIN `involuted-river-455616-u4.L1.L1_status` AS status
  ON packages.package_status = status.product_status_id
LEFT JOIN `involuted-river-455616-u4.L1.L1_product` AS product
  ON packages.id_package_template = product.product_id
WHERE packages.id_package IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY packages.id_package) = 1;
