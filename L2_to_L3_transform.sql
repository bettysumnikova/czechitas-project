-- L3_contract: rozšíření o délku trvání a rok zahájení smlouvy
CREATE OR REPLACE VIEW `involuted-river-455616-u4.L3.L3_contract` AS
SELECT
  contract_id,
  branch_id,
  contract_valid_from,
  contract_valid_to,
  registration_end_reason,
  contract_status,
  flag_prolongation,
  -- Výpočet trvání smlouvy
  CASE
    WHEN contract_valid_from IS NULL OR contract_valid_to IS NULL THEN NULL
    WHEN DATE_DIFF(contract_valid_to, contract_valid_from, MONTH) < 6 THEN 'less than half year'
    WHEN DATE_DIFF(contract_valid_to, contract_valid_from, MONTH) BETWEEN 6 AND 12 THEN '1 year'
    WHEN DATE_DIFF(contract_valid_to, contract_valid_from, MONTH) BETWEEN 13 AND 24 THEN '2 years'
    WHEN DATE_DIFF(contract_valid_to, contract_valid_from, MONTH) >= 25 THEN 'more than 2 years'
  END AS contract_duration,
  EXTRACT(YEAR FROM contract_valid_from) AS start_year_of_contract
FROM `involuted-river-455616-u4.L2.L2_contract`
WHERE contract_valid_from IS NOT NULL
  AND contract_valid_to IS NOT NULL
  AND contract_valid_to >= contract_valid_from;

-- L3_invoice: výpočet total_paid a přehled o fakturách
CREATE OR REPLACE VIEW `involuted-river-455616-u4.L3.L3_invoice` AS
SELECT
  invoice_id,
  contract_id,
  --product_id,
  amount_w_vat,
  return_w_vat,
  (amount_w_vat - return_w_vat) AS total_paid,
  paid_date
FROM `involuted-river-455616-u4.L2.L2_invoice`;

-- L3_product: základní popis produktu + informace z purchase
CREATE OR REPLACE VIEW `involuted-river-455616-u4.L3.L3_product` AS
SELECT
  pp.product_purchase_id,
  pp.product_id,
  p.product_name,
  p.product_type,
  pp.product_valid_from,
  pp.product_valid_to,
  pp.unit,
  pp.flag_unlimited_product
FROM `involuted-river-455616-u4.L2.L2_product_purchase` AS pp
LEFT JOIN `involuted-river-455616-u4.L2.L2_product` AS p
  ON pp.product_id = p.product_id
WHERE p.product_name IS NOT NULL;

-- L3_branch: přehled poboček
CREATE OR REPLACE VIEW `involuted-river-455616-u4.L3.L3_branch` AS
SELECT
  branch_id,
  branch_name
FROM `involuted-river-455616-u4.L2.L2_branch`;
