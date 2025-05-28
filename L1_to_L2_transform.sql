-- L2_contract: Jen kontrakty, které mají zadané datum registrace
CREATE OR REPLACE VIEW `involuted-river-455616-u4.L2.L2_contract` AS
SELECT
  contract_id,                      -- Primární klíč
  branch_id,                        -- FK na branch
  contract_valid_from,             -- Platnost smlouvy od
  contract_valid_to,               -- Platnost smlouvy do
  registred_date,                  -- Datum registrace (používá se jako filtr)
  registration_end_reason,         -- Důvod ukončení
  prolongation_date,               -- Datum prodloužení
  flag_prolongation,               -- Indikátor, zda bylo prodlouženo
  contract_status,                 -- Stav smlouvy
  activation_process_date,         -- Datum aktivace
  signed_date,                     -- Datum podpisu
  flag_send_email                  -- Indikátor zaslání e-mailu
FROM `involuted-river-455616-u4.L1.L1_contract`
WHERE registred_date IS NOT NULL;

-- L2_branch: Odstranění větve s názvem "unknown"
CREATE OR REPLACE VIEW `involuted-river-455616-u4.L2.L2_branch` AS
SELECT
  branch_id,                       -- Primární klíč pobočky
  branch_name                      -- Název pobočky
FROM `involuted-river-455616-u4.L1.L1_branch`
WHERE LOWER(branch_name) != 'unknown';

-- L2_product: Jen produkty s kategorií 'product' nebo 'rent'
CREATE OR REPLACE VIEW `involuted-river-455616-u4.L2.L2_product` AS
SELECT
  product_id,                      -- Primární klíč produktu
  product_name,                    -- Název produktu
  product_type,                    -- Typ produktu
  product_category                 -- Kategorie produktu (filtrujeme níže)
FROM `involuted-river-455616-u4.L1.L1_product`
WHERE LOWER(product_category) IN ('product', 'rent');

-- L2_invoice: Jen vystavené faktury typu 'invoice'
-- Doplnění částky bez DPH (odhadnuto jako amount_w_vat / 1.2)
-- Přidání invoice_order jako kombinace contract_id a date_issue
CREATE OR REPLACE VIEW `involuted-river-455616-u4.L2.L2_invoice` AS
SELECT
  invoice_id,                     -- Primární klíč faktury
  invoice_previous_id,            -- FK na předchozí fakturu (např. storno)
  contract_id,                    -- FK na smlouvu
  invoice_type,                   -- Typ faktury (pouze 'invoice' bereme)
  amount_w_vat,                   -- Částka s DPH
  return_w_vat,                   -- Vrácená částka s DPH (např. při stornu)
  ROUND(IF(amount_w_vat <= 0, 0, amount_w_vat / 1.2), 2) AS amount_wo_vat,  -- Odhad částky bez DPH
  date_issue,                     -- Datum vystavení
  flag_invoice_issued,           -- Musí být TRUE (vystavená faktura)
  invoice_status_id,             -- Stav faktury (např. vystaveno, stornováno)
  ROW_NUMBER() OVER (PARTITION BY contract_id ORDER BY date_issue) AS invoice_order
FROM `involuted-river-455616-u4.L1.L1_invoice`
WHERE invoice_type = 'invoice'
  AND flag_invoice_issued = TRUE;

-- L2_product_purchase: Pouze aktivní produkty kategorie 'product' nebo 'rent'
-- Odstranění záznamů se statusem zrušen/disconnected
-- Dopočet ceny s DPH a přidání příznaku flag_unlimited_product
CREATE OR REPLACE VIEW `involuted-river-455616-u4.L2.L2_product_purchase` AS
SELECT
  product_purchase_id,                                         -- ID nákupu produktu
  product_id,                                                  -- ID produktu
  contract_id,                                                 -- ID smlouvy
  product_category,                                            -- Kategorie produktu
  product_status,                                              -- Stav produktu
  ROUND(price_wo_vat * 1.2, 2) AS price_w_vat,                 -- Cena s DPH (dopočet)
  price_wo_vat,                                                -- Cena bez DPH
  IF(product_valid_from = DATE '2035-12-31', TRUE, FALSE) AS flag_unlimited_product,  -- Flag pro "neomezený produkt"
  product_valid_from,                                          -- Datum platnosti od
  product_valid_to,                                            -- Datum platnosti do
  unit,                                                        -- Měrná jednotka (měsíc, kus, den, ...)
  product_name,                                                -- Název produktu
  product_type,                                                -- Typ produktu
  create_date                                                  -- Datum vytvoření
FROM `involuted-river-455616-u4.L1.L1_product_purchase`
WHERE LOWER(product_category) IN ('product', 'rent')
  AND LOWER(product_status) NOT IN ('zrusen', 'disconnected', 'cancelled')
  AND product_status IS NOT NULL;
