-- Задание 1

select COUNT(*) as CNT
     , client.full_name
  from src.payments as p
  join src.account a
    on ( db_account_id = a.account_id and is_active = true ) 
        or
       ( cr_account_id = a.account_id and is_active = false) 
    on a.account_id = ar.account_id
   and execution_date between ar.start_date::date and coalesce(ar.end_date::date, '9999-01-01')
  join src.client
    on ar.client_id = client.client_id
 where execution_date >= '2025-07-01' and execution_date < '2025-08-01'
   and client.full_name ILIKE '%газ%'
group by client.full_name

 
-- Задание 2

select product_type_name, account_type
  from src.product p
  join (select product_id, contract_id from src.credit_contract  
	    union all
	    select product_id, contract_id from src.contract_other 
	   ) as c_all
    on p.id = c_all.product_id
  join src.contract_account
    on c_all.contract_id = contract_account.contract_id and contract_account.end_date is NULL
  join src.account a on a.account_id = contract_account.account_id 
 group by product_type_name, account_type



-- Задание 3

SELECT account_id
 FROM (
  select db_account_id as account_id, amount_rub, DATE_TRUNC('month', execution_date) as mm
  union all
  select cr_account_id as account_id, amount_rub, DATE_TRUNC('month', execution_date) as mm 
    from src.payments
      ) as T
GROUP BY account_id, mm       
HAVING sum(amount_rub) > 16000  
UNION
SELECT UNNEST(ARRAY[db_account_id, cr_account_id]) AS account_id 
FROM 
(
select ar_db.client_id, ar_cr.client_id, db_account_id, cr_account_id
  from src.payments
  join src.account_relationship as ar_db 
    on db_account_id = ar_db.account_id
  join src.account_relationship as ar_cr 
    on cr_account_id = ar_cr.account_id
 where ar_db.client_id = ar_cr.client_id 
) as T
ORDER BY 1

-- Задание 4

SELECT ar.client_id
FROM account_relationship ar
JOIN payments p ON ar.account_id IN (p.db_account_id, p.cr_account_id)
WHERE p.execution_date >= DATE '2025-01-01'
  AND p.execution_date <  DATE '2026-01-01'       
  AND NOT EXISTS (                         
        SELECT *
        FROM credit_contract cc
        WHERE cc.main_client_id = ar.client_id
  )
GROUP BY ar.client_id
HAVING COUNT(DISTINCT DATE_TRUNC('month', p.execution_date)) = 12;