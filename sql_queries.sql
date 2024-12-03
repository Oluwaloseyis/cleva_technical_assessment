select distinct cast(EXTRACT(MONTH FROM  created_at) as INT) from 
public.transactions
where EXTRACT(YEAR FROM  created_at) = 2024 

limit 1;

select * from 
public.users
limit 1;


create table public.final_month_on_month 
(
month INT,
retained_customers INT
)

select * from 
--truncate table
public.final_month_on_month 


insert into public.final_month_on_month 
with fact_table_transactions as 
(
select a.*,
EXTRACT(YEAR FROM  a.created_at) as year,
EXTRACT(MONTH FROM  a.created_at) as month,
email 
from 
public.transactions a join public.users b
USING(user_id)
),
user_ids_prev_month as 
(
select user_id 
from 
fact_table_transactions
where year = 2024 and month = 0
group by user_id,email
having count(*) > 0
)

select * from 
user_ids_prev_month


,
user_ids_curr_month as 
(
select user_id, count(*) transaction_count 
from 
fact_table_transactions
where year = 2024 and month = 3
group by user_id,email
having count(*) > 0
)


select 2, count(*) from user_ids_curr_month
where user_id in (select user_id from user_ids_prev_month)


select user_id, email, count(*) transaction_count 
from 
fact_table_transactions
where year = 2024 and month = 3
group by user_id,email
having count(*) > 0



SELECT EXTRACT(MONTH FROM TIMESTAMP '2001-02-16 20:38:40');


select date_part(y, '2024-04-30') 


('2024-04-30')