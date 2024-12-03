import psycopg2


conn = psycopg2.connect("dbname=d3ca7csmihkq2t port=5432 password=p539645ff8350c7034e87644aa5dcb3643cc90f141c9c79deacf04cd90dce63bf host=cc0gj7hsrh0ht8.cluster-czrs8kj4isg7.us-east-1.rds.amazonaws.com user=uddmfcekkt42ui")

# Open a cursor to perform database operations
cur = conn.cursor()

# Execute a query
cur.execute("""select distinct cast(EXTRACT(MONTH FROM  created_at) as INT) from 
public.transactions
where EXTRACT(YEAR FROM  created_at) = 2024;""")

# Retrieve query results
all_months_ = cur.fetchall()

print(all_months_)

for i in all_months_:
    print(i[0])
    run_query = f"""
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
        select user_id transaction_count 
        from 
        fact_table_transactions
        where year = 2024 and month = {i[0]-1}
        group by user_id,email
        having count(*) > 0
        ),
        user_ids_curr_month as 
        (
        select user_id, count(*) transaction_count 
        from 
        fact_table_transactions
        where year = 2024 and month = {i[0]}
        group by user_id,email
        having count(*) > 0
        )


        select {i[0]}, count(*) from user_ids_curr_month
        where user_id in (select user_id from user_ids_prev_month);
    """

    cur.execute(run_query)

    conn.commit()
    print(f'report for {i} has been inserted successfully')




