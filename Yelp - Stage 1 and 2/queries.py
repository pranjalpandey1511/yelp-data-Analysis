import time
import psycopg2


'''
Author : 1. Venkata Karteek Paladugu
         2. Pranjal Pandey
         3. Ravikiran Jois
         4. Vishnu Byreddy
'''

def main():
    try:
        connection = establish_connection()
        print("Before indexing")
        cursor = connection.cursor()

        execute_query1(cursor)
        execute_query2(cursor)
        execute_query3(cursor)
        execute_query4(cursor)
        execute_query5(cursor)

        start_time = time.time()
        print("Indexing in progress...")
        index_query = '''
        CREATE INDEX  review_text_idx  ON review(review_text);
        CREATE INDEX  checkin_date_idx  ON checkin(checkin_date);
        CREATE INDEX  review_count_idx  ON review(review_count)
        '''
        cursor.execute(index_query)
        print("Indexing done" + " " + str(time.time() - start_time))
        print("After indexing")

        execute_query1(cursor)
        execute_query2(cursor)
        execute_query3(cursor)
        execute_query4(cursor)
        execute_query5(cursor)
    except (Exception, psycopg2.DatabaseError) as error:
        print (error)
    finally:
        # closing database connection.
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")


def execute_query1(cursor):
    start_time = time.time()
    query1 = '''Select  bn.city, count(mx.count) from business bn 
Left Join (SELECT  r.business_id as id, count(*) as count
FROM review r 
where r.review_text like '%love%' or r.review_text like '%like%'
GROUP BY r.business_id ) mx on bn.business_id = mx.id
Group BY (bn.city) ORDER BY count(mx.count) DESC
'''
    cursor.execute(query1)
    print("Time taken for query1 : " + str(time.time() - start_time))


def execute_query2(cursor):
    start_time = time.time()
    query2 = '''select * from
(select b.name, b.state, count(c.checkin_date), '14th February' as date 
from (select * from checkin 
where DATE(checkin_date) in 
('2016-02-14', '2017-02-14', '2018-02-14') ) as c
join business b 
on b.business_id = c.business_id
group by b.business_id, b.name, b.state
union all
select b.name, b.state, count(c.checkin_date), '4th July' as date 
from (select * from checkin 
where DATE(checkin_date) in 
('2016-07-04', '2017-07-04', '2018-07-04') ) as c
join business b 
on b.business_id = c.business_id
group by b.business_id, b.name, b.state
union all
select b.name, b.state, count(c.checkin_date), '31st December' as date 
from (select * from checkin 
where DATE(checkin_date) in 
('2016-12-31', '2017-12-31', '2018-12-31') ) as c
join business b 
on b.business_id = c.business_id
group by b.business_id, b.name, b.state) as x
order by count desc'''
    cursor.execute(query2)
    print("Time taken for query2 : " + str(time.time() - start_time))


def execute_query3(cursor):
    start_time = time.time()
    query3 = '''select c.category, b.state, avg(b.stars) as avg_stars
from business as b
join business_category as c
on b.business_id = c.business_id
where b.review_count > 500
group by c.category, b.state
order by avg_stars desc

'''
    cursor.execute(query3)
    print("Time taken for query3: " + str(time.time() - start_time))


def execute_query4(cursor):
    start_time = time.time()
    query4 = '''select AVG( ue.elite - date_part('year'::text, yelping_since)::integer ) 
                from User_Table ut 
inner join (select min(elite) as elite, user_id from user_elite group by user_id) ue 
on ue.user_id = ut.user_id
'''
    cursor.execute(query4)
    print("Time taken for query4 : " + str(time.time() - start_time))


def execute_query5(cursor):
    start_time = time.time()
    query5 = '''select stars, avg(funny) as avg_funny,
avg(useful) as avg_useful, avg(cool) as avg_cool, sum(funny) as 
sum_funny, sum(useful) as sum_useful, sum(cool) assum_cool,
count(review_id) from review group by stars'''
    cursor.execute(query5)
    print("Time taken for query5 : " + str(time.time() - start_time))


def establish_connection():
    # username = input("Enter username: ")
    # password = input("Enter password: ")
    # host = input("Enter host: ")
    # port = input("Enter port: ")
    # database = input("Enter database name: ")
    # connection = psycopg2.connect(user=username,
    #                               password=password,
    #                               host=host,
    #                               port=port,
    #                               database=database)

    connection = psycopg2.connect(user="ravikiranjois",
                                  password="coolie01",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="yelp_data")
    return connection


main()
