import time
import psycopg2
import json
from pymongo import MongoClient


"Link to Dataset :  https://www.yelp.com/dataset/challenge"
'''
Author : 1. Venkata Karteek Paladugu
         2. Pranjal Pandey
         3. Ravikiran Jois
         4. Vishnu Byreddy
'''


def transfer_to_mongo():
    client = MongoClient('localhost', 27017)
    mydb = client['yelp_db']

    resultsetSize = 500000
    print('Business----------')
    recordlist = []
    linenumber = 0

    mycol = mydb["Business"]
    with open("/Users/ravikiranjois/Documents/RIT/Semester 2/Introduction to Big Data/Project/business1.json",
              encoding='utf-8') as business:
        for business_line in business:
            data = json.loads(business_line.replace('\\\\', '\\'))
            new_data = remove_nulls(data)
            recordlist.append(new_data)
            if linenumber == resultsetSize:
                linenumber = 0
                mycol.insert_many(recordlist)
                recordlist = []
            linenumber += 1
    if len(recordlist) > 0:
        mycol.insert_many(recordlist)

    print('Checkin----------')
    recordlist = []
    linenumber = 0

    mycol = mydb["Checkin"]
    with open("/Users/ravikiranjois/Documents/RIT/Semester 2/Introduction to Big Data/Project/checkin1.json",
              encoding='utf-8') as checkin:
        for checkin_line in checkin:
            data = json.loads(checkin_line.replace('\\\\', '\\'))
            new_data = remove_nulls(data)
            recordlist.append(new_data)
            if linenumber == resultsetSize:
                linenumber = 0
                mycol.insert_many(recordlist)
                recordlist = []
            linenumber += 1
    if len(recordlist) > 0:
        mycol.insert_many(recordlist)

    print('Review----------')
    recordlist = []
    linenumber = 0

    mycol = mydb["Review"]
    with open("/Users/ravikiranjois/Documents/RIT/Semester 2/Introduction to Big Data/Project/review1.json",
              encoding='utf-8') as review:
        for review_line in review:
            data = json.loads(review_line.replace('\\\\', '\\'))
            new_data = remove_nulls(data)
            recordlist.append(new_data)
            if linenumber == resultsetSize:
                linenumber = 0
                mycol.insert_many(recordlist)
                recordlist = []
            linenumber += 1
    if len(recordlist) > 0:
        mycol.insert_many(recordlist)

    print('Tip----------')
    recordlist = []
    linenumber = 0

    mycol = mydb["Tip"]
    with open("/Users/ravikiranjois/Documents/RIT/Semester 2/Introduction to Big Data/Project/tip1.json",
              encoding='utf-8') as tip:
        for tip_line in tip:
            data = json.loads(tip_line.replace('\\\\', '\\'))
            new_data = remove_nulls(data)
            recordlist.append(new_data)
            if linenumber == resultsetSize:
                linenumber = 0
                mycol.insert_many(recordlist)
                recordlist = []
            linenumber += 1
    if len(recordlist) > 0:
        mycol.insert_many(recordlist)

    print('Users----------')
    recordlist = []
    linenumber = 0

    mycol = mydb["Users"]
    with open("/Users/ravikiranjois/Documents/RIT/Semester 2/Introduction to Big Data/Project/users1.json",
              encoding='utf-8') as users:
        for users_line in users:
            data = json.loads(users_line.replace('\\\\', '\\'))
            new_data = remove_nulls(data)
            recordlist.append(new_data)
            if linenumber == resultsetSize:
                linenumber = 0
                mycol.insert_many(recordlist)
                recordlist = []
            linenumber += 1
    if len(recordlist) > 0:
        mycol.insert_many(recordlist)


def remove_nulls(original_dict):
    no_nulls_dict = {}
    for key, value in original_dict.items():
        if isinstance(value, dict):
            temp_dict = remove_nulls(value)
            if temp_dict.keys() > 0:
                no_nulls_dict[key] = value
        elif isinstance(value, list):
            temp_list = []
            for each_value in value:
                if each_value is not None:
                    temp_list.append(each_value)
            if len(temp_list) > 0:
                no_nulls_dict[key] = temp_list
        elif value is not None:
            no_nulls_dict[key] = value
    return no_nulls_dict


def createSubsets(columns, subsets, tempList, start):
    if len(tempList) != 0 and len(tempList) <= 2:
        copy = list(tempList)
        subsets.append(copy)
    for i in range(start, len(columns)):
        tempList.append(columns[i])
        createSubsets(columns, subsets, tempList, i + 1)
        tempList.pop(len(tempList) - 1)



def main():
    """
        main function :
        main function establishes connection with the database server and calls functions to create tables,
        alter them and finally delete rows.
    """
    try:
        total_time_taken = time.time()
        connection = psycopg2.connect(user="ravikiranjois",
                                      password="coolie01",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="yelp_data")
        connection.autocommit = True
        start_time = time.time()
        cursor = connection.cursor()
        create_table(cursor)
        print("Create tables")
        connection.commit()
        business_statement = '''COPY temp_data(data) FROM '/Users/ravikiranjois/Documents/RIT/Semester 2/Introduction to Big Data/Project/yelp_dataset/business.json'
        WITH QUOTE E'\b'  DELIMITER E'\t' CSV;
        insert into Business(business_id,name,city,state,postal_code,latitude,longitude,stars,review_count)
        select
           data ->> 'business_id' as business_id,
           data ->> 'name' as name,
           data ->> 'city' as city,
           data ->> 'state' as state,
           data ->> 'postal_code' as postal_code,
           (data ->> 'latitude') :: numeric as latitude,
           (data ->> 'longitude'):: numeric as longitude,
           (data ->> 'stars'):: numeric as stars,
           (data ->> 'review_count')::integer as review_count
        from temp_data;
        INSERT INTO business_category SELECT  data ->> 'business_id' as business_id, unnest(string_to_array(data ->> 
        'categories', ',')) as category FROM temp_data;
        '''
        delete_statement = '''DELETE FROM temp_data'''
        cursor.execute(business_statement)
        connection.commit()
        cursor.execute(delete_statement)
        connection.commit()
        print(time.time() - start_time)
        print("Business_done")

        start_time = time.time()
        user_statement = '''COPY temp_data(data) FROM '/Users/ravikiranjois/Documents/RIT/Semester 2/Introduction to Big Data/Project/yelp_dataset/user.json'
        WITH QUOTE E'\b'  DELIMITER E'\t' CSV;
        insert into User_Table(user_id,name,review_count,yelping_since,useful,funny,cool,fans,average_stars)
        select
           data ->> 'user_id' as user_id,
           data ->> 'name' as name,
           (data ->> 'review_count'):: integer as review_count,
           (data ->> 'yelping_since') :: timestamp as yelping_since,
           (data ->> 'useful'):: integer as useful,
           (data ->> 'funny'):: integer as funny,
           (data ->> 'cool'):: integer as cool,
           (data ->> 'fans'):: integer as fans,
           (data ->> 'stars'):: numeric  as average_stars
        from temp_data;
        INSERT INTO user_elite SELECT  data ->> 'user_id' as user_id, unnest(string_to_array(data ->> 'elite'
        , ',') :: int[]) as user_elite FROM temp_data;
        INSERT INTO user_friend SELECT  data ->> 'user_id' as user_id, unnest(string_to_array(data ->> 'friends', ',')) 
        as user_friend FROM temp_data;
        '''
        cursor.execute(user_statement)
        connection.commit()
        cursor.execute(delete_statement)
        connection.commit()
        print(time.time() - start_time)
        print("user_done")

        start_time = time.time()
        tip_statement = '''COPY temp_data(data) FROM '/Users/ravikiranjois/Documents/RIT/Semester 2/Introduction to Big Data/Project/yelp_dataset/tip.json'
        WITH QUOTE E'\b'  DELIMITER E'\t' CSV;
        insert into Tip(business_id,user_id,tip_text,tip_date,compliment_count)
        select
           data ->> 'business_id' as business_id,
           data ->> 'user_id' as user_id,
           data ->> 'text' as tip_text,
           (data ->> 'date'):: timestamp as tip_date,
           (data ->> 'compliment_count') :: integer as compliment_count
        from temp_data;
        '''
        cursor.execute(tip_statement)
        connection.commit()
        cursor.execute(delete_statement)
        connection.commit()
        print(time.time() - start_time)
        print("tip_done")

        print("Checking startt")
        start_time = time.time()
        checkin_statement = '''COPY temp_data(data) FROM '/Users/ravikiranjois/Documents/RIT/Semester 2/Introduction to Big Data/Project/yelp_dataset/checkin.json'
        WITH QUOTE E'\b'  DELIMITER E'\t' CSV;
        INSERT INTO checkin SELECT  data ->> 'business_id' as business_id, 
        unnest(string_to_array(data ->> 'date', ',')::timestamp[])  as checkin_date FROM temp_data limit (select count(*) from temp_data);
        '''
        cursor.execute(checkin_statement)
        connection.commit()
        print("Here")
        cursor.execute(delete_statement)
        connection.commit()
        cursor.execute('ALTER TABLE checkin ALTER COLUMN checkin_date TYPE timestamp;')
        connection.commit()
        print(time.time() - start_time)
        print("checkin_done")

        start_time = time.time()
        review_statement = '''COPY temp_data(data) FROM '/Users/ravikiranjois/Documents/RIT/Semester 2/Introduction to Big Data/Project/yelp_dataset/review.json'
        WITH QUOTE E'\b'  DELIMITER E'\t' CSV;
        insert into Review( review_id,business_id,user_id,stars,useful,funny,cool,review_text,review_date)
        select
           data ->> 'review_id' as review_id,
           data ->> 'business_id' as business_id,
           data ->> 'user_id' as user_id,
           (data ->> 'stars'):: numeric as stars,
           (data ->> 'useful'):: integer as useful,
           (data ->> 'funny'):: integer as funny,
           (data ->> 'cool'):: integer as cool,
           data ->> 'text' as review_text,
           (data ->> 'date'):: timestamp as review_date
        from temp_data;
        '''
        cursor.execute(review_statement)
        connection.commit()
        cursor.execute(delete_statement)
        connection.commit()
        print("review_done")
        primary_key(cursor)
        connection.commit()
        print("Primary key done")
        foreign_key(cursor)
        connection.commit()
        print("Foreign key done")
        print(time.time() - start_time)


        print("--------------------To JSON-----------------------")
        connection.commit()
        
        print("--------------------To Mongo-----------------------")
        transfer_to_mongo()


    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        # closing database connection.
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
            print(time.time() - total_time_taken)


def create_table(cursor):
    create_table_statement = '''CREATE TABLE IF NOT EXISTS temp_data (
    data json
    );
    CREATE TABLE IF NOT EXISTS Business (
    business_id  VARCHAR PRIMARY KEY NOT NULL,
    name  VARCHAR,
    city VARCHAR,
    state VARCHAR,
    postal_code VARCHAR,
    latitude numeric,
    longitude numeric,
    stars numeric,
    review_count integer
    );
    CREATE TABLE IF NOT EXISTS User_Table (
    user_id  VARCHAR PRIMARY KEY NOT NULL,
    name  VARCHAR,
    review_count integer,
    yelping_since timestamp,
    useful integer,
    funny integer,
    cool integer,
    fans integer,
    average_stars numeric
    );
    
    CREATE TABLE IF NOT EXISTS Tip(
        business_id VARCHAR,
        user_id VARCHAR,
        tip_text text,
        tip_date timestamp,
        compliment_count integer
        );
     CREATE TABLE IF NOT EXISTS User_Friend(
        user_id VARCHAR,
        friend VARCHAR
        );
    
    CREATE TABLE IF NOT EXISTS User_Elite(
        user_id VARCHAR,
        elite integer
        );
    
    
    CREATE TABLE IF NOT EXISTS Checkin(
        business_id VARCHAR,
        checkin_date VARCHAR
        );
    
    CREATE TABLE IF NOT EXISTS Business_Category(
        business_id VARCHAR,
        category VARCHAR
        );
    
    
    create table if not exists review(
                        review_id VARCHAR,
                        business_id VARCHAR,
                        user_id VARCHAR,
                        stars numeric,
                        useful integer,
                        funny integer,
                        cool integer,
                        review_text  VARCHAR,
                        review_date  timestamp
        );'''
    cursor.execute(create_table_statement)


def foreign_key(cursor):
    foreign_key_statement = '''ALTER TABLE Checkin ADD FOREIGN KEY(business_id) REFERENCES Business(business_id) NOT VALID;'''
    foreign_key_statement1 = '''
    ALTER TABLE Tip ADD FOREIGN KEY(business_id) REFERENCES Business(business_id) NOT VALID;
    ALTER TABLE Tip ADD FOREIGN KEY(user_id) REFERENCES User_Table(user_id) NOT VALID;
    ALTER TABLE User_Friend ADD FOREIGN KEY(user_id) REFERENCES User_Table(user_id) NOT VALID;
    ALTER TABLE User_Friend ADD FOREIGN KEY(friend) REFERENCES User_Table(user_id) NOT VALID;
    ALTER TABLE User_Elite ADD FOREIGN KEY(user_id) REFERENCES User_Table(user_id) NOT VALID;
    ALTER TABLE Checkin ADD FOREIGN KEY(business_id) REFERENCES Business(business_id) NOT VALID;
    ALTER TABLE Business_Category ADD FOREIGN KEY(business_id) REFERENCES Business(business_id) NOT VALID;
    '''
    cursor.execute(foreign_key_statement)


def primary_key(cursor):
    primary_key_statement = '''
    ALTER TABLE Checkin ADD PRIMARY KEY(business_id, checkin_date);
    ALTER TABLE User_Friend ADD PRIMARY KEY(user_id, friend);
    ALTER TABLE User_Elite ADD PRIMARY KEY(user_id, elite);
    ALTER TABLE Tip ADD PRIMARY KEY(tip_text, tip_date);
    '''
    # ALTER TABLE Business_Category ADD PRIMARY KEY(business_id, category);
    # ALTER TABLE Tip ADD PRIMARY KEY(user_id, tip_date);
    cursor.execute(primary_key_statement)


def to_json_files(cursor):
    business_query = '''copy (select row_to_json(t1) as business from  (select b.*,bc.categories from business b 
    left join (select business_id, 
    ARRAY_AGG(category) as "categories" from business_category group by business_id) bc 
    on b.business_id = bc.business_id) 
    t1) to \'/Users/ravikiranjois/Documents/RIT/Semester 2/Introduction to Big Data/Project/business1.json\' '''

    cursor.execute(business_query)
    print("1")

    review_query = '''copy (select row_to_json(t1) as review from(Select * from review)t1)
     to \'/Users/ravikiranjois/Documents/RIT/Semester 2/Introduction to Big Data/Project/review1.json\' '''

    cursor.execute(review_query)
    print("2")

    checkin_query = '''copy (select row_to_json(t1) as checkin from(Select * from checkin)t1) 
    to \'/Users/ravikiranjois/Documents/RIT/Semester 2/Introduction to Big Data/Project/checkin1.json\''''

    cursor.execute(checkin_query)
    print("3")

    tip_query = '''copy (select row_to_json(t1) as tip from(Select * from tip)t1) 
    to \'/Users/ravikiranjois/Documents/RIT/Semester 2/Introduction to Big Data/Project/tip1.json\''''

    cursor.execute(tip_query)
    print("4")

    users_query = '''copy (select row_to_json(t2) as users from (select t1.*, ue.elite from 
    (select u.*, uf.friends from user_table u left join (select user_id, 
    ARRAY_AGG(friend) as "friends" from user_friend group by user_id) uf on u.user_id = uf.user_id) t1 LEFT JOIN (select 
    user_id, ARRAY_AGG(elite) as elite from user_elite group by user_id) ue on ue.user_id = t1.user_id) t2) 
    to \'/Users/ravikiranjois/Documents/RIT/Semester 2/Introduction to Big Data/Project/users1.json\''''

    cursor.execute(users_query)
    print("5")


if __name__ == "__main__":
    main()
