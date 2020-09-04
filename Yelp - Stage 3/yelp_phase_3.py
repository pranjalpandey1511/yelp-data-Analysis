import itertools
import psycopg2
import time

'''
The Yelp Dataset Cleaning, Integration, Itemset Mining and Finding association Rules
@author: Pranjal Pandey
@author: Ravikiran Jois Yedur Prabhakar
@author: Venkata Karteek Paladugu
@author: Vishnu Saketh Byreddy
'''


def data_validation(cursor):
    data_business_category_2 = 'select * from business_category_2'
    cursor.execute(data_business_category_2)
    list_of_values = cursor.fetchall()
    for value in list_of_values:
        business_id = value[0]
        category = value[1]
        city = value[2]
        if len(business_id) != 22:
            print('Wrong Business ID pattern!')

        if city not in ['Dallas', 'North Las Vegas', 'South Las Vegas',
                        'N. Las Vegas', 'N Las Vegas', 'Las Vegas', 'Phoenix', 'Denver', 'Phoenix Valley']:
            print('Wrong City data!')


def main():
    try:
        #Dallas, Las Vegas, Phoenix, Denver
        connection = establish_connection()
        initial_start_time = time.time()
        cursor = connection.cursor()
        create_biz_category_2 = 'create table if not exists business_category_2 as ' \
                                'select c1.business_id as business_id, trim(from c.category) as category, trim(from c1.city) as city from ' \
                                '(select bz1.business_id, bz1.category from ' \
                                '(select bc.business_id, bc.category ' \
                                'from business_category as bc ' \
                                'where trim(from bc.category) = \'Restaurants\') as bz ' \
                                                                     'join business_category as bz1 ' \
                                                                     'on bz.business_id = bz1.business_id) as c ' \
                                                                     'join business as c1 ' \
                                                                     'on c.business_id = c1.business_id ' \
                                                                     'where c1.city like \'%Dallas%\' or c1.city like \'%Las Vegas%\' or c1.city like \'%Phoenix%\' or c1.city like \'%Denver%\';' \
                                'alter table business_category_2 add foreign key (business_id) references business(business_id);'
        cursor.execute(create_biz_category_2)
        connection.commit()
        data_validation(cursor)
        create_table_L1(cursor)
        # level = 2
        city_names = {'Dallas': 5, 'Las': 40, 'Phoenix': 40, 'Denver': 5}
        # city_names = ['Dallas', 'Las', 'Phoenix', 'Denver']
        for city_name in city_names.keys():
            level = 2
            print()
            if city_name == 'Las':
                print('--------------------------------------City is: Las Vegas--------------------------------------')
            else:
                print('--------------------------------------City is:', city_name+'--------------------------------------')
            print()
            while isEmpty(level - 1, city_name, cursor):
                start_time = time.time()
                actor_as_actor_list = create_actor_as_actor_list(level)
                pma_list = create_pma_list(level)
                pma_join_list = create_pma_join_list(level)
                equal_actor_list1, actors_list, equal_actor_list2 = create_equal_actor_list(level, city_name)
                query = '''
                CREATE TABLE if not exists l{level}_{city_name} AS
                (SELECT ''' + ' '.join([str(elem) for elem in actor_as_actor_list]) + ''' count(t1.business_id) from
                (Select t1.* from
                (Select ''' + ' '.join([str(elem) for elem in pma_list]) + ''' cc1.business_id, cc1.city as city from business_category_2 cc1 ''' \
                        + ' '.join([str(elem) for elem in pma_join_list]) + ''')t1 join l''' + str(level - 1) + '_' +city_name + ''' on ''' \
                        + ' '.join([str(elem) for elem in equal_actor_list1]) + ''' where t1.city like \'%{city_name}%\' ''' + ''')t1 join (Select t1.* from
                (Select ''' + ' '.join([str(elem) for elem in pma_list]) + ''' cc1.business_id, cc1.city as city from business_category_2 cc1 ''' \
                        + ' '.join([str(elem) for elem in pma_join_list]) + ''')t1 join l''' + str(level - 1) + '_' +city_name +''' on ''' \
                        + ' '.join([str(elem) for elem in equal_actor_list1]) + ''' where t1.city like \'%{city_name}%\' '''+''')t2 on t1.business_id = t2.business_id where ''' \
                        + ' '.join([str(elem) for elem in equal_actor_list2]) + ''' group by '''\
                        + ''.join([str(elem) for elem in actors_list]) + ''' having count(t1.business_id) >= '''+str(city_names[city_name])+ ''' order by count(t1.business_id) desc)'''
                # print(query.format(level=level, city_name=city_name))

                cursor.execute(query.format(level=level, city_name=city_name))


                # print('Time taken for level ' + str(level) + " : " + str(time.time() - start_time))
                level += 1
                connection.commit()
            association_level = level-2

            while association_level >= 2:
                select_statement = 'select * from l'+str(association_level)+'_'+city_name
                # print(select_statement)
                cursor.execute(select_statement)
                selected_list = list(cursor.fetchall())
                for item in selected_list:
                    combined_count = item[-1]
                    subsets = list(itertools.combinations(item[:-1], (association_level-1)))
                    subset_other = set(item[:-1])
                    # all_subs = '\', \''.join([str(elem).strip() for elem in subset_other])
                    calculation_full = ', '.join([str(elem).strip() for elem in subset_other])
                    calculation_full = set([x.strip() for x in calculation_full.split(',')])
                    # full_subset_query = 'select count(*) from business_category_2 where category in (\'' + all_subs + '\') and city = \'' + city_name + '\' join category'
                    # full_set = set(all_subs.split(','))
                    # cursor.execute(full_subset_query)
                    # full_subset_query_res = cursor.fetchall()
                    total_count_query = "select count(*) from ( select count(*) from business_category_2 where city like \'%"+city_name + "%\' group by business_id) as x;"
                    cursor.execute(total_count_query)
                    total_count = cursor.fetchall()[0][0]
                    for each_subset in subsets:
                        left_subs = '\', \''.join([str(elem).strip() for elem in each_subset])
                        calculation_left = ','.join([str(elem).strip() for elem in each_subset])
                        left_subset_query = 'select count(*) from business_category_2 where category in (\'' + left_subs + '\') and city = \'' + city_name + '\' group by category'
                        left_set = set(left_subs.split(','))
                        calculation_left = set(calculation_left.split(','))
                        right_set = calculation_full - calculation_left
                        cursor.execute(left_subset_query)
                        left_subset_query_res = cursor.fetchall()
                        # print(left_subset_query_res[0][0])

                        where_clause_left = ''
                        for i in range(1, len(left_set)+1):
                            if i == len(left_set):
                                where_clause_left += 'category' + str(i) + ' in (\'' + left_subs + '\')'
                            else:
                                where_clause_left += 'category' + str(i) +' in (\'' + left_subs + '\') and '
                        left_den = 'select count from l'+str(len(left_set))+'_'+city_name + ' where ' + where_clause_left
                        cursor.execute(left_den)
                        left_count = cursor.fetchall()
                        left_count = left_count[0][0]
                        # print(left_den)
                        right_subs = '\', \''.join([str(elem).strip() for elem in right_set])
                        where_clause_right = ''
                        for i in range(1, len(right_set)+1):
                            if i == len(right_set):
                                where_clause_right += 'category' + str(i) + ' in (\'' + right_subs + '\')'
                            else:
                                where_clause_right += 'category' + str(i) +' in (\'' + right_subs + '\') and '
                        right_den = 'select count from l'+str(len(right_set))+'_'+city_name + ' where ' + where_clause_right
                        # print(right_den)
                        cursor.execute(right_den)
                        right_count = cursor.fetchall()
                        right_count = right_count[0][0]
                        confidence = combined_count / left_count

                        lift = (combined_count * total_count) / (left_count * right_count)

                        if confidence > 0.6 and lift > 1:
                            print('For the association:', left_set, '-->', right_set)
                            print('     Confidence:', confidence)
                            print('     Lift:', lift)
                association_level -= 1

            connection.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        # closing database connection.
        if connection:
            print()
            print()
            print("Time taken : " + str(time.time() - initial_start_time))
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")


def create_actor_as_actor_list(level):
    actor_list = []
    for x in range(1, level+1):
        if x == level:
            s = '''t2.category''' + str(level-1) + ''' as category{level},'''
        else:
            s = '''t1.category{level} as category{level},'''
        actor_list.append(s.format(level=x))
    return actor_list


def create_pma_list(level):
    pma_list = []
    for x in range(1, level):
        s = '''cc{level}.category as category{level},'''
        pma_list.append(s.format(level=x))
    return pma_list


def create_pma_join_list(level):
    l = []
    for x in range(2, level):
        s = '''join business_category_2 cc{level} on cc''' + str(x - 1) + '''.business_id = cc{level}.business_id'''
        l.append(s.format(level=x))
    return l


def create_equal_actor_list(level, city_name):
    pre_level = level - 1
    l1 = []
    l2 = []
    l3 = []
    for x in range(1, level):
        if x == level - 1:
            s1 = '''t1.category{x} = l{pre_level}'''+'_{city_name}'+'''.category{x}'''
            s2 = '''t1.category{x},t2.category{x}'''
            s3 = '''t1.category{x} < t2.category{x}'''
        else:
            s1 = '''t1.category{x} = l{pre_level}'''+'_{city_name}'+'''.category{x} and'''
            s2 = '''t1.category{x},'''
            s3 = '''t1.category{x} = t2.category{x} AND'''
        l1.append(s1.format(x=x, pre_level=pre_level, city_name=city_name))
        l2.append(s2.format(x=x))
        l3.append(s3.format(x=x))
    return l1, l2, l3


def establish_connection():
    conn = psycopg2.connect(host="127.0.0.1", user="ravikiranjois", database="yelp_data", password="coolie01")
    return conn


def isEmpty(level, city_name, cursor):
    query = '''select count(*) from l{level}_{city_name}
            '''
    cursor.execute(query.format(level=level, city_name=city_name))
    result = cursor.fetchall()
    # print("Number of frequent itemsets in " + str(level) + " : " + str(result[0][0]))
    if result[0][0] == 0:
        return False
    else:
        return True


def create_table_L1(cursor):
    city_names = {'Dallas': 5, 'Las': 40, 'Phoenix': 40, 'Denver': 5}
    # city_names = ['Washington']
    for city_name in city_names.keys():
        query = '''
        CREATE TABLE if not exists L1_{city_name} AS
        (Select category as category1, count(business_id) from business_category_2 where city like \'%{city_name}%\' 
        group by category HAVING count(business_id) >= '''+str(city_names[city_name])+''' 
        ORDER BY category ASC);
        '''
        cursor.execute(query.format(city_name=city_name))


if __name__ == '__main__':
    main()