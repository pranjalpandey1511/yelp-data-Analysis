import time
import psycopg2
import pandas as pd


'''
Author : 1. Venkata Karteek Paladugu
         2. Pranjal Pandey
         3. Ravikiran Jois
         4. Vishnu Byreddy
'''

def find_fds(table, cursor, connection):
    fetch_query = f'SELECT * FROM {table}'

    cursor.execute(fetch_query)
    df = pd.DataFrame(cursor.fetchall())
    df.columns = [x.name for x in cursor.description]
    columns = list(df.columns)
    subsets = []
    fd = {}
    createSubsets(columns, subsets, [], 0)
    subsets.sort(key=len)
    level1 = []
    partitions = {}

    for subset in subsets:
        if len(subset) == 1:
            level1.append(subset)
    createPartitions(df, level1, partitions)  # creating partitions
    print("Partitions done")
    stripPartitions(partitions, columns)  # from partitions stripping sets os size 1
    print("Stripping Partitions done")
    for subset in level1:
        for col in columns:
            if col in subset:
                continue
            if checkDependency(partitions, subset, col):
                print(subset[0] + "-->" + col)
                if subset[0] in fd:
                    fd.get(subset[0]).append(col)
                else:
                    fd[subset[0]] = [col]

    print("************* LEVEL 1 DONE ***************")

    level = 2
    while level < len(columns):
        level_list = []
        for subset in subsets:
            if len(subset) == level:
                level_list.append(subset)

        for subset in level_list:
            for col in columns:
                if col in subset or pruned(fd, subset, col):
                    continue
                # if checkDependency(partitions, subset, col):
                #     print(subset, col)
                #     if tuple(subset) in fd:
                #         fd.get(tuple(subset)).append(col)
                #     else:
                #         fd[tuple(subset)] = [col]
                left = ', '.join([str(elem) for elem in subset])
                right = col
                cursor.execute(
                    f'select {left}, count(distinct ({right})) from {table} group by ({left}) having count(distinct ({right})) > 1')
                # print(f'{left} -> {right}')

                if len(cursor.fetchall()) == 0:
                    if tuple(subset) in fd:
                        fd.get(tuple(subset)).append(col)
                    else:
                        fd[tuple(subset)] = [col]
                    print(f'This is a FD: {left} -> {right}')

        print(f"************* LEVEL {level} DONE ***************")
        level += 1
    # print(len(fd))

    connection.commit()


def main():
    """
        main function :
        main function establishes connection with the database server and implements the pruning algorithm
        to find FD's
    """
    try:
        connection = establish_connection()
        total_time_taken = time.time()
        cursor = connection.cursor()
        list_of_tables = ['user_table', 'tip', 'user_elite', 'user_friend', 'business', 'business_category', 'checkin', 'review']
        for table in list_of_tables:
            print(f"Funtional Dependencies for -> {table}")
            find_fds(table, cursor, connection)


    except (Exception, psycopg2.DatabaseError) as error:
        print (error)
    finally:
        # closing database connection.
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
            print(time.time() - total_time_taken)


def partitionIntersection(right, partitions):
    """
        partitionIntersection function :
        partitionIntersection function intersects two partitions to create partitions of higher level
    """
    set1 = set(frozenset(i) for i in partitions.get(right[0]).values())
    set2 = set(frozenset(i) for i in partitions.get(right[1]).values())
    tempList = []
    count = 0
    for s1 in set1:
        for s2 in set2:
            if not len(s1.intersection(s2)) == 0:
                tempList.append(s1.intersection(s2))
    set3 = set(frozenset(i) for i in tempList)
    return set3


def checkDependency(partitions, right, left):
    """
        checkDependency function :
        checkDependency function checks if left prunes right
    """
    if len(right) > 1:  # right is of higher level then call intersection
        rightSet = partitionIntersection(right, partitions)
        leftSet = set(frozenset(i) for i in partitions.get(left).values())
    else:
        rightPart = partitions.get(right[0])  # the left side of the FD
        leftPart = partitions.get(left)  # the right side of the FD
        rightSet = set(frozenset(i) for i in rightPart.values())
        leftSet = set(frozenset(i) for i in leftPart.values())

    for set1 in rightSet:
        flag = False
        for set2 in leftSet:
            if set1.issubset(set2):
                flag = True
                break
        if not flag:
            return False
    return True


def establish_connection():
    # username = raw_input("Enter username: ")
    # password = raw_input("Enter password: ")
    # host = raw_input("Enter host: ")
    # port = raw_input("Enter port: ")
    # database = raw_input("Enter database name: ")
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


def pruned(fd, subset, col):
    """
    pruned function:
    checks is given FD is pruned
    """
    for key in fd:
        if key in subset and col in fd.get(key):
            return True
    return False


def createPartitions(df, level1, partitions):
    """
    createPartitions function:
    createPartitions creates partitions for given column
    """
    for col in level1:
        df_list = df[col].values.tolist()
        index = 0
        d = {}
        for row in df_list:
            if str(row[0]) in d:
                d.get(str(row[0])).append(index)
                index += 1
                continue
            tempList = [index]
            d[str(row[0])] = tempList
            index += 1
        partitions[col[0]] = d


def stripPartitions(partitions, columns):
    for col in columns:
        tempList = []
        for key in partitions.get(col):
            if len(partitions.get(col).get(key)) == 1:
                tempList.append(key)
        for key in tempList:
            del partitions.get(col)[key]


def createSubsets(columns, subsets, tempList, start):
    if len(tempList) != 0 and len(tempList) <= 2:
        copy = list(tempList)
        subsets.append(copy)
    for i in range(start, len(columns)):
        tempList.append(columns[i])
        createSubsets(columns, subsets, tempList, i + 1)
        tempList.pop(len(tempList) - 1)


if __name__ == "__main__":
    main()
