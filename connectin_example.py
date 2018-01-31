import pymysql.cursors

#创建连接
connection=pymysql.connect(host='172.17.xx.xx',
                           port=6002,
                           user='user',
                           password='password',
                           db='xx_autotest',
                           charset='utf8mb4',
                           cursorclass=pymysql.cursors.DictCursor
                           )


try:
    with connection.cursor() as cursor:
        #待执行的sql语句
        sql=" SELECT * FROM luck_user WHERE lid=1 AND mid=132 "
        #执行sql语句
        cursor.execute(sql)
        #获取执行结果
        result=cursor.fetchall()

    connection.commit()

finally:
    connection.close()
    print(result)