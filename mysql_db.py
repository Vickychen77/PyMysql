import pymysql.cursors
import configparser as config
import os

#os.path.dirname(__file__) 获取当前脚本的绝对路径
base_path=os.path.dirname(__file__)
#将db_config.ini 与 mysql_db.py 文件放在同一路径下
file_path=base_path+'/db_config.ini'
print(file_path)
cp=config.ConfigParser()
cp.read(file_path)

#从db_config.ini文件中读取配置信息
host=cp.get('mysqlconf','host')
port=cp.get('mysqlconf','port')
user=cp.get('mysqlconf','user')
password=cp.get('mysqlconf','password')
db_name=cp.get('mysqlconf','db_name')


class DB():

    def __init__(self):
        try:
            self.connection = pymysql.connect(host=host,
                                              port=int(port),
                                              user=user,
                                              password=password,
                                              db=db_name,
                                              charset='utf8mb4',
                                              cursorclass=pymysql.cursors.DictCursor)

        except pymysql.err.OperationalError as e:
            print('Mysql Error %d:%s',(e.args[0],e.args[1]))

    #插入数据
    def insert(self,table_name,table_data):
        for key,value in table_data.items():
            table_data[key]="'"+ str(value)+"'"
        key=','.join(table_data.keys())
        value=','.join(table_data.values())

        with self.connection.cursor() as cursor:
            sql = "INSERT INTO " + table_name + "(" + key + ")" + "VALUES" + "(" + value + ")" +';'
            print(sql)
            cursor.execute(sql)

        self.connection.commit()

    #清除数据库
    def clear(self,table_name):
        with self.connection.cursor() as cursor:
            sql = 'DELETE FROM ' + table_name + ";"
            print(sql)
            cursor.execute(sql)
        self.connection.commit()

    #更新数据
    def update(self,table_name,table_data,where=None):
        upd_where=""
        data=""
        if where is not None:
            for key,value in where.items():
                upd_where=upd_where + key + '=' +str(value) +' AND '
            for key,value in table_data.items():
                data=data + key+'='+str(value) +','

            with self.connection.cursor() as cursor:
                sql="UPDATE " +table_name +' SET ' + data[:-1] + " WHERE " + upd_where[:-4] +';'
                cursor.execute(sql)
                print(sql)
            self.connection.commit()

        else:
            for key, value in table_data.items():
                data = data + key + '=' + str(value) + ','
            with self.connection.cursor() as cursor:
                sql="UPDATE " +table_name +' SET ' + data[:-1] +';'
                print(sql)
                cursor.execute(sql)
            self.connection.commit()


    #查找数据
    def select(self,table_name,where=None):
        sel_where=""
        if where is not None:
            for key ,value in where.items():
                sel_where=sel_where + key +'='+str(value) + ' AND '

            with self.connection.cursor() as cursor:
                sql = "SELECT * FROM " + table_name + ' WHERE ' + sel_where[:-4] +';'
                print(sql)
                cursor.execute(sql)
            self.connection.commit()
        else:
            with self.connection.cursor() as cursor:
                sql="SELECT * FROM " + table_name+';'
                print(sql)
                cursor.execute(sql)
            self.connection.commit()



    def close(self):
        self.connection.close()

if __name__=='__main__':
    DB().clear('mz_topic_log')
    DB().insert('mz_topic_log',{'tlid':1,'tid':1,'did':1})
    DB().insert('mz_topic_log', {'tlid': 2,'tid': 2,'did':2})
    DB().select('mz_topic_log',{'tlid':1,'tid':1})
    DB().select('mz_topic_log')
    DB().update('mz_topic_log',{'tid':2,'did':2},{'tid':1})
    DB().update('mz_topic_log',{'tid':3,'did':3})
