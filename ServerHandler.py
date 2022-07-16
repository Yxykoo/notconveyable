import pymysql
import pandas as pd
import datetime
import math
import pdb
class ServerHandler:
    def __init__(self,host='111',database='11',user='root',password='11',charset='utf8mb4'):
        self.conn = pymysql.connect(host=host,database=database,user=user,password=password,charset=charset)
        self.cursor = self.conn.cursor()
        # self.columns_names = ['asin','source','target','offer_type','action_year','action_wk','action_date',
        #                       'buyable_date','action','tracker','subtask_type','blocker_name','core_export_export_date'
        #                       ,'blocker_status','double_check_date','login','item_package_dimensions',
        #                       'item_package_weight','comment']
    def create_table(self):
        # sql = "CREATE TABLE `base_table`(`asin` varchar(30) not null ," \
        sql = "CREATE TABLE `base_table_history`(`asin` varchar(30) not null ," \
              "`source` varchar(10) not null," \
              "`target` varchar(10) not null," \
              "`offer_type`varchar(10) not null," \
              "`task` varchar(50)," \
              "`subtask_type` varchar(100)," \
              "`blocker_name` varchar(100)," \
              "`blocker_status` varchar(20)," \
              "`requester` varchar(20)," \
              "`action_year` integer," \
              "`action_wk` integer," \
              "`action_date`  date not null," \
              "`take_action` varchar(100)," \
              "`tracker` varchar(255)," \
              "`comment` varchar(255)," \
              "`core_export_export_date` date," \
              "`double_check_date` date," \
              "`login` varchar(20)," \
              "`item_package_dimensions` varchar(10)," \
              "`item_package_weight` varchar(10)," \
              "PRIMARY KEY (asin,source,target,action_date,blocker_name,core_export_export_date,subtask_type,item_package_dimensions,item_package_weight,offer_type))"
        # "PRIMARY KEY (asin,source))"
        self.cursor.execute(sql)
        self.conn.commit()
        self.conn.close()

    def filter_inflow(self,asin_source_list):
        asin_source_list = ['"' + asin_source for asin_source in
                                             asin_source_list]
        asin_source_list = [asin_source + '"' for asin_source in
                                             asin_source_list]
        asin_source_concat = ','.join(asin_source_list)
        asin_source_concat_string = '(' + asin_source_concat + ')'
        #找到数据库中找不到这些asin和source的记录
        #2022/7/1修改 原：sql = "SELECT asin, source from `base_table` where blocker_status='unblocked' and concat(asin,source) IN {}".format(asin_source_concat_string)
        sql = "SELECT asin, source from `base_table` where  concat(asin,source) IN {}".format(asin_source_concat_string)
        result = pd.read_sql(sql, self.conn)
        # self.close_connection()
        return result
    def insert_data_to_db(self,rows):
        sql = "INSERT IGNORE INTO `base_table` (asin,source,target,offer_type,action_year,action_wk,action_date,login,blocker_name,take_action,subtask_type,blocker_status) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        # test_row = ['B07S1488TK',1,'PolicyManager','CORE-UK-ALL_Alcohol (100582 v13)','TEST','TEST','Done','2020-11-4',1,11,21,'0000-00-00','BE']
        self.cursor.executemany(sql, rows)
        # self.cursor.execute(sql,test_row)
        self.conn.commit()

    def insert_test_data_to_db(self,rows):
        sql = "INSERT IGNORE INTO `base_table` (asin,source,target,offer_type,action_date,take_action,blocker_status) VALUES (%s,%s,%s,%s,%s,%s,%s)"
        # test_row = ['B07S1488TK',1,'PolicyManager','CORE-UK-ALL_Alcohol (100582 v13)','TEST','TEST','Done','2020-11-4',1,11,21,'0000-00-00','BE']
        self.cursor.executemany(sql, rows)
        # self.cursor.execute(sql,test_row)
        self.conn.commit()

    def change_column_value(self,rows,sql_part):
        # pdb.set_trace()
        sql = "UPDATE `base_table` SET {} WHERE asin=%s and source=%s".format(sql_part)
        # pdb.set_trace()
        self.cursor.executemany(sql, rows)
        self.conn.commit()
        self.close_connection()

    #
    def get_data_to_refresh(self):
        sql = "SELECT asin,source FROM `base_table` WHERE  core_export_export_date is NULL or core_export_export_date = '0000-00-00'"
        # sql = "SELECT asin,source FROM `base_table` WHERE subtask_type ='core export' and core_export_export_date is NULL"
        result = pd.read_sql(sql,self.conn)
        # self.close_connection()
        return result

    def update_data_to_refresh(self,asin_with_exportable_info_list):
        # pdb.set_trace()
        # print(asin_with_exportable_info_list)
        sql = "UPDATE `base_table` SET core_export_export_date=%s" \
              " WHERE asin=%s AND source=%s"
        self.cursor.executemany(sql, asin_with_exportable_info_list)
        self.conn.commit()
        # self.close_connection()

    def close_connection(self):
        self.conn.close()

    def update_dimension_values(self,asin_with_dimensional_info_list):
        sql = '''
        UPDATE `base_table` SET item_package_dimensions=%s,item_package_weight=%s
         WHERE asin=%s AND source=%s
        '''
        # sql = '''
        #         UPDATE `base_table` SET item_package_dimensions=%s,item_package_weight=%s
        #          WHERE asin=%s
        #         '''
        self.cursor.executemany(sql, asin_with_dimensional_info_list)
        self.conn.commit()
        # self.close_connection()

    def update_blocker_values(self, asin_with_blocker_info_list):
        sql = '''
                UPDATE `base_table` SET blocker_name=%s, blocker_status=%s
                 WHERE asin=%s AND source=%s
                '''
        self.cursor.executemany(sql, asin_with_blocker_info_list)
        self.conn.commit()
        # self.close_connection()

    def get_status_info(self,asin_source_list):
        asin_source_list = ['"' + asin_source for asin_source in
                            asin_source_list]
        asin_source_list = [asin_source + '"' for asin_source in
                            asin_source_list]
        asin_source_concat = ','.join(asin_source_list)
        asin_source_concat_string = '(' + asin_source_concat + ')'
        sql = 'SELECT asin,source,blocker_name,blocker_status FROM `base_table` WHERE concat(asin,source) IN {}'.format(asin_source_concat_string)
        result = pd.read_sql(sql, self.conn)
        # pdb.set_trace()
        self.close_connection()
        return result

    def get_refresh_status_info(self):
        #@todo 这里
        # sql = '''
        # SELECT asin,source from `base_table` WHERE (blocker_status !='TP' OR blocker_status is null) AND (core_export_export_date is NULL OR core_export_export_date = '0000-00-00')
        # '''
        sql = '''
                SELECT asin,source from `base_table` WHERE (blocker_status not in ('TP','unblocked') OR blocker_status is null) AND (core_export_export_date is NULL OR core_export_export_date = '0000-00-00')
                '''
        result = pd.read_sql(sql, self.conn)
        self.close_connection()
        return result

    def get_data_to_delete_and_insert_to_history_table(self,asin_source_list):
        #在检查blocker status前，将已经unblocked的记录从base_table中移除，并插入到base_table_history中
        # asin_restriction_type_concat_list = ["'"+asin_restriction_type for asin_restriction_type in asin_restriction_type_concat_list]
        # asin_restriction_type_concat_list = [asin_restriction_type+"'" for asin_restriction_type in asin_restriction_type_concat_list]
        # asin_restriction_type_concat = ','.join(asin_restriction_type_concat_list)
        # asin_restriction_type_concat_string = '(' +asin_restriction_type_concat+')'
        # print(asin_restriction_type_concat_string)
        asin_source_list = ['"' + asin_source for asin_source in
                            asin_source_list]
        asin_source_list = [asin_source + '"' for asin_source in
                            asin_source_list]
        asin_source_concat = ','.join(asin_source_list)
        asin_source_concat_string = '(' + asin_source_concat + ')'
        #设置一个query packet的长度128m
        # print(asin_source_concat_string)
        # self.cursor.execute("SET GLOBAL max_allowed_packet=134217728")
        #@这里要把所有new inflow且在数据库中能找到的unblocked的ASIN拿出来
        get_data_sql = "SELECT * FROM `base_table` WHERE blocker_status='unblocked' AND concat(asin,source) in {}".format(asin_source_concat_string)
        # pdb.set_trace()
        self.cursor.execute(get_data_sql)
        result = self.cursor.fetchall()
        # print(result)
        # pdb.set_trace()
        result_list = list(result)
        if result_list != []:
            print('new inflow在database1中存在unblocked状态的ASIN记录为：')
            print(result_list)
        else:
            print('new inflow在database1中不存在unblocked状态的ASIN记录')
        entire_delete_list = []


        #asin,source,target
        for result in result_list:
            entire_delete_list.append(result[0:3])
        #把符合条件的数据插入到history_data_table中
        data_to_insert_sql = "INSERT IGNORE INTO `base_table_history` (asin,source,target," \
                             "offer_type,task,subtask_type,blocker_name,blocker_status,requester,action_year,action_wk,action_date,take_action,tracker,comment," \
                             "core_export_export_date,double_check_date,login," \
                             "item_package_dimensions," \
                             "item_package_weight) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        chunk_len = 1000
        chunk_num = math.ceil(len(result_list) / chunk_len)
        for index in range(chunk_num):
            temp_result_to_insert_list = result_list[index*chunk_len:(index+1)*chunk_len]
            self.cursor.executemany(data_to_insert_sql,temp_result_to_insert_list)
            print('备份历史数据进度：'+str(round((index / chunk_num) * 100, 2))+'%')
        # self.conn.commit()
        print('在database2中备份历史数据完成')
        #从base_table中根据composite primary key删除符合条件的ASIN
        delete_data_sql = "DELETE FROM `base_table` WHERE asin=%s and source=%s and target=%s"
        # print(chunk_num)
        for index in range(chunk_num):
            temp_result_to_delete_list = entire_delete_list[index * chunk_len:(index + 1) * chunk_len]
            #@todo 看看是不是别人更新base_table导致锁住了，如果是的话直接结束然后print个提示
            self.cursor.executemany(delete_data_sql, temp_result_to_delete_list)
            print('database1删除历史数据进度：' + str(round((index / chunk_num) * 100, 2)) + '%')
        # self.cursor.executemany(delete_data_sql,entire_delete_list)
        self.conn.commit()
        print('从database1中删除历史数据完成')

        self.close_connection()

    def bulk_delete(self):
        # 设置一个query packet的长度128m
        self.cursor.execute("SET GLOBAL max_allowed_packet=134217728")
        # 从base_table中把所有unblocked的asin抓出来
        get_data_sql = "SELECT * FROM `base_table` WHERE blocker_status='unblocked'"
        # pdb.set_trace()
        self.cursor.execute(get_data_sql)
        result = self.cursor.fetchall()
        # pdb.set_trace()
        result_list = list(result)
        print(result_list)
        entire_delete_list = []

        for result in result_list:
            entire_delete_list.append(result[0:3])
        chunk_len = 1000
        chunk_num = math.ceil(len(result_list) / chunk_len)
        delete_data_sql = "DELETE FROM `base_table` WHERE asin=%s and source=%s and target=%s"
        for index in range(chunk_num):
            temp_result_to_delete_list = entire_delete_list[index * chunk_len:(index + 1) * chunk_len]
            self.cursor.executemany(delete_data_sql, temp_result_to_delete_list)
            print('database1删除历史数据进度：' + str(round((index / chunk_num) * 100, 2)) + '%')
            self.conn.commit()
        # self.cursor.executemany(delete_data_sql,entire_delete_list)
        print('从database1中删除历史数据完成')

        self.close_connection()
    def get_asins_to_check_dimension(self):
        sql = "SELECT asin, source from `base_table` WHERE (blocker_name='BizRule_ASINLevel_NotConveyable'  OR blocker_name = '' OR blocker_name is null) AND (blocker_status not in ('unblocked','TP') OR blocker_status is null)" \
              " AND ((item_package_dimensions != 'True' or item_package_dimensions is null) OR (item_package_weight != 'True' or item_package_weight is null))"
        results = pd.read_sql(sql, self.conn)
        # print(results)
        self.close_connection()
        # pdb.set_trace()
        return results

    def initialize_db(self,data):
        # sql = "INSERT IGNORE INTO `base_table` (asin,source,target,offer_type,task,subtask_type,blocker_name,blocker_status,action_year,action_wk,action_date,take_action,tracker,comment,core_export_export_date,double_check_date,login,item_package_dimensions,item_package_weight,requester) values (%s,%s,%s,%s,%s,NULLIF(%s,''),NULLIF(%s,''),%s,%s,%s,%s,NULLIF(%s,''),NULLIF(%s,''),NULLIF(%s,''),NULLIF(%s,''),NULLIF(%s,''),NULLIF(%s,''),%s,%s,NULLIF(%s,''))"
        # # test_row = ['B07S1488TK',1,'PolicyManager','CORE-UK-ALL_Alcohol (100582 v13)','TEST','TEST','Done','2020-11-4',1,11,21,'0000-00-00','BE']
        # self.cursor.executemany(sql, rows)
        cols = ['asin','source','target','offer_type','task','subtask_type','blocker_name','blocker_status','action_year','action_wk','action_date','take_action','tracker','comment','core_export_export_date','double_check_date','login','item_package_dimensions','item_package_weight','requester']
        sql = "INSERT IGNORE INTO base_table" +" ("+ ','.join(cols) + ") VALUES"+ ','.join(["(" +",".join(["%s"] * len(cols))+")"]*len(data))
        self.cursor.execute(sql,[str(data.loc[index][col]) for index in data.index for col in cols])
        # self.cursor.execute(sql,test_row)
        self.conn.commit()
        # self.close_connection()
    def initialize_db_history(self,data):
        # sql = "INSERT IGNORE INTO `base_table` (asin,source,target,offer_type,task,subtask_type,blocker_name,blocker_status,action_year,action_wk,action_date,take_action,tracker,comment,core_export_export_date,double_check_date,login,item_package_dimensions,item_package_weight,requester) values (%s,%s,%s,%s,%s,NULLIF(%s,''),NULLIF(%s,''),%s,%s,%s,%s,NULLIF(%s,''),NULLIF(%s,''),NULLIF(%s,''),NULLIF(%s,''),NULLIF(%s,''),NULLIF(%s,''),%s,%s,NULLIF(%s,''))"
        # # test_row = ['B07S1488TK',1,'PolicyManager','CORE-UK-ALL_Alcohol (100582 v13)','TEST','TEST','Done','2020-11-4',1,11,21,'0000-00-00','BE']
        # self.cursor.executemany(sql, rows)
        cols = ['asin','source','target','offer_type','task','subtask_type','blocker_name','blocker_status','action_year','action_wk','action_date','take_action','tracker','comment','core_export_export_date','double_check_date','login','item_package_dimensions','item_package_weight','requester']
        sql = "INSERT IGNORE INTO base_table_history" +" ("+ ','.join(cols) + ") VALUES"+ ','.join(["(" +",".join(["%s"] * len(cols))+")"]*len(data))
        self.cursor.execute(sql,[str(data.loc[index][col]) for index in data.index for col in cols])
        # self.cursor.execute(sql,test_row)
        self.conn.commit()

    def insert_new_inflow(self,data):
        # pdb.set_trace()
        cols = ['asin', 'source', 'target', 'offer_type', 'task', 'subtask_type', 'requester', 'blocker_name',
                       'action_year', 'action_date','action_wk']
        sql = "INSERT IGNORE INTO base_table" +" ("+ ','.join(cols) + ") VALUES"+ ','.join(["(" +",".join(["%s"] * len(cols))+")"]*len(data))
        # pdb.set_trace()
        self.cursor.execute(sql, [str(data.loc[index][col]) for index in data.index for col in cols])
        # self.cursor.execute(sql,test_row)
        self.conn.commit()
        # self.close_connection()
class CoreExportServerHandler:
    def __init__(self,host='111',database='11',user='root',password='111',charset='utf8mb4'):
        self.conn = pymysql.connect(host=host,database=database,user=user,password=password,charset=charset)
        self.cursor = self.conn.cursor()

    def get_exportable_data(self,asin_source_list):

        ''''''

        sql = '''
        select 
       table1.asin
	 ,table1.marketplace_id
   ,table1.exportable_date 
 from (
        select 
    asin
			,marketplace_id
      ,exportable_date
      ,row_number() over (partition by asin order by exportable_date desc) as time_rank
     from base_table
			where concat(asin,marketplace_id) in 
			'''
        asin_source_list = ["'" + asin_source for asin_source in
                                             asin_source_list]
        asin_source_list = [asin_source + "'" for asin_source in
                                             asin_source_list]
        asin_restriction_type_concat = ','.join(asin_source_list)
        asin_source_concat_string = '(' + asin_restriction_type_concat + ')'
        sql += asin_source_concat_string

        sql +='''
        and exportable_date!='0000-00-00'
        ) as table1
        where time_rank = 1
   '''
        # pdb.set_trace()
        df = pd.read_sql(sql,self.conn)
        self.conn.close()
        return df




if __name__ == '__main__':
    # df = pd.read_excel(r'C:\Users\xinyyang\Desktop\not conveyable\sample asin.xlsx',sheet_name='Sheet2')
    # df['asin'] = df['asin'].map(lambda x:str(x).zfill(10))
    # source_list  = df['source'].unique().tolist()
    # for source in source_list:
    #     df_by_source = df[df['source']==source]
    #     df_info_by_source = df_by_source.to_numpy().tolist()
    #     ServerHandler(database='notconveyable_'+source.lower()).insert_test_data_to_db(df_info_by_source)
    source = 'ae'
    serverhandler = ServerHandler(database='notconveyable_'+source.lower())
    serverhandler.create_table()