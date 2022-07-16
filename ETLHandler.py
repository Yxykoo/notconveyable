import pandas as pd
import os
import datetime
import time
import getpass
import sys
import pdb
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from dependencies.blocker_status_update_query import *
from selenium.webdriver.support.select import Select
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
# from dependencies.FileHandler import FileHandler
class ETLHandler:
    def __init__(self,chrome_profile_path,headless_chrome_flag=False):
        self.chrome_options = Options()
        self.chrome_options.add_argument('disable-infobars')
        if getpass.getuser() == 'xinyyang':
            self.chrome_options.binary_location = "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe"
        chrome_path = chrome_profile_path
        self.chrome_options.add_argument("user-data-dir=" + chrome_path)
        self.chrome_options.add_argument("--no-sandbox")
        self.chrome_options.add_argument("--disable-dev-shm-usage")
        self.download_destination = os.getcwd() + '\dependencies\query_result'
        #True开启headless chrome; False关闭headless chrome
        if headless_chrome_flag:
            self.headless_chrome_mode_on()
        else:
            p = {"download.default_directory": self.download_destination}
            self.chrome_options.add_experimental_option("prefs", p)
            self.driver = webdriver.Chrome(options=self.chrome_options)
        self.wait = WebDriverWait(self.driver, 60)  # 等待的最大时间
    def create_new_driver(self):
        self.driver = webdriver.Chrome(options=self.chrome_options)
    def headless_chrome_mode_on(self):
        self.chrome_options.add_argument('--headless')
        self.chrome_options.add_argument("--no-sandbox")
        self.chrome_options.add_argument("--disable-dev-shm-usage")
        p = {"download.prompt_for_download": False,
             "download.directory_upgrade": True,
             "download.default_directory": self.download_destination}
        self.chrome_options.add_experimental_option("prefs", p)
        self.driver = webdriver.Chrome(options=self.chrome_options)
        self.driver.execute_script(
            "var x = document.getElementsByTagName('a'); var i; for (i = 0; i < x.length; i++) { x[i].target = '_self'; }")
        self.driver.command_executor._commands["send_command"] = ("POST", '/session/$sessionId/chromium/send_command')
        params = {'cmd': 'Page.setDownloadBehavior',
                  'params': {'behavior': 'allow', 'downloadPath': self.download_destination}}
        self.driver.execute("send_command", params)
    # get exportable countries before query
    def headless_chrome_mode_off(self):
        self.driver = webdriver.Chrome(options=self.chrome_options)
    def get_final_sql(self, source,asins_list):
        source_region_mapping = {
            'US':'(1)',
            'UK':'(1,2,3)',
            'DE':'(1,2,3)',
            'JP':'(1,2,3)',
            'AE':'(1,2,3)'
        }
        sql_heading = '''"/*+ ETLM {  \\n      depend:{\\n          replace:[\\n               {name:\\" aee_ddl.d_aee_asins\\"}\\n]\\n      }\\n    }\\n*/\\nselect asin, marketplace_id,pkg_length,pkg_width,pkg_height,pkg_weight,pkg_dimensional_uom,pkg_weight_uom,item_length,item_width,item_height,item_weight\\nfrom aee_ddl.d_aee_asins\\nwhere region_id in '''
        sql_heading +=source_region_mapping[source]
        sql_heading += '''\\nand marketplace_id = {MARKETPLACE_ID}\\nand (\\n    ASIN IN (\\n'''

        sql_body = ''''''
        for i in range(len(asins_list)):
            if i != len(asins_list)-1:
                sql_body += "\\'"+asins_list[i]+"\\',\\n"
            else:
                sql_body += "\\'" + asins_list[i] + "\\'\\n"


        sql_ending = '''));\\n"'''
        final_sql = ''.join(['x.value =',sql_heading,sql_body,sql_ending])
        # final_sql = ''''''+sql_heading + sql_body + sql_ending
        # pdb.set_trace()
        # print(final_sql)

        return final_sql

    def get_final_sql_blocker_status(self, source,asins_list):
        if source =='US':
            sql_heading = us_query_heading
        elif source == 'UK':
            sql_heading = uk_query_heading
        elif source == 'DE':
            sql_heading = de_query_heading
        elif source == 'JP':
            sql_heading = jp_query_heading
        elif source == 'AE':
            sql_heading = ae_query_heading


        sql_body = ''''''
        for i in range(len(asins_list)):
            if i != len(asins_list)-1:
                sql_body += "\\'"+asins_list[i]+"\\',\\n"
            else:
                sql_body += "\\'" + asins_list[i] + "\\'\\n"


        sql_ending = '''));\\n"'''
        final_sql = ''.join(['x.value =',sql_heading,sql_body,sql_ending])
        # final_sql = ''''''+sql_heading + sql_body + sql_ending
        # pdb.set_trace()
        # print(final_sql)

        return final_sql





    def get_run_date(self,dataset_date):
        if dataset_date == '2_days_ago':   ### run date先写成前天，最后再确认
            run_time = datetime.date.today() - datetime.timedelta(days=2)  # 前天，看看要改成昨天嘛
        elif dataset_date == 'manual_input':
            day = str(input('Please input your dataset date (Format: 20210312):'))
            run_time = datetime.datetime.strptime(day, '%Y%m%d')
        elif dataset_date == 'tuesday':
            tuesday = datetime.date.today()
            one_day = datetime.timedelta(days=1)
            while tuesday.weekday() != 1:
                tuesday -= one_day
            run_time = tuesday
        elif dataset_date == 'sunday':
            tuesday = datetime.date.today()
            one_day = datetime.timedelta(days=1)
            while tuesday.weekday() != 1:
                tuesday -= one_day
            sunday = tuesday - datetime.timedelta(days=2)
            run_time = sunday

        run_date = run_time.strftime("%Y/%m/%d")
        return run_date

    def edit_profile(self,final_sql,etl_url,source):
        run_time = self.get_run_date('2_days_ago')
        self.get_url(etl_url)
        self.driver.execute_script("arguments[0].click();", self.driver.find_element_by_xpath('//*[@id="editProfileBttn"]'))
        self.wait_until_located('//*[@id="Oracle_profile_sql"]')
        # self.wait_until_located('//*[@id="Oracle_profile_sql"]')
        time.sleep(10)
        js = self.get_sql_content(final_sql)
        self.driver.execute_script(js)
        self.driver.execute_script("arguments[0].click();", self.driver.find_element_by_xpath('//*[@id="saveProfileBttn"]'))
        time.sleep(5)
        print(etl_url,source)
        self.run_correct_job(source)
        # for row,ele in iter(job_table_elements):
        #     pdb.set_trace()
        # self.driver.execute_script("arguments[0].click();", self.driver.find_element_by_xpath("//*[text()='Run']"))
        self.driver.execute_script("arguments[0].click();", self.driver.find_element_by_xpath('//*[@id="single_date"]'))
        time.sleep(1)
        self.wait_until_located('//*[@id="single_date"]').clear()
        time.sleep(1)
        self.driver.find_element_by_xpath('//*[@id="single_date"]').send_keys(run_time)
        old_link = self.driver.current_url
        self.driver.execute_script("arguments[0].click();", self.driver.find_element_by_xpath('//input[@value="Run Job"]'))
        print('old_link为',old_link)
        link = self.refresh_driver(old_link)
        return link

    def run_correct_job(self,source):
        WebDriverWait(self.driver, 30).until(
            EC.element_to_be_clickable(
                (By.XPATH, '//*[@id="jobProfileForm"]/table/tbody/tr[4]/td/table/tbody/tr[3]/td/table/tbody/tr')))
        job_table_elements = self.driver.find_elements_by_xpath(
            '//*[@id="jobProfileForm"]/table/tbody/tr[4]/td/table/tbody/tr[3]/td/table/tbody/tr')
        run_elements = self.driver.find_elements_by_xpath("//*[text()='Run']")
        source_index_mapping = {}
        for index in range(len(job_table_elements)):
            # pdb.set_trace()
            if 'DE' in job_table_elements[index].text:
                source_index_mapping['DE'] = int(index/6)
            elif 'US' in job_table_elements[index].text:
                source_index_mapping['US'] = int(index/6)
            elif 'JP' in job_table_elements[index].text:
                source_index_mapping['JP'] = int(index/6)
            elif 'UK' in job_table_elements[index].text:
                source_index_mapping['UK'] = int(index/6)
            elif 'AE' in job_table_elements[index].text:
                source_index_mapping['AE'] = int(index / 6)
        print(source_index_mapping)
        self.driver.execute_script("arguments[0].click();",
                                   run_elements[source_index_mapping[source]])
        return

    def get_sql_content(self,final_sql):
        js = '''
                    var x = document.getElementById("Oracle_profile_sql");
                    ''' + final_sql + '''

                    var editor = CodeMirror.fromTextArea(x, {
                            lineNumbers : true,
                            mode : 'text/x-sql',
                            indentUnit : 4,
                            theme: 'eclipse',
                            lineWrapping : true,
                            styleActiveLine : true,
                            extraKeys : {
                                "Ctrl-Space" : "autocomplete",
                                "Esc" : function(cm) {
                                    if (cm.getOption("fullScreen"))
                                        cm.setOption("fullScreen", false);
                                }
                            },
                            autoCloseBrackets : true,
                            matchBrackets : true,
                            hint : CodeMirror.hint.sql
                        });
                    x.value = editor.getValue();
                    '''
        # print(js)
        # pdb.set_trace()
        return js
    #after用来判断是否运行exportable countries after的query
    def auto_run_sql(self,source,asins_list,etl_url,query_type):
        if query_type == 'refresh blocker status':
            final_sql = self.get_final_sql_blocker_status(source,asins_list)
        elif query_type == 'refresh dimension':
            final_sql = self.get_final_sql(source,asins_list)


        # past_query_results = os.listdir(self.download_destination)
        # # pdb.set_trace()
        # if past_query_results != []:
        #     for dest in past_query_results:
        #         try:
        #             os.remove('/'.join([self.download_destination,dest]))
        #         except:
        #             pass



        link = self.edit_profile(final_sql, etl_url,source)
        print('Query链接为：', link, '请等待结果生成。')
        '''
        try:
            # force all dependencies
            WebDriverWait(etl_obj.driver,80).until(EC.element_to_be_clickable((By.XPATH, '//input[@value="Force All Dependencies"]'))).click()
            time.sleep(1)
            etl_obj.driver.find_element_by_xpath('/html/body/div[17]/div[2]/table/tbody/tr/td[2]/input').send_keys('1')
            etl_obj.driver.find_element_by_xpath('/html/body/div[17]/div[3]/div/button[1]/span').click()

            time.sleep(3)
            etl_obj.driver.refresh()
            time.sleep(3)
            WebDriverWait(etl_obj.driver, 80).until(
                EC.element_to_be_clickable((By.XPATH, '//input[@value="Prioritize"]'))).click()
        except:
            print('工具尝试了prioritize job但失败了（或者已经prioritize过了），请手动加速')
        '''
        prioritize_flag = False
        while not prioritize_flag:
            print('当前query: ',link,' 正在等待Prioritize')
            time.sleep(60)
            self.driver.refresh()
            # pdb.set_trace()
            try_time = 10
            current_time = 0
            while current_time<try_time:
                try:
                    status = WebDriverWait(self.driver, 80).until(
                        EC.presence_of_element_located((By.XPATH, '//*[@id="imp_details"]/li[3]/span')))
                    current_time=try_time
                except:
                    current_time+=1
                    self.driver.refresh()
                    time.sleep(5)

            if status.text == 'Waiting for Resources':
                # pdb.set_trace()
                prioritize_button = WebDriverWait(self.driver, 80).until(
                    EC.element_to_be_clickable((By.XPATH, '//input[@value="Prioritize"]')))
                self.driver.execute_script("arguments[0].click();", prioritize_button)
                prioritize_flag = True
                good_query_status = True
                print('当前query: ', link, ' 已经点击prioritize按钮')
            elif 'Executing' in status.text:
                prioritize_flag = True
                good_query_status = True
                print('当前query: ', link, ' 已经点击prioritize按钮')
            elif 'Deteled'in status.text:
                prioritize_flag = True
                good_query_status = False
                print('当前query: ', link, ' Job deleted')
                # print('Job deleted')
            elif 'Success' in status.text:
                prioritize_flag = True
                good_query_status = True
                print('当前query: ', link, ' 已有结果，等待下载')
                # print('query已有结果，等待下载')
            elif 'Error' in status.text:
                prioritize_flag = True
                good_query_status = False
                # print('Job error')
                print('当前query: ', link, ' Job error')
            elif 'Publishing' in status.text:
                prioritize_flag = True
                good_query_status = True
                # print('Job error')
                print('当前query: ', link, ' Publishing')
        self.driver.quit()
        # self.driver.close()
        return link,good_query_status



    def wait_query_and_get_result(self,query_link,source,wait_ahead=1,need_download_flag=True):
        is_query_finish = False
        while not is_query_finish:
            if wait_ahead ==1:
                time.sleep(600)
                # self.headless_chrome_mode_on()
                self.headless_chrome_mode_off()
                # self.driver = webdriver.Chrome(options=self.chrome_options)

            # self.wait = WebDriverWait(self.driver, 60)  # 等待的最大时间
            # etlhandler = ETLHandler()
            # print(1)
            # pdb.set_trace()
            self.get_url(query_link)
            # print(2)
            time.sleep(2)
            status = WebDriverWait(self.driver, 80).until(
                EC.presence_of_element_located(
                    (By.XPATH, '//*[@id="imp_details"]/li[3]/span')))
            if 'Success' in status.text:
                download_file_type = Select(
                    self.driver.find_element_by_xpath('//*[@id="actionsSection"]/ul[3]/li/form/select[2]'))
                download_file_type.select_by_visible_text('Text')
                download_button = self.driver.find_element_by_xpath(
                    '//*[@id="actionsSection"]/ul[3]/li/form/input[2]')
                self.driver.execute_script("arguments[0].click();", download_button)
                print('query', query_link, ' ended successfully, waiting for next process')
                is_query_finish = True
            elif 'Error' in status.text:
                print('当前Query运行失败: '+query_link+' 原因：job error')
                self.driver.quit()
                sys.exit()
            elif 'Deleted' in status.text:
                print('当前Query运行失败: '+query_link+' 原因：job deleted')
                self.driver.quit()
                sys.exit()
            else:
                print('当前Query: '+query_link+' 目前没有结果')
                self.driver.quit()

        if need_download_flag == True:
            filehandler = FileHandler()
            #等待当前结果下载完成后再关闭浏览器
            seconds = 0
            # pdb.set_trace()
            dl_wait = True
            while dl_wait and seconds < 120:
                time.sleep(1)
                dl_wait = False
                if os.listdir(self.download_destination) ==[]:
                    dl_wait = True
                else:
                    for fname in os.listdir(self.download_destination):
                        if fname.endswith('.crdownload'):
                            dl_wait = True
                            print('当前结果文件未下载完成')

                        else:
                            try:
                                df = pd.read_csv('/'.join([self.download_destination,fname]),error_bad_lines=False,sep='\t')
                                # pdb.set_trace()
                                if filehandler.source_mapping[df.loc[0,'country_code']] == source:
                                    print(df.loc[0,'country_code'],source)
                                    filehandler.change_query_result_names(fname,source)
                                    print('文件下载完成')
                                    dl_wait = False
                                    break
                                else:
                                    dl_wait = True
                            except:
                                dl_wait = True

                seconds += 1

        self.driver.quit()

    def wait_query_and_get_result_after(self,query_link):
        is_query_finish = False
        good_query_status = True
        #20220104发现打开headless chrome后有在等待结果时卡住不动的现象，尝试关闭headless chrome，让浏览器正常打开刷新
        # self.headless_chrome_mode_off()
        # self.headless_chrome_mode_on()
            # self.driver = webdriver.Chrome(options=self.chrome_options)

        # self.wait = WebDriverWait(self.driver, 60)  # 等待的最大时间
        # etlhandler = ETLHandler()
        # print(1)
        # pdb.set_trace()
        self.get_url(query_link)
        # print(2)
        time.sleep(2)
        status = WebDriverWait(self.driver, 80).until(
            EC.presence_of_element_located(
                (By.XPATH, '//*[@id="imp_details"]/li[3]/span')))

        if 'Success' in status.text:
            download_file_type = Select(
                self.driver.find_element_by_xpath('//*[@id="actionsSection"]/ul[3]/li/form/select[2]'))
            download_file_type.select_by_visible_text('Text')
            download_button = self.driver.find_element_by_xpath(
                '//*[@id="actionsSection"]/ul[3]/li/form/input[2]')
            self.driver.execute_script("arguments[0].click();", download_button)
            print('query',query_link,' ended successfully, waiting for next process')
            good_query_status = True
            is_query_finish = True
            self.driver.quit()
        elif 'Error' in status.text:
            print('当前Query运行失败: '+query_link+' 原因：job error')
            self.driver.quit()
            good_query_status = False
            return good_query_status,is_query_finish
        elif 'Deleted' in status.text:
            print('当前Query运行失败: '+query_link+' 原因：job deleted')
            self.driver.quit()
            good_query_status = False
            return good_query_status,is_query_finish
        else:
            print('当前Query: '+query_link+' 目前没有结果')
            self.driver.quit()

        return good_query_status,is_query_finish

    def wait_until_located(self,xpath):
        item = self.wait.until(
            EC.presence_of_element_located((
                By.XPATH, xpath)))
        return item

    def get_url(self, url):
        self.driver.get(url)
        while 'midway' in self.driver.current_url:
            time.sleep(20)

    def refresh_driver(self, old_link):
        try:
            while (('midway' in self.driver.current_url) or (self.driver.current_url == old_link)):
                time.sleep(5)
            self.driver.find_element_by_xpath('//img[@title="Data Warehouse"]')
            link = self.driver.current_url
            return link
        except:
            self.driver.refresh()
            time.sleep(10)
            return self.refresh_driver(old_link)


