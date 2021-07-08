import configparser
import boto3
import json
import base64
import pyodbc
import requests
import time
import datetime
import os
from EnDecrypt import _EnDecrypt as crypt

class OCR_Main:
    def __init__(self) -> None:
        self._crypt = crypt()

    def _OCR_Connect(self, prefix ):
        # ini 파일 정보 가져오기
        self._config_parser()

        test_text = 'I_LOVE_YOU'
        # 암호화
        encrypt_text = self._crypt.encrypt(test_text)
        #print(encrypt_text)
        # 복호화
        decrypt_text = self._crypt.decrypt(encrypt_text)
        #print(decrypt_text)

        # OCR 탬플릿 추가시 INI파일에 APIGW Invoke URL,Secret Key 정보 추가
        if prefix == 'PFT':
            _URL = self.PFT_Invoke_URL   # APIGW Invoke URL
            _KEY = self.PFT_secret_key   # Secret Key
        elif prefix == 'PWV':
            _URL = self.PWV_Invoke_URL   # APIGW Invoke URL
            _KEY = self.PWV_secret_key   # Secret Key    

        # 
        s3 = boto3.client(self.service_name, endpoint_url= self.endpoint_url, aws_access_key_id= self.access_key,
                      aws_secret_access_key= self.secret_key)

        is_Break = False
        i_Count = 0

        response = s3.list_objects(Bucket = self.bucket_name, Delimiter = self.delimiter, MaxKeys= self.max_keys , Prefix = prefix + '/')

        i_Count = len(response.get('Contents'))

        Result_Data = []

        while True:
        
            if is_Break == True :
                break

            for content in response.get('Contents'):

                i_Count = i_Count - 1

                if content.get('Size') > 0 :

                    data = s3.get_object(Bucket = self.bucket_name, Key = content.get('Key')) 

                    img = base64.b64encode(data['Body'].read())     

                    header = {"Content-Type" : "application/json", "X-OCR-SECRET" : _KEY}

                    data = {"version": "V1",
                            "requestId": "sample_id",
                            "timestamp": 0,
                            "images": [
                                        {"name": "samle_image",
                                        "format": "jpg",
                                        "data": img.decode("utf-8")
                                        }
                                        ]
                            }

                    data = json.dumps(data)
                    response = requests.post(_URL, data=data, headers=header)
                    res = json.loads(response.text)

                    for e in res['images']:
                        resArray = e.get('fields')
                    Result_Data = []
                    for list in resArray:
                        """ if list.get('name') == 'ID':    
                            print('아이디', list.get('inferText'))
                        print(list.get('name'), ':', list.get('inferText')) """
                        Result_Data = Result_Data + [list.get('inferText')]
                    #print('----------------------------------------') 
                    #print(resArray)
                    # print('========================================')
                    # print(Result_Data)
                    # print('****************************************')
                    self._DB_Connect(prefix,Result_Data)    
                if i_Count == 0 :
                    is_Break = True
                    break  

    def _DB_Connect( self, prefix, Result_Data):

        cnxn = pyodbc.connect('DSN='+self.DSNNAME+';UID='+self.DBUSER+';PWD='+self.DBPWD)

        try:
            # 테스트용 삭제쿼리    
            with cnxn.cursor() as curs:
                sql = "DELETE FROM ANAM_CDW.CL_PFT;"    
                curs.execute(sql)
            
            # OCR 탬플릿 추가시 쿼리문 작성 함수도 생성해야함.
            with cnxn.cursor() as curs:
                
                if prefix == 'PFT':
                    sql = self._pft_sql_text()
                elif prefix == 'PWV':    
                    sql = self._pwv_sql_text()

                #print(sql)
                #print(Result_Data)
                curs.execute(sql, Result_Data)
                #curs.executemany(sql, Result_Data)

            cnxn.commit()

            with cnxn.cursor() as curs:
                sql = "SELECT * FROM ANAM_CDW.CL_PFT;"    
                curs.execute(sql)
                rs = curs.fetchall()
                for row in rs:
                    print(row)

        except Exception:
            cnxn.commit()
            print("error 발생")                    
        finally:
            cnxn.close() 

    def _config_parser(self):
        config = configparser.ConfigParser()
        config.read('config.ini', encoding='utf-8')
        config.sections()

        self.service_name = config['service']['service_name']
        self.endpoint_url = config['service']['endpoint_url']
        self.region_name = config['service']['region_name']
        self.access_key = config['service']['access_key']
        self.secret_key = config['service']['secret_key']
        self.bucket_name = config['service']['bucket_name']
        
        self.PFT_Invoke_URL = config['PFT']['Invoke_URL']
        self.PFT_secret_key = config['PFT']['secret_key']
        self.PWV_Invoke_URL = config['PWV']['Invoke_URL']
        self.PWV_secret_key = config['PWV']['secret_key']

        self.DSNNAME = config['DataBase']['DSNNAME']
        self.DBUSER = config['DataBase']['DBUSER']
        self.DBPWD = config['DataBase']['DBPWD']
        self.delimiter = '/'
        self.max_keys = 3000   

    

    def resource_path(relative_path):
        try:
            base_path = os.system._MEIPASS
        except Exception:
            base_path = os.path.abspath(".")

        return os.path.join(base_path, relative_path)     

    # PFT 쿼리문 생성            
    def _pft_sql_text(self):
        SQL =       "INSERT INTO ANAM_CDW.CL_PFT( ID"
        SQL = SQL + ",SDATE                         "
        SQL = SQL + ",HEIGHT                        "
        SQL = SQL + ",WEIGHT                        "
        SQL = SQL + ",PYHSICIAN                     "
        SQL = SQL + ",FVC_PRED                      "
        SQL = SQL + ",FEV1_PRED                     "
        SQL = SQL + ",FEV1_FVC_PRED                 "
        SQL = SQL + ",FEF2775_PRED                  "
        SQL = SQL + ",ISO_FEF2775_PRED              "
        SQL = SQL + ",FEF7585_PRED                  "
        SQL = SQL + ",PEF_PRED                      "
        SQL = SQL + ",FET100_PRED                   "
        SQL = SQL + ",FIVC_PRED                     "
        SQL = SQL + ",FIV1_PRED                     "
        SQL = SQL + ",FEF_FIF50_PRED                "
        SQL = SQL + ",VOL_EXTRAP_PRED               "
        SQL = SQL + ",FVL_ECODE_PRED                "
        SQL = SQL + ",FVC_PRE                       "
        SQL = SQL + ",FEV1_PRE                      "
        SQL = SQL + ",FEV1_FVC_PRE                  "
        SQL = SQL + ",FEF2575_PRE                   "
        SQL = SQL + ",ISO_FEF2575_PRE               "
        SQL = SQL + ",FEF7585_PRE                   "
        SQL = SQL + ",PEF_PRE                       "
        SQL = SQL + ",FET100_PRE                    "
        SQL = SQL + ",FIVC_PRE                      "
        SQL = SQL + ",FIV1_PRE                      "
        SQL = SQL + ",FEF_FIF50_PRE                 "
        SQL = SQL + ",VOL_EXTRAP_PRE                "
        SQL = SQL + ",FVL_ECODE_PRE                 "
        SQL = SQL + ",FVC_POST                      "
        SQL = SQL + ",FEV1_POST                     "
        SQL = SQL + ",FEV1_FVC_POST                 "
        SQL = SQL + ",FEF2575_POST                  "
        SQL = SQL + ",ISO_FEF2575_POST              "
        SQL = SQL + ",FEF7585_POST                  "
        SQL = SQL + ",PEF_POST                      "
        SQL = SQL + ",FET100_POST                   "
        SQL = SQL + ",FIVC_POST                     "
        SQL = SQL + ",FIV1_POST                     "
        SQL = SQL + ",FEF_FIF50_POST                "
        SQL = SQL + ",VOL_EXTRAP_POST               "
        SQL = SQL + ",FVL_ECODE_POST                "
        SQL = SQL + ") VALUES (                     "
        SQL = SQL + ":ID                            "
        SQL = SQL + ",:SDATE                        "
        SQL = SQL + ",:HEIGHT                       "
        SQL = SQL + ",:WEIGHT                       "
        SQL = SQL + ",:PYHSICIAN                    "
        SQL = SQL + ",:FVC_PRED                     "
        SQL = SQL + ",:FEV1_PRED                    "
        SQL = SQL + ",:FEV1_FVC_PRED                "
        SQL = SQL + ",:FEF2775_PRED                 "
        SQL = SQL + ",:ISO_FEF2775_PRED             "
        SQL = SQL + ",:FEF7585_PRED                 "
        SQL = SQL + ",:PEF_PRED                     "
        SQL = SQL + ",:FET100_PRED                  "
        SQL = SQL + ",:FIVC_PRED                    "
        SQL = SQL + ",:FIV1_PRED                    "
        SQL = SQL + ",:FEF_FIF50_PRED               "
        SQL = SQL + ",:VOL_EXTRAP_PRED              "
        SQL = SQL + ",:FVL_ECODE_PRED               "
        SQL = SQL + ",:FVC_PRE                      "
        SQL = SQL + ",:FEV1_PRE                     "
        SQL = SQL + ",:FEV1_FVC_PRE                 "
        SQL = SQL + ",:FEF2575_PRE                  "
        SQL = SQL + ",:ISO_FEF2575_PRE              "
        SQL = SQL + ",:FEF7585_PRE                  "
        SQL = SQL + ",:PEF_PRE                      "
        SQL = SQL + ",:FET100_PRE                   "
        SQL = SQL + ",:FIVC_PRE                     "
        SQL = SQL + ",:FIV1_PRE                     "
        SQL = SQL + ",:FEF_FIF50_PRE                "
        SQL = SQL + ",:VOL_EXTRAP_PRE               "
        SQL = SQL + ",:FVL_ECODE_PRE                "
        SQL = SQL + ",:FVC_POST                     "
        SQL = SQL + ",:FEV1_POST                    "
        SQL = SQL + ",:FEV1_FVC_POST                "
        SQL = SQL + ",:FEF2575_POST                 "
        SQL = SQL + ",:ISO_FEF2575_POST             "
        SQL = SQL + ",:FEF7585_POST                 "
        SQL = SQL + ",:PEF_POST                     "
        SQL = SQL + ",:FET100_POST                  "
        SQL = SQL + ",:FIVC_POST                    "
        SQL = SQL + ",:FIV1_POST                    "
        SQL = SQL + ",:FEF_FIF50_POST               "
        SQL = SQL + ",:VOL_EXTRAP_POST              "
        SQL = SQL + ",:FVL_ECODE_POST               "
        SQL = SQL + ")                              "
        return SQL

    # PWV 쿼리문 생성
    def _pwv_sql_text(self):
        SQL =       "INSERT INTO ANAM_CDW.CL_PFT( ID"
        SQL = SQL + ",SDATE                         "
        SQL = SQL + ",HEIGHT                        "
        SQL = SQL + ",WEIGHT                        "
        SQL = SQL + ",PYHSICIAN                     "
        SQL = SQL + ",FVC_PRED                      "
        SQL = SQL + ",FEV1_PRED                     "
        SQL = SQL + ",FEV1_FVC_PRED                 "
        SQL = SQL + ",FEF2775_PRED                  "
        SQL = SQL + ",ISO_FEF2775_PRED              "
        SQL = SQL + ",FEF7585_PRED                  "
        SQL = SQL + ",PEF_PRED                      "
        SQL = SQL + ",FET100_PRED                   "
        SQL = SQL + ",FIVC_PRED                     "
        SQL = SQL + ",FIV1_PRED                     "
        SQL = SQL + ",FEF_FIF50_PRED                "
        SQL = SQL + ",VOL_EXTRAP_PRED               "
        SQL = SQL + ",FVL_ECODE_PRED                "
        SQL = SQL + ",FVC_PRE                       "
        SQL = SQL + ",FEV1_PRE                      "
        SQL = SQL + ",FEV1_FVC_PRE                  "
        SQL = SQL + ",FEF2575_PRE                   "
        SQL = SQL + ",ISO_FEF2575_PRE               "
        SQL = SQL + ",FEF7585_PRE                   "
        SQL = SQL + ",PEF_PRE                       "
        SQL = SQL + ",FET100_PRE                    "
        SQL = SQL + ",FIVC_PRE                      "
        SQL = SQL + ",FIV1_PRE                      "
        SQL = SQL + ",FEF_FIF50_PRE                 "
        SQL = SQL + ",VOL_EXTRAP_PRE                "
        SQL = SQL + ",FVL_ECODE_PRE                 "
        SQL = SQL + ",FVC_POST                      "
        SQL = SQL + ",FEV1_POST                     "
        SQL = SQL + ",FEV1_FVC_POST                 "
        SQL = SQL + ",FEF2575_POST                  "
        SQL = SQL + ",ISO_FEF2575_POST              "
        SQL = SQL + ",FEF7585_POST                  "
        SQL = SQL + ",PEF_POST                      "
        SQL = SQL + ",FET100_POST                   "
        SQL = SQL + ",FIVC_POST                     "
        SQL = SQL + ",FIV1_POST                     "
        SQL = SQL + ",FEF_FIF50_POST                "
        SQL = SQL + ",VOL_EXTRAP_POST               "
        SQL = SQL + ",FVL_ECODE_POST                "
        SQL = SQL + ") VALUES (                     "
        SQL = SQL + ":ID                            "
        SQL = SQL + ",:SDATE                        "
        SQL = SQL + ",:HEIGHT                       "
        SQL = SQL + ",:WEIGHT                       "
        SQL = SQL + ",:PYHSICIAN                    "
        SQL = SQL + ",:FVC_PRED                     "
        SQL = SQL + ",:FEV1_PRED                    "
        SQL = SQL + ",:FEV1_FVC_PRED                "
        SQL = SQL + ",:FEF2775_PRED                 "
        SQL = SQL + ",:ISO_FEF2775_PRED             "
        SQL = SQL + ",:FEF7585_PRED                 "
        SQL = SQL + ",:PEF_PRED                     "
        SQL = SQL + ",:FET100_PRED                  "
        SQL = SQL + ",:FIVC_PRED                    "
        SQL = SQL + ",:FIV1_PRED                    "
        SQL = SQL + ",:FEF_FIF50_PRED               "
        SQL = SQL + ",:VOL_EXTRAP_PRED              "
        SQL = SQL + ",:FVL_ECODE_PRED               "
        SQL = SQL + ",:FVC_PRE                      "
        SQL = SQL + ",:FEV1_PRE                     "
        SQL = SQL + ",:FEV1_FVC_PRE                 "
        SQL = SQL + ",:FEF2575_PRE                  "
        SQL = SQL + ",:ISO_FEF2575_PRE              "
        SQL = SQL + ",:FEF7585_PRE                  "
        SQL = SQL + ",:PEF_PRE                      "
        SQL = SQL + ",:FET100_PRE                   "
        SQL = SQL + ",:FIVC_PRE                     "
        SQL = SQL + ",:FIV1_PRE                     "
        SQL = SQL + ",:FEF_FIF50_PRE                "
        SQL = SQL + ",:VOL_EXTRAP_PRE               "
        SQL = SQL + ",:FVL_ECODE_PRE                "
        SQL = SQL + ",:FVC_POST                     "
        SQL = SQL + ",:FEV1_POST                    "
        SQL = SQL + ",:FEV1_FVC_POST                "
        SQL = SQL + ",:FEF2575_POST                 "
        SQL = SQL + ",:ISO_FEF2575_POST             "
        SQL = SQL + ",:FEF7585_POST                 "
        SQL = SQL + ",:PEF_POST                     "
        SQL = SQL + ",:FET100_POST                  "
        SQL = SQL + ",:FIVC_POST                    "
        SQL = SQL + ",:FIV1_POST                    "
        SQL = SQL + ",:FEF_FIF50_POST               "
        SQL = SQL + ",:VOL_EXTRAP_POST              "
        SQL = SQL + ",:FVL_ECODE_POST               "
        SQL = SQL + ")                              "
        return SQL

if __name__ == "__main__":
    
    OCR_Main = OCR_Main()
    # PFT 처리
    OCR_Main._OCR_Connect('PFT')
    # PWV 처리        
    #OCR_Main._OCR_Connect('PWV')
else:
    OCR_Main = OCR_Main()
    # PFT 처리
    OCR_Main._OCR_Connect('PFT')        
