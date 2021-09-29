import os
import configparser
import boto3
import json
import base64
import pyodbc
import requests
import time
import datetime
from EnDecrypt import _EnDecrypt as crypt
from Make_Sql import _Make_Sql_Text as SQL
from cryptography.fernet import Fernet as frt

class OCR_Main:
    def __init__(self) -> None:
        self._SQL   = SQL()

    def _OCR_Connect(self, prefix ):

        # ini 파일 정보 가져오기
        self._config_parser()

        # encoding        
        _encode_key = bytes(self.key[1:], encoding='utf-8')
        # decoding
        _decode_key = _encode_key.decode('utf-8')
        # crypt key
        _crypt = crypt(bytes(_decode_key, encoding='utf-8'))

        # 복호화
        _PFT_Invoke_URL = _crypt.decrypt(self.PFT_Invoke_URL)
        _PFT_secret_key = _crypt.decrypt(self.PFT_secret_key)
        _PWV_Invoke_URL = _crypt.decrypt(self.PWV_Invoke_URL)
        _PWV_secret_key = _crypt.decrypt(self.PWV_secret_key)
        _service_name = _crypt.decrypt(self.service_name)
        _endpoint_url = _crypt.decrypt(self.endpoint_url)
        _access_key = _crypt.decrypt(self.access_key)
        _secret_key = _crypt.decrypt(self.secret_key)
        _bucket_name = _crypt.decrypt(self.bucket_name)
        _DSNNAME = _crypt.decrypt(self.DSNNAME)
        _DBUSER = _crypt.decrypt(self.DBUSER)
        _DBPWD = _crypt.decrypt(self.DBPWD)

        # OCR 탬플릿 추가시 INI파일에 APIGW Invoke URL,Secret Key 정보 추가
        if prefix == 'PFT':
            _URL = _PFT_Invoke_URL   # APIGW Invoke URL
            _KEY = _PFT_secret_key   # Secret Key
        elif prefix == 'PWV':
            _URL = _PWV_Invoke_URL   # APIGW Invoke URL
            _KEY = _PWV_secret_key   # Secret Key    

        # object storage 접속
        s3 = boto3.client(_service_name, endpoint_url= _endpoint_url, aws_access_key_id= _access_key,
                      aws_secret_access_key= _secret_key)
        
        is_Break = False
        i_Count = 0

        # 버킷에 등록된 파일정보 가져오기
        response = s3.list_objects(Bucket = _bucket_name, Delimiter = self.delimiter, MaxKeys= self.max_keys , Prefix = prefix + '/')

        i_Count = len(response.get('Contents'))

        Result_Data = []
        Make_SQL_A = []
        Make_SQL_B = []

        while True:
        
            if is_Break == True :
                break

            for content in response.get('Contents'):

                i_Count = i_Count - 1

                if content.get('Size') > 0 :

                    data = s3.get_object(Bucket = _bucket_name, Key = content.get('Key')) 

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
                    # json 데이터 읽기
                    data = json.dumps(data)

                    # cloud ocr 호출
                    response = requests.post(_URL, data=data, headers=header)

                    # json 데이터 쓰기
                    res = json.loads(response.text)

                    for e in res['images']:
                        resArray = e.get('fields')
                        
                    Make_SQL_A = []
                    Make_SQL_B = []
                    Result_Data = []

                    for list in resArray:
                        """ if list.get('name') == 'ID':    
                            print('아이디', list.get('inferText'))
                        print(list.get('name'), ':', list.get('inferText')) """
                        print(list.get('name'), ':', list.get('inferText'))

                        if list.get('name') == 'Date':
                            Make_SQL_A = Make_SQL_A + ['s'+ list.get('name')]    
                            Make_SQL_B = Make_SQL_B + [':s'+ list.get('name')]    
                        else :
                            Make_SQL_A = Make_SQL_A + [list.get('name')]
                            Make_SQL_B = Make_SQL_B + [':' + list.get('name')]
                            
                        Result_Data = Result_Data + [list.get('inferText')]
                    #print('----------------------------------------') 
                    #print(resArray)
                    # print('========================================')
                    # print(Result_Data)
                    # print('****************************************')

                    self._DB_Connect(prefix,Result_Data, Make_SQL_A, Make_SQL_B, _DSNNAME, _DBUSER, _DBPWD)    
                if i_Count == 0 :
                    is_Break = True
                    break  

    def _DB_Connect( self, prefix, Result_Data, Make_SQL_A, Make_SQL_B, DSNNAME, DBUSER, DBPWD):
        
        # DB 접속 (접속정보 변경시 Config.py 재실행하여 생성된 config.ini 파일을 배포해야됨)
        cnxn = pyodbc.connect('DSN='+DSNNAME+';UID='+DBUSER+';PWD='+DBPWD)

        try:
            # 테스트용 삭제쿼리    
            # with cnxn.cursor() as curs:
            #     sql = "DELETE FROM ANAM_CDW.CL_PFT;"    
            #     curs.execute(sql)
            
            # OCR 탬플릿 추가시 쿼리문 작성 함수도 생성해야함.
            with cnxn.cursor() as curs:
                
                # 서식지별로 쿼리문 생성하여 가져오게 함.
                if prefix == 'PFT':
                    sql = self._SQL._PFT(Make_SQL_A, Make_SQL_B)
                elif prefix == 'PWV':    
                    sql = self._SQL._PWV(Make_SQL_A, Make_SQL_B)

                print(sql)
                print(Result_Data)
                # sql 실행
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

    # config.ini 파일 읽기
    def _config_parser(self):
        
        config = configparser.ConfigParser()
        config.read('config.ini', encoding='utf-8')
        config.sections()
        
        self.key = config['service']['key']
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


if __name__ == "__main__":
    OCR_Main = OCR_Main()
    # PFT 처리
    OCR_Main._OCR_Connect('PFT')
    # PWV 처리        
    OCR_Main._OCR_Connect('PWV')
