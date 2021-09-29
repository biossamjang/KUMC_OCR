import os
import boto3
import json
import base64
import pyodbc
import requests
from Make_Sql import _Make_Sql_Text as SQL
import pydicom as dicom
import cv2
import csv
import copy

class _Main:
    
    _CS_Invoke_URL = 'https://fe3392a9d2e544ef85988121bf9bb4df.apigw.ntruss.com/custom/v1/10493/3a22ef4d24956d59cc29ab7830c590dee924d345708278860fe21f23a0bd2147/infer'
    _CS_secret_key = 'Smt5cEVYbmZwRFdpUEFhbkdDRkFqZVFrYWd5bFdKRW0='
    # 오브젝트 스토리지에 버킷이 추가될때 생성
    _AA_Invoke_URL = ''
    _AA_secret_key = ''

    _service_name = 's3'
    _endpoint_url = 'https://kr.object.ncloudstorage.com'
    _access_key = 'Q0CDf5nfhL6SGfePmhS9'
    _secret_key = 'iV0Vua6dJCWcpYkZ5RyPMNcn1uZuDjT8r64sHGIn'
    _bucket_name = 'obj-calsium-score-storage'

    _DSNNAME = ''
    _DBUSER = ''
    _DBPWD = ''
    # make it True if you want in PNG format
    _PNG = False
    # Specify the .dcm folder path
    _folder_path = "C:\GitHub\KUMC_OCR\dicom"
    # Specify the output jpg/png folder path
    _jpg_folder_path = "C:\GitHub\KUMC_OCR\jpg"
    _txt_folder_path = "C:\\GitHub\\KUMC_OCR\\txt"
    _folder_name = 'CS/'
    _delimiter = '/'
    _prefix = 'CS'
    _max_keys = 3000
    

    def _OCR_Connect(self, prefix ):

        # OCR 탬플릿 추가시 INI파일에 APIGW Invoke URL,Secret Key 정보 추가
        if prefix == 'CS':
            _URL = self._CS_Invoke_URL   # APIGW Invoke URL
            _KEY = self._CS_secret_key   # Secret Key
        elif prefix == 'AA':
            _URL = self._AA_Invoke_URL   # APIGW Invoke URL
            _KEY = self._AA_secret_key   # Secret Key

        # object storage 접속
        s3 = boto3.client(self._service_name, endpoint_url= self._endpoint_url, aws_access_key_id= self._access_key,
                      aws_secret_access_key= self._secret_key)
        
        is_Break = False
        i_Count = 0

        # 버킷에 등록된 파일정보 가져오기
        response = s3.list_objects(Bucket = self._bucket_name, Delimiter = self._delimiter, MaxKeys= self._max_keys , Prefix = prefix + '/')

        i_Count = len(response.get('Contents'))

        Result_Data = []

        while True:
        
            if is_Break == True :
                break

            for content in response.get('Contents'):

                i_Count = i_Count - 1

                if content.get('Size') > 0 :

                    data = s3.get_object(Bucket = self._bucket_name, Key = content.get('Key'))

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
                        
                    Result_Data = []
                    for list in resArray:
                        """ if list.get('name') == 'ID':    
                            print('아이디', list.get('inferText'))
                        print(list.get('name'), ':', list.get('inferText')) """
                        print(list.get('name'), ':', list.get('inferText'))
                        Result_Data = Result_Data + [list.get('inferText')]
                    
                    # vList = list(content.values())
                    # print(vList)
                    
                    self._Make_File(str(content.get('Key')), Result_Data)

                    #print('----------------------------------------') 
                    #print(resArray)
                    # print('========================================')
                    # print(Result_Data)
                    # print('****************************************')

                    # self._DB_Connect(prefix,Result_Data, self._DSNNAME, self._DBUSER, self._DBPWD)
                if i_Count == 0 :
                    is_Break = True
                    break  
    def _Make_File(self, key, Result_Data):
        # f = open("C:\\GitHub\\KUMC_OCR\\new.txt",'wb')    
        _file_name = key.replace( self._folder_name , '').replace('jpg', 'txt')
        _path = "C:\\GitHub\\KUMC_OCR\\" + _file_name
        #_path = self._txt_folder_path + '\\' + _file_name
        with open(_path,'w') as f:
                        # f.write(os.urandom(10000000))
            # for Data in Result_Data:
            #     print(Data) 

            f.write(str(self.ptnm) + '\n')
            f.write(str(self.ptid) + '\n')
            f.write(str(self.BirthDay) + '\n')
            f.write(str(self.sex) + '\n')

            # print(type(self.StudyDescription))
            #print(self.StudyDescription)
            _StudyDescription = self.StudyDescription.encode('utf-8')

            #print(_StudyDescription.decode('utf-8'))

            # print(type(_StudyDescription))

            f.write(str(_StudyDescription)+ '\n')
            f.write(str(self.InstitutionName)+ '\n')
            f.write(str(self.DeviceSerialNumber)+ '\n')
            f.write(str(self.contentDate)+ '\n')
            f.write(str(Result_Data)+ '\n')

            # print(str(self.ptnm), file=f)
            # print(str(self.ptid), file=f)
            # print(str(self.BirthDay), file=f)
            # print(str(self.sex), file=f)
            # print(str(_StudyDescription), file=f)
            # print(str(self.InstitutionName), file=f)
            # print(str(self.DeviceSerialNumber), file=f)
            # print(str(self.contentDate), file=f)
            # print(str(Result_Data), file=f)
            
            f.close()

    def _DB_Connect( self, prefix, Result_Data, DSNNAME, DBUSER, DBPWD):
        
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
                    sql = self._SQL._PFT()
                elif prefix == 'PWV':    
                    sql = self._SQL._PWV()

                #print(sql)
                #print(Result_Data)
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

    def resource_path(relative_path):
        try:
            base_path = os.system._MEIPASS
        except Exception:
            base_path = os.path.abspath(".")

        return os.path.join(base_path, relative_path)     

    # def _Bucket_Control(self):
    #     s3.create_bucket(Bucket= self._bucket_name)

    def _Dicom_Control(self, PNG):

        s3 = boto3.client(self._service_name, endpoint_url= self._endpoint_url, aws_access_key_id= self._access_key,
                          aws_secret_access_key= self._secret_key)

        s3.put_object(Bucket= self._bucket_name, Key= self._folder_name)

        images_path = os.listdir(self._folder_path)

        for n, image in enumerate(images_path):
            # dicom 파일 읽기
            ds = dicom.dcmread(os.path.join(self._folder_path, image), force=True)

            # print(ds)
            # print(dir(ds))
            # print(ds.pixel_array)
            # DICOM_file = ds
            # plt.imshow(DICOM_file.pixel_array, cmap=plt.cm.gray)
            # plt.show()

            # Header 정보 불러오기 ( 확인 작업 필요함 )
            self.ptnm = ds.PatientName
            self.ptid = ds.PatientID
            self.BirthDay = ds.PatientBirthDate
            self.sex = ds.PatientSex
            self.StudyDescription = ds.StudyDescription

            self.InstitutionName = ds.InstitutionName
            self.DeviceSerialNumber = ds.DeviceSerialNumber
            self.contentDate = ds.ContentDate

            # Image Array 불러오기
            pixel_array_numpy = ds.pixel_array

            # 확장자 변경
            if PNG == False:
                image = image.replace('.dcm', '.jpg')
            else:
                image = image.replace('.dcm', '.png')

            # 이미지 저장
            cv2.imwrite(os.path.join(self._jpg_folder_path, image), pixel_array_numpy)

            object_name = self._folder_name + image
            local_file_path = self._jpg_folder_path + '\\' + image

            # 오브젝트 업로드
            s3.upload_file(local_file_path, self._bucket_name, object_name)

    def CSV_Read(self):
        

        with open('C:\\GitHub\\KUMC_OCR\\test.csv', 'r') as f:
            # list
            reader = csv.reader(f , delimiter = ',')

            text = []
            for txt in reader:
                text = text + txt

            print(text)
            # dictionary
            # reader = csv.DictReader(f)

            # for c in reader:
            #     print(type(c))
            #     print(c)
                
            #     for k, v in c.items():
            #         print(k,v)

            
if __name__ == "__main__":
    Calsium_Score = _Main()
    # csv 파일 읽기
    Calsium_Score.CSV_Read()
    # Dicom 파일 처리
    Calsium_Score._Dicom_Control(False)
    # CS 처리
    Calsium_Score._OCR_Connect('CS')

