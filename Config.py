import configparser

config = configparser.ConfigParser()
config['service'] = {}
config['service']['service_name'] = 's3'
config['service']['endpoint_url'] = 'https://kr.object.ncloudstorage.com'
config['service']['region_name'] = 'kr-standard'
config['service']['access_key'] = 'Q0CDf5nfhL6SGfePmhS9'
config['service']['secret_key'] = 'iV0Vua6dJCWcpYkZ5RyPMNcn1uZuDjT8r64sHGIn'
config['service']['bucket_name'] = 'obj-kumc-cdw-storage'

config['PFT'] = {}
config['PFT']['Invoke_URL'] = 'https://1846704f86f048c88eef33c8c6f25bf9.apigw.ntruss.com/custom/v1/9283/31ef3d7299496acfc1851b8e73dc7b1a29ff5ada752259ba49a64ce0cb393af1/infer'
config['PFT']['secret_key'] = 'RFRESE5ycld5UnJaa3NnQm5TVnNCdXB0SFZib1ZSTFU='

config['PWV'] = {}
config['PWV']['Invoke_URL'] = 'https://1846704f86f048c88eef33c8c6f25bf9.apigw.ntruss.com/custom/v1/9284/41486255f694d57f2071bb9626a279a53c21ddc60fae1fdc26ceb3cfc8efb79e/infer'
config['PWV']['secret_key'] = 'VFZmWUR1VkNtbmx6Q2N0aHN3QVVhRHlTcElvclBIaUY='

config['DataBase'] = {}
config['DataBase']['DSNNAME'] = 'KUMC_CDW'
config['DataBase']['DBUSER'] = 'sys'
config['DataBase']['DBPWD'] = 'admin06##'

with open('config.ini','w', encoding='utf-8') as configfile:
    config.write(configfile)