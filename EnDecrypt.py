from cryptography.fernet import Fernet 

class _EnDecrypt:
    def __init__(self, key=None):
        if key is None:
            key = Fernet.generate_key()

        self.key = key
        self.fer = Fernet(self.key)

    # 암호화 처리
    def encrypt(self, data, _Bool = True):
        if isinstance(data, bytes):
            out = self.fer.encrypt(data)
        else:
            out = self.fer.encrypt(data.encode('utf-8'))    

        if _Bool is True:
            return out.decode('utf-8')    
        else:
            return out    

    # 복호화 처리
    def decrypt(self, data, _Bool = True):
        if isinstance(data, bytes):
            out = self.fer.decrypt(data)
        else:
            out = self.fer.decrypt(data.encode('utf-8'))    

        if _Bool is True:
            return out.decode('utf-8')    
        else:
            return out   

