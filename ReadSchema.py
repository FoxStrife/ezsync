from PropertyReader import *

class ReadSchema:

    resposta = PropertyReader().readProperty('ClientSection','CNPJ')

    print ('Resposta = '+ resposta)