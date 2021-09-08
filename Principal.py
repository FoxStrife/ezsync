from PropertyReader import *
from Conexao import *
import json

class Principal:

    cnpj = PropertyReader().readProperty('ClientSection','CNPJ')
    banco = PropertyReader().readProperty('DatabaseSection','BANCO')
    caminho = PropertyReader().readProperty('DatabaseSection','CAMINHO')
    usuario = PropertyReader().readProperty('DatabaseSection','USER')
    senha = PropertyReader().readProperty('DatabaseSection','PWD')
    caminhoSQL = '172.18.1.99\SQLSERVER'
    usuarioSQL = 'sa'
    senhaSQL = 'MngJq8gpg0'
    base = 'DW2TECH'
    filars = pkdwsync = 0
    fila = ""

# conecta no tipo de banco que o cliente possui e extrai informacoes sobre a fila que será integrada
    if banco == 'firebird':
        #se o banco for Firebird
        print ('Banco = '+ banco)
        result= conectarFB('',caminho,usuario,senha,"SELECT FIRST 1 DISTINCT A.IDFILA, (SELECT FIRST 1 ID FROM RHPROTINTEGRATERC WHERE SYNC = 0 AND IDFILA = A.IDFILA), F.DESCRICAO FROM RHPROTINTEGRATERC A LEFT JOIN RHSISTEMAINTEGRATERCFILA F ON F.IDSISTEMA = A.IDSISTEMA WHERE  SYNC = 0")
        print(result)
        for r in result:
            filars = str(r[0])
            #pkdwsync = str(r[0])
            fila = r[2]
    elif banco == 'sqlserver':
        #se o banco for SQL Server
        print ('Banco = '+ banco)
    else:
        print ('Banco de dados não suportado.')

    sql = conectarSQL(caminhoSQL,base,usuarioSQL,senhaSQL,"SELECT CAST(SQL AS VARCHAR(MAX)) AS SQL FROM DW_SQLS WHERE ID_FILA = "
    + str(filars) + " AND ID_SISTEMA_ORIGEM = 4 AND ID_SISTEMA_DESTINO = 1 AND ID_BANCO_DE_DADOS = " + str(3) + "")
    for s in sql:
        sql1 = str(s[0])

    condicao = conectarSQL(caminhoSQL,base,usuarioSQL,senhaSQL,"SELECT CAST(CONDICAO AS VARCHAR(MAX)) AS CONDICAO FROM DW_SQLS WHERE ID_FILA = "
    + str(filars) + " AND ID_SISTEMA_ORIGEM = 4 AND ID_SISTEMA_DESTINO = 1 AND ID_BANCO_DE_DADOS = " + str(3) + "")
    for s in condicao:
        condicao1 = str(s[0])

    print (sql1)
    print ('aaa '+ condicao1)

    if banco == 'firebird':
        #se o banco for Firebird
        print ('Banco = '+ banco)
        result2= conectarFB('',caminho,usuario,senha,condicao1 + filars)
        for r in result2:
            result3 = str(r[0])
        print (result3)

        result4 = conectarFB('',caminho,usuario,senha,sql1 + result3)
        for r in result4:
            result5 = json.dumps(result4)
        print (result5)
        
        
    elif banco == 'sqlserver':
        #se o banco for SQL Server
        print ('Banco = '+ banco)

    #print ("ID =",filars)
    #print ("LIST =",pkdwsync)
    #print ("DESCRICAO =",fila)