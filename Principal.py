from PropertyReader import *
from Conexao import *
import json

class Principal:

    cnpj = PropertyReader().readProperty('ClientSection','CNPJ')
    sistema = PropertyReader().readProperty('ClientSection','SISTEMA')
    integracao = PropertyReader().readProperty('IntegrationSection','INTEGRACAO')
    user_integracao = PropertyReader().readProperty('IntegrationSection','USER')
    pwd_integracao = PropertyReader().readProperty('IntegrationSection','PWD')
    token_integracao = PropertyReader().readProperty('IntegrationSection','TOKEN')
    banco = PropertyReader().readProperty('DatabaseSection','BANCO')
    caminho = PropertyReader().readProperty('DatabaseSection','CAMINHO')
    usuario = PropertyReader().readProperty('DatabaseSection','USER')
    senha = PropertyReader().readProperty('DatabaseSection','PWD')
    caminhoSQL = '172.18.1.99\SQLSERVER'
    #caminhoSQL = '187.95.114.73\SQLSERVER'
    usuarioSQL = 'sa'
    senhaSQL = 'MngJq8gpg0'
    base = 'DW2TECH'
    filars = pkdwsync = 0
    fila = ""

# conecta no tipo de banco que o cliente possui e extrai informacoes sobre a fila que será integrada
    if banco == '3':
        #se o banco for Firebird
        result= selectFB('',caminho,usuario,senha,"SELECT FIRST 1 DISTINCT A.IDFILA, (SELECT FIRST 1 ID FROM RHPROTINTEGRATERC WHERE SYNC = 0 AND IDFILA = A.IDFILA), F.DESCRICAO FROM RHPROTINTEGRATERC A LEFT JOIN RHSISTEMAINTEGRATERCFILA F ON F.IDSISTEMA = A.IDSISTEMA WHERE  SYNC = 0")
        print(result)
        for r in result:
            filars = str(r[0])
            fila = r[2]
    elif banco == '2':
        #se o banco for SQL Server
        print()
    else:
        print ('Banco de dados não suportado.')

    sql = selectSQL(caminhoSQL,base,usuarioSQL,senhaSQL,"SELECT URL, URLPARAM FROM DW_FILAS WHERE ID_DW_FILAS = " + filars + "")
    for s in sql:
        url = str(s[0])
        urlparam = str(s[1])

        print(url)
        print(urlparam)

    sql = selectSQL(caminhoSQL,base,usuarioSQL,senhaSQL,"SELECT CAST(SQL AS VARCHAR(MAX)) AS SQL FROM DW_SQLS WHERE ID_FILA = "
    + str(filars) + " AND ID_SISTEMA_ORIGEM = 4 AND ID_SISTEMA_DESTINO = " + sistema + " AND ID_BANCO_DE_DADOS = " + str(banco) + "")
    for s in sql:
        sql1 = str(s[0])

    condicao = selectSQL(caminhoSQL,base,usuarioSQL,senhaSQL,"SELECT CAST(CONDICAO AS VARCHAR(MAX)) AS CONDICAO FROM DW_SQLS WHERE ID_FILA = "
    + str(filars) + " AND ID_SISTEMA_ORIGEM = 4 AND ID_SISTEMA_DESTINO = " + sistema + " AND ID_BANCO_DE_DADOS = " + str(banco) + "")
    for s in condicao:
        condicao1 = str(s[0])

    print (sql1)
    print (condicao1)

    if banco == '3':
        #se o banco for Firebird
        result2= selectFB('',caminho,usuario,senha,condicao1 + filars)
        for r in result2:
            result3 = str(r[0])
        print (result3)
        result4 = selectFBJson('',caminho,usuario,senha,sql1 + result3)
        for r in result4:
            result5 = json.dumps(result4, indent=4)
        print (result5)        
    elif banco == '2':
        #se o banco for SQL Server
        print()


    #variaveis de homologacao
    token_integracao = '523bc781-6eca-11ea-b13e-d531c192bf1e'
    url = 'https://social.flash.nela.com.br'

    result6 = executeSQL(caminhoSQL,base,usuarioSQL,senhaSQL,'EXEC DW_HUB ',filars,user_integracao,pwd_integracao,token_integracao,sistema,integracao,result5,url,urlparam)

    #print ("ID =",filars)
    #print ("LIST =",pkdwsync)
    #print ("DESCRICAO =",fila)