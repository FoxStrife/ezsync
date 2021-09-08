import fdb
import pyodbc
from PropertyReader import *

def conectarFB(self,caminho,usuario,senha,query):
    try:
        con = fdb.connect(dsn=caminho, user=usuario, password=senha)
        print("Firebird: Conexão aberta.")
        try:
            cur = con.cursor()
            result = cur.execute(query)
            print("Query executada com sucesso.")
            return (result.fetchall())
        except:
            print("Falha na Consulta.")
            raise
        finally:
            con.close()
            print("Firebird: Conexão fechada.")
    except Exception as e:
        print('Alguma coisa deu errado:', e)

def conectarSQL(caminho,base,usuario,senha,query):
    try:
        driver_name = ''
        driver_names = [x for x in pyodbc.drivers() if x.endswith(' for SQL Server')]
        if driver_names:
            driver_name = driver_names[0]
        consql = pyodbc.connect('DRIVER={' + driver_name + '};SERVER='+caminho+';DATABASE='+base+';UID='+usuario+';PWD='+ senha)
        print("SQLServer: Conexão aberta.")
        try:
            cursql = consql.cursor()
            result = cursql.execute(query)
            return (result.fetchall())
        except:
            print("Falha na Consulta.")
            raise
        finally:
            cursql.close()
            print("SQLServer: Conexão fechada.")
    except Exception as e:
        print('Alguma coisa deu errado:', e)