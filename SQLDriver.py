import pyodbc
driver_name = ''
driver_names = [x for x in pyodbc.drivers() if x.endswith(' for SQL Server')]
if driver_names:
    driver_name = driver_names[0]
if driver_name:
    print(driver_name)
    # then continue with ...
    # pyodbc.connect(conn_str)
    # ... etc.
else:
    print('(No suitable driver found. Cannot connect.)')