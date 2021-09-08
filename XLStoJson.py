import pandas
import json 

csvFilePath = r'teste empregados.csv'
jsonFilePath = r'data.json'

excel_data_df = pandas.read_excel('teste empregados.xlsx', sheet_name='teste empregados')

json_str = excel_data_df.to_json()

with open(jsonFilePath, 'w', encoding='utf-8-sig') as jsonf: 
        json_str = excel_data_df.to_json(orient='records')
        jsonf.write(json_str)

print('Excel Sheet to JSON:\n', json_str)

