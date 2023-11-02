import requests
import pandas as pd
import os
from datetime import datetime
from rocketry import Rocketry

from bs4 import BeautifulSoup

app = Rocketry(execution="main")

#função de execução a cada 1 hora
@app.task('every 1m')
def a_cada_1_hora():
    #determina a hora de execução da tarefa
    dt = datetime.now()

    #funções que determinam dia, mês, ano e horário do dia 
    dh = dt.hour
    dy = str(dt.year)
    dm = str(dt.month)
    dd = str(dt.day)    

    #login, senha e url do odata
    MyUser = '*****'
    MyPassword = '*****'
    SERVICE_URL = "*****"+dy+dm+dd+"'&format=json"
    response = requests.get(SERVICE_URL,auth = (MyUser, MyPassword), headers = {"Prefer": "odata.track-changes"})
    
    if dh >= 8 and dh < 20:
        
        soup = BeautifulSoup(response.text,'xml')
        exemplo1 = soup.find_all('exemplo1')
        exemplo2 = soup.find_all('exemplo2')
        exemplo3 = soup.find_all('exemplo3')
        exemplo4 = soup.find_all('exemplo4')
        exemplo5 = soup.find_all('exemplo5')
        data = []
        for i in range(0, len(exemplo1)):
            rows = [
               exemplo1[i].get_text(),
               exemplo2[i].get_text(),
               exemplo3[i].get_text(),
               exemplo4[i].get_text(),
               exemplo5[i].get_text(),
            ]
            data.append(rows)
        df = pd.DataFrame(data, columns=[
            'exemplo1',
            'exemplo2',
            'exemplo3',
            'exemplo4',
            'exemplo5',
        ], dtype = str)
        destino = r"C:\\Users\\joaop\\Downloads\\projeto_python"
        os.chdir(destino)
        df.to_parquet('NOME_DO_ARQUIVO'+'_'+dy+'_'+dm+'_'+dd+'.parquet', compression='gzip')
        print(df)
    else:
        print("Não roda horário")
        print(SERVICE_URL)

app.run()
