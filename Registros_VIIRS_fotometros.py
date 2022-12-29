#Este scripts unifica los pasos, los registros satelitales y los registros de fotometros en un unico data. 

#Ejemplo ejecución
#python Registros_VIIRS_posicion.py --satelite_corrected_records /home/borjafernanruiz/datos_borja/VIIRS/Diarios_corregidos/Registros_satelite.csv --satelite_records /home/borjafernanruiz/datos_borja/VIIRS/Diarios/Registros_satelite.csv --photometers_records /home/borjafernanruiz/datos_borja/Datos_fotometros --out /home/borjafernanruiz/datos_borja/Corregidos
#Paquetes
import numpy as np
import pandas as pd
from datetime import datetime, timezone
import datetime
import pytz

import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument('--satelite_records',  required=True,  type=str, help='Satelite records filename')
###
parser.add_argument('--satelite_corrected_records',  required=True,  type=str, help='Satelite corrected records filename')
###
parser.add_argument('--photometers_records',  required=True,  type=str, help='Photometers records folder')
#parser.add_argument('--photometers', required=True, type=str, help='Photometers filename')
parser.add_argument('--out','--output', required=True, type=str, help='Output filename')
args = parser.parse_args()

satelite_records=args.satelite_records
###
satelite_corrected_records=args.satelite_corrected_records
###
photometers_records=args.photometers_records
#photometers=args.photometers
output=args.out

#Obtener los registros de satelite
Satelite=pd.read_csv(satelite_records)
###
Satelite_corregido=pd.read_csv(satelite_corrected_records)
Sin_corregir=Satelite
Corregidos=Satelite_corregido
Corregidos['Fecha']=pd.to_datetime(Corregidos['Fecha'])
Sin_corregir['Fecha']=pd.to_datetime(Sin_corregir['Fecha'])
Sin_corregir['Fecha_2']=Sin_corregir['Fecha'].dt.date
Sin_corregir['Fecha_2']=pd.to_datetime(Sin_corregir['Fecha_2'])
Corregidos_resumido=Corregidos[['name','DNB_BRDF_Corrected_NTL', 'Gap_Filled_DNB_BRDF_Corrected_NTL', 'Mandatory_Quality_Flag','Latest_High_Quality_Retrieval_number_days','Fecha']]
F=Sin_corregir.merge(Corregidos_resumido,left_on=['name','Fecha_2'],right_on=['name','Fecha'])
F=F.drop(['Fecha_2','Fecha_y'], axis=1)
F=F.rename(columns={'Fecha_x':'Fecha'})
Satelite=F
###
#Poner formatos fecha y obtener ano dia y mes
Satelite['Fecha']=pd.to_datetime(Satelite['Fecha'])
Satelite['ano']=[i.year for i in Satelite['Fecha']]
Satelite['mes']=[i.month for i in Satelite['Fecha']]
Satelite['dia']=[i.day for i in Satelite['Fecha']]

Listado=os.listdir(photometers_records)
Medidas_fotometros=pd.DataFrame()
for i in range(0,len(Listado)):
    Med_foto=pd.read_csv(photometers_records+'/'+Listado[i])
    Medidas_fotometros=pd.concat([Medidas_fotometros,Med_foto])

#print(Medidas_fotometros)

"""
#Obtiene los regitros de los fotometros
Medidas_fotometros=pd.read_csv(r"/mnt/data/datos_borja/Datos_fotometros/Registros_1.csv")
Medidas_fotometros2=pd.read_csv(r"/mnt/data/datos_borja/Datos_fotometros/Registros_2.csv")
Medidas_fotometros=pd.concat([Medidas_fotometros,Medidas_fotometros2])
"""

#Poner formatos fecha y obtener ano dia y mes
Medidas_fotometros['time']=pd.to_datetime(Medidas_fotometros['time'])

#Reducir el dataset al periodo donde hay datos de enlace para hacerlo menos pesado
Fecha=datetime.datetime(int(min(Satelite['ano'])),1,1)
timezone = pytz.timezone("UTC")
Fecha_tz = timezone.localize(Fecha)
Medidas_fotometros2=Medidas_fotometros[Medidas_fotometros['time']>=Fecha_tz]

#Obtener ano dia y mes

Medidas_fotometros2.loc[:,'ano']=[i.year for i in Medidas_fotometros2['time']]
Medidas_fotometros2.loc[:,'mes']=[i.month for i in Medidas_fotometros2['time']]
Medidas_fotometros2.loc[:,'dia']=[i.day for i in Medidas_fotometros2['time']]

Medidas_fotometros2['time']=[i.replace(tzinfo=None) for i in Medidas_fotometros2['time']] #Quita el formato tz

#Calculamos las fechas del dia anterior y del posterior por si el paso mas cercano se realiza con un dia de diferencia, por ejemplo que sea el paso a las 00:05 y la medida fotometros a las 23:55
Medidas_fotometros2.loc[:,'ano_ant']=[i.year for i in (Medidas_fotometros2['time']- datetime.timedelta(days=1))]
Medidas_fotometros2.loc[:,'mes_ant']=[i.month for i in (Medidas_fotometros2['time']- datetime.timedelta(days=1))]
Medidas_fotometros2.loc[:,'dia_ant']=[i.day for i in (Medidas_fotometros2['time']- datetime.timedelta(days=1))]

Medidas_fotometros2.loc[:,'ano_post']=[i.year for i in (Medidas_fotometros2['time']+ datetime.timedelta(days=1))]
Medidas_fotometros2.loc[:,'mes_post']=[i.month for i in (Medidas_fotometros2['time']+ datetime.timedelta(days=1))]
Medidas_fotometros2.loc[:,'dia_post']=[i.day for i in (Medidas_fotometros2['time']+ datetime.timedelta(days=1))]

#Generamos DataFrames para el dia enterior, posterior y presente, donde unimos los registros de los fotometros con los datos satelitales

Total_ayer=Medidas_fotometros2.merge(Satelite,left_on=['name','ano_ant','mes_ant','dia_ant'],right_on=['name','ano','mes','dia'])
Total_hoy=Medidas_fotometros2.merge(Satelite,left_on=['name','ano','mes','dia'],right_on=['name','ano','mes','dia'])
Total_hoy=Total_hoy.rename(columns={'ano':'ano_x','mes':'mes_x','dia':'dia_x'})
Total_manana=Medidas_fotometros2.merge(Satelite,left_on=['name','ano_post','mes_post','dia_post'],right_on=['name','ano','mes','dia'])

#Dataset con todos los pasos del dia anterior, siguente y actual
Total=pd.concat([Total_ayer,Total_hoy,Total_manana])

#print(Total)

#Obtienes la diferencia en segundos entre la medida fotometros y el paso satelite
B=Total['time']-Total['Fecha']
C=[abs(i.seconds+i.days*24*60*60) for i in B]
Total['Dif']=C

#Eliminamos aquellos con mas de un dia de diferencia ya que tendria que haber otro paso satelital y se considera una diferencia excesiva
Total=Total[Total['Dif']<24*60*60]
#print(Total)

#Selecciona aquellos registros con minima diferencia por cada paso satelital
Min_dif=Total.groupby(['name','time']).min(['Dif'])
MIN_DIF=Min_dif.index.to_frame(index=False)
MIN_DIF['Dif']=Min_dif['Dif'].values


del Satelite, Medidas_fotometros2, Total_ayer, Total_hoy, Total_manana, Medidas_fotometros

#En caso de querer segmentar el procesamiento
"""
inter=list(range(0,len(MIN_DIF)))[::1000]+[len(MIN_DIF)]
F=pd.DataFrame()
for i in range(0,len(inter)-1):
    f=MIN_DIF[inter[i]:inter[i+1]].merge(Total,left_on=['name','time','Dif'],right_on=['name','time','Dif'],how='left')
    F=pd.concat([F,f])
    print(F)
"""


F=MIN_DIF.merge(Total,left_on=['name','time','Dif'],right_on=['name','time','Dif'],how='left')

#print(F)

#Eliminamos aquellos con mas de un dia de diferencia ya que tendria que haber otro paso satelital y se considera una diferencia excesiva
F=F[F['Dif']<24*60*60]

#Simplificamos 
F=F.drop(['ano_x','mes_x','dia_x','ano_ant','mes_ant','dia_ant','ano_post','mes_post','dia_post','ano_y','dia_y','mes_y'],axis=1)

#Obtenemos ano, mes y dia segun el paso satelital

#Mejor según el apso satelite Fecha que segun la medida concreta el time
F['ano']=[i.year for i in F['Fecha']]
F['mes']=[i.month for i in F['Fecha']]
F['dia']=[i.day for i in F['Fecha']]



#Eliminamos posibles duplicados por posible equdistancio entre el paso satelital y dos medidas, nos quedatemos con la primera 
F=F.drop_duplicates()

#Cambio de nombre  para el paso satelital y guardado
F=F.rename(columns={'Fecha':'Paso_sat'})
#print(F)
F.to_csv(output+"/Dataset_global.csv")

