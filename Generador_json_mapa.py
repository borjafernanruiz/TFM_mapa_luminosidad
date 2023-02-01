#!/usr/bin/env python
# coding: utf-8

import numpy as np
import pandas as pd
#import plotly.express as px
#from plotly.subplots import make_subplots
#import plotly.graph_objects as go 
import matplotlib.pyplot as plt 
from scipy.interpolate import griddata
#import math as m
import datetime
import matplotlib.pyplot as plt
#import folium
import branca
#from folium import plugins
from scipy.interpolate import griddata
import geojsoncontour
import scipy as sp
import scipy.ndimage
#from folium.features import DivIcon
import json
from PIL import Image
import io
import base64
#from folium.plugins import MousePosition

import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--satellite_images',  required=True, type=str, help='Satellite images filename')
parser.add_argument('--photometers_data',  required=True, type=str, help='Photometers data filename')
parser.add_argument('--week_from',  required=True, type=int, help='Week from, week of the year number, between 1 and 52')
parser.add_argument('--week_to',  required=True, type=int, help='Week to, week of the year number, between 1 and 52')
parser.add_argument('--photometers', required=True, type=str, help='Photometers filename')
args = parser.parse_args()

satellite_images=args.satellite_images
photometers_data=args.photometers_data
week_from=args.week_from
week_to=args.week_to
photometers=args.photometers

Datos=pd.read_csv(satellite_images)

Datos['Fecha']=pd.to_datetime(Datos['Fecha'])
Datos['lon']=np.round(Datos['lon'],6)
Datos['lat']=np.round(Datos['lat'],6)
Datos['DNB_At_Sensor_Radiance_500m_lim']=Datos['DNB_At_Sensor_Radiance_500m']
Datos.loc[(Datos['Shadow_Detected']=='Yes') & (Datos['Cirrus_Detection']=='Cloud'),'DNB_At_Sensor_Radiance_500m_lim']=np.NAN
Datos['DNB_BRDF-Corrected_NTL_lim']=Datos['DNB_BRDF-Corrected_NTL']
Datos.loc[(Datos['Shadow_Detected']=='Yes') & (Datos['Cirrus_Detection']=='Cloud'),'DNB_BRDF-Corrected_NTL_lim']=np.NAN

fechas=np.array(list(set(Datos['Fecha'])))
fechas=sorted(fechas)

def Acumular(anterior,siguiente):
    anterior=anterior.copy()
    siguiente=siguiente.copy()
    anterior[np.where(siguiente>-1)]=np.NAN
    anterior[np.where(anterior==0)]=-1
    siguiente[np.where(siguiente==0)]=-1
    anterior[np.where(np.isnan(anterior))]=0
    siguiente[np.where(np.isnan(siguiente))]=0
    C=siguiente+anterior
    C[np.where(C==0)]=np.NAN
    C[np.where(C==-1)]=0
    return C

PP=[]
nNAN=np.zeros((118,104))
T=np.zeros((118,104))
cont=-1
for i in fechas:
    P=np.array(Datos[Datos['Fecha']==str(i)]['DNB_BRDF-Corrected_NTL_lim']).reshape(118,104)
    ceros=np.zeros((118,104))
    ceros[np.where(np.isnan(P))]=1
    nNAN=nNAN+ceros
    P[np.where(np.isnan(P))]=0
    T=T+P
    cont=cont+1
    if cont==6:
        PP=PP+[T/(7-nNAN)]
        cont=-1
        nNAN=np.zeros((118,104))
        T=np.zeros((118,104)) 

def media(A):
    return A[np.where(np.logical_not(np.isnan(A)))].mean()

PP2=[]
nNAN=np.zeros((118,104))
T=np.zeros((118,104))
cont=-1
for i in fechas:
    P=np.array(Datos[Datos['Fecha']==str(i)]['DNB_At_Sensor_Radiance_500m_lim']).reshape(118,104)
    if (media(P[0:10,0:10])<0.3) & (media(P[-10:,-10:])<0.3) & (media(P[-10:,0:10])<0.3) & (media(P[0:10,-10:])<0.3):
        ceros=np.zeros((118,104))
        ceros[np.where(np.isnan(P))]=1
        nNAN=nNAN+ceros
        P[np.where(np.isnan(P))]=0
        T=T+P
    else:
        ceros=np.ones((118,104))
        nNAN=nNAN+ceros
        P=np.zeros((118,104))
        T=T+P
    cont=cont+1
    if cont==6:
        PP2=PP2+[T/(7-nNAN)]
        cont=-1
        nNAN=np.zeros((118,104))
        T=np.zeros((118,104))  

def Acumulacion(PP):
    cont=0
    PP_ac=[]
    for i in PP:
        if cont==0:
            PP_ac=PP_ac+[i]
            cont=1
        else:
            PP_ac=PP_ac+[Acumular(PP_ac[-1],i)]   
    return PP_ac

PP_ac=Acumulacion(PP)
PP2_ac=Acumulacion(PP2)

F=[]
for i in range(0,len(PP_ac)):
    A=PP_ac[i].copy()
    B=PP2_ac[i].copy()
    B[np.where(np.logical_not(np.isnan(A)))]=A[np.where(np.logical_not(np.isnan(A)))]
    F=F+[B]

SAT=[]
for i in F:
    Sat=np.log(i)*-1.18+20.86#[0:100,0:20]
    Sat[np.where(Sat==np.inf)]=22
    Sat[np.where(Sat>=22)]=22
    SAT=SAT+[Sat] 

Fotometros=pd.read_csv(photometers)
Fotometros_LaPalma=Fotometros[(Fotometros['latitude']>Datos['lat'].min()) & (Fotometros['latitude']<Datos['lat'].max()) & (Fotometros['longitude']>Datos['lon'].min()) & (Fotometros['longitude']<Datos['lon'].max())]

Registros2=pd.read_csv(photometers_data) #corregidos 
Registros=Registros2.drop(['Unnamed: 0'],axis=1)
A=Registros.merge(Fotometros,left_on=['name'],right_on=['name'],how='left')
AA=A[(A['latitude']>Datos['lat'].min()) & (A['latitude']<Datos['lat'].max()) & (A['longitude']>Datos['lon'].min()) & (A['longitude']<Datos['lon'].max())]
del Registros2
A=AA
A['from_date']=A['from_date'].fillna('2000-01-01 00:00:00+00:00')
A['to_date']=A['to_date'].fillna('2050-01-01 00:00:00+00:00')
A['time']=pd.to_datetime(A['time'])
A['from_date']=pd.to_datetime(A['from_date'])
A['to_date']=pd.to_datetime(A['to_date'])
A['Paso_sat']=pd.to_datetime(A['Paso_sat'])
A['to_date']=[i.replace(tzinfo=None) for i in A['to_date']]
A['from_date']=[i.replace(tzinfo=None) for i in A['from_date']]
A=A[(A['from_date']<A['time']) & (A['to_date']>A['time'])]
data=A
del A

dataFF=data[(data['luna']==False) & (data['nubes']==False) & (data['galaxia']==False) & (data['zodiacal']==False) & (((data['Dif']<=120) & (data['Shadow_Detected']=='No') & (data['Cirrus_Detection']=='No_cloud')) | (data['Dif']>120))]

STD=dataFF.groupby(['name']).std()[['mag']]
MEDIAN=dataFF.groupby(['name']).median()[['mag']]
STD.columns=['std']
MEDIAN.columns=['median']
R=STD.merge(MEDIAN,left_on='name',right_on='name')

dataF=data[(data['luna']==False) & (data['nubes']==False) & (((data['Dif']<=60*60) & (data['Shadow_Detected']=='No') & (data['Cirrus_Detection']=='No_cloud')) | (data['Dif']>60*60))]
del data
dataF=dataF[['name','galaxia','zodiacal','TYPE','time','ano','mes','dia','mag','latitude','longitude']].merge(R,left_on='name',right_on='name')
del dataFF
dataF=dataF[(((dataF['median']>20) & (dataF['galaxia']==False) & (dataF['zodiacal']==False)) | (dataF['median']<=20)) & (dataF['mag']<dataF['median']+3*dataF['std']) & (dataF['mag']>dataF['median']-3*dataF['std'])]

D=dataF[['name','TYPE','time','ano','mes','dia','mag','latitude','longitude']]
DD=D.groupby(['name','ano','mes','dia']).median()
DDD=pd.DataFrame(list(DD.index))
DDD['mag']=DD['mag'].values
DDD['latitude']=DD['latitude'].values
DDD['longitude']=DD['longitude'].values
DDD.columns=['name','ano','mes','dia','mag','latitude','longitude']

semana=[]
for i in DDD.values:
    semana=semana+[int((int(datetime.datetime.strptime(str(i[1])+"/"+str(i[2])+"/"+str(i[3]), "%Y/%m/%d").strftime("%j"))-1)/7)+1]
DDD['semana']=semana

d=DDD.groupby(['name','ano','semana']).median()
dd=pd.DataFrame(list(d.index))
dd['mag']=d['mag'].values
dd['latitude']=d['latitude'].values
dd['longitude']=d['longitude'].values
dd.columns=['name','ano','semana','mag','latitude','longitude']
Foto=dd.merge(Fotometros[['name','TYPE']],left_on=['name'],right_on=['name'],how='left')

dat=Datos[Datos['Fecha']=='2022-01-01']
LO=np.array(dat['lon']).reshape(118,104)
LA=np.array(dat['lat']).reshape(118,104)

def Cercano(data,lat,lon):
    a=np.abs(data['lat']-lat).min()
    if lat in list(data['lat']):
        LAT=lat
    elif (a+lat) in list(data['lat']):
        LAT=a+lat
    elif -(a-lat) in list(data['lat']):
        LAT=-(a-lat)
    b=np.abs(data['lon']-lon).min()
    if lon in list(data['lon']):
        LON=lon
    elif (b+lon) in list(data['lon']):
        LON=b+lon
    elif -(b-lon) in list(data['lon']):
        LON=-(b-lon)
    return LAT,LON

satelite=pd.DataFrame()
for i in range(0,len(SAT)):
    s=pd.DataFrame()
    s['mag_sat']=[i[0] for i in SAT[i].reshape(-1,1)]
    s['lon']=[i[0] for i in LO.reshape(-1,1)]
    s['lat']=[i[0] for i in LA.reshape(-1,1)]
    s['semana']=i+1
    satelite=pd.concat([satelite,s])

Coor=list(Foto.groupby(['latitude','longitude']).count().index)
Coor_aprox=[Cercano(satelite,i[0],i[1]) for i in Coor]

Aproximacion=pd.DataFrame(Coor)
Aproximacion['2']=[i[0] for i in Coor_aprox]
Aproximacion['3']=[i[1] for i in Coor_aprox]
Aproximacion.columns=['latitude','longitude','latitude_aprox','longitude_aprox']

Foto2=Foto.merge(Aproximacion,left_on=['latitude','longitude'],right_on=['latitude','longitude'])
Foto_sat=Foto2.merge(satelite,left_on=['latitude_aprox','longitude_aprox','semana'],right_on=['lat','lon','semana'])

def mapa_corr(Datos,n):
    Est1=Cercano(Datos,Fotometros_LaPalma['latitude'].values[n],Fotometros_LaPalma['longitude'].values[n])
    EST=Datos[(Datos['lat']==Est1[0]) & (Datos['lon']==Est1[1])]
    UNION=Datos.merge(EST,right_on='Fecha',left_on='Fecha')
    UNION=UNION[(UNION['DNB_At_Sensor_Radiance_500m_lim_x']>1) & (np.round(UNION['DNB_At_Sensor_Radiance_500m_lim_y'],5)>1)]
    COR=UNION[['lon_x', 'lat_x','DNB_At_Sensor_Radiance_500m_x','DNB_At_Sensor_Radiance_500m_y']].groupby(['lon_x','lat_x']).corr()
    COR=COR[['DNB_At_Sensor_Radiance_500m_x']]
    COR['lon']=[i[0] for i in COR.index.values]
    COR['lat']=[i[1] for i in COR.index.values]
    COR['campos']=[i[2] for i in COR.index.values]
    mapa_corr=pd.DataFrame(COR[COR['campos']=='DNB_At_Sensor_Radiance_500m_y'][['DNB_At_Sensor_Radiance_500m_x','lon','lat']].values)
    mapa_corr.columns=['Corr','lon','lat']
    #lo=np.array(mapa_corr['lon']).reshape(104,118)
    #la=np.array(mapa_corr['lat']).reshape(104,118)
    v=np.array(mapa_corr['Corr']).reshape(104,118)
    return v

Vcc=[]
for i in range(0,len(Fotometros_LaPalma)):
    v=mapa_corr(Datos,i)
    v[np.where(v<0)]=0
    Vcc=Vcc+[v]
    print(i)

Fotometros_LaPalma['M_corr']=Vcc
Foto_sat2=Fotometros_LaPalma[['name','M_corr']].merge(Foto_sat,left_on='name',right_on='name')

M1=Datos[Datos['Fecha']==fechas[0]][['lon','lat']]
M1['lon']=np.round(M1['lon'],8)
M1['lat']=np.round(M1['lat'],8)
M2=Foto_sat2[['name','mag','lon','lat','semana']].groupby(['lon','lat','semana']).median()
M3=pd.DataFrame(list(M2.index.values))
M3['mag']=M2['mag'].values
M3.columns=['lon','lat','semana','mag']
M3['lon']=np.round(M3['lon'],8)
M3['lat']=np.round(M3['lat'],8)


# In[71]:


Pix_fot=[]
cont=0
i=0
while i<max(M3['semana']):
    i=i+1
    UM=M1.merge(M3[M3['semana']==i],left_on=['lon','lat'],right_on=['lon','lat'],how='left')
    R=np.array(UM['mag']).reshape(118,104)
    Pix_fot=Pix_fot+[R]

ff=[]
matrix=None
for i in range(1,45): #45
    #print(i)
    try:
        a=Foto_sat2[(Foto_sat2['semana']==i) & (Foto_sat2['mag_sat']>0)]
        matrix=sum(a['M_corr']*a['mag'])/sum(a['M_corr'])
        matrix[np.where(np.isnan(matrix))]=0
        rest=np.transpose((1-sum(a['M_corr'])/len(a['M_corr'])))[::-1]*SAT[i-1]+np.transpose((sum(a['M_corr'])/len(a['M_corr']))*matrix)[::-1]    
    except:
        try:
            rest=np.transpose((1-sum(a['M_corr'])/len(a['M_corr'])))[::-1]*SAT[i-1]+np.transpose((sum(a['M_corr'])/len(a['M_corr']))*matrix)[::-1]    
        except:
            cont=0
            j=0
            while cont==0:
                try:
                    j=j+1
                    a=Foto_sat2[(Foto_sat2['semana']==j) & (Foto_sat2['mag_sat']>0)]
                    matrix=sum(a['M_corr']*a['mag'])/sum(a['M_corr'])
                    matrix[np.where(np.isnan(matrix))]=0
                    cont=1
                except:
                    cont=0
            rest=np.transpose((1-sum(a['M_corr'])/len(a['M_corr'])))[::-1]*SAT[i-1]+np.transpose((sum(a['M_corr'])/len(a['M_corr']))*matrix)[::-1]    
    try:
        rest[np.where(np.logical_not(np.isnan(Pix_fot[i-1])))]=Pix_fot[i-1][np.where(np.logical_not(np.isnan(Pix_fot[i-1])))]
    except:
        rest=rest
    rest[np.where(rest==np.inf)]=22
    rest[np.where(rest>=22)]=22   
    ff=ff+[rest]

mapa=pd.DataFrame()
for i in range(0,len(ff)):
    s=pd.DataFrame()
    s['mag']=[i[0] for i in ff[i].reshape(-1,1)]
    s['lon']=[i[0] for i in LO.reshape(-1,1)]
    s['lat']=[i[0] for i in LA.reshape(-1,1)]
    s['semana']=i+1
    mapa=pd.concat([mapa,s])

data=mapa
datafoto=Foto_sat

def semana(i):
    data2=data[data['semana']==i]
    data2=data2[data2.notnull().all(1)]
    la=list(set(data2['lat']))
    lo=list(set(data2['lon']))
    la=np.linspace(data2['lat'].min(),data2['lat'].max(),len(la)*10)
    lo=np.linspace(data2['lon'].min(),data2['lon'].max(),len(lo)*10)
    LA, LO = np.meshgrid(la, lo) #En forma de matrices
    datafoto2=datafoto[datafoto['semana']==i]
    a=np.array(datafoto2['mag'])
    a[a>22]=22
    datafoto2['mag']=a
    z=np.array(data2['mag'])
    LATITUD=np.array(data2['lat'])
    LONGITUD=np.array(data2['lon'])
    Z = griddata((list(LATITUD)+list(datafoto2['lat']), list(LONGITUD)+list(datafoto2['lon'])), list(z)+list(datafoto2['mag']), (LA, LO), method='linear')
    sigma = [0.1, 0.1]
    Z2 = sp.ndimage.filters.gaussian_filter(Z, sigma, mode='constant')
    v_sat=Quitar_bordes(Z2,Z)
    del Z
    v=v_sat
    Z2=Z2[v[0]:v[1],v[2]:v[3]]
    LO2=LO[v[0]:v[1],v[2]:v[3]]
    LA2=LA[v[0]:v[1],v[2]:v[3]]
    Z2[Z2>22]=22
    return Z2,LA2,LO2,datafoto2

#Función que elimina las zona más exterior que se distorsioana al aplicar el filtro gaussiano
def Quitar_bordes(Z2,Z):
    h=np.where(np.round(Z2,2) == round(Z[0,0],2))
    h2=np.where(np.round(Z2,2) == round(Z[-1,-1],2))
    return [h[0][0],h2[0][-1],h[1][0],h2[1][-1]]

def poner_ceros(a):
    if len(a)<2:
        return '0'+a
    else:
        return a
def rgb_to_hex(r, g, b): #Función cambio rgb a hex
    r=poner_ceros(('{:X}').format(r))
    g=poner_ceros(('{:X}').format(g))
    b=poner_ceros(('{:X}').format(b))
    return r+g+b

#Escala de colores dada por el IAC
Colores= ["#FFFFFF","#FFFDFF","#FFFBFF","#FFF9FF","#FFF7FF","#FFF5FF","#FFF3FF","#FFF1FF","#FFEFFF","#FFEDFF","#FFEBFF","#FFE9FF","#FFE7FF","#FFE5FF","#FFE2FF","#FFE0FE","#FFDFFC","#FFDEFA","#FFDDF8","#FFDCF6","#FDDBF4","#FDDBEE","#FDDBF1","#FDDCEC","#FDDDEA","#FDDEE8","#FDE0E6","#FDE2E5","#FDE4E4","#FDE6E3","#FDE8E3","#FDEAE1","#FDECE0","#FDEEDF","#FDF0DE","#FDF2DD","#FDF4DC","#FDF5DB","#FDF6DA","#FCF6D9","#FAF6D8","#F8F7D8","#F6F8D8","#F4F9D7","#F2FAD7","#F0FBD6","#EEFCD6","#ECFDD6","#EAFED6","#E8FFD6","#E6FFD8","#E4FFDA","#E2FFDC","#E0FFDE","#DFFFE0","#DEFFE2","#DDFFE4","#DCFFE6","#DBFFE8","#DAFFEA","#D9FFEC","#D9FEEE","#D9FDF0","#D9FCF2","#D9FBF4","#D9FAF6","#D9F9F8","#D9F8FA","#D9F7FC","#D9F6FE","#D8F4FE","#D7F2FE","#D6F0FD","#D5EDFC","#D4EAFB","#D3E7FA","#D2E4F9","#D1E1F8","#D0DEF7","#CFDAF7","#CFD4F8","#CFC8F6","#D0BAF4","#D1AAF2","#D39AEE","#D58AE9","#D77CE0","#D970D6","#DB68CB","#DD61BB","#DF5AAC","#E0539D","#E04B8C","#DE437B","#DC3B6A","#DA335D","#D82C50","#D62644","#D42238","#D21E31","#D01A2B","#CE222A","#CC2B29","#CB3428","#CB4327","#CD4E26","#D25A25","#D86824","#DE7823","#E48A22","#E89E21","#EEAB20","#F2B71F","#F6C31E","#FECD1E","#F8D11F","#EBCE20","#DEC921","#CCC521","#BAC221","#A8BE21","#94BC21","#7FBA23","#6CB928","#58B92D","#4CBA36","#42BC40","#3ABE4C","#35C05B","#32C26A","#2FC578","#2CC886","#28C994","#24C8A3","#21C5B0","#1EC1BD","#1ABCC9","#17B2D1","#14A6D6","#119ADA","#118EDC","#1382DC","#1576DC","#176ADC","#195EDB","#1B52D9","#1D46D7","#1F3AD5","#212ED3","#2324D1","#261CCE","#2D19CB","#341AC8","#3B1BC4","#421DBF","#491FBA","#5021B4","#5523AB","#5825A2","#5B2799","#5D2A90","#5C2E87","#5A337E","#583875","#553B6C","#523E63","#4F3F5A","#4C3F51","#493C49","#463842","#44333C","#422E36","#402A30","#3E262B","#3C2427","#3A2224","#382021","#361E1F","#341C1D","#321A1B","#2E1919","#2A1717","#261515","#221313","#1E1111","#1A0F0F","#160D0D","#120C0C","#0E0B0B","#0A0A0A","#090909","#080808","#070707","#060606","#050505","#040404","#030303","#020202","#010101","#000000"]
Min_escala=14.0 #Minimo valor de escala  9.5 9.1
Max_escala=24
Step_escala=len(Colores)*10#0 #10, 100 va más lento
cm=branca.colormap.LinearColormap(Colores,vmin=Min_escala, vmax=Max_escala).to_step(Step_escala)

#Pasos escala
inter=round((Max_escala-Min_escala)/Step_escala,10)
#Escala completa, valores
Leyenda_escala=[round(Min_escala+i*inter,10) for i in range(0,Step_escala+1)]
#La escala en RGB
Colores_RGB=np.array(cm.colors)*255

def Dar_color(Z2):
    Z2[Z2>-min(Leyenda_escala)]=-min(Leyenda_escala) #Trunca los maximos de brillo por el máximo brillo posible
    CZ=np.round(np.trunc(-Z2*1/inter)*inter,4) #Trunca al valor de la escala correspondiente segmentando por colores
    PZ=(CZ-Min_escala)/inter #Sabiendo el valor de la escala identifica la posición en el vector de la escala
    #Las tres matrices de cada color
    SIN_NULOS=PZ.copy()
    SIN_NULOS[np.isnan(SIN_NULOS)] = -max(Leyenda_escala)+inter #Lo nulos los pone como mínimos para evitar posibles fallos 
    RED=SIN_NULOS.copy().astype(int) 
    GREEN=SIN_NULOS.copy().astype(int)
    BLUE=SIN_NULOS.copy().astype(int)
    #Creción de la matriz de transparencia
    TRANS=PZ.copy()
    TRANS[TRANS>-100000000]=255
    TRANS[np.isnan(TRANS)] =0
    TRANS=TRANS.astype(int)
    #print(TRANS)
    #Bucle que cambia cada valor de posición por su correspondiente valor de color para RGB en matrices separadas
    for i in range(0,len(Colores_RGB)):
        RED[RED==i] =Colores_RGB[i][0]
        GREEN[GREEN==i] = Colores_RGB[i][1]
        BLUE[BLUE==i] = Colores_RGB[i][2]
        if i%(len(Colores_RGB)/4)==0:
            print(str(round(i/len(Colores_RGB)*100,1))+'%')
    #Redimensiona las matrices de colores para operar con ellas
    REDL=list(RED.reshape(-1,1))
    GREENL=list(GREEN.reshape(-1,1))
    BLUEL=list(BLUE.reshape(-1,1))
    TRANSL=list(TRANS.reshape(-1,1))
    Col_zeros=np.zeros(len(REDL)).reshape(-1,1) #Vector columna de ceros
    #Crea matrices de color 
    RED_M=np.append(np.append(np.append(REDL, Col_zeros, axis = 1), Col_zeros, axis = 1),Col_zeros, axis = 1)
    GREEN_M=np.append(np.append(np.append(Col_zeros, GREENL, axis = 1), Col_zeros, axis = 1),Col_zeros, axis = 1)
    BLUE_M=np.append(np.append(np.append(Col_zeros,Col_zeros, axis = 1), BLUEL, axis = 1),Col_zeros, axis = 1)
    TRANS_M=np.append(np.append(np.append(Col_zeros,Col_zeros, axis = 1), Col_zeros, axis = 1),TRANSL, axis = 1)
    #Obtenemos la matriz de color general
    Z_COLOR=RED_M+GREEN_M+BLUE_M+TRANS_M
    if len(RED.shape)>1:
        Z_COLOR=Z_COLOR.reshape(RED.shape[0],RED.shape[1],4)
    else:
        Z_COLOR=Z_COLOR
    Z_final=Z_COLOR.astype(int) #Convertimos en enteros para evitar probblmas por tipología 
    return Z_final

def Pasar_a_bortle(Z):
    Z2_b=Z.copy()
    Z2_b[np.where(Z2_b>=21.99)]=1
    Z2_b[np.where((Z2_b<21.99) & (Z2_b>=21.89))]=2
    Z2_b[np.where((Z2_b<21.89) & (Z2_b>=21.69))]=3
    Z2_b[np.where((Z2_b<21.69) & (Z2_b>=20.49))]=4
    Z2_b[np.where((Z2_b<20.49) & (Z2_b>=19.50))]=5
    Z2_b[np.where((Z2_b<19.50) & (Z2_b>=18.94))]=6
    Z2_b[np.where((Z2_b<18.94) & (Z2_b>=18.38))]=7
    Z2_b[np.where((Z2_b<18.38) & (Z2_b>=8))]=8
    return Z2_b

#Escala de colores dada por el IAC
Colores_bortle=__mag_colors = [
    "#000000",  
    "#474852",
    "#091BF3",
    "#08BD1E",
    "#F3F70C",
    "#F8CA0F",
    "#F00B0B",
    "#FFFFFF"
]
Min_escala_b=1 #Minimo valor de escala  9.5 9.1
Max_escala_b=9
Step_escala_b=len(Colores_bortle)#0 #10, 100 va más lento
cm_b=branca.colormap.LinearColormap(Colores_bortle,vmin=Min_escala_b, vmax=Max_escala_b).to_step(Step_escala_b)

#Pasos escala
inter_b=round((Max_escala_b-Min_escala_b)/Step_escala_b,10)
#Escala completa, valores
Leyenda_escala_b=[round(Min_escala_b+i*inter_b,10) for i in range(0,Step_escala_b+1)]
#La escala en RGB
Colores_RGB_b=np.array(cm_b.colors)*255

def Dar_color_bortle(Z2):
    Z2[Z2>max(Leyenda_escala)]=max(Leyenda_escala_b) #Trunca los maximos de brillo por el máximo brillo posible
    CZ=Z2 #Trunca al valor de la escala correspondiente segmentando por colores
    PZ=(CZ-Min_escala_b)/inter_b #Sabiendo el valor de la escala identifica la posición en el vector de la escala
    #Las tres matrices de cada color
    SIN_NULOS=PZ.copy()
    SIN_NULOS[np.isnan(SIN_NULOS)] = -max(Leyenda_escala_b)+inter_b #Lo nulos los pone como mínimos para evitar posibles fallos 
    RED=SIN_NULOS.copy().astype(int) 
    GREEN=SIN_NULOS.copy().astype(int)
    BLUE=SIN_NULOS.copy().astype(int)
    #Creción de la matriz de transparencia
    TRANS=PZ.copy()
    TRANS[TRANS>-100000000]=255
    TRANS[np.isnan(TRANS)] =0
    TRANS=TRANS.astype(int)
    #print(TRANS)
    #Bucle que cambia cada valor de posición por su correspondiente valor de color para RGB en matrices separadas
    for i in range(0,len(Colores_RGB_b)):
        RED[RED==i] =Colores_RGB_b[i][0]
        GREEN[GREEN==i] = Colores_RGB_b[i][1]
        BLUE[BLUE==i] = Colores_RGB_b[i][2]
        if i%(len(Colores_RGB_b)/4)==0:
            print(str(round(i/len(Colores_RGB_b)*100,1))+'%')
    #Redimensiona las matrices de colores para operar con ellas
    REDL=list(RED.reshape(-1,1))
    GREENL=list(GREEN.reshape(-1,1))
    BLUEL=list(BLUE.reshape(-1,1))
    TRANSL=list(TRANS.reshape(-1,1))
    Col_zeros=np.zeros(len(REDL)).reshape(-1,1) #Vector columna de ceros
    #Crea matrices de color 
    RED_M=np.append(np.append(np.append(REDL, Col_zeros, axis = 1), Col_zeros, axis = 1),Col_zeros, axis = 1)
    GREEN_M=np.append(np.append(np.append(Col_zeros, GREENL, axis = 1), Col_zeros, axis = 1),Col_zeros, axis = 1)
    BLUE_M=np.append(np.append(np.append(Col_zeros,Col_zeros, axis = 1), BLUEL, axis = 1),Col_zeros, axis = 1)
    TRANS_M=np.append(np.append(np.append(Col_zeros,Col_zeros, axis = 1), Col_zeros, axis = 1),TRANSL, axis = 1)
    #Obtenemos la matriz de color general
    Z_COLOR=RED_M+GREEN_M+BLUE_M+TRANS_M
    if len(RED.shape)>1:
        Z_COLOR=Z_COLOR.reshape(RED.shape[0],RED.shape[1],4)
    else:
        Z_COLOR=Z_COLOR
    Z_final=Z_COLOR.astype(int) #Convertimos en enteros para evitar probblmas por tipología 
    return Z_final

#Fución que obtiene las matriz, latitud y longitud en un forma string que permitirá introducir en el código javascript a través de python
def MAG(Z2,LA,LO):
    Z3=np.round(Z2,2)
    #Z3=Z3[::10,::10]
    Z3=Z3[::,::]
    Z3=Z3*100
    Z3=Z3.astype(int)
    Z3=np.transpose(Z3)[::-1]
    #r=LO[::10,::10]
    r=LO[::,::]
    r=np.transpose(r)[::-1]
    #kk=(r[0,0]-r[0,-1])/len(r)
    kk=(r[0,0]-r[0,-1])/(len(r[0])-1)
    #rr=LA[::10,::10]
    rr=LA[::,::]
    rr=np.transpose(rr)[::-1]
    #KK=(rr[0,0]-rr[-1,0])/len(rr[0])
    KK=(rr[0,0]-rr[-1,0])/(len(rr)-1)
    V=[r[0,0],-kk,rr[0,0],-KK]
    I=[]
    cont=0
    for i in Z3:
        cont=cont+1
        I=I+[i.tolist()]
    #STR=str(I)
    return I,V

#Función que limpia el partados propiedades de JSON ya que suele producir errores a la hora del dibujado
def limpia(pp):
    A=json.loads(pp)
    for i in range(0,len(A['features'])):
        A['features'][i]['properties']={}
    return A

def MAPA(Z2,LO2,LA2,datafoto2,CIR):
    #Trasponer la imagen
    PZ2=np.transpose(-Z2)[::-1]
    #Dar color y reducir en un orden la matriz ya que no es necesaria tanta para el color
    #Z_final=Dar_color(PZ2[::10,::10])
    Z_final=Dar_color(PZ2[::,::])
    del PZ2
    #Para los bordes si que necesitamos todos los puntos
    ZL2=-Z2
    #Elementos que meteremos en javascript mediantes folium en la posición del ratón
    STR,V=MAG(Z2,LA2,LO2)
    #Genera los bordes
    try:
        del CS
    except:
        print('')
    CS=plt.contour(LO2, LA2, -ZL2, levels=np.arange(14, 24, 1), alpha=1, colors='red', linestyles='solid',antialiased=True) #Los contornos
    del ZL2
    #COnvertir los bordes en geojson lineas
    pp = geojsoncontour.contour_to_geojson(
      contour=CS,
      min_angle_deg=3.0,
      ndigits=5,
      stroke_width=1)
    del CS
    #Estaciones
    fotometros=[] ###
    if len(datafoto2)!=0:
        for i in range(0,len(datafoto2)):
            #print(i)
            lat=datafoto2['lat'].values[i]
            log=datafoto2['lon'].values[i]
            color='#'+rgb_to_hex(CIR[i][0],CIR[i][1],CIR[i][2])
            nombre=datafoto2['name'].values[i]
            magnitud=np.round(datafoto2['mag'].values,2)[i]
            tipo=datafoto2['TYPE'].values[i]
            foto={"name":nombre,"mag":magnitud,"TYPE":tipo,"lat":lat,"lon":log,"color":color} ###
            fotometros=fotometros+[foto] ###
    #Bordes
    a=json.loads(pp)
    bordes=[] ###
    for i in a["features"]:
        b=a.copy()
        b["features"]=[i]
        b=str(json.dumps(b)).replace(' ','')
        bordes=bordes+[{"valor":int(float(i["properties"]["title"])),"linea":limpia(b)}] ###
    
    arr=Z_final ###
    im = Image.fromarray(arr.astype("uint8")) ###
    #im.show()  ###
    rawBytes = io.BytesIO() ###
    im.save(rawBytes, "PNG") ###
    rawBytes.seek(0)  ###
    imagen=base64.b64encode(rawBytes.read()) ##
    #del img
    #del Z_final
    J={"Estaciones":fotometros,"Bordes":bordes,"Imagen":str(imagen)[2:-1],"Raton":{"V":V,"STR":STR}} ###
    return J ###

def MAPA_b(Z2,LO2,LA2,datafoto2,CIR_bor):
    #Trasponer la imagen
    PZ2=np.transpose(Z2)[::-1]
    #Dar color y reducir en un orden la matriz ya que no es necesaria tanta para el color
    #Z_final=Dar_color(PZ2[::10,::10])
    Z_final=Dar_color_bortle(PZ2[::,::])
    del PZ2
    #Para los bordes si que necesitamos todos los puntos
    ZL2=-Z2
    #Elementos que meteremos en javascript mediantes folium en la posición del ratón
    STR,V=MAG(Z2,LA2,LO2)
    #Genera los bordes
    try:
        del CS
    except:
        print('')
    CS=plt.contour(LO2, LA2, -ZL2, levels=np.arange(1, 10, 1), alpha=1, colors='red', linestyles='solid',antialiased=True) #Los contornos
    del ZL2
    #COnvertir los bordes en geojson lineas
    pp = geojsoncontour.contour_to_geojson(
      contour=CS,
      min_angle_deg=3.0,
      ndigits=5,
      stroke_width=1)
    del CS
    #Estaciones
    fotometros=[] ###
    if len(datafoto2)!=0:
        for i in range(0,len(datafoto2)):
            lat=datafoto2['lat'].values[i]
            log=datafoto2['lon'].values[i]
            color='#'+rgb_to_hex(CIR_bor[i][0],CIR_bor[i][1],CIR_bor[i][2])
            nombre=datafoto2['name'].values[i]
            magnitud=np.round(datafoto2['mag'].values,2)[i]
            bortle=Pasar_a_bortle(np.round(datafoto2['mag'].values,2))[i]
            tipo=datafoto2['TYPE'].values[i]
            foto={"name":nombre,"mag":magnitud,"bortle":bortle,"TYPE":tipo,"lat":lat,"lon":log,"color":color} ###
            fotometros=fotometros+[foto] ###
    #Bordes
    a=json.loads(pp)
    bordes=[] ###
    for i in a["features"]:
        b=a.copy()
        b["features"]=[i]
        b=str(json.dumps(b)).replace(' ','')
        bordes=bordes+[{"valor":int(float(i["properties"]["title"])),"linea":limpia(b)}] ###

    arr=Z_final.copy() ###
    im = Image.fromarray(arr.astype("uint8")) ###
    #im.show()  ###
    rawBytes = io.BytesIO() ###
    im.save(rawBytes, "PNG") ###
    rawBytes.seek(0)  ###
    imagen=base64.b64encode(rawBytes.read())
    del Z_final
    J={"Estaciones":fotometros,"Bordes":bordes,"Imagen":str(imagen)[2:-1],"Raton":{"V":V,"STR":STR}} ###
    return J ###

def Global(semana_ini,semana_fin):
    semana_lim=min(data['semana'].max(),datafoto['semana'].max())
    archivos_mag=[]
    archivos_bortle=[]
    for i in range(semana_ini,semana_fin):
        fromm=datetime.datetime(2022,1,1)+(i-1)*datetime.timedelta(days=7)
        to=datetime.datetime(2022,1,1)+i*datetime.timedelta(days=7)-datetime.timedelta(days=1)
        fecha={"Fecha":fromm.strftime('%d-%m-%Y')+' al '+to.strftime('%d-%m-%Y')}
        
        S=semana(i)
        Z2=S[0]
        LA2=S[1]
        LO2=S[2]
        datafoto2=S[3]
        
        MAG_N=-np.array(datafoto2['mag'].copy())
        if len(MAG_N)!=0:
            CIR=Dar_color(MAG_N)
        else:
            CIR=None
        #print(CIR)
        JSON=MAPA(Z2,LO2,LA2,datafoto2.copy(),CIR)
        JSON.update(fecha)
        archivos_mag=archivos_mag+[JSON]
        
        Z2_b=Pasar_a_bortle(Z2)
        MAG_N_b=Pasar_a_bortle(np.array(datafoto2['mag'].copy()))
        if len(MAG_N_b)!=0:
            CIR_bor=Dar_color_bortle(MAG_N_b)
        else:
            CIR_bor=None
        #print(CIR_bor)
        JSON_b=MAPA_b(Z2_b,LO2,LA2,datafoto2.copy(),CIR_bor)
        JSON_b.update(fecha)
        archivos_bortle=archivos_bortle+[JSON_b]
        
        print('Semana:'+str(i))
    with open('Magnitudes.json', 'w') as json_file:
        json.dump(archivos_mag, json_file)
    with open('Bortle.json', 'w') as json_file:
        json.dump(archivos_bortle, json_file)

Global(week_from,week_to)
