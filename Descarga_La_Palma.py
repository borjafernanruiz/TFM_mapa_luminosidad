#python Descarga_La_Palma.py --year_from 2022 --year_to 2022 --out /mnt/data/datos_borja/VIIRS/La_Palma --right_upper_corner [-18.060,28.893] --lower_left_corner [-17.634,28.410]
#day_from 223
#Entorno EELab
import pandas as pd
import numpy as np 
import h5py
import os
import time
from datetime import datetime
from shutil import rmtree

import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument('--year_from',  required=True, type=int, help='Year from')
parser.add_argument('--year_to',  required=True, type=int, help='Year to')
parser.add_argument('--right_upper_corner',  required=True, type=str, help='Format example [longitude,latitude]: [-18,28]')
parser.add_argument('--lower_left_corner',  required=True, type=str, help='Format example [longitude,latitude]: [-17,29]')
parser.add_argument('--day_from',  type=int, help='Day from, day of the year number, between 1 and 365')
parser.add_argument('--day_to',  type=int, help='Day to, day of the year number, between 1 and 365')
parser.add_argument('--out','--output', required=True, type=str, help='Output filename')
args = parser.parse_args()

year_from=args.year_from
year_to=args.year_to
day_from=args.day_from
day_to=args.day_to
output=args.out
esquina_superior_derecha=args.right_upper_corner
esquina_inferior_izquierda=args.lower_left_corner

esquina_superior_derecha=esquina_superior_derecha[1:-1].split(',')
esquina_inferior_izquierda=esquina_inferior_izquierda[1:-1].split(',')
esquina_superior_derecha[0]
def Cuadrante(lon,lat):
    v=np.floor((90-lat)/10)
    h=np.floor((lon+180)/10)
    return int(v),int(h)
Cuadrante_sup_der=Cuadrante(float(esquina_superior_derecha[0]),float(esquina_superior_derecha[1]))
Cuadrante_inf_izq=Cuadrante(float(esquina_inferior_izquierda[0]),float(esquina_inferior_izquierda[1]))

#Función que añade ceros a números por la izquierde si es menor que 10 por notación de los acudrantes 
def Poner_cero(a):
    if a<10:
        return ('0'+str(a))
    else:
        return str(a)

#Formato de los días con los 1->001, 23->023 0 124->124
def dia_format(dia):
    if dia<10:
        return '00'+str(dia)
    elif dia<100:
        return '0'+str(dia)
    else:
        return str(dia)
cuadrantes=[]
for i in range(Cuadrante_sup_der[0],Cuadrante_inf_izq[0]+1):
    for ii in range(Cuadrante_sup_der[1],Cuadrante_inf_izq[1]+1):
        cuadrantes=cuadrantes+['h'+Poner_cero(ii)+'v'+Poner_cero(i)]
#Obteiene el listado de carchivos de los caudrantes que se deben descargar para determinado día y año
def Nombre(ano,dia,producto):
    dia=dia_format(dia)
    csv=pd.read_csv('https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/5000/'+producto+'/'+str(ano)+'/'+dia+'.csv')
    archivos=[(None if i==[] else i[0]) for i in [[i for i in csv['name'] if ii in i] for ii in cuadrantes]]
    return archivos


#FUNCIONES DE DESCARGA OBTENIDAS DE LA PÇAGINA DE LA NASA MODIFICADAS PARA QUE DESCARGUEN SÓLO CIERTOS ARCHIVOS Y NO LA TOTALIDAD COMO INDICA LA NASA
#from __future__ import (division, print_function, absolute_import, unicode_literals)
import argparse
import os
import os.path
import shutil
import sys
from io import StringIO       

USERAGENT = 'tis/download.py_1.0--' + sys.version.replace('\n','').replace('\r','')


def geturl(url, token=None, out=None):
    headers = { 'user-agent' : USERAGENT }
    if not token is None:
        headers['Authorization'] = 'Bearer ' + token
    try:
        import ssl
        CTX = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        if sys.version_info.major == 2:
            import urllib2
            try:
                fh = urllib2.urlopen(urllib2.Request(url, headers=headers), context=CTX)
                if out is None:
                    return fh.read()
                else:
                    shutil.copyfileobj(fh, out)
            except urllib2.HTTPError as e:
                print('HTTP GET error code: %d' % e.code(), file=sys.stderr)
                print('HTTP GET error message: %s' % e.message, file=sys.stderr)
            except urllib2.URLError as e:
                print('Failed to make request: %s' % e.reason, file=sys.stderr)
            return None

        else:
            from urllib.request import urlopen, Request, URLError, HTTPError
            try:
                fh = urlopen(Request(url, headers=headers), context=CTX)
                print(fh)
                if out is None:
                    return fh.read().decode('utf-8')
                else:
                    shutil.copyfileobj(fh, out)
            except HTTPError as e:
                print('HTTP GET error code: %d' % e.code(), file=sys.stderr)
                print('HTTP GET error message: %s' % e.message, file=sys.stderr)
            except URLError as e:
                print('Failed to make request: %s' % e.reason, file=sys.stderr)
            return None

    except AttributeError:
        # OS X Python 2 and 3 don't support tlsv1.1+ therefore... curl
        import subprocess
        try:
            args = ['curl', '--fail', '-sS', '-L', '--get', url]
            for (k,v) in headers.items():
                args.extend(['-H', ': '.join([k, v])])
            if out is None:
                # python3's subprocess.check_output returns stdout as a byte string
                result = subprocess.check_output(args)
                return result.decode('utf-8') if isinstance(result, bytes) else result
            else:
                subprocess.call(args, stdout=out)
        except subprocess.CalledProcessError as e:
            print('curl GET error message: %' + (e.message if hasattr(e, 'message') else e.output), file=sys.stderr)
        return None

def sync(src, dest, tok,A):
    '''synchronize src url with dest directory'''
    try:
        import csv
        files = [ f for f in csv.DictReader(StringIO(geturl('%s.csv' % src, tok)), skipinitialspace=True) ]
    except ImportError:
        import json
        files = json.loads(geturl(src + '.json', tok))

    # use os.path since python 2/3 both support it while pathlib is 3.4+
    for f in files:
        print(f['name'])
        if f['name'] in A:
          # currently we use filesize of 0 to indicate directory
          filesize = int(f['size'])
          path = os.path.join(dest, f['name'])
          url = src + '/' + f['name']
          if filesize == 0:
              try:
                  print('creating dir:', path)
                  os.mkdir(path)
                  sync(src + '/' + f['name'], path, tok)
              except IOError as e:
                  print("mkdir `%s': %s" % (e.filename, e.strerror), file=sys.stderr)
                  sys.exit(-1)
          else:
              try:
                  if not os.path.exists(path):
                      print('downloading: ' , path)
                      with open(path, 'w+b') as fh:
                          geturl(url, tok, fh)
                  else:
                      print('skipping: ', path)
              except IOError as e:
                  print("open `%s': %s" % (e.filename, e.strerror), file=sys.stderr)
                  sys.exit(-1)
    return 0

#FIN DE FUNCIONES NASA
#TOKEN de mi usuario
#token='eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJBUFMgT0F1dGgyIEF1dGhlbnRpY2F0b3IiLCJpYXQiOjE2NjYxNzMxMDUsIm5iZiI6MTY2NjE3MzEwNSwiZXhwIjoxNjgxNzI1MTA1LCJ1aWQiOiJib3JqYWZlcm5hbnJ1aXoiLCJlbWFpbF9hZGRyZXNzIjoiYm9yamFmZXJuYW5ydWl6QGdtYWlsLmNvbSIsInRva2VuQ3JlYXRvciI6ImJvcmphZmVybmFucnVpeiJ9.K_u1vgecmNPAXerweQKDGVk218LTkXnZ5NzurVuBHe0'
token='eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJBUFMgT0F1dGgyIEF1dGhlbnRpY2F0b3IiLCJpYXQiOjE2Njc0NjYwOTEsIm5iZiI6MTY2NzQ2NjA5MSwiZXhwIjoxNjgzMDE4MDkxLCJ1aWQiOiJib3JqYWZlcm5hbnJ1aXoiLCJlbWFpbF9hZGRyZXNzIjoiYm9yamFmZXJuYW5ydWl6QGdtYWlsLmNvbSIsInRva2VuQ3JlYXRvciI6ImJvcmphZmVybmFucnVpeiJ9.TlzzZ3cJYwZIAbFQ9tOft_PAIdY_KPsMeeSJThN5Lqg'

def Descarga(ano,dia,producto):
    A=set(Nombre(ano,dia,producto))
    url='https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/5000/'+producto+'/'+str(ano)+'/'+dia_format(dia) 
    #out=r'/mnt/data/datos_borja/VIIRS/Diarios_3/ano_'+str(ano)+'/dia_'+dia_format(dia)
    out=output+'/ano_'+str(ano)+'/dia_'+dia_format(dia)
    os.makedirs(out, exist_ok=True)
    sync(url,out,token,A)

ancho_cuadricula=15/60/60
malla=np.array(range(0,2400))*ancho_cuadricula
#Funcion para comprobar si la posicion se sale de la cuadricula
def Se_sale(a):
    if a<0 or a>=2400:
        return None
    else:
        return a

def Descarga_TOTAL(ano,dia):
    Descarga(ano,dia,'VNP46A1')
    carpeta=output+'/ano_'+str(ano)+'/dia_'+dia_format(dia)
    archivos=os.listdir(carpeta)
    Data=pd.DataFrame()
    for ii in archivos:
        h5file = h5py.File(carpeta+"/"+ii,"r")
        cuadrante=ii.split('.')[2]
        h=cuadrante[1:3]
        v=cuadrante[4:6]
    
        Pos_1_sd=int(np.round((-float(esquina_superior_derecha[1])+(90-int(v)*10))/ancho_cuadricula))
        Pos_2_sd=int(np.round((float(esquina_superior_derecha[0])-(-180+int(h)*10))/ancho_cuadricula))
        Pos_1_ii=int(np.round((-float(esquina_inferior_izquierda[1])+(90-int(v)*10))/ancho_cuadricula))
        Pos_2_ii=int(np.round((float(esquina_inferior_izquierda[0])-(-180+int(h)*10))/ancho_cuadricula))

        Pos_1_sd=Se_sale(Pos_1_sd)
        Pos_2_sd=Se_sale(Pos_2_sd)
        Pos_1_ii=Se_sale(Pos_1_ii)
        Pos_2_ii=Se_sale(Pos_2_ii)

        lon=-180+int(h)*10
        lat=90-int(v)*10
        LON=np.array(2400*list(lon+malla)).reshape(2400,2400)
        LAT=np.transpose(np.array(2400*list(lat-malla)).reshape(2400,2400))
    
        var1=np.array(h5file['HDFEOS']['GRIDS']['VNP_Grid_DNB']['Data Fields']['DNB_At_Sensor_Radiance_500m'])
        var2=np.array(h5file['HDFEOS']['GRIDS']['VNP_Grid_DNB']['Data Fields']['QF_Cloud_Mask'])

        Data['lon']=LON[Pos_1_sd-1:Pos_1_ii+1,Pos_2_sd-1:Pos_2_ii+1].reshape(1,-1)[0]
        Data['lat']=LAT[Pos_1_sd-1:Pos_1_ii+1,Pos_2_sd-1:Pos_2_ii+1].reshape(1,-1)[0]
        Data['DNB_At_Sensor_Radiance_500m']=var1[Pos_1_sd-1:Pos_1_ii+1,Pos_2_sd-1:Pos_2_ii+1].reshape(1,-1)[0]
        Data['QF_Cloud_Mask']=var2[Pos_1_sd-1:Pos_1_ii+1,Pos_2_sd-1:Pos_2_ii+1].reshape(1,-1)[0]

        Data['DNB_At_Sensor_Radiance_500m']=Data['DNB_At_Sensor_Radiance_500m'].replace({65535:np.nan})*0.1 #Cambio nulos
    
        QF_Cloud_Mask=Data['QF_Cloud_Mask'].fillna(65535) #Cambiar los nulos por 65535 para que no de error al trabajar en bits
        QF_Cloud_Mask=["{:16b}".format(int(i)).replace(' ','0') for i in QF_Cloud_Mask] #Paso a binario
        Nulos=list(np.where(np.array(QF_Cloud_Mask)=='1111111111111111')[0]) #Identificacion de nulos
    
        Cloud_Detection_Results=[i[6:8] for i in QF_Cloud_Mask]
        Shadow_Detected=[i[8] for i in QF_Cloud_Mask]
        Cirrus_Detection=[i[9] for i in QF_Cloud_Mask]
        #introduccion nulos
        for i in Nulos:
            Cloud_Detection_Results[i]=np.nan
            Shadow_Detected[i]=np.nan
            Cirrus_Detection[i]=np.nan

        Data['Cloud_Detection_Results']=Cloud_Detection_Results
        Data['Shadow_Detected']=Shadow_Detected
        Data['Cirrus_Detection']=Cirrus_Detection
        Data['Cloud_Detection_Results']=Data['Cloud_Detection_Results'].replace({'00':'Confident_clear','01':'Probably_clear','10':'Probably_cloudy','11':'Confident_cloudy'})
        Data['Shadow_Detected']=Data['Shadow_Detected'].replace({'0':'No','1':'Yes'}) #Cambio a categorica
        Data['Cirrus_Detection']=Data['Cirrus_Detection'].replace({'0':'No_cloud','1':'Cloud'}) #Cambio a categorica
        Data.replace({None:np.nan})

        Data=Data.drop(['QF_Cloud_Mask'], axis=1)

    rmtree(carpeta)

    Descarga(ano,dia,'VNP46A2')
    carpeta=output+'/ano_'+str(ano)+'/dia_'+dia_format(dia)
    archivos=os.listdir(carpeta)    
    for ii in archivos:
        h5file = h5py.File(carpeta+"/"+ii,"r")
        cuadrante=ii.split('.')[2]
        h=cuadrante[1:3]
        v=cuadrante[4:6]
    
        Pos_1_sd=int(np.round((-float(esquina_superior_derecha[1])+(90-int(v)*10))/ancho_cuadricula))
        Pos_2_sd=int(np.round((float(esquina_superior_derecha[0])-(-180+int(h)*10))/ancho_cuadricula))
        Pos_1_ii=int(np.round((-float(esquina_inferior_izquierda[1])+(90-int(v)*10))/ancho_cuadricula))
        Pos_2_ii=int(np.round((float(esquina_inferior_izquierda[0])-(-180+int(h)*10))/ancho_cuadricula))

        Pos_1_sd=Se_sale(Pos_1_sd)
        Pos_2_sd=Se_sale(Pos_2_sd)
        Pos_1_ii=Se_sale(Pos_1_ii)
        Pos_2_ii=Se_sale(Pos_2_ii)
   
        var1=np.array(h5file['HDFEOS']['GRIDS']['VNP_Grid_DNB']['Data Fields']['DNB_BRDF-Corrected_NTL'])
        var2=np.array(h5file['HDFEOS']['GRIDS']['VNP_Grid_DNB']['Data Fields']['Gap_Filled_DNB_BRDF-Corrected_NTL'])

        Data['DNB_BRDF-Corrected_NTL']=var1[Pos_1_sd-1:Pos_1_ii+1,Pos_2_sd-1:Pos_2_ii+1].reshape(1,-1)[0]
        Data['Gap_Filled_DNB_BRDF-Corrected_NTL']=var2[Pos_1_sd-1:Pos_1_ii+1,Pos_2_sd-1:Pos_2_ii+1].reshape(1,-1)[0]
    
        Data['DNB_BRDF-Corrected_NTL']=Data['DNB_BRDF-Corrected_NTL'].replace({65535:np.nan})*0.1 #Cambio nulos
        Data['Gap_Filled_DNB_BRDF-Corrected_NTL']=Data['Gap_Filled_DNB_BRDF-Corrected_NTL'].replace({65535:np.nan})*0.1 #Cambio nulos
        Data.replace({None:np.nan})
    rmtree(carpeta)
    Data['Fecha']=datetime.strptime(str(ano)+','+str(dia),'%Y,%j')
    return Data

#print(Descarga_TOTAL(2022,66))

def DESCARGA_RESUMIR(inicio_ano,fin_ano,inicio_dia=1,fin_dia=365):
    #Mira hasta que dia y ano ya han sido descargados
    try:
        Registros=pd.read_csv(output+"/Mapas_satelite.csv")
        Registros['Fecha']=pd.to_datetime(Registros['Fecha'])
        f=max(Registros['Fecha'])
        if (inicio_dia<=int(f.strftime("%j"))):
            inicio_dia=int(f.strftime("%j"))+1
        if (((inicio_ano%4!=0) & (inicio_dia==365)) |((inicio_ano%4==0) & (inicio_dia==366))):
            inicio_ano=f.year+1
            inicio_dia=1
        else:
            inicio_ano=f.year

    except:
        Registros=pd.DataFrame()
    for i in range(inicio_ano,fin_ano+1):
        #Para saber si bisiestos
        if i%4==0:
            n=1
        else:
            n=0
        if i==inicio_ano:
            day_0=inicio_dia
            day_f=365+n
        elif i==fin_ano:  
            day_0=1
            day_f=fin_dia 
        elif inicio_ano==fin_ano:
            day_0=inicio_dia
            day_f=fin_dia      
        else:
            day_0=1
            day_f=365+n
        for ii in range(day_0,day_f+1):
            try:
                Registros=pd.read_csv(output+"/Mapas_satelite.csv")
            except:
                Registros=pd.DataFrame()
            exito=False
            #Realizará los intentos que sean necesarios hasta lograr la descarga
            while exito==False:
                try:
                    Dataset=Descarga_TOTAL(i,ii)
                    exito=True
                except:
                    exito=False
            Registros_nuevos=pd.concat([Registros,Dataset])
            Registros_nuevos.to_csv(output+"/Mapas_satelite.csv", index = False)

if day_from and day_to:
    DESCARGA_RESUMIR(year_from,year_to,day_from,day_to)
elif day_from and not day_to:
    DESCARGA_RESUMIR(year_from,year_to,day_from)
elif not day_from and day_to:
    DESCARGA_RESUMIR(year_from,year_to,1,day_to)
else:
     DESCARGA_RESUMIR(year_from,year_to)

