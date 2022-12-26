#SCRIPT QUE DESCARGA LOS DATOS DIARIOS DE VIIRS DESDE LA NASA Y LOS GUARDA EN CARPETAS DIARIAS POR ANOS del producto VNP46A1

#python Descarga_VIIRS_diarios_corregidos.py --year_from 2022 --year_to 2022 --photometers /mnt/data/datos_borja/Todos_fotometros.csv --out /mnt/data/datos_borja/VIIRS/Diarios_corregidos


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
parser.add_argument('--day_from',  type=int, help='Day from, day of the year number, between 1 and 365')
parser.add_argument('--day_to',  type=int, help='Day to, day of the year number, between 1 and 365')
parser.add_argument('--photometers', required=True, type=str, help='Photometers filename')
parser.add_argument('--out','--output', required=True, type=str, help='Output filename')
args = parser.parse_args()

year_from=args.year_from
year_to=args.year_to
day_from=args.day_from
day_to=args.day_to
photometers=args.photometers
output=args.out



#Listado de fotómetros
Fotometros=pd.read_csv(photometers)

#Obtenemos la h y v del cuadrante al que pertenecen esos fotómetros
v=np.floor((90-Fotometros['latitude'])/10)
h=np.floor((Fotometros['longitude']+180)/10)
#Función que añade ceros a números por la izquierde si es menor que 10 por notación de los acudrantes 
def Poner_cero(a):
    if a<10:
        return ('0'+str(a))
    else:
        return str(a)
#Obtenemos el cuadrante con la h y v
W=[]
for i, j in zip(h,v):
    a=Poner_cero(int(i))
    b=Poner_cero(int(j))
    W=W+['h'+a+'v'+b]
#Obtenemos el listado de cuadrantes en orden
Fotometros['Cuadrante']=W
cuadrantes=np.sort(np.array(list(set(Fotometros['Cuadrante']))))
#Formato de los días con los 1->001, 23->023 0 124->124
def dia_format(dia):
    if dia<10:
        return '00'+str(dia)
    elif dia<100:
        return '0'+str(dia)
    else:
        return str(dia)
#Obteiene el listado de carchivos de los caudrantes que se deben descargar para determinado día y año
def Nombre(ano,dia):
    dia=dia_format(dia)
    csv=pd.read_csv('https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/5000/VNP46A2/'+str(ano)+'/'+dia+'.csv')
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
#Descarga una año y día concreto
def Descarga(ano,dia):
    A=set(Nombre(ano,dia))
    url='https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/5000/VNP46A2/'+str(ano)+'/'+dia_format(dia) 
    #out=r'/mnt/data/datos_borja/VIIRS/Diarios_3/ano_'+str(ano)+'/dia_'+dia_format(dia)
    out=output+'/ano_'+str(ano)+'/dia_'+dia_format(dia)
    os.makedirs(out, exist_ok=True)
    sync(url,out,token,A)



#Cuadricula de 15 arcosegundos
cuadricula=15/60/60 #En grados
#Calculo de posiciones en la cuadricula
Pos_1=np.round((Fotometros['longitude']-(-180+h*10))/cuadricula)
Pos_2=np.round((-Fotometros['latitude']+(90-v*10))/cuadricula)


#anadimos los cuadrantes y archivos de un anos de referncia, 2021. Esto lo hacemos porque obtendremos de aqui las coordendas que por desgracia en dan en los archivos diarios, cuadricula 15 segundos de arco
#NOTA: Otra seria calcularlo
Fotometros['Pos_1']=Pos_2
Fotometros['Pos_2']=Pos_1

#Función que obtiene un dataframe con los datos de intenrés para un día y año concretos        
def Data_sat(ano,dia):
    #carpeta=r'/mnt/data/datos_borja/VIIRS/Diarios/ano_'+str(ano)+'/dia_'+dia_format(dia)
    carpeta=output+'/ano_'+str(ano)+'/dia_'+dia_format(dia) #CAMBIAR LA CARPETA DE DESTINO
    archivos=os.listdir(carpeta)
    Fotometros2=Fotometros.copy()
#Ira buscando fotometro por fotometro con los cuadrantes y posiciones recogiendo valores
    for i,r in Fotometros.iterrows():
        for ii in archivos:
            if r['Cuadrante'] in ii:
                print(ii)
                #h5file = h5py.File(r"/mnt/data/datos_borja/VIIRS/Diarios/ano_"+str(ano)+"/dia_"+dia_format(dia)+"/"+ii,"r")
                h5file = h5py.File(carpeta+"/"+ii,"r")
                Fotometros2.at[i,"DNB_BRDF-Corrected_NTL"]=np.array(h5file['HDFEOS']['GRIDS']['VNP_Grid_DNB']['Data Fields']['DNB_BRDF-Corrected_NTL'])[int(r['Pos_1'])][int(r['Pos_2'])]
                Fotometros2.at[i,"Gap_Filled_DNB_BRDF-Corrected_NTL"]=np.array(h5file['HDFEOS']['GRIDS']['VNP_Grid_DNB']['Data Fields']['Gap_Filled_DNB_BRDF-Corrected_NTL'])[int(r['Pos_1'])][int(r['Pos_2'])]
                Fotometros2.at[i,"DNB_Lunar_Irradiance"]=np.array(h5file['HDFEOS']['GRIDS']['VNP_Grid_DNB']['Data Fields']['DNB_Lunar_Irradiance'])[int(r['Pos_1'])][int(r['Pos_2'])]
                Fotometros2.at[i,"Mandatory_Quality_Flag"]=np.array(h5file['HDFEOS']['GRIDS']['VNP_Grid_DNB']['Data Fields']['Mandatory_Quality_Flag'])[int(r['Pos_1'])][int(r['Pos_2'])]
                Fotometros2.at[i,"Latest_High_Quality_Retrieval"]=np.array(h5file['HDFEOS']['GRIDS']['VNP_Grid_DNB']['Data Fields']['Latest_High_Quality_Retrieval'])[int(r['Pos_1'])][int(r['Pos_2'])]
                Fotometros2.at[i,"Snow_Flag"]=np.array(h5file['HDFEOS']['GRIDS']['VNP_Grid_DNB']['Data Fields']['Snow_Flag'])[int(r['Pos_1'])][int(r['Pos_2'])]
                
                #Fotometros2.at[i,"DNB_At_Sensor_Radiance_500m"]=np.array(h5file['HDFEOS']['GRIDS']['VNP_Grid_DNB']['Data Fields']['DNB_At_Sensor_Radiance_500m'])[int(r['Pos_1'])][int(r['Pos_2'])]
                #Fotometros2.at[i,"Sensor_Zenith"]=np.array(h5file['HDFEOS']['GRIDS']['VNP_Grid_DNB']['Data Fields']['Sensor_Zenith'])[int(r['Pos_1'])][int(r['Pos_2'])]
                #Fotometros2.at[i,"Sensor_Azimuth"]=np.array(h5file['HDFEOS']['GRIDS']['VNP_Grid_DNB']['Data Fields']['Sensor_Azimuth'])[int(r['Pos_1'])][int(r['Pos_2'])]
                #Fotometros2.at[i,"Solar_Zenith"]=np.array(h5file['HDFEOS']['GRIDS']['VNP_Grid_DNB']['Data Fields']['Solar_Zenith'])[int(r['Pos_1'])][int(r['Pos_2'])]
                #Fotometros2.at[i,"Solar_Azimuth"]=np.array(h5file['HDFEOS']['GRIDS']['VNP_Grid_DNB']['Data Fields']['Solar_Azimuth'])[int(r['Pos_1'])][int(r['Pos_2'])]
                #Fotometros2.at[i,"Lunar_Zenith"]=np.array(h5file['HDFEOS']['GRIDS']['VNP_Grid_DNB']['Data Fields']['Lunar_Zenith'])[int(r['Pos_1'])][int(r['Pos_2'])]
                #Fotometros2.at[i,"Lunar_Azimuth"]=np.array(h5file['HDFEOS']['GRIDS']['VNP_Grid_DNB']['Data Fields']['Lunar_Azimuth'])[int(r['Pos_1'])][int(r['Pos_2'])]
                #Fotometros2.at[i,"Glint_Angle"]=np.array(h5file['HDFEOS']['GRIDS']['VNP_Grid_DNB']['Data Fields']['Glint_Angle'])[int(r['Pos_1'])][int(r['Pos_2'])]
                #Fotometros2.at[i,"UTC_Time"]=np.array(h5file['HDFEOS']['GRIDS']['VNP_Grid_DNB']['Data Fields']['UTC_Time'])[int(r['Pos_1'])][int(r['Pos_2'])]
                Fotometros2.at[i,"QF_Cloud_Mask"]=np.array(h5file['HDFEOS']['GRIDS']['VNP_Grid_DNB']['Data Fields']['QF_Cloud_Mask'])[int(r['Pos_1'])][int(r['Pos_2'])]
                #Fotometros2.at[i,"QF_DNB"]=np.array(h5file['HDFEOS']['GRIDS']['VNP_Grid_DNB']['Data Fields']['QF_DNB'])[int(r['Pos_1'])][int(r['Pos_2'])]
                #Fotometros2.at[i,"Radiance_M10"]=np.array(h5file['HDFEOS']['GRIDS']['VNP_Grid_DNB']['Data Fields']['Radiance_M10'])[int(r['Pos_1'])][int(r['Pos_2'])]
                #Fotometros2.at[i,"Radiance_M11"]=np.array(h5file['HDFEOS']['GRIDS']['VNP_Grid_DNB']['Data Fields']['Radiance_M11'])[int(r['Pos_1'])][int(r['Pos_2'])]
                #Fotometros2.at[i,"BrightnessTemperature_M12"]=np.array(h5file['HDFEOS']['GRIDS']['VNP_Grid_DNB']['Data Fields']['BrightnessTemperature_M12'])[int(r['Pos_1'])][int(r['Pos_2'])]
                #Fotometros2.at[i,"BrightnessTemperature_M13"]=np.array(h5file['HDFEOS']['GRIDS']['VNP_Grid_DNB']['Data Fields']['BrightnessTemperature_M13'])[int(r['Pos_1'])][int(r['Pos_2'])]
                #Fotometros2.at[i,"BrightnessTemperature_M15"]=np.array(h5file['HDFEOS']['GRIDS']['VNP_Grid_DNB']['Data Fields']['BrightnessTemperature_M15'])[int(r['Pos_1'])][int(r['Pos_2'])]
                #Fotometros2.at[i,"BrightnessTemperature_M16"]=np.array(h5file['HDFEOS']['GRIDS']['VNP_Grid_DNB']['Data Fields']['BrightnessTemperature_M16'])[int(r['Pos_1'])][int(r['Pos_2'])]
                #Fotometros2.at[i,"QF_VIIRS_M10"]=np.array(h5file['HDFEOS']['GRIDS']['VNP_Grid_DNB']['Data Fields']['QF_VIIRS_M10'])[int(r['Pos_1'])][int(r['Pos_2'])]
                #Fotometros2.at[i,"QF_VIIRS_M11"]=np.array(h5file['HDFEOS']['GRIDS']['VNP_Grid_DNB']['Data Fields']['QF_VIIRS_M11'])[int(r['Pos_1'])][int(r['Pos_2'])]
                #Fotometros2.at[i,"QF_VIIRS_M12"]=np.array(h5file['HDFEOS']['GRIDS']['VNP_Grid_DNB']['Data Fields']['QF_VIIRS_M12'])[int(r['Pos_1'])][int(r['Pos_2'])]
                #Fotometros2.at[i,"QF_VIIRS_M13"]=np.array(h5file['HDFEOS']['GRIDS']['VNP_Grid_DNB']['Data Fields']['QF_VIIRS_M13'])[int(r['Pos_1'])][int(r['Pos_2'])]
                #Fotometros2.at[i,"QF_VIIRS_M15"]=np.array(h5file['HDFEOS']['GRIDS']['VNP_Grid_DNB']['Data Fields']['QF_VIIRS_M15'])[int(r['Pos_1'])][int(r['Pos_2'])]
                #Fotometros2.at[i,"QF_VIIRS_M16"]=np.array(h5file['HDFEOS']['GRIDS']['VNP_Grid_DNB']['Data Fields']['QF_VIIRS_M16'])[int(r['Pos_1'])][int(r['Pos_2'])]
                #Fotometros2.at[i,"Moon_Phase_Angle"]=np.array(h5file['HDFEOS']['GRIDS']['VNP_Grid_DNB']['Data Fields']['Moon_Phase_Angle'])[int(r['Pos_1'])][int(r['Pos_2'])]
                #Fotometros2.at[i,"Moon_Illumination_Fraction"]=np.array(h5file['HDFEOS']['GRIDS']['VNP_Grid_DNB']['Data Fields']['Moon_Illumination_Fraction'])[int(r['Pos_1'])][int(r['Pos_2'])]
                #Fotometros2.at[i,"Granule"]=np.array(h5file['HDFEOS']['GRIDS']['VNP_Grid_DNB']['Data Fields']['Granule'])[int(r['Pos_1'])][int(r['Pos_2'])]
    DNB_BRDF_Corrected_NTL=Fotometros2['DNB_BRDF-Corrected_NTL'].replace({65535:np.nan})*0.1 #Cambio nulos
    Gap_Filled_DNB_BRDF_Corrected_NTL=Fotometros2['Gap_Filled_DNB_BRDF-Corrected_NTL'].replace({65535:np.nan})*0.1 #Cambio nulos
    DNB_Lunar_Irradiance=Fotometros2['DNB_Lunar_Irradiance'].replace({65535:np.nan})*0.1 #Cambio nulos
    Mandatory_Quality_Flag=Fotometros2['Mandatory_Quality_Flag'].replace({255:np.nan,0:'Alta',1:'Alta',2:'Baja'}) #Cambio nulos
    Latest_High_Quality_Retrieval_number_days=Fotometros2['Latest_High_Quality_Retrieval'].replace({255:np.nan}) #Cambio nulos
    Snow_Flag=Fotometros2['Snow_Flag'].replace({255:np.nan,0:'Sin_nieve',1:'Nieve'}) #Cambio nulos
    #DNB_At_Sensor_Radiance=Fotometros2['DNB_At_Sensor_Radiance_500m'].replace({65535:np.nan})*0.1 #Cambio nulos
    #Sensor_Zenith=Fotometros2['Sensor_Zenith'].replace({-32768:np.nan})*0.01#Cambio nulos
    #Sensor_Azimuth=Fotometros2['Sensor_Azimuth'].replace({-32768:np.nan})*0.01#Cambio nulos
    #Solar_Zenith=Fotometros2['Solar_Zenith'].replace({-32768:np.nan})*0.01#Cambio nulos
    #Solar_Azimuth=Fotometros2['Solar_Azimuth'].replace({-32768:np.nan})*0.01#Cambio nulos
    #Lunar_Zenith=Fotometros2['Lunar_Zenith'].replace({-32768:np.nan})*0.01#Cambio nulos
    #Lunar_Azimuth=Fotometros2['Lunar_Azimuth'].replace({-32768:np.nan})*0.01#Cambio nulos
    #Glint_Angle=Fotometros2['Glint_Angle'].replace({-32768:np.nan})*0.01#Cambio nulos
    #UTC_Time=Fotometros2['UTC_Time'].replace({-999.9:np.nan})#Cambio nulos
    #QF_DNB=Fotometros2['QF_DNB'].replace({65535:np.nan,0:'High',1:'Substitute_Cal',2:'Out_of_Range',4:'Saturation',8:'Temp_not_Nominal',16:'Stray_light',256:'Bowtie_Deleted',512:'Missing_EV',1024:'Cal_Fail',2048:'Dead_Detector'})#Cambio nulos
    #Radiance_M10=Fotometros2['Radiance_M10'].replace({65535:np.nan})*0.0013#Cambio nulos
    #Radiance_M11=Fotometros2['Radiance_M11'].replace({65535:np.nan})*0.00058#Cambio nulos
    #BrightnessTemperature_M12=Fotometros2['BrightnessTemperature_M12'].replace({65535:np.nan})*0.0025#Cambio nulos
    #BrightnessTemperature_M13=Fotometros2['BrightnessTemperature_M13'].replace({65535:np.nan})*0.0025#Cambio nulos
    #BrightnessTemperature_M15=Fotometros2['BrightnessTemperature_M15'].replace({65535:np.nan})*0.0041#Cambio nulos
    #BrightnessTemperature_M16=Fotometros2['BrightnessTemperature_M16'].replace({65535:np.nan})*0.0043#Cambio nulos
    #QF_VIIRS_M10=Fotometros2['QF_VIIRS_M10'].replace({65535:np.nan,0:'High',1:'Substitute_Cal',2:'Out_of_Range',4:'Saturation',8:'Temp_not_Nominal',16:'Stray_light',256:'Bowtie_Deleted',512:'Missing_EV',1024:'Cal_Fail',2048:'Dead_Detector'})#Cambio nulos
    #QF_VIIRS_M11=Fotometros2['QF_VIIRS_M11'].replace({65535:np.nan,0:'High',1:'Substitute_Cal',2:'Out_of_Range',4:'Saturation',8:'Temp_not_Nominal',16:'Stray_light',256:'Bowtie_Deleted',512:'Missing_EV',1024:'Cal_Fail',2048:'Dead_Detector'})#Cambio nulos
    #QF_VIIRS_M12=Fotometros2['QF_VIIRS_M12'].replace({65535:np.nan,0:'High',1:'Substitute_Cal',2:'Out_of_Range',4:'Saturation',8:'Temp_not_Nominal',16:'Stray_light',256:'Bowtie_Deleted',512:'Missing_EV',1024:'Cal_Fail',2048:'Dead_Detector'})#Cambio nulos
    #QF_VIIRS_M13=Fotometros2['QF_VIIRS_M13'].replace({65535:np.nan,0:'High',1:'Substitute_Cal',2:'Out_of_Range',4:'Saturation',8:'Temp_not_Nominal',16:'Stray_light',256:'Bowtie_Deleted',512:'Missing_EV',1024:'Cal_Fail',2048:'Dead_Detector'})#Cambio nulos
    #QF_VIIRS_M15=Fotometros2['QF_VIIRS_M15'].replace({65535:np.nan,0:'High',1:'Substitute_Cal',2:'Out_of_Range',4:'Saturation',8:'Temp_not_Nominal',16:'Stray_light',256:'Bowtie_Deleted',512:'Missing_EV',1024:'Cal_Fail',2048:'Dead_Detector'})#Cambio nulos
    #QF_VIIRS_M16=Fotometros2['QF_VIIRS_M16'].replace({65535:np.nan,0:'High',1:'Substitute_Cal',2:'Out_of_Range',4:'Saturation',8:'Temp_not_Nominal',16:'Stray_light',256:'Bowtie_Deleted',512:'Missing_EV',1024:'Cal_Fail',2048:'Dead_Detector'})#Cambio nulos
    #Moon_Phase_Angle=Fotometros2['Moon_Phase_Angle'].replace({-32768:np.nan})*0.01#Cambio nulos
    #Moon_Illumination_Fraction=Fotometros2['Moon_Illumination_Fraction'].replace({-32768:np.nan})*0.01#Cambio nulos
    #Granule=Fotometros2['Granule'].replace({255:np.nan})#Cambio nulos
    QF_Cloud_Mask=Fotometros2['QF_Cloud_Mask'].fillna(65535) #Cambiar los nulos por 65535 para que no de error al trabajar en bits
    QF_Cloud_Mask=["{:16b}".format(int(i)).replace(' ','0') for i in QF_Cloud_Mask] #Paso a binario
    Nulos=list(np.where(np.array(QF_Cloud_Mask)=='1111111111111111')[0]) #Identificacion de nulos
    Day_Night=[i[0] for i in QF_Cloud_Mask]
    Land_Water_Background=[i[1:4] for i in QF_Cloud_Mask]
    Cloud_Mask_Quality=[i[4:6] for i in QF_Cloud_Mask]
    Cloud_Detection_Results=[i[6:8] for i in QF_Cloud_Mask]
    Shadow_Detected=[i[8] for i in QF_Cloud_Mask]
    Cirrus_Detection=[i[9] for i in QF_Cloud_Mask]
    Snow_Surface=[i[10] for i in QF_Cloud_Mask]
    #introduccion nulos
    for i in Nulos:
        Day_Night[i]=np.nan
        Land_Water_Background[i]=np.nan
        Cloud_Mask_Quality[i]=np.nan
        Cloud_Detection_Results[i]=np.nan
        Shadow_Detected[i]=np.nan
        Cirrus_Detection[i]=np.nan
        Snow_Surface[i]=np.nan
    New_fotometros=pd.DataFrame({'name':Fotometros2['name'],'DNB_BRDF_Corrected_NTL':DNB_BRDF_Corrected_NTL,'Gap_Filled_DNB_BRDF_Corrected_NTL':Gap_Filled_DNB_BRDF_Corrected_NTL,'DNB_Lunar_Irradiance':DNB_Lunar_Irradiance,'Mandatory_Quality_Flag':Mandatory_Quality_Flag,'Latest_High_Quality_Retrieval_number_days':Latest_High_Quality_Retrieval_number_days,'Snow_Flag':Snow_Flag,'Day_Night':Day_Night,'Land_Water_Background':Land_Water_Background,'Cloud_Mask_Quality':Cloud_Mask_Quality,'Cloud_Detection_Results':Cloud_Detection_Results,'Shadow_Detected':Shadow_Detected,'Cirrus_Detection':Cirrus_Detection,'Snow_Surface':Snow_Surface})    
    #New_fotometros=pd.DataFrame({'name':Fotometros2['name'],'DNB_At_Sensor_Radiance':DNB_At_Sensor_Radiance,'Sensor_Zenith':Sensor_Zenith,'Sensor_Azimuth':Sensor_Azimuth,'Solar_Zenith':Solar_Zenith,'Solar_Azimuth':Solar_Azimuth,'Lunar_Zenith':Lunar_Zenith,'Lunar_Azimuth':Lunar_Azimuth,'Glint_Angle':Glint_Angle,'UTC_Time':UTC_Time,'QF_DNB':QF_DNB,'Radiance_M10':Radiance_M10,'Radiance_M11':Radiance_M11,'BrightnessTemperature_M12':BrightnessTemperature_M12,'BrightnessTemperature_M13':BrightnessTemperature_M13,'BrightnessTemperature_M15':BrightnessTemperature_M15,'BrightnessTemperature_M16':BrightnessTemperature_M16,'QF_VIIRS_M10':QF_VIIRS_M10,'QF_VIIRS_M11':QF_VIIRS_M11,'QF_VIIRS_M12':QF_VIIRS_M12,'QF_VIIRS_M13':QF_VIIRS_M13,'QF_VIIRS_M15':QF_VIIRS_M15,'QF_VIIRS_M16':QF_VIIRS_M16,'Moon_Phase_Angle':Moon_Phase_Angle,'Moon_Illumination_Fraction':Moon_Illumination_Fraction,'Granule':Granule,'Day_Night':Day_Night,'Land_Water_Background':Land_Water_Background,'Cloud_Mask_Quality':Cloud_Mask_Quality,'Cloud_Detection_Results':Cloud_Detection_Results,'Shadow_Detected':Shadow_Detected,'Cirrus_Detection':Cirrus_Detection,'Snow_Surface':Snow_Surface})
    New_fotometros['Day_Night']=New_fotometros['Day_Night'].replace({'0':'Night','1':'Day'}) #Cambio a categorica
    New_fotometros['Land_Water_Background']=New_fotometros['Land_Water_Background'].replace({'000':'Land_desert','001':'Land_no_desert','010':'Inland_water','011':'Sea_water','101':'Coastal'}) #Cambio a categorica
    New_fotometros['Cloud_Mask_Quality']=New_fotometros['Cloud_Mask_Quality'].replace({'00':'Poor','01':'Low','10':'Medium','11':'High'})
    New_fotometros['Cloud_Detection_Results']=New_fotometros['Cloud_Detection_Results'].replace({'00':'Confident_clear','01':'Probably_clear','10':'Probably_cloudy','11':'Confident_cloudy'})
    New_fotometros['Shadow_Detected']=New_fotometros['Shadow_Detected'].replace({'0':'No','1':'Yes'}) #Cambio a categorica
    New_fotometros['Cirrus_Detection']=New_fotometros['Cirrus_Detection'].replace({'0':'No_cloud','1':'Cloud'}) #Cambio a categorica
    New_fotometros['Snow_Surface']=New_fotometros['Snow_Surface'].replace({'0':'No_snow','1':'Snow'}) #Cambio a categorica
    New_fotometros.replace({None:np.nan})
    New_fotometros['Fecha']=datetime.strptime(str(ano)+','+str(dia),'%Y,%j')
    #hora=np.floor(New_fotometros['UTC_Time'])
    #minutos=np.floor((New_fotometros['UTC_Time']-hora)*60)
    #segundos=((New_fotometros['UTC_Time']-hora)*60-minutos)*60
    #FECHAS=[]
    #for i in range(0,len(New_fotometros)):
        #try:
            #fecha=datetime(New_fotometros['Fecha'][i].year,New_fotometros['Fecha'][i].month,New_fotometros['Fecha'][i].day, int(hora[i]),int(minutos[i]),int(segundos[i]))
        #except:
            #fecha=np.nan
        #FECHAS=FECHAS+[fecha]
    #New_fotometros['Fecha']=FECHAS
    #New_fotometros.drop(['UTC_Time'],axis=1)
    #return Fotometros2
    print(New_fotometros)
    return New_fotometros

#Realiza la descarga y el procesamiento devolviendo el dataset del dia y ano pedido
def Descarga_resumir(ano,dia):
    carpeta=output+'/ano_'+str(ano)+'/dia_'+dia_format(dia)
    exito=False
    #Realizará los intentos que sean necesarios hasta lograr la descarga
    while exito==False:
        #Borrará aquellos que se han descargado mal
        try:
            c=os.listdir(carpeta)
            for i in c:
                if os.stat(carpeta+'/'+i).st_size==0:
                    os.remove(carpeta+'/'+i)
        except:
            print('Ano: '+str(ano)+'    Dia: '+str(dia))
        try:
            Descarga(ano,dia)
            exito=True
        except:
            exito=False
            time.sleep(1)
            print('Se ha cortado, siguiente intento')
            f = open (output+'/Historial_caidas.txt','a')
            date = str(datetime.now())
            f.write('\n'+'Se ha cortado a: '+date)
            f.close()           
    Dataset=Data_sat(ano,dia)
    rmtree(carpeta)
    return Dataset

#Descarga los datos entre dos fechas, ano, dia del ano
def DESCARGA_RESUMIR(inicio_ano,fin_ano,inicio_dia=1,fin_dia=365):
    #Mira hasta que dia y ano ya han sido descargados
    try:
        Registros=pd.read_csv(output+"/Registros_satelite.csv")
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
                Registros=pd.read_csv(output+"/Registros_satelite.csv")
            except:
                Registros=pd.DataFrame()
            Dataset=Descarga_resumir(i,ii)
            Registros_nuevos=pd.concat([Registros,Dataset])
            Registros_nuevos.to_csv(output+"/Registros_satelite.csv", index = False)

if day_from and day_to:
    DESCARGA_RESUMIR(year_from,year_to,day_from,day_to)
elif day_from and not day_to:
    DESCARGA_RESUMIR(year_from,year_to,day_from)
elif not day_from and day_to:
    DESCARGA_RESUMIR(year_from,year_to,1,day_to)
else:
     DESCARGA_RESUMIR(year_from,year_to)
