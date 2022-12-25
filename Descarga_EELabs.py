#Pasos a seguir:
#1) Descargar el repositorio y descomprimirlo en una carpeta dentro de Respositorio
#2)Copiar el archivo credentials.json de Respositorio a Repositorio\eelabs_portal-master\eelabs_portal\resources_folder creando la carpeta \google y pegar dentro
#3) Activar el entorno en cmd de anaconda pront con: conda activate EELab
# 4)Desplazarse con a: cd C:\Users\borja\Desktop\TODO\Mis_documentos\Trabajo\Investigador_universidad_Las_Palmas\Proyecto_fotometros_TFM\Repositorio\eelabs_portal-master\eelabs_portal
#5) Meter tess_borja y Skyglow_borja en utils
#6) Ejecutar allí
#Descarga todos los datos entre unas fechas determiandas, estaciones, y filtros elegidos. Necesita de cargar el .env y de tener activa la VPN del IAC
# Ejecucion ejemplo: python Descarga_EELabs.py --env C:\Users\borja\Desktop\TODO\Mis_documentos\Trabajo\Investigador_universidad_Las_Palmas\Proyecto_fotometros_TFM\Repositorio\env.env --output C:\Users\borja\Desktop\TODO\Mis_documentos\Trabajo\Investigador_universidad_Las_Palmas\Proyecto_fotometros_TFM\Datos_CSVs --filter [sol]
import dotenv as e
from utils.devices_api.eelabs_devices_api import EELabsDevicesApi
from utils.devices.tess_borja import TESS
from utils.devices.skyglow_borja import SkyGlow
from utils.devices.sqm import SQM
from utils.devices.astmon import ASTMON
import pandas as pd
import numpy as np
from datetime import datetime
from utils.my_utils import Utils

import config as conf #Variables de configuración, consultar para cambiar
from utils.filter import Filter
from datetime import date

import argparse
import os

#Variables de entrada del script
parser = argparse.ArgumentParser()
parser.add_argument('--f', '--from',  type=int, help='Year from')
parser.add_argument('--to',  type=int, help='Year to')
parser.add_argument('--env', required=True, type=str, help='Environment filename')
parser.add_argument('--out','--output', required=True, type=str, help='Output filename')
parser.add_argument('--filter', type=str, help='Data filtering: sol, luna, nubes, galaxia, zodiacal, sigma. Format example: [sol,galaxia,zodaical] Write all without brackets for all filters. Format example: all')
parser.add_argument('--station',type=str,help='Format exalmple: [LPL1_001,LPL2_033,stars1] rite all without brackets for ones station. Format example: stars1')
parser.add_argument('--ephemeris',type=bool,help='True for ephemeris included')
args = parser.parse_args()
ano_ini = args.f
ano_fin= args.to
env=args.env
output=args.out
filtro=args.filter
efemerides=args.ephemeris
if filtro:
    if filtro[0]=='[':
        filtro=filtro[1:-1].split(',')
    else:
        filtro=filtro
else:
    filtro=[]
estaciones_elegidas=args.station
if estaciones_elegidas:
    if estaciones_elegidas[0]=='[':
        estaciones_elegidas=set(estaciones_elegidas[1:-1].split(','))
    else:
        estaciones_elegidas=set([estaciones_elegidas])


e.load_dotenv(env)



#Crear carpeta donde se guardará todo
output=output+'\Datos_fotometros'
if not os.path.exists(output):
    os.mkdir(output)


#Listado fotómetros LPL
fotometros=EELabsDevicesApi().get_all_devices_info()
fotometros=pd.DataFrame(fotometros)
fotometros=fotometros.drop(['sg_type','lpl','zero_point','filters','mov_sta_position','local_timezone','location','info_img','info_tess','place','tester','info_org','__v','latitude','longitude','country','city'],axis=1)
localizacion=pd.DataFrame(list(fotometros['info_location'])).drop(['latitude_hide','longitude_hide'],axis=1)
fotometros['place']=localizacion['place']
fotometros['town']=localizacion['town']
fotometros['sub_region']=localizacion['sub_region']
fotometros['region']=localizacion['region']
fotometros['country']=localizacion['country']
fotometros['latitude']=localizacion['latitude']
fotometros['longitude']=localizacion['longitude']
fotometros['elevation']=localizacion['elevation']
fotometros=fotometros.drop(['info_location'],axis=1)
#fotometros.to_csv(r'C:\Users\borja\Desktop\TODO\Mis_documentos\Trabajo\Investigador_universidad_Las_Palmas\Proyecto_fotometros_TFM\Datos_CSVs\Todos_fotometros.csv', index = False)
fotometros.to_csv(output+'\Todos_fotometros.csv', index = False)

#Crear carpeta para guardar registros en caso de no existir 
if not os.path.exists(output+'\Registros'):
    os.mkdir(output+'\Registros')



#Clasifica la estacion
def Estacion(estacion): #Obtiene la clase estación independientemente de su tipología 
    #fotometros=pd.read_csv(r'C:\Users\borja\Desktop\TODO\Mis_documentos\Trabajo\Investigador_universidad_Las_Palmas\Proyecto_fotometros_TFM\Datos_CSVs\Todos_fotometros.csv')
    fotometros=pd.read_csv(output+'\Todos_fotometros.csv')
    tipo=fotometros[fotometros['name']==estacion]['TYPE'].values[0]
    if tipo==TESS.TYPE:
        device_obj=TESS(name=estacion)
    elif tipo==SkyGlow.TYPE:
        device_obj=SkyGlow(name=estacion)
    elif tipo==SQM.TYPE:
        device_obj=SQM(name=estacion)
    elif tipo==ASTMON.TYPE:
        device_obj=ASTMON(name=estacion)
    return device_obj
#Obteines los datos filtrados para una estación y año
def Datos(estacion,ano,filtro): #Filtro vector de datos como ['sol','luna'] por ejemplo
    device_obj=Estacion(estacion)
    FIRST_DATE=pd.Timestamp(datetime(ano, 1, 1, 0, 0), tz='UTC')
    LAST_DATE=pd.Timestamp(datetime(ano+1, 1, 1, 0, 0), tz='UTC')
    df_all=None
    try:
        df_all = device_obj.get_all_data(date_from=FIRST_DATE, date_to=LAST_DATE,force=True)
        No_data=False
    except:
        df_all=None
        No_data=True
    if No_data:
        print('La estacion '+estacion+' no ha respondido por fallo')
    df_all=df_all[(df_all['mag']>conf.MAG_MIN) & (df_all['mag']<conf.MAG_MAX)] #Filtro por si acaso
    if __name__ == '__main__':
        df_all = Utils().add_ephems(df_all, device_obj.getObserver(), parallelize=False) # No se ha seleccionado la opción de multiprocesamiento
    V=[]
    if 'sol' in filtro or filtro=='all':
        df_all = Filter().filter_sun(df_all, max_sun_alt=conf.SUN_ALT_MAX)
    else:
        df_filter=Filter().filter_sun(df_all, max_sun_alt=conf.SUN_ALT_MAX)
        F=np.array([True]*(df_all.index[-1]+1)) #Vector con todo True con todos los índices
        F[df_filter.index]=False #Cambia por False los indices que quedan tras ser filtrados
        df_all['sol']=F[df_all.index] #Coge los datos según el índice original
        V=V+['sol']
    if 'luna' in filtro or filtro=='all':
        df_all = Filter().filter_moon(df_all, max_moon_alt=conf.MOON_ALT_MAX)
    else:
        df_filter=Filter().filter_moon(df_all, max_moon_alt=conf.MOON_ALT_MAX)
        F=np.array([True]*(df_all.index[-1]+1))
        F[df_filter.index]=False
        df_all['luna']=F[df_all.index]
        V=V+['luna']
    if 'nubes' in filtro or filtro=='all':
        clouds_threshold=conf.CLOUD_STD_FREQ
        df_all = Filter().filter_column(df_all, device_obj.getMagSTDColname(), max=clouds_threshold)
    else:
        clouds_threshold=conf.CLOUD_STD_FREQ
        df_filter=Filter().filter_column(df_all, device_obj.getMagSTDColname(), max=clouds_threshold)
        F=np.array([True]*(df_all.index[-1]+1))
        F[df_filter.index]=False
        df_all['nubes']=F[df_all.index]
        V=V+['nubes']
    if 'galaxia' in filtro or filtro=='all':
        df_all = Filter().filter_galactic_abs_lat(df_all, min_lat=conf.GALACTIC_LAT_MIN, max_lat=180)
    else:
        df_filter=Filter().filter_galactic_abs_lat(df_all, min_lat=conf.GALACTIC_LAT_MIN, max_lat=180)
        F=np.array([True]*(df_all.index[-1]+1))
        F[df_filter.index]=False
        df_all['galaxia']=F[df_all.index]
        V=V+['galaxia']
    if 'zodiacal' in filtro or filtro=='all':
        df_all = Filter().filter_column(df_all, col_name='ecliptic_f', max=conf.ECLIPTIC_F_MAX)
    else:
        df_filter=Filter().filter_column(df_all, col_name='ecliptic_f', max=conf.ECLIPTIC_F_MAX)
        F=np.array([True]*(df_all.index[-1]+1))
        F[df_filter.index]=False
        df_all['zodiacal']=F[df_all.index]
        V=V+['zodiacal']
    if 'sigma' in filtro or filtro=='all':
        sigma=conf.NSIGMA
        df_all = Filter().filter_nsigma(df_all, col_name='mag', sigma=sigma)
    else:
        sigma=conf.NSIGMA
        df_filter=Filter().filter_nsigma(df_all, col_name='mag', sigma=sigma)
        F=np.array([True]*(df_all.index[-1]+1))
        F[df_filter.index]=False
        df_all['sigma']=F[df_all.index]
        V=V+['sigma'] 
    if efemerides:
        df=pd.DataFrame({'time':df_all['time'],'mag':df_all['mag'],'name':estacion,'moon_phase':df_all['moon_phase'],'moon_alt':df_all['moon_alt'],'galactic_lat':df_all['galactic_lat'],'galactic_lon':df_all['galactic_lon'],'helioecliptic_lon_abs':df_all['helioecliptic_lon_abs'],'ecliptic_lat_abs':df_all['ecliptic_lat_abs']})
    else:
        df=pd.DataFrame({'time':df_all['time'],'mag':df_all['mag'],'name':estacion}) #,'mag_err':df_all['mag_err']
    for ii in V:
        df[ii]=df_all[ii]
    return df
#Obtienes todos los datos buscados
def Coger_datos(V,ano_ini=None,ano_fin=None,iterar=True): #Iterar nos indica si queremos hacer que nos pida enter
    #Historial de estaciones registradas
    #Las que aparecen en los registros
    #Modificacion para la lectura de todos los csv de registros fruto del guardador en particones de 1 GB
    Estaciones_registradas=set()
    for j in range(0,1000):
        try:
            A=pd.read_csv(output+'\Registros\Registros_'+str(j)+'.csv')
            Estaciones_registradas=Estaciones_registradas|set(A['name'])
        except:
            Estaciones_registradas=Estaciones_registradas
    """
    try:
        A=pd.read_csv(output+'\Registros\Registros.csv')
        Estaciones_registradas=set(A['name'])
        Datos_previos=True
    except:
        Estaciones_registradas=set()
        Datos_previos=False
    """
    #Las que aparecen en el historial
    try:
        B=pd.read_csv(output+'\Historial_registradas.csv')
        Estaciones_registradas_lista=set(B['Estaciones'])
        existe_B=True
    except:
        Estaciones_registradas_lista=set()
        existe_B=False
    dif=Estaciones_registradas-Estaciones_registradas_lista
    Estaciones_registradas=Estaciones_registradas|Estaciones_registradas_lista
    print(Estaciones_registradas)
    Todos_fotometros_dataset=pd.read_csv(output+'\Todos_fotometros.csv')
    Todos_fotometros=set(Todos_fotometros_dataset['name'])
    if estaciones_elegidas:
        Estaciones_a_registrar=estaciones_elegidas-Estaciones_registradas
    else:
        Estaciones_a_registrar=Todos_fotometros-Estaciones_registradas #Para saber que estaciones tiene que registrar 
    n_estaciones_a_registrar=len(Estaciones_a_registrar)
    if ano_ini:
        ano_i=ano_ini
    else:
        ano_i=2010
    if ano_fin:
        ano_f=ano_fin
    else:
        ano_f=date.today().year
    #Ano_actual=date.today().year
    Registradas=[]
    v_vacio=[]
    v_tiempo=[]
    #Bucle donde va estación a estación y luego año a año
    for i in Estaciones_a_registrar:
        df=pd.DataFrame()
        vacio=True
        for ii in range(ano_i,ano_f+1): #PONER otro año para acotar
            try:
                #Cambio de linea para coger datos con diferentes filtros
                #dat=Datos(i,ii,'all')
                #dat=Datos(i,ii,['sol','luna','nubes','sigma'])
                dat=Datos(i,ii,V)
                #print(dat)
                df=pd.concat([df,dat])
                if list(dat.values)!=[]:
                    vacio=False
            except:
                df=df
            print('Año: '+str(ii))
        #Guardar
        #Guardado para con archivos limitados a 1 GB
        try:
            A=pd.read_csv(output+'\Registros\Registros_1.csv')
            Hay_registros=True
        except:
            df_final=df
            df_final.to_csv(output+'\Registros\Registros_1.csv', index = False)
            Hay_registros=False
        if Hay_registros==True:
            contador_2=0
            for j in range(1,1000):    
                try:
                    A=pd.read_csv(output+'\Registros\Registros_'+str(j)+'.csv')
                    if os.stat(output+'\Registros\Registros_'+str(j)+'.csv').st_size<1000000000:
                        df_final=pd.concat([A,df])
                        df_final.to_csv(output+'\Registros\Registros_'+str(j)+'.csv', index = False)
                        contador_2=1
                except:
                    if contador_2==0:
                        df_final=df
                        df_final.to_csv(output+'\Registros\Registros_'+str(j)+'.csv', index = False)
                        contador_2=1

        """
        try:
            A=pd.read_csv(output+'\Registros\Registros.csv')
            df_final=pd.concat([A,df])
            #print(df_final)
            df_final.to_csv(output+'\Registros\Registros.csv', index = False)
        except:
            df_final=df
            #print(df_final)
            df_final.to_csv(output+'\Registros\Registros.csv', index = False)
        """
        tiempo=datetime.now()
        v_vacio=v_vacio+[vacio]
        v_tiempo=v_tiempo+[tiempo]
        Registradas=Registradas+[i]
        Historial_registradas=pd.DataFrame({'Estaciones':Registradas,'Tiempo':v_tiempo,'Vacio':v_vacio})
        Historial_registradas_2=pd.DataFrame({'Estaciones':list(dif),'Tiempo':None,'Vacio':False})
        Historial=pd.concat([Historial_registradas_2,Historial_registradas])
        #Guardar historial
        if existe_B:
            Historial_2=pd.concat([B,Historial])
        else:
            Historial_2=Historial
        Historial_2.to_csv(output+'\Historial_registradas.csv', index = False)       
        n_estaciones_restantes=n_estaciones_a_registrar-len(Registradas)
        print('Quedan '+str(n_estaciones_restantes)+' estaciones por registrar')
        if iterar:
            if input('Cargada la estacion:'+i+'\n')=='exit': 
                break
        else:
            print('Cargada la estacion:'+i+'\n')
#Ejecutar proceso
Coger_datos(filtro,ano_ini,ano_fin,iterar=False)
#print(Datos('stars1',2022,['sol']))

