import logging
import datetime
logging.basicConfig(filename=f'./logs/log_{datetime.date.today().strftime("%d_%m_%Y")}.log', level=logging.INFO)

import os
import re
import json
import time
from datetime import date

from private import *
import requests
from sodapy import Socrata
client = Socrata("www.datos.gov.co", TOKEN,username=username, password=password)


#
def get_category(description,default):
    if description.find('|categoria:') != -1:
        start = description.find('|categoria:')+len('|categoria:')
        end = description.rfind('|')
        category = description[start:end].replace('\n',' ').replace('\t',' ')
    else:
        category = default
    return category

#fetch the info from the portal for ArcGIS
def fetch_portal_data(portal_url):
    '''from the url of the portal return the list of the valid datasets'''
    datasets_arcgis = requests.get(f'{portal_url}/data.json').json()['dataset']
    total_datasets = len(datasets_arcgis)
    print(f'Total datasets recieved {total_datasets}')
    logging.info(f'Total datasets recieved {total_datasets}')
    datasets = []

    for dataset in datasets_arcgis:
        data = {
            'url' : dataset['identifier'],
            'description':dataset['description']
            # 'Title' : dataset['title'],
            # 'Publisher' : dataset['publisher']['name'],
            # 'keyword': dataset['keyword'],
        }
        datasets.append(data)
    return datasets


#find the item in the goberment portal (doesnt work if its private, the auth dont makes the diference)
def get4b4id(id_agol):
    api_link_search = 'https://datos.gov.co/api/catalog/v1?Common-Core_Unique-Identifier'
    response = requests.get(f'{api_link_search}={id_agol}')#, auth=(username, password))
    results = response.json()['results']
    
    if len(results)>1:
        print('duplicated')
    fourByFour_id = results[0]['resource']['id']
    return fourByFour_id

#save to the data folder the previous and new data
def prepare_data(i):
    name = i['name'].replace(' ','_')
    logging.info(f'Starting process for {name}')
    print(f'Starting process for {name}')
    metadata = i['info']
    datasets = fetch_portal_data(metadata['portal_url'])
    default_cat = metadata['default_category']

    #compare with a previous data info
    if os.path.exists(f'data/{name}.json'):

        #save the previous
        with open(f'data/{name}.json') as f:
            datasets_id = json.load(f)
        prev_ids = [i['url'] for i in datasets_id]
        new_ids = [i['url'] for i in datasets]

        #save the common
        comun_ids = [i for i in prev_ids if i in new_ids]
        logging.info(f'\t{len(comun_ids)} datasets already in the list')
        print(f'{len(comun_ids)} datasets already in the list')

        #the new ids
        added_ids = [i for i in new_ids if i not in prev_ids]
        logging.info(f'\t{len(added_ids)} datasets new in the list')
        print(f'{len(added_ids)} datasets new in the list')


        if len(added_ids) == 0:
            # print(len(datasets_id.keys()))
            return datasets_id
        else:
            # new_file = comun_ids + added_ids
            new_file = [i for i in datasets_id if i['url'] in comun_ids]
            # new_file = {k:v for k,v in datasets_id.items() if k in comun_ids}
            added_ids_data = [i for i in datasets if i['url'] in added_ids]
            
            for _id in added_ids_data:
                data_reg = {}
                try:
                    data_reg['url'] = _id['url']
                    data_reg['id'] = get4b4id(_id['url'])
                    data_reg['category'] = get_category(_id['description'],default_cat)
                    new_file.append(data_reg)
                except:
                    url_id = _id['url']
                    logging.warning(f'\t {url_id} not in the data.gov.co portal')
                    continue
            with open(f'data/{name}.json', 'w') as json_file:
                json.dump(new_file, json_file)
            # print(len(new_file.keys()))
            return new_file

        # print(prev_ids[0],new_ids[0])

    #if the file doesn´t exist create a file
    else:
        logging.warning(f'\t File not founded creating a new file')
        datasets_ids = []

        for dataset in datasets:
            data_reg = {}
            try:
                data_reg['url'] = dataset['url']
                data_reg['id'] = get4b4id(dataset['url'])
                data_reg['category'] = get_category(dataset['description'],default_cat)
                datasets_ids.append(data_reg)
            except:
                url_id = dataset['url']
                logging.warning(f'\t {url_id} not in the data.gov.co portal')
                continue
        else:
            with open(f'data/{name}.json', 'w') as json_file:
                json.dump(datasets_ids, json_file)
            return datasets_ids


        print(datasets_ids)

''' because the update metadata overwrites the metadata this select the previous 
and add the info of the organization and the info to a dict to upgrade in the metadata '''

def transform_metadata(uid, category , entity_info= {}):
    
    try:
        metadata_request = requests.get(f'https://www.datos.gov.co/api/views/{uid}.json',auth=(username, password)).json()
        dict_metadata = metadata_request['metadata']
    except Exception as e:
        uid_id = uid['id']
        logging.error(f'Cant find {uid_id} in datos.gov')
        
        
    # dict_metadata = client.get_metadata(uid)['metadata']
    # Municipio = 
    # Nombre_de_la_Entidad = 
    # Orden = 
    # categoria = 'Ambiente y Desarrollo Sostenible'
    # Nombre_de_la_Entidad = 'Corporación Autónoma Regional de Cundinamarca'

    dict_metadata['custom_fields']['Información de la Entidad'] = entity_info
                                                # {
                                                # 'Municipio': 'Útica',
                                                # 'Nombre de la Entidad': entity_name,
                                                # 'Orden': 'Territorial',
                                                # 'Sector': 'No Aplica',
                                                # 'Área o dependencia': 'Secretaria de Bienestar Social y Desarrollo Económico',
                                                # 'Departamento': 'Cundinamarca'
                                                # }

    # dict_metadata['custom_fields']['Información de Datos'] = {
    #                                             'Cobertura Geográfica': 'Municipal',
    #                                             'Idioma': 'Español',
    #                                             'Frecuencia de Actualización': 'Anual',
    #                                             'URL Documentación': 'http://utica-cundinamarca.gov.co',
    #                                             'Fecha Emisión (aaaa-mm-dd)': '2020-05-10',
    #                                             'URL Normativa': 'http://utica-cundinamarca.gov.co'
    #                                               }

    data = {
        "category":category,
        "metadata":dict_metadata
    }
    return data

def run ():
        # print(os.listdir())
    
    with open('info.json', encoding="utf8") as f:
        info = json.load(f)
        info = info[1:]
    
    total_registers = 0
    for ent in info:
        name_ent = ent['name']
        try:
            uids = prepare_data(ent)
        except:
            #print('error in ')
            logging.error(f'Errors founded with {name_ent}')
            continue
        success = 0
        failed = 0
        for uid in uids:
            #print(A)
            
            try:
                transformed_metadata = transform_metadata(uid['id'],category= uid['category'],entity_info = ent['info']['Información de la Entidad'])
                logging.info(f'\t\tData prepared for {uid}')
                client.update_metadata(uid['id'], transformed_metadata)
                logging.info(f'\t\tData updated for {uid}')
                success = success+1
            except:
                try:
                    time.sleep(4)
                    transformed_metadata = transform_metadata(uid['id'],category= uid['category'],entity_info = ent['info']['Información de la Entidad'])
                    logging.info(f'\t\tData prepared for {uid}')
                    client.update_metadata(uid['id'], transformed_metadata)
                    logging.info(f'\t\tData updated for {uid}')
                    success = success+1
                except Exception as e:
                    logging.error(f'\t\tCant update for {uid}, not founded public in datos.gov.co')
                    failed = failed +1
                    continue
        else:
            logging.info(f'\t{success} registers for {name_ent} succesfully updated with errors in {failed} registers')
            total_registers = total_registers + success +failed
    return total_registers
    
    

if __name__ == "__main__":
    begin_time = datetime.datetime.now()
    
    total_registers = run()

    time_final = datetime.datetime.now() - begin_time
    logging.info(f'The script takes {time_final} to run a total of {total_registers} registers')


