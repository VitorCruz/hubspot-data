import requests
import json
import pandas as pd
from hubspot import HubSpot
from dateutil import parser
import hubspot
from datetime import datetime
import pytz


## GET API KEY IN MY HD
with open('C:\\Users\\vitorc\\Desktop\\Celcoin\\api-key.txt') as f:
    api_key = f.readline()

### CHOOSE OBJECT TYPE AND PROPERTY WITH HISTORY (LOG OF CHANGES IN THIS PROPERTY)        
prop_with_history = ["status_celcoin"]
object_type = 'contacts'
            

def main():
    ## GET FILTERED CONTACTS
    list_objects = []

    after = 1
    while int(after) > 0:
        x = searchContacts(after).json()
        for object in x["results"]:
            list_objects.append(object["id"])
        try:
            after = int(x["paging"]["next"]["after"])
        except:
            after = 0   

    ## PUT IN JSON FORMAT TO SEARCH INFO IN BATCH     
    obj_dict_list = []
    for i in list_objects:
        dict_aux = {"id": i}    
        obj_dict_list.append(dict_aux)   


    ## SLICE LIST BECAUSE OF 50 OBJECTS LIMIT PER REQUEST
    ## NUMBER OF SLICES
    divide = 1
    if len(obj_dict_list) / 50 > 0:
        divide += int(len(obj_dict_list) / 50)  

    lista_obj_final = []

    ## SEARCH IN BATCH
    start = 0
    while (start / 50 < divide):
        try:
            objects_req = getObjectsBatch(obj_dict_list[start:start+50])
            objects_req.raise_for_status()

            for i in objects_req.json()["results"]:
                lista_obj_final.append(i)        
        except:
            print("Request Error")

        start += 50 


    dict_final_info = {}

    ## IT'S ALREADY IN DESCENDING ORDER, SO I'LL BREAK AFTER 1 INSERT -- INSERTING ONLY THE LAST DISQUALIFIED DATE
    for object in lista_obj_final:
        for history in object["propertiesWithHistory"]["status_celcoin"]:
            if(history["value"] == 'Não qualificado'):
                dict_final_info[object["id"]] = history["timestamp"]
                break

    ## UPDATE DISQUALIFIED DATE == TIMESTAMP IN MILLIS AND UTM MIDNIGHT -- DAMN HUBSPOT
    for contact in dict_final_info:    
        date = dict_final_info[contact][:10]
        date = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=pytz.UTC)   
        date = int(datetime.timestamp(date))* 1000    
        updateContact(contact, date)
        print(contact, date)          



## SEARCH CONTACTS TO UPDATE   
def searchContacts(after):
    url = f"https://api.hubapi.com/crm/v3/objects/{object_type}/search?hapikey={api_key}"
    payload = json.dumps(
        {"filterGroups":[
            {"filters":[
                {"values": ["Não qualificado"]
                 ,"propertyName":"status_celcoin"
                 ,"operator":"IN"
                }
            ]},
            ]
        ,"sorts":["ASCENDING"]
        ,"properties":["createdate","proprietario_celcoin","name","status_celcoin"]
        ,"limit":100
        ,"after": after
        })
    headers = {
        'accept': "application/json",
        'content-type': "application/json"
        }
    return requests.request("POST", url, data=payload, headers=headers)


## GET HISTORY OF THOSE CONTACTS
def getObjectsBatch(input):
    url = f"https://api.hubapi.com/crm/v3/objects/{object_type}/batch/read?hapikey={api_key}"
    payload = json.dumps({"inputs": input
                         ,"propertiesWithHistory": prop_with_history
                         })
    headers = {
        'accept': "application/json",
        'content-type': "application/json"
        }

    return requests.request("POST", url, data=payload, headers=headers)

 
## UPDATE CONTACT PROPERTY WITH LAST DISQUALIFIED DATE    
def updateContact(id, date):
    url = f"https://api.hubapi.com/contacts/v1/contact/vid/{id}/profile?hapikey={api_key}"
    data = json.dumps({
            "properties": [
                {
                    "property": "data_de_desqualificacao_celcoin",
                    "value": date              
                }
            ]
        })
    headers = {'Content-Type': 'application/json'}
    return requests.post(url=url, headers=headers, data=data)    


if __name__ == "__main__":
    main()
    