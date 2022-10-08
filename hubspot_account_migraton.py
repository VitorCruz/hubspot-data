import requests
import json
import datetime
import mysql.connector
import pandas as pd
from hubspot import HubSpot
from dateutil import parser
import os


###### HUBSPOT ACCOUNT MIGRATION 
###### CELCOIN TO GALAX PAY'S ACCOUNT (USING SAME ACCOUNT FOR BOTH BUSINESS UNITS)

## API KEYS TO HUBSPOT
with open('C:\\Users\\vitorc\\Desktop\\Celcoin\\api-key.txt') as f:
    api_key_galax = f.readline()

with open('C:\\Users\\vitorc\\Desktop\\Celcoin\\api-key-celcoin.txt') as f:
    api_key_celcoin = f.readline()
    
    
## GET ALL ENGAGEMENTS FROM BOTH ACCOUNTS
def getActivitiesCelcoinAcc(offset):    
    url = f"https://api.hubapi.com/engagements/v1/engagements/paged?limit=250&offset={offset}&hapikey={api_key_celcoin}"
    payload = ""
    headers = {}
    return requests.request("GET", url, headers=headers, data=payload)

def getActivitiesGalaxAcc(offset):    
    url = f"https://api.hubapi.com/engagements/v1/engagements/paged?limit=250&offset={offset}&hapikey={api_key_galax}"
    payload = ""
    headers = {}
    return requests.request("GET", url, headers=headers, data=payload)


offset = 1
hasMore = 1
eng_lista = []

while(hasMore == 1):
    z = getActivitiesGalaxAcc(offset)       
    for i in z.json()["results"]:
        eng_lista.append(i)
        
    offset = z.json()["offset"]
    if(z.json()["hasMore"]) == False:
        hasMore = 0

## SEPARAR TIPOS DE ATIVIDADES
lista_notes = []
lista_tasks = []
lista_meetings = []
lista_email = []
lista_inc_email = []
lista_call = []

for i in eng_lista:
    if(i["engagement"]["type"] == 'NOTE'):
        lista_notes.append(i)
        
for i in eng_lista:
    if(i["engagement"]["type"] == 'TASK'):
        lista_tasks.append(i)
        
for i in eng_lista:
    if(i["engagement"]["type"] == 'MEETING'):
        lista_meetings.append(i)
                
for i in eng_lista:
    if(i["engagement"]["type"] == 'EMAIL'):
        lista_email.append(i)       

for i in eng_lista:
    if(i["engagement"]["type"] == 'INCOMING_EMAIL'):
        lista_inc_email.append(i)   

for i in eng_lista:
    if(i["engagement"]["type"] == 'CALL'):
        lista_call.append(i)  


#####DE-PARAS DE IDS DA CONTA
owner_dict = {}
user_dict = {}

### PEGAR OWNERS DA CONTA
url = f"https://api.hubapi.com/owners/v2/owners?hapikey={api_key_celcoin}"
payload = ""
headers = {}
ow = requests.request("GET", url, headers=headers, data=payload)

### PEGAR USERS DA CONTA
url = f"https://api.hubapi.com/owners/v2/owners?hapikey={api_key_galax}"
payload = ""
headers = {}
ow2 = requests.request("GET", url, headers=headers, data=payload)

### INSERIR EM UM DICTIONARY OS PARES OWNER_ANTIGO > OWNER_NOVO E USER_ANTIGO > USER_NOVO
for i in ow.json():
    for x in ow2.json():
        if(i["firstName"] == x["firstName"] and i["lastName"] == x["lastName"]):            
            owner_dict[i["ownerId"]] = x["ownerId"]
            user_dict[i["userIdIncludingInactive"]] = x["userIdIncludingInactive"]       
            
### ADICIONAR USERS E OWNERS QUE NÃO EXISTEM MAIS NA CONTA NOVA, PARA O MEU ID (PARA NÃO QUEBRAR NADA)            
for i in ow.json():
    if(i["ownerId"] not in list(owner_dict.keys())):
        owner_dict[i["ownerId"]] = 145676288 

for i in ow.json():
    if(i["userIdIncludingInactive"] not in list(user_dict.keys())):
        user_dict[i["userIdIncludingInactive"]] = 28449591        

            
            
## IMPORTAR ARQUIVO COM CONTACTS -- IDS DA CONTA ANTIGA X CONTA NOVA
contact_relation = pd.read_csv('C:\\Users\\vitorc\\Desktop\\Celcoin\\migracao\\API_dados\\contacts_dados.csv', sep = ';')
contact_relation = contact_relation.set_index('Contact_old').to_dict()['Contact_new']

## IMPORTAR ARQUIVO COM COMPANYS -- IDS DA CONTA ANTIGA X CONTA NOVA
company_relation = pd.read_csv('C:\\Users\\vitorc\\Desktop\\Celcoin\\migracao\\API_dados\\company_dados.csv', sep = ';')
company_relation = company_relation.set_index('Company_old').to_dict()['Company_new']

## IMPORTAR ARQUIVO COM DEALS -- IDS DA CONTA ANTIGA X CONTA NOVA
deal_relation = pd.read_csv('C:\\Users\\vitorc\\Desktop\\Celcoin\\migracao\\API_dados\\deals_dados.csv', sep = ';')
deal_relation = deal_relation.set_index('Deal_old').to_dict()['Deal_new']

## IMPORTAR ARQUIVO COM CONTATOS CELCOIN - NÃO PASSAR HISTÓRICO DE E-MAILS
nao_importar = pd.read_csv('C:\\Users\\vitorc\\Desktop\\Celcoin\\migracao\\API_dados\\celcoin_naoimportar.csv', sep = ';')
nao_importar = list(nao_importar)


## FUNÇÕES PARA PEGAR OS IDS NOVOS, EM ALGUNS CASOS SÃO VÁRIOS IDS ATRIBUÍDOS EM UMA LISTA
def searchContacts(contacts, dic, tipo):
    
    resposta_contacts = []
    
    for i in contacts:
        if(tipo == 'INCOMING_EMAIL' or tipo == 'EMAIL'):
            if(i not in nao_importar):
                resposta_contacts.append(dic.get(int(i)))
        else:
            resposta_contacts.append(dic.get(int(i)))
        
    resposta_contacts = list(filter(None, resposta_contacts))    
    return resposta_contacts

def searchCompanies(companies, dic):
    
    resposta_companies = []

    for i in companies:
        resposta_companies.append(dic.get(int(i)))
    
    resposta_companies = list(filter(None, resposta_companies))    
    return resposta_companies

def searchDeals(deals, dic):
    
    resposta_deals = []

    for i in deals:
        resposta_deals.append(dic.get(int(i)))
    
    resposta_deals = list(filter(None, resposta_deals))   
    return resposta_deals

def searchOwners(owners, dic, tipo):      
    
    if(type(owners) == int):
        owners = [owners]
    
    resposta_owners = []

    for i in owners:
        resposta_owners.append(dic.get(int(i)))                
            
    resposta_owners = list(filter(None, resposta_owners))   
    return resposta_owners

def deleteEngagement(id):
    url = f"https://api.hubapi.com/engagements/v1/engagements/{id}?hapikey={api_key_galax}"
    payload = json.dumps({})
    headers = {'Content-Type': 'application/json'}
    requests.request("DELETE", url, headers=headers, data=payload)
    
def deleteDealAssociation(deal,company):
    url = f"https://api.hubapi.com/deals/v1/deal/{deal}/associations/COMPANY?id={company}&hapikey={api_key_galax}"
    payload = json.dumps({})
    headers = {'Content-Type': 'application/json'}
    requests.request("DELETE", url, headers=headers, data=payload)
    
def dealAssociation(dealId, contactId):
    url = f"https://api.hubapi.com/crm-associations/v1/associations?hapikey={api_key_galax}"
    payload = json.dumps(
        {
      "fromObjectId": dealId,
      "toObjectId": contactId,
      "category": "HUBSPOT_DEFINED",
      "definitionId": 3
        })
    headers = {'Content-Type': 'application/json'}
    requests.put(url, headers=headers, data=payload)
    
def getEngagementGalaxAcc(engagementId):
    url = f"https://api.hubapi.com/engagements/v1/engagements/{engagementId}?hapikey={api_key_galax}"
    payload = json.dumps({})
    headers = {'Content-Type': 'application/json'}
    return requests.request("GET", url = url, headers=headers, data=payload)     
    
def getEngagementCelcoinAcc(engagementId):
    url = f"https://api.hubapi.com/engagements/v1/engagements/{engagementId}?hapikey={api_key_galax}"
    payload = json.dumps({})
    headers = {'Content-Type': 'application/json'}
    return requests.request("GET", url = url, headers=headers, data=payload)   

    
    
list_response = []
ids_response = []

################################################################################################################################
###################################################### INSERT ACTIVITIES ######################################################
################################################################################################################################

i = 0  
while(i < len(lista_notes)):
    note = lista_notes[i]
    i += 1
    dict_inserir = {}

    try:
        dict_inserir['active'] = str(note["engagement"]["active"])
    except:
        active = ''
    try:
        dict_inserir['ownerId']  = searchOwners(note["engagement"]["ownerId"], owner_dict, note["engagement"]["type"])[0]
    except:
        ownerId  = '' 
    try:
        dict_inserir['createdBy'] = user_dict.get(note["engagement"]["createdBy"])  
    except:
        createdBy = ''   
    try:
        dict_inserir['modifiedBy'] = user_dict.get(note["engagement"]["modifiedBy"])
    except:
        modifiedBy = ''      
    try:
        allAccessibleTeamIds = note["engagement"]["allAccessibleTeamIds"]                                     
    except:
        allAccessibleTeamIds = ''
    if(allAccessibleTeamIds == '8684816'):                                               
        dict_inserir['allAccessibleTeamIds'] = 9310301      
    try:
        teamId = note["engagement"]["teamId"]                                       
    except:
        teamId = '' 
    if(teamId == '8684816'):                                               
        dict_inserir['teamId'] = 9310301 
    try:
        dict_inserir['queueMembershipIds'] = note["engagement"]["queueMembershipIds"] 
    except:
        queueMembershipIds = '' 
    try:   
        dict_inserir['bodypreview'] = note["engagement"]["bodyPreview"]
    except:
        bodypreview = ''
    try:
        dict_inserir['bodyPreviewIsTruncated'] = note["engagement"]["bodyPreviewIsTruncated"]
    except:
        bodyPreviewIsTruncated = ''
    try:
        dict_inserir['gdprDeleted'] = str(note["engagement"]["gdprDeleted"])
    except:
        gdprDeleted = ''
    try:
        dict_inserir['source'] = note["engagement"]["source"]
    except:
        source = ''
    try:
        dict_inserir['sourceId'] = note["engagement"]["sourceId"]
    except:
        sourceId = ''
    try:
        dict_inserir['bodyPreviewHtml'] = note["engagement"]["bodyPreviewHtml"]
    except:
        bodyPreviewHtml = ''
    try:
        dict_inserir['activityType'] = note["engagement"]["activityType"] 
    except:
        activityType = ''
    try:
        dict_inserir['createdAt'] = note["engagement"]["createdAt"]
    except:
        createdAt = ''
    try:
        dict_inserir['lastUpdated'] = note["engagement"]["lastUpdated"]
    except:
        lastUpdated = ''    
    try:
        dict_inserir['timestamp'] = note["engagement"]["timestamp"]
    except:
        timestamp = ''    
    dict_inserir['portalId'] = 20745543                                                  
    dict_inserir['type'] = note["engagement"]["type"]    


    try:
        contactIds = searchContacts(note["associations"]["contactIds"], contact_relation, note["engagement"]["type"]) 
    except:
        contactIds = []
    try:
        companyIds = searchCompanies(note["associations"]["companyIds"], company_relation) 
    except:
        companyIds = []
    try:
        dealIds = searchDeals(note["associations"]["dealIds"], deal_relation) 
    except:
        dealIds = [] 
    try:
        ownerIds  = searchOwners(note["associations"]["ownerIds"], owner_dict, note["engagement"]["type"])  
    except:
        ownerIds  = []                                      


    url = "https://api.hubapi.com/engagements/v1/engagements"
    querystring = {"hapikey": api_key_galax} 
    payload = json.dumps({
    "engagement": dict_inserir,
    "associations": {
    'contactIds': contactIds,         
    'companyIds': companyIds,                 
    'dealIds': dealIds,                    
    'ownerIds': ownerIds,
    'workflowIds': [],
    'ticketIds': [],
    'contentIds': [],
    'quoteIds': []
    },    
    "attachments": note["attachments"],
    "metadata": note["metadata"]
    });
    headers = {'Content-Type': "application/json"}
    
    ## BACKUP
    if(len(ids_response)) % 100 == 0:
        with open('C:\\Users\\vitorc\\Desktop\\Celcoin\\migracao\\ids_inseridos.json', 'w') as f:
            json.dump(ids_response, f)      

    try:
        response = requests.request("POST", url, data=payload, headers=headers, params=querystring)
        ids_response.append(response.json()['engagement']['id'])
        print("Ok")
    except: 
        print("erro: EngagementID = ", note["engagement"]["id"], "ownerId = ", note["engagement"]["ownerId"])
        print(response.json())

    list_response.append(response.json())
