'''
mysql.connector : pour interroger et modifier des bdd mysql depuis python
geopy : module python connecté à l'API Nominatim d'OpenStreetMap
'''

import mysql.connector as MC
import geopy
from geopy.geocoders import Nominatim

geopy.geocoders.options.default_user_agent = "data"
nom = Nominatim(user_agent='data')

# On commence par établir la connexion avec la bdd
# On créer le curseur qui permettra d'executer les différentes requêtes SQl
try:
    connection = MC.connect(host='localhost',database='dataengineer',user='root',password='DataEngineer2022')
    cursor = connection.cursor()

    # Selection de la table address
    request_addresses = 'SELECT * FROM address'
    cursor.execute(request_addresses)
    datalist = cursor.fetchall()
    
    # On enregistre les données de la table address dans des listes street/city/postal_code
    street_list = []
    city_list = []
    postal_code_list = []

    for street in datalist:
        street_list.append(street[1])
    for city in datalist:
        city_list.append(city[2])
    for postal_code in datalist:
        postal_code_list.append(postal_code[3])

    # On fusionne en un seul string les rues, villes et code postaux
    # On intègre les adresses ainsi obtenues dans une liste
    address_list = []

    for i,y in enumerate(street_list):
        string_address = street_list[i] + ' ' + city_list[i] + ' ' + postal_code_list[i]
        address_list.append(string_address)

    # Pour toutes les adresses on récupère la latitude et la longitude via geopy
    # On intègre les données dans deux listes, latitude et longitude
    latitude = []
    
    for i,y in enumerate(address_list):
        lat_address=nom.geocode(address_list[i])
        if type(lat_address) == geopy.location.Location:
            latitude.append(lat_address.latitude)
        else:
            latitude.append('Null')

    longitude = []

    for i,y in enumerate(address_list):
        lon_address=nom.geocode(address_list[i])
        if type(lon_address) == geopy.location.Location:
            longitude.append(lon_address.longitude)
        else:
            longitude.append('Null')

    # Création des colonnes latitude et longitude dans la table address
    column_latitude = 'ALTER TABLE address ADD latitude VARCHAR(20)'
    column_longitude = 'ALTER TABLE address ADD longitude VARCHAR(20)'
    cursor.execute(column_latitude)
    cursor.execute(column_longitude)

    # Ajout des données dans les colonnes latitude et longitude
    for i,y in enumerate(latitude):
        set_latitude = f'UPDATE address SET latitude = {latitude[i]} WHERE address_id = {i+1}'
        cursor.execute(set_latitude)

    for i,y in enumerate(longitude):
        set_longitude = f'UPDATE address SET longitude = {longitude[i]} WHERE address_id = {i+1}'
        cursor.execute(set_longitude)

    # Les lignes ci-dessous permettent simplement de vérifier la bonne intégration des données
    cursor.execute('SELECT * FROM address')
    result = cursor.fetchall()
    for rows in result:
        print(rows)

    # Le commit() permet de mettre à jour la bdd de manière effective
    connection.commit()

# La connexion est placée dans un try/except/finally
# Si la connexion échoue, le except va print le message d'erreur,
# cela permet de savoir pourquoi la connexion a échoué.
# Dans tous les cas, le finally viendra fermer la connexion
except MC.Error  as error :
    print(error)
finally:
    if (connection.is_connected()):
        cursor.close()
        connection.close()