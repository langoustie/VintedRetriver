import pandas as pd
from pyVinted import Vinted
import time
import os

# Initialisation de l'API Vinted
vinted = Vinted()

# Récupération du nombre d'éléments à extraire
N = int(input("Entrez le nombre d'articles à récupérer : "))
Itera = N//100
items = []
print(Itera)
# Requête de recherche
url = "https://www.vinted.fr/catalog?search_text=Pokemon block EX cardprice_from=100.00&currency=EUR&price_to=400"

# Fonction pour sauvegarder les données dans un CSV existant ou nouveau
def save_to_csv(data, filename="vinted_data_high.csv"):
    # Si le fichier existe déjà, ajouter les nouvelles données sans écraser
    if os.path.isfile(filename):
        # Ouvrir le fichier en mode "append" sans écraser le contenu et sans les en-têtes
        data.to_csv(filename, mode='a', header=False, index=False, encoding='utf-8')
    else:
        # Sinon, créer le fichier avec les en-têtes
        data.to_csv(filename, index=False, encoding='utf-8')


# Boucle pour récupérer les articles par lots
for i in range(Itera):
    # Récupérer les articles en utilisant l'API Vinted
    items = items + vinted.items.search(url, (100), 1)
    
    
    # Pause pour éviter d'atteindre les limites de l'API
    time.sleep(1)
    print("boucle numéro ", i)
    
    # Toutes les 10 itérations, sauvegarder dans le CSV et vider `items`
    if (i + 1) % 10 == 0:
        # Extraire les informations nécessaires et ajouter au DataFrame
        photos = [item.photo for item in items]
        prices = [item.price for item in items]
        titles = [item.title for item in items]

        # Création d'un DataFrame pour le lot actuel
        data_batch = pd.DataFrame({
            "Photo": photos,
            "Prix": prices,
            "Titre": titles,
        })

        # Sauvegarde du lot dans le fichier CSV
        save_to_csv(data_batch, "vinted_data_high.csv")
        print("Sauvé dans le csv boucle if")

        # Vider `items` après la sauvegarde pour limiter la taille en mémoire
        items = []



photos = [item.photo for item in items]
prices = [item.price for item in items]
titles = [item.title for item in items]

# Création d'un DataFrame pour le lot actuel
data_batch = pd.DataFrame({
    "Photo": photos,
    "Prix": prices,
    "Titre": titles,
    })

        # Sauvegarde du lot dans le fichier CSV
save_to_csv(data_batch, "vinted_data_high.csv")
print("Les données ont été sauvegardées dans le fichier vinted_data.csv")




