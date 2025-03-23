import pandas as pd
#import matplotlib.pyplot as plt

# Charger le fichier CSV 
df_initial = pd.read_csv(r"C:\Users\MAGNE\AVO Carbon\Assembly - AI-deployment\scapt_analyse\SCRAP_2025_03_13-08_03_42 V1.csv",
    encoding="cp1252",
    sep=None,
    engine="python")
print(df_initial.columns)

#######  Vérification et affichage des colonnes requises #########
if 'Defaut' not in df_initial.columns or 'Categorie' not in df_initial.columns:
    raise ValueError("Le fichier CSV doit contenir les colonnes 'défaut' et 'catégorie'")


###################################################################################################################
                                    # CATEGORIES #
###################################################################################################################

###### Calcul du nombre total de défauts par catégorie et renommage de la colonne ######

   ### calcul pour toutes les catégories ###
def nbre_total_defaut_par_cate(colonne_categorie, colonne_qte_defaut,date_specifique=None, colonne_date=None,df=None):

    if df is None:
        df = df_initial

    if date_specifique is not None and colonne_date is not None:
        df_filtre = df[df[colonne_date] == date_specifique]
    else:
        df_filtre = df
    ## somme defaut par catégorie groupé
    defaut_par_cate = df_filtre.groupby(colonne_categorie)[colonne_qte_defaut].sum()
    #print(defaut_par_cate)
    ## somme des défauts pour chaque catégorie plus renomage 
    nbre_defaut_par_cate = df_filtre.groupby(colonne_categorie)[colonne_qte_defaut].sum().reset_index().rename(columns={'Defaut': 'nombre_total_defauts'})
    return nbre_defaut_par_cate

colonne_categorie = 'Categorie'
colonne_qte_defaut = 'Qte defaut'
date_specifique = '12/03/2025'
colonne_date= 'ï»¿Date de creation'
nbre_defaut_par_cate = nbre_total_defaut_par_cate(colonne_categorie, colonne_qte_defaut,date_specifique, colonne_date,df=None)
if date_specifique is not None:
    message = f"Nombre total de défaut pour chaque catégorie pour la date {date_specifique} :"
else:
    message = "Nombre total de défaut pour chaque catégorie pour toutes dates :"
print(message)
print(nbre_defaut_par_cate)
print('')


      ####### calcul pour une catégorie précise ###
def qte_defaut_pour_jour(date_creation, categorie, df = None):

    if df is None:
        df = df_initial
    df_1 = df[(df['ï»¿Date de creation'] == date_creation) & (df['Categorie'] == categorie)]
    nbre_defaut_06_03 = df_1['Qte defaut'].sum()
    return nbre_defaut_06_03

date_creation = '06/03/2025'
categorie = 'Formation'
nbre_defaut_06_03 = qte_defaut_pour_jour(date_creation, categorie)
print("le nombre total de défaut pour la catégorie"+" "+ categorie +" "+"pour le"+" "+ str(date_creation)+" "+"est de ", nbre_defaut_06_03)
print('')


###################################################################################################################
                                    # LIGNES #
###################################################################################################################

###### Calcul du nombre total de défauts par lignes et renommage de la colonne ######

def nbre_total_defaut_par_ligne(colonne_ligne,  colonne_qte_defaut,date_specifique=None, colonne_date=None,df=None):

    if df is None:
        df = df_initial

    if date_specifique is not None and colonne_date is not None:
        df_filtre = df[df[colonne_date] == date_specifique]
    else:
        df_filtre = df
    
    #print(df_filtre)
    ## somme defaut par ligne groupé
    defaut_par_ligne = df_filtre.groupby(colonne_ligne)[colonne_qte_defaut].sum()
    #print(defaut_par_cate)
    ## somme des défauts pour chaque catégorie plus renomage 
    nbre_defaut_par_ligne = df_filtre.groupby(colonne_ligne)[colonne_qte_defaut].sum().reset_index().rename(columns={'Defaut': 'nombre_total_defauts'})
    return nbre_defaut_par_ligne

colonne_ligne = 'Ligne'
colonne_qte_defaut = 'Qte defaut'
date_specifique = '10/03/2025'
colonne_date= 'ï»¿Date de creation'
nbre_defaut_par_ligne = nbre_total_defaut_par_ligne(colonne_ligne, colonne_qte_defaut,date_specifique, colonne_date)
if date_specifique is not None:
    message = f"Nombre total de défaut pour chaque groupe de ligne pour la date {date_specifique} :"
else:
    message = "Nombre total de défaut pour chaque groupe de lignes pour toutes dates :"
print(message)
print(nbre_defaut_par_ligne)
print('')

###################################################################################################################
                        ### ANALYSE SUR LES 5 DERNIERS JOURS ###
###################################################################################################################

############## Extraction des 5 dernières dates ###################
def extract_last_dates(colonne_date):
    unique_dates = df_initial[colonne_date].unique()
    unique_dates_sorted = sorted(unique_dates, key=lambda x: pd.to_datetime(x, format="%d/%m/%Y"))
    result = unique_dates_sorted[-5:]
    print("les 5 derniers jours receuillis sont")
    print(result)
    print(' ')
    df_last5 = df_initial[df_initial[colonne_date].isin(result)]
    return df_last5

df_last5 = extract_last_dates(colonne_date)
result = nbre_total_defaut_par_ligne(colonne_ligne, colonne_qte_defaut,date_specifique=None, colonne_date=None, df= df_last5)
print('nombre defaut par ligne pour les derniers jours')
print(result)
print(" ")


##### identification des origines(colonne phase d'origine) et de la liste des défauts pour chacune des familles de ligne ayant le plus de qte defaut ####

def top_defauts_par_ligne(colonne_ligne, colonne_qte_defaut,colonnes_affichage, colonne_date,date_specifique=None, df= None):
    if df is None:
        df = df_initial

    if date_specifique is not None:
        df_filtre = df[df[colonne_date] == date_specifique]
    else:
        df_filtre = df
    #df_filtre = df_initial[df_initial[colonne_date] == '12/03/2025']
    df_top3 = df_filtre.groupby(colonne_ligne, group_keys=False).apply(lambda x: x.sort_values(by=colonne_qte_defaut, ascending=False).head(1))
    #print(df_top3)
    df_final = df_top3[colonnes_affichage]
    df_final = df_final.reset_index()
    #print(df_final)
    return df_final
    
colonnes_affichage = ["ï»¿Date de creation",'Ligne','Defaut','Qte defaut', 'Phase d\'origine', 'Phase de detection']
colonne_ligne = 'Ligne'
# Appel de la fonction avec ces paramètres
resultat = top_defauts_par_ligne(colonne_ligne,colonne_qte_defaut,colonnes_affichage,colonne_date,date_specifique= None, df=None)
print("autre manière l'étape précédente")
print(resultat)
#resultat.to_csv("resultat_1.csv", index=False)

#### autre manière ####
# print("autre manière l'étape précédente")
# last_5_dates = unique_dates_sorted[-5:]
# result = df_last5.sort_values(by="Qte defaut", ascending=False).groupby("Ligne").head(1)
# print(result)
# df_test = result[["ï»¿Date de creation","Ligne", "Qte defaut", "Phase d'origine", "Phase de detection"]]
# df_test.to_excel("resultat_2.xlsx", index=False)