import pandas as pd
#import matplotlib.pyplot as plt  # Décommenter si vous souhaitez faire des graphiques

#------------------------------------------------------------------------------
# 1. Chargement du fichier CSV et affichage des colonnes
#------------------------------------------------------------------------------
file_path = r"C:\Users\MAGNE\AVO Carbon\Assembly - AI-deployment\scapt_analyse\SCRAP_2025_03_13-08_03_42 V1.csv"
df_initial = pd.read_csv(file_path, encoding="cp1252", sep=None, engine="python")
print("Colonnes du fichier CSV:")
print(df_initial.columns)

# Vérification des colonnes requises
if 'Defaut' not in df_initial.columns or 'Categorie' not in df_initial.columns:
    raise ValueError("Le fichier CSV doit contenir les colonnes 'Defaut' et 'Categorie'")

#------------------------------------------------------------------------------
# 2. Définition des paramètres globaux
#------------------------------------------------------------------------------
# Noms de colonnes tels qu'ils apparaissent dans le CSV
colonne_date     = 'ï»¿Date de creation'
colonne_categorie= 'Categorie'
colonne_qte_defaut = 'Qte defaut'
colonne_ligne    = 'Ligne'
colonne_defaut = 'Defaut'
date_specifique ='12/03/2025'
nbre_top_defaut = 2
nb_days = 5
# Colonnes à afficher dans certains résultats
colonnes_affichage = [colonne_date, colonne_ligne, colonne_defaut, colonne_qte_defaut, "Phase d'origine", "Phase de detection"]
colonnes_affichage_2 = [colonne_date, colonne_categorie, colonne_defaut, colonne_qte_defaut, "Phase d'origine", "Phase de detection"]

# Définir la date spécifique (exemple : dernière date présente dans le fichier)
# unique_dates = df_initial[colonne_date].unique()
# unique_dates_sorted = sorted(unique_dates, key=lambda x: pd.to_datetime(x, format="%d/%m/%Y"))
# date_specifique = unique_dates_sorted[-1]  # La dernière date du fichier

print("\nDate spécifique retenue (dernière date du fichier) :", date_specifique)

#------------------------------------------------------------------------------
# 3. Définition des fonctions
#------------------------------------------------------------------------------

def nbre_total_defaut_par_cate(colonne_categorie, colonne_qte_defaut, date_specifique=None, colonne_date=None, df=None):
    """
    Calcule le nombre total de défauts par catégorie.
    Si date_specifique et colonne_date sont fournis, le calcul se fait pour cette date.
    """
    if df is None:
        df = df_initial
    if date_specifique is not None and colonne_date is not None:
        df_filtre = df[df[colonne_date] == date_specifique]
    else:
        df_filtre = df
    # Calcul de la somme des défauts par catégorie
    nbre_defaut_par_cate = df_filtre.groupby(colonne_categorie)[colonne_qte_defaut].sum().reset_index()
    return nbre_defaut_par_cate

def qte_defaut_pour_jour(date_creation, categorie, df=None):
    """
    Calcule le nombre total de défauts pour une catégorie donnée à une date précise.
    """
    if df is None:
        df = df_initial
    df_filtre = df[(df[colonne_date] == date_creation) & (df[colonne_categorie] == categorie)]
    nbre_defaut = df_filtre[colonne_qte_defaut].sum()
    return nbre_defaut

def nbre_total_defaut_par_ligne(colonne_ligne, colonne_qte_defaut, date_specifique=None, colonne_date=None, df=None):
    """
    Calcule le nombre total de défauts par groupe de ligne.
    Si date_specifique et colonne_date sont fournis, le calcul se fait pour cette date.
    """
    if df is None:
        df = df_initial
    if date_specifique is not None and colonne_date is not None:
        df_filtre = df[df[colonne_date] == date_specifique]
    else:
        df_filtre = df
    nbre_defaut_par_ligne = df_filtre.groupby(colonne_ligne)[colonne_qte_defaut].sum().reset_index()
    return nbre_defaut_par_ligne

def extract_last_dates(colonne_date, nb_days):
    """
    Extrait les nb_days dernières dates du DataFrame et retourne un DataFrame filtré.
    Ce DataFrame sera utilisé pour l'analyse de tendances.
    """
    unique_dates = df_initial[colonne_date].unique()
    unique_dates_sorted = sorted(unique_dates, key=lambda x: pd.to_datetime(x, format="%d/%m/%Y"))
    last_dates = unique_dates_sorted[-nb_days:]
    print(f"\nLes {nb_days} derniers jours recueillis sont:")
    print(last_dates)
    df_last = df_initial[df_initial[colonne_date].isin(last_dates)]
    return df_last

def top_defauts_par_ligne(colonne_ligne, colonne_qte_defaut, colonnes_affichage, colonne_date,nbre_top_defaut, date_specifique=None, df=None):
    """
    Pour chaque groupe de ligne, identifie la ligne ayant le plus grand nombre de défauts.
    Retourne un DataFrame avec les colonnes spécifiées.
    """
    if df is None:
        df = df_initial
    if date_specifique is not None and colonne_date is not None:
        df_filtre = df[df[colonne_date] == date_specifique]
    else:
        df_filtre = df
    df_top = df_filtre.groupby(colonne_ligne, group_keys=False).apply(
        lambda x: x.sort_values(by=colonne_qte_defaut, ascending=False).head(nbre_top_defaut)
    )
    df_final = df_top[colonnes_affichage].reset_index(drop=True)
    return df_final


def top_defauts_par_cate(colonne_categorie, colonne_qte_defaut, colonnes_affichage_2, colonne_date,nbre_top_defaut, date_specifique=None, df=None):
    """
    Pour chaque groupe de ligne, identifie la ligne ayant le plus grand nombre de défauts.
    Retourne un DataFrame avec les colonnes spécifiées.
    """
    if df is None:
        df = df_initial
    if date_specifique is not None and colonne_date is not None:
        df_filtre = df[df[colonne_date] == date_specifique]
    else:
        df_filtre = df
    df_top = df_filtre.groupby(colonne_categorie, group_keys=False).apply(
        lambda x: x.sort_values(by=colonne_qte_defaut, ascending=False).head(nbre_top_defaut)
    )
    df_final = df_top[colonnes_affichage_2].reset_index(drop=True)
    return df_final
#------------------------------------------------------------------------------
# 4. Obtention des résultats en appelant les fonctions
#------------------------------------------------------------------------------

print("\n--- Nombre total de défauts par catégorie (toutes dates) ---")
result_cate_all = nbre_total_defaut_par_cate(colonne_categorie, colonne_qte_defaut, date_specifique=None, colonne_date=colonne_date)
print(result_cate_all)

print(f"\n--- Nombre total de défauts par catégorie pour la date {date_specifique} ---")
result_cate_specific = nbre_total_defaut_par_cate(colonne_categorie, colonne_qte_defaut, date_specifique, colonne_date)
print(result_cate_specific)

print("\n--- Nombre total de défauts par groupe de ligne (toutes dates) ---")
result_ligne_all = nbre_total_defaut_par_ligne(colonne_ligne, colonne_qte_defaut, date_specifique=None, colonne_date=colonne_date)
print(result_ligne_all)

print(f"\n--- Nombre total de défauts par groupe de ligne pour la date {date_specifique} ---")
result_ligne_specific = nbre_total_defaut_par_ligne(colonne_ligne, colonne_qte_defaut, date_specifique, colonne_date)
print(result_ligne_specific)

print("\n--- Top défauts par ligne (pour toutes dates) ---")
result_top_all = top_defauts_par_ligne(colonne_ligne, colonne_qte_defaut, colonnes_affichage, colonne_date,nbre_top_defaut, date_specifique=None, df=None)
print(result_top_all)

print(f"\n--- Top défauts par ligne pour la date {date_specifique} ---")
result_top_specific = top_defauts_par_ligne(colonne_ligne, colonne_qte_defaut, colonnes_affichage, colonne_date,nbre_top_defaut, date_specifique, df=None)
print(result_top_specific)

print("\n--- Top défauts par catégorie (pour toutes dates) ---")
result_top_all = top_defauts_par_cate(colonne_categorie, colonne_qte_defaut, colonnes_affichage_2, colonne_date,nbre_top_defaut, date_specifique=None, df=None)
print(result_top_all)

print(f"\n--- Top défauts par catégorie pour la date {date_specifique} ---")
result_top_specific = top_defauts_par_cate(colonne_categorie, colonne_qte_defaut, colonnes_affichage_2, colonne_date,nbre_top_defaut, date_specifique, df=None)
print(result_top_specific)

print("\n--- DataFrame pour les 5 derniers jours (pour analyse de tendances) ---")
df_last5 = extract_last_dates(colonne_date, nb_days=5)
print(df_last5)

print("\n--- Nombre total de défauts par catégorie pour les 5 derniers jours ---")
result_cate_last5 = nbre_total_defaut_par_cate(colonne_categorie, colonne_qte_defaut, date_specifique=None, colonne_date=colonne_date, df=df_last5)
print(result_cate_last5)

print("\n--- Nombre total de défauts par groupe de ligne pour les 5 derniers jours ---")
result_ligne_last5 = nbre_total_defaut_par_ligne(colonne_ligne, colonne_qte_defaut, date_specifique=None, colonne_date=colonne_date, df=df_last5)
print(result_ligne_last5)


print("\n--- DataFrame pour l'analyse de recommandations (lignes avec le plus de défauts) ---")
result_reco = top_defauts_par_ligne(colonne_ligne, colonne_qte_defaut, colonnes_affichage, colonne_date,nbre_top_defaut,date_specifique=None, df=df_initial)
print(result_reco)
