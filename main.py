# 





import pandas as pd
from fastapi import FastAPI, HTTPException, Form
from pydantic import BaseModel
from b2sdk.v2 import InMemoryAccountInfo, B2Api

app = FastAPI()

# Configuration du client B2
info = InMemoryAccountInfo()
b2_api = B2Api(info)
API_key_id = 'b2eff4365da9'
API_key = '0051f6525ef86de92145b66acfe51adfe352eee548'
b2_api.authorize_account('production', API_key_id, API_key)
bucket_name = "testfichier"
bucket = b2_api.get_bucket_by_name(bucket_name)

#------------------------------------------------------------------------------
# 1. Modèle de requête (uniquement pour les paramètres autres que les colonnes)
#------------------------------------------------------------------------------
class ProcessRequest(BaseModel):
    date_specifique: str
    nbre_top_defaut: int
    nb_days: int
    # Ces listes indiquent quelles colonnes afficher dans certains résultats.
    colonnes_affichage: list[str]
    colonnes_affichage_2: list[str]

#------------------------------------------------------------------------------
# 2. Fonction pour détecter automatiquement les colonnes dans le CSV
#------------------------------------------------------------------------------
def detect_columns(headers: list[str]) -> dict:
    """
    Parcourt les en-têtes du CSV et retourne un dictionnaire avec les noms de colonnes
    attendus en fonction d'un critère de correspondance (insensible à la casse).
    Vous pouvez adapter les critères de détection selon votre contexte.
    """
    def find_header(pattern: str):
        for header in headers:
            if pattern.lower() in header.lower():
                return header
        return None

    return {
         "colonne_date": find_header("date"),
         "colonne_type": find_header("type"),
         "colonne_catégorie": find_header("catégorie"),
         "colonne_equipe": find_header("équipe"),
         "colonne_produit": find_header("produit"),
         "colonne_ligne": find_header("ligne"),
         # Ici, on essaie de distinguer le nom d'une colonne "défaut" d'une éventuelle colonne "qte" si elles existent
         "colonne_defaut": find_header("défaut") if find_header("défaut") and not find_header("qte") else None,
         "colonne_qte_defaut": find_header("qte"),
         "colonnes_phase_origine": find_header("phase_origine"),
         "colonne_phase_detection": find_header("phase_detection")
    }

#------------------------------------------------------------------------------
# 3. Fonctions de traitement (inchangées)
#------------------------------------------------------------------------------
def nbre_total_defaut_par_cate(colonne_categorie, colonne_qte_defaut, date_specifique=None, colonne_date=None, df=None):
    if df is None:
        raise ValueError("DataFrame est requis")
    if date_specifique is not None and colonne_date is not None:
        df_filtre = df[df[colonne_date] == date_specifique]
    else:
        df_filtre = df
    nbre_defaut_par_cate = df_filtre.groupby(colonne_categorie)[colonne_qte_defaut].sum().reset_index()
    return nbre_defaut_par_cate

def qte_defaut_pour_jour(date_creation, categorie, df=None, colonne_date=None, colonne_categorie=None, colonne_qte_defaut=None):
    if df is None:
        raise ValueError("DataFrame est requis")
    df_filtre = df[(df[colonne_date] == date_creation) & (df[colonne_categorie] == categorie)]
    nbre_defaut = df_filtre[colonne_qte_defaut].sum()
    return nbre_defaut

def nbre_total_defaut_par_ligne(colonne_ligne, colonne_qte_defaut, date_specifique=None, colonne_date=None, df=None):
    if df is None:
        raise ValueError("DataFrame est requis")
    if date_specifique is not None and colonne_date is not None:
        df_filtre = df[df[colonne_date] == date_specifique]
    else:
        df_filtre = df
    nbre_defaut_par_ligne = df_filtre.groupby(colonne_ligne)[colonne_qte_defaut].sum().reset_index()
    return nbre_defaut_par_ligne

def extract_last_dates(colonne_date, nb_days, df=None):
    if df is None:
        raise ValueError("DataFrame est requis")
    unique_dates = df[colonne_date].unique()
    unique_dates_sorted = sorted(unique_dates, key=lambda x: pd.to_datetime(x, format="%d/%m/%Y"))
    last_dates = unique_dates_sorted[-nb_days:]
    df_last = df[df[colonne_date].isin(last_dates)]
    return df_last

def top_defauts_par_ligne(colonne_ligne, colonne_qte_defaut, colonnes_affichage, colonne_date, nbre_top_defaut, date_specifique=None, df=None):
    if df is None:
        raise ValueError("DataFrame est requis")
    if date_specifique is not None and colonne_date is not None:
        df_filtre = df[df[colonne_date] == date_specifique]
    else:
        df_filtre = df
    df_top = df_filtre.groupby(colonne_ligne, group_keys=False).apply(
        lambda x: x.sort_values(by=colonne_qte_defaut, ascending=False).head(nbre_top_defaut)
    )
    df_final = df_top[colonnes_affichage].reset_index(drop=True)
    return df_final

def top_defauts_par_cate(colonne_categorie, colonne_qte_defaut, colonnes_affichage_2, colonne_date, nbre_top_defaut, date_specifique=None, df=None):
    if df is None:
        raise ValueError("DataFrame est requis")
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
# 4. Endpoint FastAPI
#------------------------------------------------------------------------------
@app.post("/process_csv/")
async def process_csv(request: ProcessRequest):
    try:
        # Téléchargement et sauvegarde du fichier depuis le bucket B2
        file_name = "Scrap S12.csv"
        downloaded_file = bucket.download_file_by_name(file_name)
        downloaded_file.save_to("data.csv")
        print("Le fichier data.csv a bien été téléchargé et sauvegardé.")
        
        # Lecture du CSV sans préciser le séparateur (adaptable si nécessaire)
        df = pd.read_csv("data.csv", encoding="cp1252", engine="python")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erreur lors de la lecture du fichier CSV: {e}")
    
    # Récupération des en-têtes du CSV
    headers = list(df.columns)
    print("En-têtes du CSV :", headers)
    
    # Détection automatique des colonnes attendues
    colonnes_detectees = detect_columns(headers)
    
    # Vérification que les colonnes essentielles ont été trouvées
    required = ["colonne_date", "colonne_catégorie", "colonne_qte_defaut", "colonne_ligne"]
    for col in required:
        if colonnes_detectees.get(col) is None:
            raise HTTPException(status_code=400, detail=f"Le fichier CSV doit contenir une colonne correspondant à '{col}'.")
    
    # Conversion des listes de colonnes à afficher (déjà sous forme de liste)
    colonnes_affichage_list = request.colonnes_affichage
    colonnes_affichage_2_list = request.colonnes_affichage_2
    
    # Utilisation des colonnes détectées pour les traitements
    result_cate_all = nbre_total_defaut_par_cate(
        colonnes_detectees["colonne_catégorie"],
        colonnes_detectees["colonne_qte_defaut"],
        date_specifique=None,
        colonne_date=colonnes_detectees["colonne_date"],
        df=df
    )
    result_cate_specific = nbre_total_defaut_par_cate(
        colonnes_detectees["colonne_catégorie"],
        colonnes_detectees["colonne_qte_defaut"],
        request.date_specifique,
        colonnes_detectees["colonne_date"],
        df=df
    )
    result_ligne_all = nbre_total_defaut_par_ligne(
        colonnes_detectees["colonne_ligne"],
        colonnes_detectees["colonne_qte_defaut"],
        date_specifique=None,
        colonne_date=colonnes_detectees["colonne_date"],
        df=df
    )
    result_ligne_specific = nbre_total_defaut_par_ligne(
        colonnes_detectees["colonne_ligne"],
        colonnes_detectees["colonne_qte_defaut"],
        request.date_specifique,
        colonnes_detectees["colonne_date"],
        df=df
    )
    result_top_all = top_defauts_par_ligne(
        colonnes_detectees["colonne_ligne"],
        colonnes_detectees["colonne_qte_defaut"],
        colonnes_affichage_list,
        colonnes_detectees["colonne_date"],
        request.nbre_top_defaut,
        date_specifique=None,
        df=df
    )
    result_top_specific = top_defauts_par_ligne(
        colonnes_detectees["colonne_ligne"],
        colonnes_detectees["colonne_qte_defaut"],
        colonnes_affichage_list,
        colonnes_detectees["colonne_date"],
        request.nbre_top_defaut,
        request.date_specifique,
        df=df
    )
    result_top_all_cate = top_defauts_par_cate(
        colonnes_detectees["colonne_catégorie"],
        colonnes_detectees["colonne_qte_defaut"],
        colonnes_affichage_2_list,
        colonnes_detectees["colonne_date"],
        request.nbre_top_defaut,
        date_specifique=None,
        df=df
    )
    result_top_specific_cate = top_defauts_par_cate(
        colonnes_detectees["colonne_catégorie"],
        colonnes_detectees["colonne_qte_defaut"],
        colonnes_affichage_2_list,
        colonnes_detectees["colonne_date"],
        request.nbre_top_defaut,
        request.date_specifique,
        df=df
    )
    
    df_last5 = extract_last_dates(colonnes_detectees["colonne_date"], request.nb_days, df=df)
    result_cate_last5 = nbre_total_defaut_par_cate(
        colonnes_detectees["colonne_catégorie"],
        colonnes_detectees["colonne_qte_defaut"],
        date_specifique=None,
        colonne_date=colonnes_detectees["colonne_date"],
        df=df_last5
    )
    result_ligne_last5 = nbre_total_defaut_par_ligne(
        colonnes_detectees["colonne_ligne"],
        colonnes_detectees["colonne_qte_defaut"],
        date_specifique=None,
        colonne_date=colonnes_detectees["colonne_date"],
        df=df_last5
    )
    result_reco = top_defauts_par_ligne(
        colonnes_detectees["colonne_ligne"],
        colonnes_detectees["colonne_qte_defaut"],
        colonnes_affichage_list,
        colonnes_detectees["colonne_date"],
        request.nbre_top_defaut,
        date_specifique=None,
        df=df
    )
    
    # Les colonnes détectées sont désormais stockées dans le dictionnaire "colonnes_detectees"
    # et pourront être utilisées ultérieurement si besoin.
    results = {
         "colonnes_detectees": colonnes_detectees,
         "result_cate_all": result_cate_all.to_dict(orient="records"),
         "result_cate_specific": result_cate_specific.to_dict(orient="records"),
         "result_ligne_all": result_ligne_all.to_dict(orient="records"),
         "result_ligne_specific": result_ligne_specific.to_dict(orient="records"),
         "result_top_all": result_top_all.to_dict(orient="records"),
         "result_top_specific": result_top_specific.to_dict(orient="records"),
         "result_top_all_cate": result_top_all_cate.to_dict(orient="records"),
         "result_top_specific_cate": result_top_specific_cate.to_dict(orient="records"),
         "result_cate_last5": result_cate_last5.to_dict(orient="records"),
         "result_ligne_last5": result_ligne_last5.to_dict(orient="records"),
         "result_reco": result_reco.to_dict(orient="records"),
         "date_specifique": request.date_specifique
    }
    return results
