import pandas as pd
from fastapi import FastAPI,File, HTTPException, Form 
from io import BytesIO
import base64
from pydantic import BaseModel
#import uvicorn

app = FastAPI()

#------------------------------------------------------------------------------
# 1. Définition du modèle de requête (JSON)
#------------------------------------------------------------------------------

class CSVRequest(BaseModel):
    file_data: str  # Le contenu du fichier CSV encodé en base64
    colonne_date: str
    colonne_categorie: str
    colonne_qte_defaut: str
    colonne_ligne: str
    #colonne_defaut: str
    nbre_top_defaut: int
    nb_days: int
    colonnes_affichage: list[str]
    colonnes_affichage_2: list[str]

#------------------------------------------------------------------------------
# 2. Définition des fonctions de traitement
#------------------------------------------------------------------------------

def nbre_total_defaut_par_cate(colonne_categorie, colonne_qte_defaut, date_specifique=None, colonne_date=None, df=None):
    """
    Calcule le nombre total de défauts par catégorie.
    Si date_specifique et colonne_date sont fournis, le calcul se fait pour cette date.
    """
    if df is None:
        raise ValueError("DataFrame est requis")
    if date_specifique is not None and colonne_date is not None:
        df_filtre = df[df[colonne_date] == date_specifique]
    else:
        df_filtre = df
    nbre_defaut_par_cate = df_filtre.groupby(colonne_categorie)[colonne_qte_defaut].sum().reset_index()
    return nbre_defaut_par_cate

def qte_defaut_pour_jour(date_creation, categorie, df=None, colonne_date=None, colonne_categorie=None, colonne_qte_defaut=None):
    """
    Calcule le nombre total de défauts pour une catégorie donnée à une date précise.
    """
    if df is None:
        raise ValueError("DataFrame est requis")
    df_filtre = df[(df[colonne_date] == date_creation) & (df[colonne_categorie] == categorie)]
    nbre_defaut = df_filtre[colonne_qte_defaut].sum()
    return nbre_defaut

def nbre_total_defaut_par_ligne(colonne_ligne, colonne_qte_defaut, date_specifique=None, colonne_date=None, df=None):
    """
    Calcule le nombre total de défauts par groupe de ligne.
    Si date_specifique et colonne_date sont fournis, le calcul se fait pour cette date.
    """
    if df is None:
        raise ValueError("DataFrame est requis")
    if date_specifique is not None and colonne_date is not None:
        df_filtre = df[df[colonne_date] == date_specifique]
    else:
        df_filtre = df
    nbre_defaut_par_ligne = df_filtre.groupby(colonne_ligne)[colonne_qte_defaut].sum().reset_index()
    return nbre_defaut_par_ligne

def extract_last_dates(colonne_date, nb_days, df=None):
    """
    Extrait les nb_days dernières dates du DataFrame et retourne un DataFrame filtré.
    Ce DataFrame sera utilisé pour l'analyse de tendances.
    """
    if df is None:
        raise ValueError("DataFrame est requis")
    unique_dates = df[colonne_date].unique()
    unique_dates_sorted = sorted(unique_dates, key=lambda x: pd.to_datetime(x, format="%d/%m/%Y"))
    last_dates = unique_dates_sorted[-nb_days:]
    df_last = df[df[colonne_date].isin(last_dates)]
    return df_last

def top_defauts_par_ligne(colonne_ligne, colonne_qte_defaut, colonnes_affichage, colonne_date, nbre_top_defaut, date_specifique=None, df=None):
    """
    Pour chaque groupe de ligne, identifie la ou les lignes ayant le plus grand nombre de défauts.
    Retourne un DataFrame avec les colonnes spécifiées.
    """
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
    """
    Pour chaque catégorie, identifie la ou les lignes ayant le plus grand nombre de défauts.
    Retourne un DataFrame avec les colonnes spécifiées.
    """
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
# 3. Création de l'API avec FastAPI et réception des paramètres via Form
#------------------------------------------------------------------------------

@app.post("/process_csv/")
# async def process_csv(
#     file: UploadFile = File(...),
#     # Paramètres globaux envoyés avec la requête (via des champs de formulaire)
#     colonne_date: str = Form(...),
#     colonne_categorie: str = Form(...),
#     colonne_qte_defaut: str = Form(...),
#     colonne_ligne: str = Form(...),
#     date_specifique = Form(...),
#     nbre_top_defaut: int = Form(...),
#     nb_days: int = Form(...),
#     # Les colonnes à afficher sont envoyées sous forme de chaînes séparées par des virgules
#     colonnes_affichage: str = Form(...),
#     colonnes_affichage_2: str = Form(...)
# ):
async def process_csv(request: CSVRequest):
    # Lecture et conversion du fichier CSV envoyé en DataFrame
    try:
        # Vérification et ajout du padding nécessaire pour une chaîne base64 valide
        file_data = request.file_data
        missing_padding = len(file_data) % 4
        if missing_padding:
            file_data += '=' * (4 - missing_padding)

        decoded_bytes = base64.b64decode(file_data)
        #contents = await file.read()
        df = pd.read_csv(BytesIO(decoded_bytes), encoding="cp1252", sep=";", engine="python")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erreur lors de la lecture du fichier CSV: {e}")

    # Vérification des colonnes requises dans le CSV
    # required_columns = [request.colonne_defaut, 'Categorie']
    # for col in required_columns:
    #     if col not in df.columns:
    #         raise HTTPException(status_code=400, detail=f"Le fichier CSV doit contenir la colonne '{col}'")

    # Conversion des listes de colonnes depuis une chaîne (séparateur virgule)
    #colonnes_affichage = [c.strip() for c in colonnes_affichage.split(",")]
    #colonnes_affichage_2 = [c.strip() for c in colonnes_affichage_2.split(",")]
    colonnes_affichage = request.colonnes_affichage
    colonnes_affichage_2 = request.colonnes_affichage_2

    # Définir la date spécifique : la dernière date présente dans le fichier
    # unique_dates = df[colonne_date].unique()
    # unique_dates_sorted = sorted(unique_dates, key=lambda x: pd.to_datetime(x, format="%d/%m/%Y"))
    # date_specifique = unique_dates_sorted[-1]

    #------------------------------------------------------------------------------
    # Calcul des résultats en appelant vos fonctions
    #------------------------------------------------------------------------------
    result_cate_all = nbre_total_defaut_par_cate(request.colonne_categorie, request.colonne_qte_defaut, date_specifique=None, colonne_date=request.colonne_date, df=df)
    result_cate_specific = nbre_total_defaut_par_cate(request.colonne_categorie, request.colonne_qte_defaut, request.date_specifique, request.colonne_date, df=df)
    result_ligne_all = nbre_total_defaut_par_ligne(request.colonne_ligne, request.colonne_qte_defaut, date_specifique=None, colonne_date=request.colonne_date, df=df)
    result_ligne_specific = nbre_total_defaut_par_ligne(request.colonne_ligne, request.colonne_qte_defaut, request.date_specifique, request.colonne_date, df=df)
    result_top_all = top_defauts_par_ligne(request.colonne_ligne, request.colonne_qte_defaut, colonnes_affichage, request.colonne_date, request.nbre_top_defaut, date_specifique=None, df=df)
    result_top_specific = top_defauts_par_ligne(request.colonne_ligne, request.colonne_qte_defaut, colonnes_affichage, request.colonne_date, request.nbre_top_defaut, request.date_specifique, df=df)
    result_top_all_cate = top_defauts_par_cate(request.colonne_categorie, request.colonne_qte_defaut, colonnes_affichage_2, request.colonne_date, request.nbre_top_defaut, date_specifique=None, df=df)
    result_top_specific_cate = top_defauts_par_cate(request.colonne_categorie, request.colonne_qte_defaut, colonnes_affichage_2, request.colonne_date, request.nbre_top_defaut, request.date_specifique, df=df)
    
    df_last5 = extract_last_dates(request.colonne_date, request.nb_days, df=df)
    result_cate_last5 = nbre_total_defaut_par_cate(request.colonne_categorie, request.colonne_qte_defaut, date_specifique=None, colonne_date=request.colonne_date, df=df_last5)
    result_ligne_last5 = nbre_total_defaut_par_ligne(request.colonne_ligne, request.colonne_qte_defaut, date_specifique=None, colonne_date=request.colonne_date, df=df_last5)
    result_reco = top_defauts_par_ligne(request.colonne_ligne, request.colonne_qte_defaut, colonnes_affichage, request.colonne_date, request.nbre_top_defaut, date_specifique=None, df=df)

    # Conversion des DataFrames en listes de dictionnaires pour retourner du JSON
    results = {
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

#------------------------------------------------------------------------------
# 3. Lancement de l'API
#------------------------------------------------------------------------------
# if __name__ == "__main__":
#     uvicorn.run(app, host="0.0.0.0", port=8000)
