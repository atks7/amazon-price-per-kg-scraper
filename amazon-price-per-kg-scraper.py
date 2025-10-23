import asyncio
import re
import csv
import os
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field

from playwright.async_api import async_playwright, Page, BrowserContext
from bs4 import BeautifulSoup, Tag

# ==============================================================================
# 1. CONFIGURATION ET CONSTANTES
# ==============================================================================

# Configuration de base pour le scraping
URL_BASE_AMAZON = "https://www.amazon.fr"
URL_RECHERCHE = f"{URL_BASE_AMAZON}/s?k=Barre+Prot%C3%A9in%C3%A9es"
MAX_PAGES = 3
NOM_FICHIER_CSV = 'resultats_amazon_barres_proteinees_clean.csv'

# Sélecteurs CSS pour l'extraction (centralisés pour faciliter la maintenance)
SELECTEURS = {
    "PRODUIT_BLOC": 'div[data-component-type="s-search-result"]',
    "TITRE": 'h2 span',
    "LIEN": 'h2 a',
    "PRIX_TOTAL": '.a-price .a-offscreen',
    "PRIX_UNITAIRE_TEXTE": "span.a-size-base.a-color-secondary", # Bloc contenant l'info (€/kg)
}

# Structure des données pour un produit (dataclass)
@dataclass
class ProduitAmazon:
    """Représente les données structurées d'un produit Amazon."""
    id_produit: str
    titre: str
    prix_total_str: str # Prix total brut (€)
    lien: str
    
    # Prix unitaire calculé
    prix_unitaire_kg: float = 0.0 # Résultat final en €/kg
    source_prix_kg: str = "N/A"
    
    # Données brutes de l'extraction unitaire Amazon
    prix_unitaire_texte_amazon: str = "N/A"
    prix_unitaire_valeur_amazon: float = 0.0
    prix_unitaire_unite_amazon: str = "N/A"

# ==============================================================================
# 2. FONCTIONS DE CALCUL ET DE TRANSFORMATION
# ==============================================================================

def _nettoyer_prix_total(prix_str: str) -> float:
    """Convertit une chaîne de prix (ex: '19,99 €') en float."""
    if not prix_str or prix_str == "N/A":
        return 0.0
    try:
        # Supprime €, espaces, et remplace la virgule par un point
        prix_net = prix_str.replace('€', '').replace('\xa0', '').replace(' ', '').replace(',', '.').strip()
        return float(prix_net)
    except ValueError:
        return 0.0

def calculer_prix_au_kg_direct(valeur: float, unite: str) -> float:
    """
    Convertit le prix unitaire en Prix au Kilogramme (€/kg) basé sur l'unité 
    fournie par Amazon (ex: €/kg ou €/100g).
    """
    if valeur <= 0:
        return 0.0
        
    unite = unite.lower().strip()
    if unite == 'kg':
        return valeur
    elif unite == '100g':
        # Conversion: Prix pour 100g * 10 = Prix pour 1000g (1 kg)
        return valeur * 10.0
    return 0.0

def calculer_prix_par_format(prix_total_str: str, titre: str) -> float:
    """
    Tente de calculer le prix au kg à partir du prix total et du format 
    (ex: 12x64g) trouvé dans le titre.
    
    Returns: Le prix au kg calculé, ou 0.0 si le calcul est impossible.
    """
    
    prix_total = _nettoyer_prix_total(prix_total_str)
    if prix_total <= 0.0:
        return 0.0

    # Regex pour capturer le format : (Nombre de barres) x (Poids par barre) + 'g'
    # Ex: 12x64g, 24x50g, 10 x 40 g
    match_format = re.search(r'(\d+)\s*x\s*(\d+)\s*g', titre, re.IGNORECASE)
    
    if match_format:
        try:
            nombre_unites = int(match_format.group(1))
            poids_par_unite_g = int(match_format.group(2))
            
            # Calcul du poids total en KILOGRAMMES
            poids_total_kg = (nombre_unites * poids_par_unite_g) / 1000.0
            
            if poids_total_kg > 0:
                # Calcul du Prix au Kilogramme
                return prix_total / poids_total_kg
        except (ValueError, TypeError, ZeroDivisionError):
            # En cas d'erreur dans les groupes de capture ou division par zéro
            pass

    return 0.0

# ==============================================================================
# 3. FONCTIONS D'EXTRACTION HTML
# ==============================================================================

def _extraire_prix_unitaire_amazon(produit: Tag) -> tuple[float, str, str]:
    """
    Extrait les données de prix unitaire (valeur, unité et texte brut) 
    telles que fournies par Amazon (ex: '20,50€/kg').
    """
    prix_unitaire_valeur = 0.0
    prix_unitaire_unite = "N/A"
    prix_unitaire_texte = "N/A"

    prix_blocs = produit.select(SELECTEURS["PRIX_UNITAIRE_TEXTE"])
    
    for span in prix_blocs:
        texte = span.get_text(strip=True)
        # Regex pour capturer valeur et unité (kg ou 100g)
        match = re.search(r"([\d.,\s]+)€\s*(?:/|/\s*|/)?\s*(kg|100\s*g)", texte)
        if match:
            prix_unitaire_texte = texte
            valeur_str = match.group(1).replace(',', '.').replace(' ', '').strip()
            unite_str = match.group(2).replace(' ', '').strip()
            
            try:
                prix_unitaire_valeur = float(valeur_str)
                prix_unitaire_unite = unite_str
                break 
            except ValueError:
                continue
                
    return prix_unitaire_valeur, prix_unitaire_unite, prix_unitaire_texte

def _traiter_produit(produit: Tag, id_prefix: str) -> ProduitAmazon:
    """Extrait et calcule toutes les données pour un seul bloc produit HTML."""
    
    # 1. Extraction des données de base
    titre_element = produit.select_one(SELECTEURS["TITRE"])
    titre = titre_element.text.strip() if titre_element else "Titre non trouvé"
    
    lien_element = produit.select_one(SELECTEURS["LIEN"])
    lien = URL_BASE_AMAZON + lien_element.get('href', "Lien non trouvé") if lien_element else "Lien non trouvé"

    prix_total_element = produit.select_one(SELECTEURS["PRIX_TOTAL"])
    prix_total_str = prix_total_element.text if prix_total_element else "N/A"
    
    # Création de l'objet de données
    produit_data = ProduitAmazon(
        id_produit=id_prefix,
        titre=titre,
        prix_total_str=prix_total_str,
        lien=lien
    )

    # 2. Extraction et calcul du prix unitaire Amazon
    valeur_amz, unite_amz, texte_amz = _extraire_prix_unitaire_amazon(produit)
    produit_data.prix_unitaire_valeur_amazon = valeur_amz
    produit_data.prix_unitaire_unite_amazon = unite_amz
    produit_data.prix_unitaire_texte_amazon = texte_amz
    
    # Tentative 1: Prix au kg via l'unité fournie par Amazon
    prix_kg_direct = calculer_prix_au_kg_direct(valeur_amz, unite_amz)
    
    # 3. Décision et Stockage
    if prix_kg_direct > 0.0:
        produit_data.prix_unitaire_kg = prix_kg_direct
        produit_data.source_prix_kg = "Amazon (Direct)"
    else:
        # Tentative 2: Prix au kg via le calcul du format (si Amazon a échoué)
        prix_calcule_format = calculer_prix_par_format(prix_total_str, titre)
        
        if prix_calcule_format > 0.0:
            produit_data.prix_unitaire_kg = prix_calcule_format
            produit_data.source_prix_kg = "Calculé (NxG)"

    # Logique de débogage / affichage
    statut = "✅ Trouvé" if produit_data.prix_unitaire_kg > 0.0 else "❌ Non trouvé"
    affichage_prix_kg = f"~{produit_data.prix_unitaire_kg:.2f}€/kg ({produit_data.source_prix_kg})" if produit_data.prix_unitaire_kg > 0 else "N/A"
    print(f"[{produit_data.id_produit}] - {statut} ({affichage_prix_kg}): {titre[:60]}...")
    
    return produit_data


async def extraire_donnees_page(page: Page, row_num_base: int) -> List[ProduitAmazon]:
    """Extrait toutes les données des produits de la page courante."""
    
    html_content = await page.content()
    soup = BeautifulSoup(html_content, 'html.parser')
    
    produits = soup.select(SELECTEURS["PRODUIT_BLOC"])
    resultats: List[ProduitAmazon] = []
    
    for i, produit in enumerate(produits):
        produit_id = f"R{row_num_base + i + 1}"
        
        try:
            produit_data = _traiter_produit(produit, produit_id)
            resultats.append(produit_data)
        except Exception as e:
            print(f"Erreur fatale lors du traitement du produit {produit_id}: {e}")
            continue
            
    return resultats

# ==============================================================================
# 4. FONCTIONNALITÉS DE NETTOYAGE ET D'ÉCRITURE CSV
# ==============================================================================

def _preparer_ligne_csv(produit: ProduitAmazon) -> Dict[str, Any]:
    """Formate l'objet ProduitAmazon pour l'écriture CSV."""
    
    # Colonnes finales à conserver
    ligne = {
        "ID": produit.id_produit,
        "Titre": produit.titre,
        "Prix Unitaire (€/kg)": "",
        "Prix Total (€)": "",
        "Source Prix (€/kg)": produit.source_prix_kg,
        "Prix Unitaire (texte Amazon)": produit.prix_unitaire_texte_amazon,
        "Prix Unitaire (Unité Amazon)": produit.prix_unitaire_unite_amazon
    }
    
    # Formatage du Prix Unitaire (€/kg) (float -> str avec virgule)
    if produit.prix_unitaire_kg > 0.0:
        ligne["Prix Unitaire (€/kg)"] = f"{produit.prix_unitaire_kg:.2f}".replace('.', ',')
        
    # Formatage du Prix Total (€) (nettoyage et formatage float -> str avec virgule)
    prix_total_float = _nettoyer_prix_total(produit.prix_total_str)
    if prix_total_float > 0.0:
        ligne["Prix Total (€)"] = f"{prix_total_float:.2f}".replace('.', ',')
    else:
        ligne["Prix Total (€)"] = produit.prix_total_str # Garde la chaîne brute si échec
        
    return ligne

def ecrire_csv(produits_tries: List[ProduitAmazon], nom_fichier: str):
    """Écrit la liste des objets ProduitAmazon triés dans un fichier CSV."""
    
    if not produits_tries:
        print("Aucun produit à enregistrer dans le CSV.")
        return

    lignes_formatees = [_preparer_ligne_csv(p) for p in produits_tries]
    fieldnames = list(lignes_formatees[0].keys())
    
    with open(nom_fichier, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';') 
        writer.writeheader() 
        writer.writerows(lignes_formatees)
        
# ==============================================================================
# 5. ORCHESTRATION PRINCIPALE
# ==============================================================================

def nettoyer_et_trier_resultats(resultats: List[ProduitAmazon]) -> List[ProduitAmazon]:
    """Filtre les produits sans prix/kg, puis les trie par prix croissant."""
    
    print("\n--- Nettoyage et Tri des données ---")
    nb_avant_filtre = len(resultats)
    
    # 1. Filtration : Supprimer les produits où Prix Unitaire (€/kg) est 0.0 (non trouvé)
    resultats_filtres = [
        r for r in resultats if r.prix_unitaire_kg > 0.0
    ]
    nb_apres_filtre = len(resultats_filtres)
    print(f"Produits sans Prix/kg (filtrés): {nb_avant_filtre - nb_apres_filtre} ({nb_apres_filtre} produits conservés).")
    
    # 2. Tri des résultats par Prix Unitaire (€/kg), du moins cher au plus cher
    resultats_tries = sorted(
        resultats_filtres, 
        key=lambda x: x.prix_unitaire_kg
    )
    
    return resultats_tries

async def main():
    """Fonction principale pour orchestrer le scraping multi-pages."""
    
    total_resultats: List[ProduitAmazon] = []
    current_row = 0

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context: BrowserContext = await browser.new_context()
        page: Page = await context.new_page()

        print(f"Démarrage du scraping de {MAX_PAGES} pages pour '{URL_RECHERCHE.split('=')[-1]}'...")

        for page_num in range(1, MAX_PAGES + 1):
            url = f"{URL_RECHERCHE}&page={page_num}"
            print(f"\n--- Scraping Page {page_num}/{MAX_PAGES} ---")
            
            try:
                # Amélioration: gérer les timeouts de navigation et de sélecteur séparément
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_selector(SELECTEURS["PRODUIT_BLOC"], timeout=15000)

                resultats_page = await extraire_donnees_page(page, current_row)
                total_resultats.extend(resultats_page)
                current_row += len(resultats_page)

            except Exception as e:
                print(f"Erreur critique ou timeout lors du traitement de la page {page_num}. Arrêt. Détail: {e}")
                break

        await browser.close()
    
    # --- POST-TRAITEMENT ---
    
    produits_tries = nettoyer_et_trier_resultats(total_resultats)
    
    # --- Écriture dans le fichier CSV ---
    ecrire_csv(produits_tries, NOM_FICHIER_CSV)
        
    print(f"\nScraping terminé. {len(produits_tries)} produits finaux enregistrés dans '{NOM_FICHIER_CSV}'.")


if __name__ == "__main__":
    if os.name == 'nt':
        # Correction de la politique de boucle d'événements pour Windows
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
    asyncio.run(main())