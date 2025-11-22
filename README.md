#  Analyse WiFi Paris - Sites Culturels vs Bibliothèques

Analyse comportementale des usages WiFi dans les lieux publics parisiens basée sur les données OpenData Paris.

##  Objectif du Projet

Ce projet analyse **198 120 sessions WiFi** dans les lieux publics parisiens pour identifier les différences comportementales entre sites culturels et bibliothèques. 

**Résultat principal :** Les sites culturels montrent un usage mobile de **92%** contre **64,5%** pour les bibliothèques (différence de 27,5pp, p < 0,001).

## Structure du Projet

```
paris-wifi-analysis/
├── data/
│   ├── processed/
│   │   └── wifi_cleaned.csv          # Données nettoyées (198K sessions)
│   └── raw/
│       └── paris_wifi_*.csv          # Données brutes extraites
├── notebooks/
│   └── eda.ipynb                     # Analyse exploratoire complète
├── src/
│   ├── clean.py                      # Script de nettoyage des données
│   ├── data_extractor.py             # Extraction API OpenData Paris
│   └── postgresfix.py                # Configuration PostgreSQL
├── .env                               # Variables d'environnement 
├── .gitignore
├── README.md
└── requirements.txt                   # Dépendances Python
```

##  Installation & Configuration

### Prérequis

- Python 3.8+
- PostgreSQL 12+

### Installation

```bash
# Cloner le repo
git clone https://github.com/votre-username/paris-wifi-analysis.git
cd paris-wifi-analysis

# Créer environnement virtuel
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Installer dépendances
pip install -r requirements.txt
```

### Configuration Base de Données

Créer un fichier `.env` à la racine :

```env
DB_NAME=your_database_name
DB_USER=your_username
DB_PASSWORD=your_password
DB_HOST=localhost
DB_PORT=5432
```

##  Utilisation

### 1. Extraction des Données

```bash
python src/data_extractor.py
```

- Extrait 200 000 sessions depuis l'API OpenData Paris
- Stocke dans PostgreSQL
- Durée : ~30-40 minutes

### 2. Nettoyage des Données

```bash
python src/clean.py
```

- Nettoie et transforme les données
- Crée variables temporelles, géographiques, d'usage
- Classifie les types de lieux
- Output : `data/processed/wifi_cleaned.csv`

### 3. Analyse Exploratoire

```bash
jupyter notebook notebooks/eda.ipynb
```

Ouvre le notebook avec :
- Statistiques descriptives
- Tests d'hypothèses (t-tests, chi-square)
- 8 visualisations détaillées
- Insights clés

##  Méthodologie

### Classification des Lieux

**4 catégories identifiées :**

1. **Sites Culturels (4,9%)** : Musées, monuments
   - Mots-clés : "musée", "tour saint jacques", etc.

2. **Bibliothèques (35,9%)** : Bibliothèques publiques
   - Mots-clés : "bib", "bibliothèque"

3. **Lieux à Fort Trafic (7,3%)** : Mairies, parvis
   - Mots-clés : "hotel de ville", "parvis", "mairie"

4. **Résidentiel (51,9%)** : Autres lieux publics

### Variables Créées

- **Temporelles** : heure, jour_semaine, est_weekend, moment_journée
- **Géographiques** : arrondissement
- **Appareils** : catégorie_appareil (Mobile/Ordinateur/Autre)
- **Usage** : total_data_mb, data_par_minute
- **Flags qualité** : durée_extrême, utilisateur_intensif

##  Résultats Clés

### Résultat Principal : La Fracture Mobile

| Métrique | Sites Culturels | Bibliothèques | Différence |
|----------|----------------|---------------|------------|
| Usage Mobile | **92,0%** | 64,5% | **+27,5pp** |
| Usage Ordinateur | 7,2% | **33,3%** | -26,1pp |

**Test statistique :** χ² = 2978,34, p < 0,000001 

###  Durée des Sessions

- **Sites culturels :** 35,1 min (moyenne)
- **Bibliothèques :** 40,3 min (moyenne)
- **Différence :** -12,9% (t = -10,497, p < 0,001) 

###  Patterns Temporels

- **Pic culturel :** 13h00 (après-midi visiteurs)
- **Pic bibliothèques :** 14h00 (après-école/travail)
- **Weekend :** 25,4% vs 22,9%

###  Consommation de Données

- **Sites culturels :** 87,7 MB (moyenne)
- **Bibliothèques :** 81,7 MB (moyenne)
- **Différence :** +7,3% (t = 3,092, p = 0,002) 

##  Visualisations

Le notebook génère 8 visualisations :

1. Distribution durée sessions (histogramme)
2. Distribution appareils (barres)
3. Patterns horaires (courbe)
4. Weekend vs semaine (barres)
5. Box plots durée
6. Heatmap culturel (jour × heure)
7. Heatmap bibliothèques (jour × heure)
8. Comparaison usage mobile (barres)

##  Limites

- **Échantillon** : 99% des données du 4ème arrondissement
- **Classification** : Basée sur noms de lieux (~10% erreur potentielle)
- **Temporel** : Données 2019-2020 (pré-COVID)
- **Scope** : WiFi public uniquement

Malgré ces limites, tous les résultats sont statistiquement significatifs (p < 0,001).

## 🛠️ Technologies Utilisées

- **Python 3.8+** : Langage principal
- **PostgreSQL** : Stockage données
- **pandas** : Manipulation données
- **scipy** : Tests statistiques
- **matplotlib/seaborn** : Visualisations
- **requests** : Extraction API
- **python-dotenv** : Gestion configuration

## Source des Données

**OpenData Paris - Utilisation des Hotspots Paris WiFi**

- URL : https://opendata.paris.fr/explore/dataset/paris-wi-fi-utilisation-des-hotspots-paris-wi-fi/
- API : https://opendata.paris.fr/api/explore/v2.1/
- Période : 2019-2024
- Volume : 6,2M+ sessions disponibles
