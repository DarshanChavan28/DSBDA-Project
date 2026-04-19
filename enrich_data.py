"""
Enrich existing IMDB-sourced movies with TMDB ratings/popularity,
and fetch popular 2024-2025 movies from TMDB discover API.
Uses unbuffered prints and robust error handling.
"""
import sys
import pandas as pd
import requests
import time
import ast

# Force unbuffered output
def log(msg):
    print(msg)
    sys.stdout.flush()

TMDB_API_KEY = "92652e0ae1066082e5d33800cd26f207"
BASE = "https://api.tmdb.org/3"

movies_df = pd.read_csv("tmdb_5000_movies.csv")
credits_df = pd.read_csv("tmdb_5000_credits.csv")

log(f"Starting with {len(movies_df)} movies, {len(credits_df)} credits")

# ═══════════════════════════════════════════════════════════════════════════
# PART 1: Enrich movies with vote_average == 0 using TMDB search
# ═══════════════════════════════════════════════════════════════════════════
zero_mask = movies_df['vote_average'] == 0.0
zero_indices = movies_df[zero_mask].index.tolist()
log(f"\n[Part 1] Enriching {len(zero_indices)} movies with missing ratings...")

enriched = 0
failed = 0

for count, idx in enumerate(zero_indices, 1):
    title = str(movies_df.at[idx, 'title'])
    try:
        resp = requests.get(f"{BASE}/search/movie", params={
            "api_key": TMDB_API_KEY, "query": title, "language": "en-US"
        }, timeout=8)

        if resp.status_code == 429:
            # Rate limited - wait and retry
            time.sleep(2)
            resp = requests.get(f"{BASE}/search/movie", params={
                "api_key": TMDB_API_KEY, "query": title, "language": "en-US"
            }, timeout=8)

        if resp.status_code == 200:
            results = resp.json().get('results', [])
            if results:
                best = results[0]
                movies_df.at[idx, 'vote_average'] = best.get('vote_average', 0)
                movies_df.at[idx, 'vote_count'] = best.get('vote_count', 0)
                movies_df.at[idx, 'popularity'] = best.get('popularity', 0)
                rd = best.get('release_date', '')
                cur_rd = str(movies_df.at[idx, 'release_date'])
                if rd and (cur_rd == '' or cur_rd == 'nan' or cur_rd.endswith('-01-01')):
                    movies_df.at[idx, 'release_date'] = rd
                enriched += 1
            else:
                failed += 1
        else:
            failed += 1

    except Exception as e:
        failed += 1

    # Rate limiting
    if count % 38 == 0:
        time.sleep(1.5)

    if count % 100 == 0:
        log(f"  Progress: {count}/{len(zero_indices)} ({enriched} enriched, {failed} not found)")

log(f"  [Part 1 DONE] {enriched} enriched, {failed} not found")

# ═══════════════════════════════════════════════════════════════════════════
# PART 2: Fetch popular 2024-2025 movies from TMDB discover
# ═══════════════════════════════════════════════════════════════════════════
log(f"\n[Part 2] Fetching popular 2024-2025 movies...")

existing_titles = set(movies_df['title'].str.strip().str.lower())
existing_ids_set = set(movies_df['id'].astype(int).tolist())

new_movies = []
new_credits = []

for year in [2024, 2025]:
    for page in range(1, 11):  # 10 pages = ~200 movies per year
        try:
            resp = requests.get(f"{BASE}/discover/movie", params={
                "api_key": TMDB_API_KEY,
                "language": "en-US",
                "sort_by": "popularity.desc",
                "primary_release_year": year,
                "page": page,
                "vote_count.gte": 20,
            }, timeout=8)

            if resp.status_code == 429:
                time.sleep(3)
                resp = requests.get(f"{BASE}/discover/movie", params={
                    "api_key": TMDB_API_KEY,
                    "language": "en-US",
                    "sort_by": "popularity.desc",
                    "primary_release_year": year,
                    "page": page,
                    "vote_count.gte": 20,
                }, timeout=8)

            if resp.status_code != 200:
                log(f"  Discover failed: status {resp.status_code}")
                break

            data = resp.json()
            results = data.get('results', [])
            if not results:
                break

            for movie_brief in results:
                tmdb_id = movie_brief['id']
                title_lower = movie_brief.get('title', '').strip().lower()

                if title_lower in existing_titles or tmdb_id in existing_ids_set:
                    continue

                # Fetch details
                try:
                    det_resp = requests.get(f"{BASE}/movie/{tmdb_id}", params={
                        "api_key": TMDB_API_KEY, "language": "en-US"
                    }, timeout=8)
                    if det_resp.status_code == 429:
                        time.sleep(2)
                        det_resp = requests.get(f"{BASE}/movie/{tmdb_id}", params={
                            "api_key": TMDB_API_KEY, "language": "en-US"
                        }, timeout=8)
                    if det_resp.status_code != 200:
                        continue
                    det = det_resp.json()
                except:
                    continue

                # Fetch credits
                try:
                    cred_resp = requests.get(f"{BASE}/movie/{tmdb_id}/credits", params={
                        "api_key": TMDB_API_KEY, "language": "en-US"
                    }, timeout=8)
                    if cred_resp.status_code == 429:
                        time.sleep(2)
                        cred_resp = requests.get(f"{BASE}/movie/{tmdb_id}/credits", params={
                            "api_key": TMDB_API_KEY, "language": "en-US"
                        }, timeout=8)
                    if cred_resp.status_code != 200:
                        continue
                    cred = cred_resp.json()
                except:
                    continue

                # Build ast-compatible data using repr()
                genres_list = [{"id": g["id"], "name": g["name"]} for g in det.get("genres", [])]
                spoken = [{"iso_639_1": l.get("iso_639_1", ""), "name": l.get("name", "")} for l in det.get("spoken_languages", [])]
                prod_co = [{"name": c.get("name", ""), "id": c.get("id", 0)} for c in det.get("production_companies", [])]
                prod_cn = [{"iso_3166_1": c.get("iso_3166_1", ""), "name": c.get("name", "")} for c in det.get("production_countries", [])]

                cast_list = []
                for i, c in enumerate(cred.get("cast", [])[:10]):
                    cast_list.append({
                        "cast_id": c.get("cast_id", i),
                        "character": c.get("character", ""),
                        "credit_id": c.get("credit_id", ""),
                        "gender": c.get("gender", 0),
                        "id": c.get("id", 0),
                        "name": c.get("name", ""),
                        "order": c.get("order", i),
                        "profile_path": c.get("profile_path"),
                    })

                crew_list = []
                for c in cred.get("crew", []):
                    crew_list.append({
                        "credit_id": c.get("credit_id", ""),
                        "department": c.get("department", ""),
                        "gender": c.get("gender", 0),
                        "id": c.get("id", 0),
                        "job": c.get("job", ""),
                        "name": c.get("name", ""),
                        "profile_path": c.get("profile_path"),
                    })

                new_movies.append({
                    "budget": det.get("budget", 0),
                    "genres": repr(genres_list),
                    "homepage": det.get("homepage", "") or "",
                    "id": tmdb_id,
                    "keywords": "[]",
                    "original_language": det.get("original_language", "en"),
                    "original_title": det.get("original_title", ""),
                    "overview": det.get("overview", ""),
                    "popularity": det.get("popularity", 0),
                    "production_companies": repr(prod_co),
                    "production_countries": repr(prod_cn),
                    "release_date": det.get("release_date", ""),
                    "revenue": det.get("revenue", 0),
                    "runtime": det.get("runtime", 0),
                    "spoken_languages": repr(spoken),
                    "status": det.get("status", "Released"),
                    "tagline": det.get("tagline", "") or "",
                    "title": det.get("title", ""),
                    "vote_average": det.get("vote_average", 0),
                    "vote_count": det.get("vote_count", 0),
                })

                new_credits.append({
                    "movie_id": tmdb_id,
                    "title": det.get("title", ""),
                    "cast": repr(cast_list),
                    "crew": repr(crew_list),
                })

                existing_titles.add(title_lower)
                existing_ids_set.add(tmdb_id)

                # Rate limiting: ~2 detail+credit calls per movie
                time.sleep(0.12)

            if page >= data.get('total_pages', 1):
                break

        except Exception as e:
            log(f"  Error year={year} page={page}: {e}")
            continue

    log(f"  Year {year} done: {len(new_movies)} total new movies so far")

log(f"  [Part 2 DONE] {len(new_movies)} new 2024-2025 movies fetched")

# ═══════════════════════════════════════════════════════════════════════════
# PART 3: Save
# ═══════════════════════════════════════════════════════════════════════════
if new_movies:
    movies_df = pd.concat([movies_df, pd.DataFrame(new_movies)], ignore_index=True)
    credits_df = pd.concat([credits_df, pd.DataFrame(new_credits)], ignore_index=True)

movies_df.to_csv("tmdb_5000_movies.csv", index=False)
credits_df.to_csv("tmdb_5000_credits.csv", index=False)

log(f"\n[OK] Final dataset: {len(movies_df)} movies, {len(credits_df)} credits")
log(f"  Rating > 0: {len(movies_df[movies_df['vote_average'] > 0])}")
log(f"  Rating = 0: {len(movies_df[movies_df['vote_average'] == 0])}")

for t in ["Dabangg", "Bodyguard", "Jawan", "Dhurandhar", "Project Hail Mary"]:
    f = movies_df[movies_df['title'].str.contains(t, case=False, na=False)]
    if len(f) > 0:
        log(f"  {t}: FOUND (rating={f.iloc[0]['vote_average']}, pop={f.iloc[0]['popularity']:.1f})")
    else:
        log(f"  {t}: NOT FOUND")
