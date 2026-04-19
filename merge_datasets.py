"""
Merge IMDB-Movie-Dataset(2023-1951).csv into tmdb_5000_movies.csv and tmdb_5000_credits.csv.
Deduplicates by (title, year). Generates ast.literal_eval()-compatible Python dicts (None not null).
"""
import pandas as pd
import json

# ── Load datasets ──────────────────────────────────────────────────────────
imdb = pd.read_csv("IMDB-Movie-Dataset(2023-1951).csv")
tmdb = pd.read_csv("tmdb_5000_movies.csv")
credits = pd.read_csv("tmdb_5000_credits.csv")

print(f"IMDB movies:   {len(imdb)}")
print(f"TMDB movies:   {len(tmdb)}")
print(f"Credits rows:  {len(credits)}")

# ── Normalise for matching ─────────────────────────────────────────────────
tmdb["_title_lower"] = tmdb["title"].str.strip().str.lower()
tmdb["_year"] = pd.to_datetime(tmdb["release_date"], errors="coerce").dt.year

imdb["_title_lower"] = imdb["movie_name"].str.strip().str.lower()
imdb["_year"] = pd.to_numeric(imdb["year"], errors="coerce").astype("Int64")

existing = set(zip(tmdb["_title_lower"], tmdb["_year"]))

mask_new = imdb.apply(lambda r: (r["_title_lower"], r["_year"]) not in existing, axis=1)
imdb_new = imdb[mask_new].copy().reset_index(drop=True)
print(f"\nDuplicates skipped: {(~mask_new).sum()}")
print(f"New movies to add:  {len(imdb_new)}")

# ── Genre mapping ──────────────────────────────────────────────────────────
GENRE_ID_MAP = {
    "action": 28, "adventure": 12, "animation": 16, "comedy": 35,
    "crime": 80, "documentary": 99, "drama": 18, "family": 10751,
    "fantasy": 14, "history": 36, "horror": 27, "music": 10402,
    "musical": 10402, "mystery": 9648, "romance": 10749, "sci-fi": 878,
    "science fiction": 878, "sport": 9805, "thriller": 53,
    "tv movie": 10770, "war": 10752, "western": 37, "biography": 36,
    "film-noir": 53, "news": 99, "short": 99, "talk-show": 10770,
    "reality-tv": 10770, "game-show": 10770,
}

def genres_to_pystr(genre_str):
    """Convert 'Action, Drama' -> Python repr list string like
    [{'id': 28, 'name': 'Action'}, {'id': 18, 'name': 'Drama'}]"""
    if pd.isna(genre_str):
        return "[]"
    parts = [g.strip() for g in str(genre_str).split(",")]
    result = []
    for g in parts:
        gid = GENRE_ID_MAP.get(g.lower(), 99999)
        result.append({"id": gid, "name": g.strip()})
    # Use repr() so we get None instead of null, True/False instead of true/false
    return repr(result)

def cast_to_pystr(cast_str):
    """Convert 'Actor1, Actor2, ...' -> Python repr cast list string."""
    if pd.isna(cast_str):
        return "[]"
    actors = [a.strip() for a in str(cast_str).split(",")]
    result = []
    for i, name in enumerate(actors):
        result.append({
            "cast_id": i,
            "character": "",
            "credit_id": "",
            "gender": 0,
            "id": 0,
            "name": name,
            "order": i,
            "profile_path": None,
        })
    return repr(result)

def director_to_crew_pystr(director_str):
    """Convert 'Director Name' -> Python repr crew list string."""
    if pd.isna(director_str):
        return "[]"
    return repr([{
        "credit_id": "",
        "department": "Directing",
        "gender": 0,
        "id": 0,
        "job": "Director",
        "name": str(director_str).strip(),
        "profile_path": None,
    }])

# ── Assign new IDs ────────────────────────────────────────────────────────
max_id = int(tmdb["id"].max())
new_ids = list(range(max_id + 1, max_id + 1 + len(imdb_new)))
imdb_new["new_id"] = new_ids

# ── Build new TMDB-format rows ────────────────────────────────────────────
new_tmdb_rows = pd.DataFrame({
    "budget":               0,
    "genres":               imdb_new["genre"].apply(genres_to_pystr),
    "homepage":             "",
    "id":                   imdb_new["new_id"],
    "keywords":             "[]",
    "original_language":    "hi",
    "original_title":       imdb_new["movie_name"],
    "overview":             imdb_new["overview"],
    "popularity":           0.0,
    "production_companies": "[]",
    "production_countries": "[]",
    "release_date":         imdb_new["_year"].apply(lambda y: f"{y}-01-01" if pd.notna(y) else ""),
    "revenue":              0,
    "runtime":              0.0,
    "spoken_languages":     repr([{"iso_639_1": "hi", "name": "Hindi"}]),
    "status":               "Released",
    "tagline":              "",
    "title":                imdb_new["movie_name"],
    "vote_average":         0.0,
    "vote_count":           0,
})

# ── Build new credits rows ────────────────────────────────────────────────
new_credits_rows = pd.DataFrame({
    "movie_id": imdb_new["new_id"],
    "title":    imdb_new["movie_name"],
    "cast":     imdb_new["cast"].apply(cast_to_pystr),
    "crew":     imdb_new["director"].apply(director_to_crew_pystr),
})

# ── Merge & save ──────────────────────────────────────────────────────────
tmdb.drop(columns=["_title_lower", "_year"], inplace=True)

merged_tmdb = pd.concat([tmdb, new_tmdb_rows], ignore_index=True)
merged_credits = pd.concat([credits, new_credits_rows], ignore_index=True)

merged_tmdb.to_csv("tmdb_5000_movies.csv", index=False)
merged_credits.to_csv("tmdb_5000_credits.csv", index=False)

print(f"\n[OK] Merged successfully!")
print(f"   tmdb_5000_movies.csv : {len(tmdb)} -> {len(merged_tmdb)} rows")
print(f"   tmdb_5000_credits.csv: {len(credits)} -> {len(merged_credits)} rows")

# ── Verify ast.literal_eval works on new data ─────────────────────────────
import ast
sample_cast = new_credits_rows.iloc[0]["cast"]
sample_crew = new_credits_rows.iloc[0]["crew"]
sample_genres = new_tmdb_rows.iloc[0]["genres"]
try:
    parsed_cast = ast.literal_eval(sample_cast)
    parsed_crew = ast.literal_eval(sample_crew)
    parsed_genres = ast.literal_eval(sample_genres)
    print(f"\n[OK] ast.literal_eval verification passed!")
    print(f"   Sample cast:     {[c['name'] for c in parsed_cast[:3]]}")
    print(f"   Sample director: {[c['name'] for c in parsed_crew if c.get('job')=='Director']}")
    print(f"   Sample genres:   {[g['name'] for g in parsed_genres]}")
except Exception as e:
    print(f"\n[FAIL] ast.literal_eval failed: {e}")
