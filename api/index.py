from flask import Flask, render_template, jsonify, request
import pandas as pd
import ast
import requests
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import os

app = Flask(__name__, template_folder='../templates', static_folder='../static')

TMDB_API_KEY = "92652e0ae1066082e5d33800cd26f207"

def load_data():
    try:
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        movies_path = os.path.join(base_dir, "tmdb_5000_movies.csv")
        credits_path = os.path.join(base_dir, "tmdb_5000_credits.csv")
        
        movies = pd.read_csv(movies_path)
        credits = pd.read_csv(credits_path)
        movies = movies.merge(credits, left_on='id', right_on='movie_id')
        movies['overview'] = movies['overview'].fillna('')
        
        def parse_name(x):
            try:
                res = ast.literal_eval(x)
                return [i['name'] for i in res]
            except:
                return []
                
        def get_director(x):
            try:
                res = ast.literal_eval(x)
                for i in res:
                    if i['job'] == 'Director':
                        return i['name']
                return "Unknown"
            except:
                return "Unknown"

        movies['genres_list'] = movies['genres'].apply(parse_name)
        movies['cast_list'] = movies['cast'].apply(lambda x: [i['name'] for i in ast.literal_eval(x)[:5]] if isinstance(x, str) else [])
        movies['director'] = movies['crew'].apply(get_director)

        tfidf = TfidfVectorizer(stop_words='english')
        tfidf_matrix = tfidf.fit_transform(movies['overview'])
        cosine_sim = cosine_similarity(tfidf_matrix, tfidf_matrix)

        return movies, cosine_sim
    except Exception as e:
        print(f"Error loading data: {e}")
        return None, None

_movies_df = None
_cosine_sim = None

def get_data():
    global _movies_df, _cosine_sim
    if _movies_df is None:
        _movies_df, _cosine_sim = load_data()
    return _movies_df, _cosine_sim

def get_poster(movie_id):
    try:
        url = f"https://api.tmdb.org/3/movie/{movie_id}?api_key={TMDB_API_KEY}"
        res = requests.get(url, timeout=3).json()
        path = res.get('poster_path')
        if path:
            return f"https://image.tmdb.org/t/p/w500{path}"
    except:
        pass
    return "https://dummyimage.com/500x750/1a1c23/666666.png&text=No+Poster"

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/health')
def health():
    return jsonify({"status": "alive"})

@app.route('/api/trending')
def trending():
    movies_df, _ = get_data()
    if movies_df is None: return jsonify({"error": "Data load failed"}), 500
    top = movies_df.sort_values('popularity', ascending=False).head(10)
    results = []
    for _, row in top.iterrows():
        results.append({
            "id": int(row['id']),
            "title": row['title_x'],
            "poster": get_poster(row['id'])
        })
    return jsonify(results)

@app.route('/api/genres')
def genres():
    movies_df, _ = get_data()
    if movies_df is None: return jsonify({"error": "Data load failed"}), 500
    all_genres = set()
    for gs in movies_df['genres_list']:
        all_genres.update(gs)
    return jsonify(sorted(list(all_genres)))

@app.route('/api/genre/<genre>')
def genre_movies(genre):
    movies_df, _ = get_data()
    if movies_df is None: return jsonify({"error": "Data load failed"}), 500
    mask = movies_df['genres_list'].apply(lambda x: genre in x)
    filtered = movies_df[mask].sort_values('popularity', ascending=False).head(10)
    results = []
    for _, row in filtered.iterrows():
        results.append({
            "id": int(row['id']),
            "title": row['title_x'],
            "poster": get_poster(row['id'])
        })
    return jsonify(results)

@app.route('/api/top-rated')
def top_rated():
    movies_df, _ = get_data()
    if movies_df is None: return jsonify({"error": "Data load failed"}), 500
    top = movies_df.sort_values('vote_average', ascending=False).head(10)
    results = []
    for _, row in top.iterrows():
        results.append({
            "id": int(row['id']),
            "title": row['title_x'],
            "poster": get_poster(row['id'])
        })
    return jsonify(results)

@app.route('/api/movie/<int:movie_id>')
def movie_details(movie_id):
    movies_df, _ = get_data()
    if movies_df is None: return jsonify({"error": "Data load failed"}), 500
    row = movies_df[movies_df['id'] == movie_id]
    if row.empty:
        return jsonify({"error": "Not found"}), 404
    row = row.iloc[0]
    return jsonify({
        "id": int(row['id']),
        "title": row['title_x'],
        "overview": row['overview'],
        "vote_average": row['vote_average'],
        "release_date": row['release_date'],
        "genres": row['genres_list'],
        "cast": row['cast_list'],
        "director": row['director'],
        "poster": get_poster(row['id'])
    })

@app.route('/api/recommend')
def recommend():
    movies_df, cosine_sim = get_data()
    if movies_df is None: return jsonify({"error": "Data load failed"}), 500
    title = request.args.get('title')
    try:
        idx = movies_df[movies_df['title_x'].str.contains(title, case=False)].index[0]
        sim_scores = list(enumerate(cosine_sim[idx]))
        sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)[1:6]
        movie_indices = [i[0] for i in sim_scores]
        recs = movies_df.iloc[movie_indices]
        results = []
        for _, row in recs.iterrows():
            results.append({
                "id": int(row['id']),
                "title": row['title_x'],
                "poster": get_poster(row['id'])
            })
        return jsonify(results)
    except:
        return jsonify({"error": "Movie not found"}), 404

# Export the app for Vercel
application = app

if __name__ == '__main__':
    app.run(debug=True)
