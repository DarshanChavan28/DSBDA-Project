import streamlit as st
import pandas as pd
import requests
import ast
import html
import concurrent.futures
import streamlit.components.v1 as components
import os
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

TMDB_API_KEY = "92652e0ae1066082e5d33800cd26f207"

st.set_page_config(page_title="Premium Streaming Dashboard", layout="wide", initial_sidebar_state="collapsed")

# Debug: Check if files exist
for f in ["tmdb_5000_movies.csv", "tmdb_5000_credits.csv"]:
    if os.path.exists(f):
        print(f"DEBUG: Found {f}")
    else:
        print(f"DEBUG: MISSING {f}!!")

# Inject Base Custom CSS
st.markdown("""
<style>
    /* Dark background & Typography */
    .stApp {
        background-color: #0E1117;
        color: white;
        font-family: 'Inter', sans-serif;
    }
    
    /* Hide Streamlit default UI elements */
    header {visibility: hidden;}
    footer {visibility: hidden;}
    
    /* Custom Scrollbar */
    ::-webkit-scrollbar {
        width: 8px;
    }
    ::-webkit-scrollbar-track {
        background: #0E1117; 
    }
    ::-webkit-scrollbar-thumb {
        background: #e50914; 
        border-radius: 4px;
    }
    ::-webkit-scrollbar-thumb:hover {
        background: #f40612; 
    }
    
    /* Styled buttons to look like the "View Details" overlay */
    .stButton button {
        background-color: transparent;
        color: #e50914;
        border: 2px solid #e50914;
        border-radius: 5px;
        font-weight: 700;
        width: 100%;
        transition: all 0.3s ease;
        margin-top: -10px;
    }
    .stButton button:hover {
        background-color: #e50914;
        transform: translateY(-2px);
        color: white;
        box-shadow: 0 5px 15px rgba(229, 9, 20, 0.4);
    }
    
    /* Make images flush with the button */
    [data-testid="stImage"] img {
        border-radius: 10px 10px 0 0;
        box-shadow: 0 4px 10px rgba(0,0,0,0.5);
        transition: filter 0.3s;
    }
    [data-testid="stImage"] img:hover {
        filter: brightness(0.8);
    }
    
    /* Genre Tags */
    .genre-tag {
        display: inline-block;
        background: rgba(255,255,255,0.1);
        padding: 5px 10px;
        border-radius: 15px;
        font-size: 0.85em;
        margin-right: 5px;
        margin-bottom: 5px;
        color: #ccc;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data
def load_data():
    movies = pd.read_csv("tmdb_5000_movies.csv")
    credits = pd.read_csv("tmdb_5000_credits.csv")
    
    movies = movies.merge(credits, left_on='id', right_on='movie_id')
    movies['overview'] = movies['overview'].fillna('')
    movies['release_date'] = movies['release_date'].fillna('')
    
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
    movies['keywords_list'] = movies['keywords'].apply(parse_name)
    
    def get_cast(x):
        try:
            res = ast.literal_eval(x)
            return [i['name'] for i in res[:5]]
        except:
            return []
            
    movies['cast_list'] = movies['cast'].apply(get_cast)
    movies['director'] = movies['crew'].apply(get_director)

    # Build combined tags from all content features for better recommendations
    def make_tags(row):
        parts = []
        # Overview text
        parts.append(str(row['overview']) if pd.notna(row['overview']) else '')
        # Genres repeated 3x for higher weight
        genres = row['genres_list'] if isinstance(row['genres_list'], list) else []
        parts.append((' '.join(genres) + ' ') * 3)
        # Keywords repeated 2x for higher weight
        kw = row['keywords_list'] if isinstance(row['keywords_list'], list) else []
        parts.append((' '.join(kw) + ' ') * 2)
        # Cast names joined without spaces so each name is a single token
        cast = row['cast_list'] if isinstance(row['cast_list'], list) else []
        parts.append(' '.join([c.replace(' ', '') for c in cast]))
        # Director repeated 2x for weight
        director = str(row['director']) if pd.notna(row['director']) else ''
        parts.append((director.replace(' ', '') + ' ') * 2)
        return ' '.join(parts)

    movies['tags'] = movies.apply(make_tags, axis=1)

    tfidf = TfidfVectorizer(stop_words='english', max_features=5000)
    tfidf_matrix = tfidf.fit_transform(movies['tags'])
    cosine_sim = cosine_similarity(tfidf_matrix, tfidf_matrix)

    movies['search_corpus'] = (
        movies['title_x'].fillna('') + ' ' + 
        movies['genres_list'].apply(lambda x: ' '.join(x) if isinstance(x, list) else '') + ' ' + 
        movies['keywords_list'].apply(lambda x: ' '.join(x) if isinstance(x, list) else '') + ' ' +
        movies['cast_list'].apply(lambda x: ' '.join(x) if isinstance(x, list) else '') + ' ' +
        movies['director'].fillna('')
    ).str.lower()

    return movies, cosine_sim

@st.cache_data(show_spinner=False)
def get_tmdb_assets(movie_id, title=""):
    fallback_poster = "https://dummyimage.com/500x750/1a1c23/666666.png&text=No+Poster"
    
    def _extract(data):
        p_path = data.get('poster_path')
        b_path = data.get('backdrop_path')
        if p_path and p_path.startswith('/'):
            p_path = p_path[1:]
        if b_path and b_path.startswith('/'):
            b_path = b_path[1:]
        poster = f"https://image.tmdb.org/t/p/w500/{p_path}" if p_path else fallback_poster
        backdrop = f"https://image.tmdb.org/t/p/original/{b_path}" if b_path else None
        return poster, backdrop
    
    try:
        # IMDB-sourced movies have synthetic IDs > 459488 — search by title
        if movie_id > 459488 and title:
            search_url = f"https://api.tmdb.org/3/search/movie?api_key={TMDB_API_KEY}&query={requests.utils.quote(str(title))}&language=en-US"
            resp = requests.get(search_url, timeout=3)
            if resp.status_code == 200:
                results = resp.json().get('results', [])
                if results:
                    return _extract(results[0])
            return fallback_poster, None
        
        # Original TMDB movie — direct ID lookup
        url = f"https://api.tmdb.org/3/movie/{movie_id}?api_key={TMDB_API_KEY}&language=en-US"
        response = requests.get(url, timeout=3)
        if response.status_code == 200:
            return _extract(response.json())
    except Exception:
        pass
    return fallback_poster, None

# Initialize Data
movies_df, cosine_sim = load_data()

@st.dialog("Movie Details", width="large")
def movie_modal(movie_row):
    poster_url, backdrop_url = get_tmdb_assets(movie_row['id'], movie_row.get('title_x', ''))
    
    col1, col2 = st.columns([1, 2])
    with col1:
        st.image(poster_url, width="stretch")
    with col2:
        st.markdown(f"<h1 style='margin-bottom:0;'>{movie_row['title_x']}</h1>", unsafe_allow_html=True)
        
        year = movie_row['release_date'][:4] if pd.notna(movie_row['release_date']) and str(movie_row['release_date']) != '' else 'N/A'
        st.markdown(f"<p style='color: #f5c518; font-weight: 600; font-size: 1.2em; text-shadow: 1px 1px 2px rgba(0,0,0,0.5);'>⭐ {movie_row['vote_average']} &nbsp;&nbsp;|&nbsp;&nbsp; 📅 {year}</p>", unsafe_allow_html=True)
        
        # Display Genre Tags
        genres_html = "".join([f"<span class='genre-tag'>{g}</span>" for g in movie_row['genres_list']])
        st.markdown(f"<div style='margin-bottom: 15px;'>{genres_html}</div>", unsafe_allow_html=True)
        
        st.write(movie_row['overview'])
        
        st.markdown("---")
        st.markdown(f"**🎬 Director:** {movie_row['director']}")
        cast_str = ", ".join(movie_row['cast_list']) if isinstance(movie_row['cast_list'], list) else str(movie_row['cast_list'])
        st.markdown(f"**🎭 Cast:** {cast_str}")

def prefetch_assets(movies_list):
    """Pre-fetch TMDB assets in parallel to eliminate one-by-one UI loading lag."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        list(executor.map(lambda r: get_tmdb_assets(r['id'], r.get('title_x', '')), [row for _, row in movies_list.iterrows()]))

def display_movie_grid(movies_list):
    # Pre-fetch all posters in parallel BEFORE Streamlit renders the UI
    prefetch_assets(movies_list)
    
    # display 5 movies per row
    for i in range(0, len(movies_list), 5):
        cols = st.columns(5)
        batch = movies_list.iloc[i:i+5]
        for col, (_, row) in zip(cols, batch.iterrows()):
            with col:
                poster_url, _ = get_tmdb_assets(row['id'], row.get('title_x', ''))
                st.image(poster_url, width="stretch")
                st.markdown(f"<div style='text-align: center; font-weight: 700; font-size: 14px; margin-bottom: 8px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; color: #ddd;' title=\"{row['title_x']}\">{row['title_x']}</div>", unsafe_allow_html=True)
                if st.button("View Details", key=f"btn_{row['id']}_{i}"):
                    movie_modal(row)

# Top Horizontal Navigation Bar
col_logo, col_nav, col_search = st.columns([1, 2, 1], vertical_alignment="center")

with col_logo:
    st.markdown("<h2 style='color: #e50914; font-weight: 900; margin-bottom: 0; letter-spacing: 2px;'>STREAMIFY</h2>", unsafe_allow_html=True)
    
with col_nav:
    # Horizontal pill-shaped navigation
    page = st.segmented_control(
        "Navigation",
        options=["Home", "Explore by Genre", "Top Rated"],
        default="Home",
        label_visibility="collapsed"
    )
    if not page:
        page = "Home"

with col_search:
    search_query = st.text_input("🔍 Quick Search", placeholder="Search any movie...", label_visibility="collapsed")

st.markdown("<hr style='margin-top: 10px; margin-bottom: 30px;'/>", unsafe_allow_html=True)

# Display Logic
if search_query:
    st.markdown(f"## 🔍 Search Results for '{search_query}'")
    search_results = movies_df[movies_df['search_corpus'].str.contains(search_query.lower(), na=False)].sort_values('popularity', ascending=False).head(20)
    if len(search_results) > 0:
        display_movie_grid(search_results)
    else:
        st.warning("No movies found. Try a different search term.")

elif page == "Home":
    # IFRAME Carousel: Bypasses all Streamlit HTML sanitizers!
    top_3_movies = movies_df.sort_values('popularity', ascending=False).head(3)
    
    # Pre-fetch hero images instantly
    prefetch_assets(top_3_movies)
    
    slides_html = ""
    for idx, (_, movie) in enumerate(top_3_movies.iterrows()):
        poster, backdrop = get_tmdb_assets(movie['id'], movie.get('title_x', ''))
        hero_bg = backdrop if backdrop else poster
        
        safe_title = html.escape(str(movie['title_x']))
        safe_overview = html.escape(str(movie['overview'])).replace("'", "&#39;")
        
        slides_html += f"""
        <div class="carousel-slide" style="background: linear-gradient(90deg, #0E1117 10%, rgba(14, 17, 23, 0.4) 100%), url('{hero_bg}') no-repeat center center/cover;">
            <div class="hero-content">
                <div class="featured-tag">#{idx+1} TRENDING</div>
                <h1>{safe_title}</h1>
                <p>{safe_overview}</p>
            </div>
        </div>
        """
        
    carousel_code = f"""
    <!DOCTYPE html>
    <html>
    <head>
    <style>
        body {{ margin: 0; padding: 0; background-color: #0E1117; font-family: 'Inter', sans-serif; overflow: hidden; }}
        .carousel-wrapper {{
            position: relative;
            width: 100vw;
            height: 600px;
            overflow: hidden;
            box-shadow: inset 0 -50px 50px -20px #0E1117;
        }}
        /* Hidden Radio Buttons */
        input[type="radio"] {{ display: none; }}
        
        .carousel-track {{
            display: flex;
            width: 300%;
            height: 100%;
            transition: transform 0.6s cubic-bezier(0.25, 1, 0.5, 1);
            position: absolute;
            top: 0; left: 0;
            z-index: 1;
        }}
        
        .carousel-slide {{
            width: 33.3333%;
            height: 100%;
            padding: 50px 100px;
            display: flex;
            align-items: center;
            color: white;
            box-sizing: border-box;
            box-shadow: inset 0 -50px 50px -20px #0E1117;
        }}
        
        .carousel-slide h1 {{ font-size: 4.5em; margin-bottom: 15px; font-weight: 900; text-shadow: 2px 2px 15px rgba(0,0,0,0.9); }}
        .carousel-slide p {{ font-size: 1.3em; line-height: 1.6; max-width: 700px; text-shadow: 1px 1px 10px rgba(0,0,0,0.9); color: #eee; }}
        .featured-tag {{ background-color: #e50914; color: white; padding: 8px 15px; border-radius: 4px; font-weight: bold; font-size: 1em; letter-spacing: 2px; margin-bottom: 20px; display: inline-block; }}
        
        #slide1:checked ~ .carousel-track {{ transform: translateX(0); }}
        #slide2:checked ~ .carousel-track {{ transform: translateX(-33.3333%); }}
        #slide3:checked ~ .carousel-track {{ transform: translateX(-66.6666%); }}
        
        .arrows {{ position: absolute; top: 0; left: 0; width: 100%; height: 100%; pointer-events: none; z-index: 10; }}
        .arrow-label {{
            display: none; position: absolute; top: 50%; transform: translateY(-50%); width: 60px; height: 60px;
            background: rgba(0,0,0,0.6); color: white; font-size: 35px; align-items: center; justify-content: center;
            border-radius: 50%; cursor: pointer; transition: background 0.3s, transform 0.3s; pointer-events: auto;
            border: 2px solid rgba(255,255,255,0.2); font-family: sans-serif;
        }}
        .arrow-label:hover {{ background: #e50914; transform: translateY(-50%) scale(1.1); border-color: #e50914; }}
        
        #slide1:checked ~ .arrows .arrow-next-1 {{ display: flex; right: 30px; }}
        #slide1:checked ~ .arrows .arrow-prev-1 {{ display: flex; left: 30px; }}
        #slide2:checked ~ .arrows .arrow-next-2 {{ display: flex; right: 30px; }}
        #slide2:checked ~ .arrows .arrow-prev-2 {{ display: flex; left: 30px; }}
        #slide3:checked ~ .arrows .arrow-next-3 {{ display: flex; right: 30px; }}
        #slide3:checked ~ .arrows .arrow-prev-3 {{ display: flex; left: 30px; }}
        
        .carousel-dots {{ position: absolute; bottom: 30px; left: 50%; transform: translateX(-50%); display: flex; gap: 15px; z-index: 10; pointer-events: none; }}
        .dot {{ width: 14px; height: 14px; border-radius: 50%; background: rgba(255,255,255,0.4); cursor: pointer; transition: background 0.3s, transform 0.3s; pointer-events: auto; }}
        .dot:hover {{ transform: scale(1.2); }}
        
        #slide1:checked ~ .carousel-dots label[for="slide1"],
        #slide2:checked ~ .carousel-dots label[for="slide2"],
        #slide3:checked ~ .carousel-dots label[for="slide3"] {{ background: #e50914; box-shadow: 0 0 10px #e50914; }}
    </style>
    </head>
    <body>
        <div class="carousel-wrapper">
            <input type="radio" name="slider" id="slide1" checked>
            <input type="radio" name="slider" id="slide2">
            <input type="radio" name="slider" id="slide3">

            <div class="carousel-track">
                {slides_html}
            </div>
            
            <div class="arrows">
                <label for="slide3" class="arrow-label arrow-prev-1">&#10094;</label>
                <label for="slide2" class="arrow-label arrow-next-1">&#10095;</label>
                
                <label for="slide1" class="arrow-label arrow-prev-2">&#10094;</label>
                <label for="slide3" class="arrow-label arrow-next-2">&#10095;</label>
                
                <label for="slide2" class="arrow-label arrow-prev-3">&#10094;</label>
                <label for="slide1" class="arrow-label arrow-next-3">&#10095;</label>
            </div>
            
            <div class="carousel-dots">
                <label for="slide1" class="dot"></label>
                <label for="slide2" class="dot"></label>
                <label for="slide3" class="dot"></label>
            </div>
        </div>
    </body>
    </html>
    """
    
    st.markdown("""
        <style>
        .block-container {
            padding-top: 1rem !important;
            padding-left: 0 !important;
            padding-right: 0 !important;
            max-width: 100% !important;
        }
        iframe {
            border: none;
            margin-bottom: 20px;
        }
        </style>
    """, unsafe_allow_html=True)
    
    components.html(carousel_code, height=600)

    
    st.markdown("### 🔥 Trending Now")
    trending = movies_df.sort_values('popularity', ascending=False).head(5)
    display_movie_grid(trending)
    
    st.markdown("---")
    st.markdown("### 🎬 Recommend Similar Movies")
    movie_list = movies_df['title_x'].values
    selected_movie = st.selectbox("Search for a movie you like to get smart recommendations:", movie_list)
    
    if st.button("Generate Recommendations", width="content"):
        idx = movies_df[movies_df['title_x'] == selected_movie].index[0]
        sim_scores = list(enumerate(cosine_sim[idx]))
        sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)[1:6]
        
        movie_indices = [i[0] for i in sim_scores]
        rec_movies = movies_df.iloc[movie_indices]
        
        st.markdown(f"#### Because you liked **{selected_movie}**, we highly recommend:")
        display_movie_grid(rec_movies)

elif page == "Explore by Genre":
    st.markdown("<h1 style='margin-bottom: 0;'>Explore by Genre</h1>", unsafe_allow_html=True)
    
    all_genres = set()
    for genres in movies_df['genres_list']:
        if isinstance(genres, list):
            all_genres.update(genres)
            
    selected_genre = st.selectbox("Filter database by your favorite genre:", sorted(list(all_genres)))
    
    mask = movies_df['genres_list'].apply(lambda x: selected_genre in x if isinstance(x, list) else False)
    genre_movies = movies_df[mask].sort_values('popularity', ascending=False).head(20)
    
    # Display Metrics
    col1, col2 = st.columns(2)
    col1.metric("Total Movies in Genre", len(movies_df[mask]))
    col2.metric("Highest Rated", genre_movies.iloc[0]['title_x'] if len(genre_movies) > 0 else "N/A")
    st.markdown("---")
    
    display_movie_grid(genre_movies)

elif page == "Top Rated":
    st.markdown("<h1 style='margin-bottom: 0;'>Top Rated Masterpieces</h1>", unsafe_allow_html=True)
    st.write("The highest-rated films of all time, curated by thousands of user reviews.")
    st.markdown("---")
    top_rated_movies = movies_df[movies_df['popularity'] > 20].sort_values('vote_average', ascending=False).head(20)
    display_movie_grid(top_rated_movies)
