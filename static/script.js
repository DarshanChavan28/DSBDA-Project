document.addEventListener('DOMContentLoaded', () => {
    // Navigation
    const navBtns = document.querySelectorAll('.nav-btn');
    const sections = document.querySelectorAll('.page-section');

    navBtns.forEach(btn => {
        btn.addEventListener('click', (e) => {
            e.preventDefault();
            navBtns.forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            
            const targetId = btn.getAttribute('data-target');
            sections.forEach(sec => sec.classList.remove('active'));
            document.getElementById(targetId).classList.add('active');

            if (targetId === 'explore' && !window.genresLoaded) {
                loadGenres();
                window.genresLoaded = true;
            } else if (targetId === 'top-rated' && !window.topRatedLoaded) {
                loadTopRated();
                window.topRatedLoaded = true;
            }
        });
    });

    // Initial Load for Home
    loadTrending();

    // Recommend Button
    document.getElementById('recommend-btn').addEventListener('click', () => {
        const title = document.getElementById('search-input').value;
        if(title) {
            showLoader('recommend-grid');
            fetch(`/api/recommend?title=${encodeURIComponent(title)}`)
                .then(res => res.json())
                .then(data => {
                    if (data.error) {
                        alert(data.error);
                        document.getElementById('recommend-grid').innerHTML = '';
                    } else {
                        renderGrid('recommend-grid', data);
                    }
                })
                .catch(err => {
                    alert("Movie not found in database or error occurred.");
                    document.getElementById('recommend-grid').innerHTML = '';
                });
        }
    });

    // Genre Select
    document.getElementById('genre-select').addEventListener('change', (e) => {
        loadGenreMovies(e.target.value);
    });

    // Modal Close
    document.getElementById('close-modal').addEventListener('click', () => {
        document.getElementById('movie-modal').classList.remove('active');
    });

    document.getElementById('movie-modal').addEventListener('click', (e) => {
        if(e.target === document.getElementById('movie-modal')){
             document.getElementById('movie-modal').classList.remove('active');
        }
    });
});

function loadTrending() {
    showLoader('trending-grid');
    fetch('/api/trending')
        .then(res => res.json())
        .then(data => renderGrid('trending-grid', data));
}

function loadGenres() {
    fetch('/api/genres')
        .then(res => res.json())
        .then(genres => {
            const select = document.getElementById('genre-select');
            select.innerHTML = '';
            genres.forEach(g => {
                const opt = document.createElement('option');
                opt.value = g;
                opt.textContent = g;
                select.appendChild(opt);
            });
            if (genres.length > 0) {
                loadGenreMovies(genres[0]);
            }
        });
}

function loadGenreMovies(genre) {
    showLoader('genre-grid');
    fetch(`/api/genre/${encodeURIComponent(genre)}`)
        .then(res => res.json())
        .then(data => renderGrid('genre-grid', data));
}

function loadTopRated() {
    showLoader('top-rated-grid');
    fetch('/api/top-rated')
        .then(res => res.json())
        .then(data => renderGrid('top-rated-grid', data));
}

function showLoader(containerId) {
    const container = document.getElementById(containerId);
    container.innerHTML = '<div class="loader"></div>';
}

function renderGrid(containerId, movies) {
    const container = document.getElementById(containerId);
    container.innerHTML = '';
    movies.forEach(m => {
        const card = document.createElement('div');
        card.className = 'movie-card';
        card.innerHTML = `
            <img src="${m.poster}" alt="${m.title}">
            <div class="info">
                <div class="title">${m.title}</div>
            </div>
        `;
        card.addEventListener('click', () => openModal(m.id));
        container.appendChild(card);
    });
}

function openModal(movieId) {
    fetch(`/api/movie/${movieId}`)
        .then(res => res.json())
        .then(data => {
            if (data.error) {
                alert("Could not fetch details.");
                return;
            }
            document.getElementById('modal-poster').src = data.poster;
            document.getElementById('modal-title').textContent = data.title;
            document.getElementById('modal-rating').textContent = `★ ${data.vote_average}`;
            
            // Extract year from release_date if available
            let year = "N/A";
            if (data.release_date && data.release_date.length >= 4) {
                year = data.release_date.substring(0, 4);
            }
            document.getElementById('modal-year').textContent = year;
            
            document.getElementById('modal-overview').textContent = data.overview || "No overview available.";
            document.getElementById('modal-director').textContent = data.director || "Unknown";
            
            const cast = (data.cast && data.cast.length > 0) ? data.cast.join(', ') : "Unknown";
            document.getElementById('modal-cast').textContent = cast;
            
            document.getElementById('movie-modal').classList.add('active');
        })
        .catch(err => {
            console.error(err);
            alert("Error loading movie details.");
        });
}
