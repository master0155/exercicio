import requests
import time
import csv
import random
import concurrent.futures
from bs4 import BeautifulSoup

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246'
}

MAX_WORKERS = 10


def extract_movie_details(movie_link):
    """Função que baixa os detalhes de 1 filme"""
    time.sleep(random.uniform(0, 0.2))
    response = requests.get(movie_link, headers=headers)


    if response.status_code != 200:
        return None

    movie_soup = BeautifulSoup(response.content, 'html.parser')

    title = None
    date = None
    rating = None
    plot_text = None

    try:
        page_section = movie_soup.find('section', attrs={'class': 'ipc-page-section'})
        if page_section:
            divs = page_section.find_all('div', recursive=False)
            if len(divs) > 1:
                target_div = divs[1]

                title_tag = target_div.find('h1')
                if title_tag:
                    title = title_tag.find('span').get_text()

                date_tag = target_div.find('a', href=lambda href: href and 'releaseinfo' in href)
                if date_tag:
                    date = date_tag.get_text().strip()
            
                rating_tag = movie_soup.find('div', attrs={'data-testid': 'hero-rating-bar__aggregate-rating__score'})
                rating = rating_tag.get_text() if rating_tag else None

                plot_tag = movie_soup.find('span', attrs={'data-testid': 'plot-xs_to_m'})
                plot_text = plot_tag.get_text().strip() if plot_tag else None
    except Exception:
        return None

    if all([title, date, rating, plot_text]):
        return (title, date, rating, plot_text)
    return None

def extract_movies(soup):
    """Extrai os links dos filmes mais populares"""
    movies_table = soup.find('div', attrs={'data-testid': 'chart-layout-main-column'}).find('ul')
    movies_table_rows = movies_table.find_all('li')
    movie_links = ['https://imdb.com' + movie.find('a')['href'] for movie in movies_table_rows]
    return movie_links


def run_with_threads(movie_links):
    """Executa o scraping usando threads"""
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for res in executor.map(extract_movie_details, movie_links):
            if res:
                results.append(res)
    return results

def run_with_processes(movie_links):
    """Executa o scraping usando processos"""
    results = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for res in executor.map(extract_movie_details, movie_links):
            if res:
                results.append(res)
    return results

def save_to_csv(data, filename):
    """Salva no CSV"""
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        movie_writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        movie_writer.writerow(["Title", "Date", "Rating", "Plot"])
        movie_writer.writerows(data)

def main():
    popular_movies_url = 'https://www.imdb.com/chart/moviemeter/?ref_=nv_mv_mpm'
    response = requests.get(popular_movies_url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')

    movie_links = extract_movies(soup)

    start = time.time()
    data_threads = run_with_threads(movie_links)
    end = time.time()
    print(f"Tempo com Threads: {end - start:.2f} segundos")
    save_to_csv(data_threads, "movies_threads.csv")

    start = time.time()
    data_processes = run_with_processes(movie_links)
    end = time.time()
    print(f"Tempo com Processos: {end - start:.2f} segundos")
    save_to_csv(data_processes, "movies_processes.csv")




if __name__ == '__main__':
    main()