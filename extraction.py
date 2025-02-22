import requests
import pandas as pd
from bs4 import BeautifulSoup
import time

def fetch_books_from_google(genres, max_results=40):
    books = []
    for genre in genres:
        for start_index in range(0, 200, max_results):
            url = f"https://www.googleapis.com/books/v1/volumes?q=subject:{genre}&startIndex={start_index}&maxResults={max_results}"
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                for item in data.get("items", []):
                    volume_info = item.get("volumeInfo", {})
                    books.append({
                        "title": volume_info.get("title"),
                        "authors": ", ".join(volume_info.get("authors", [])),
                        "published_date": volume_info.get("publishedDate"),
                        "categories": ", ".join(volume_info.get("categories", [])),
                        "description": volume_info.get("description")
                    })
            time.sleep(1)
    return books

def scrape_skoob_books(pages=5):
    books = []
    for page in range(1, pages + 1):
        url = f"https://www.skoob.com.br/livro/lista/todos/todos/todos/{page}.html"
        response = requests.get(url)
        soup = BeautifulSoup(response.text, "html.parser")
        for book in soup.find_all("div", class_="box"):
            title = book.find("h3").text.strip()
            books.append({"title": title})
        time.sleep(1)
    return books

def fetch_books_from_csv(csv_path):
    try:
        return pd.read_csv(csv_path).to_dict(orient="records")
    except FileNotFoundError:
        return []

def save_to_csv(books, filename="books_dataset.csv"):
    df = pd.DataFrame(books)
    df.to_csv(filename, index=False)

def main():
    genres = [
        "fiction", "non-fiction", "mystery", "fantasy", "science fiction", "romance",
        "history", "biography", "self-help", "philosophy", "psychology", "thriller",
        "poetry", "graphic novels", "business", "education", "technology", "cooking",
        "health", "science", "sports", "travel", "art", "religion", "humor",
        "young adult", "children", "horror", "comics", "music", "drama", "politics"
    ]
    google_books = fetch_books_from_google(genres)
    skoob_books = scrape_skoob_books(pages=10)
    csv_books = fetch_books_from_csv("books_dataset.csv")
    all_books = google_books + skoob_books + csv_books
    save_to_csv(all_books)

if __name__ == "__main__":
    main()
