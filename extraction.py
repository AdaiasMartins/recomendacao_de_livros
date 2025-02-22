import requests
import pandas as pd
from bs4 import BeautifulSoup
import time
import kagglehub

def fetch_books_from_google(genres, max_results=40):
    books = []
    for genre in genres:
        for start_index in range(0, 200, max_results):
            url = f"https://www.googleapis.com/books/v1/volumes?q=subject:{genre}&filter=paid-ebooks&orderBy=relevance&startIndex={start_index}&maxResults={max_results}"
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                for item in data.get("items", []):
                    volume_info = item.get("volumeInfo", {})
                    if "averageRating" in volume_info:
                        books.append({
                            "title": volume_info.get("title"),
                            "authors": ", ".join(volume_info.get("authors", [])),
                            "published_date": volume_info.get("publishedDate"),
                            "categories": ", ".join(volume_info.get("categories", [])),
                            "description": volume_info.get("description"),
                            "rating": volume_info.get("averageRating")
                        })
            time.sleep(1)
    return books

def scrape_skoob_books(pages=15):
    books = []
    for page in range(1, pages + 1):
        url = f"https://www.skoob.com.br/livro/lista/todos/todos/todos/{page}.html"
        response = requests.get(url)
        soup = BeautifulSoup(response.text, "html.parser")
        for book in soup.find_all("div", class_="box"):
            title = book.find("h3").text.strip()
            rating_tag = book.find("span", class_="rating")
            rating = rating_tag.text.strip() if rating_tag else "N/A"
            books.append({"title": title, "rating": rating})
        time.sleep(1)
    return books

def fetch_books_from_openlibrary():
    url = "https://openlibrary.org/subjects/science_fiction.json?limit=100"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return [{
            "title": book["title"],
            "authors": ", ".join([author["name"] for author in book.get("authors", [])]),
            "rating": book.get("ratings_average", "N/A")
        } for book in data.get("works", [])]
    return []

def fetch_books_from_kaggle():
    path = kagglehub.dataset_download("jealousleopard/goodreadsbooks")
    csv_path = f"{path}/books.csv"
    try:
        df = pd.read_csv(csv_path, on_bad_lines="skip")
        if "title" in df.columns and "average_rating" in df.columns:
            return df[["title", "average_rating"]].rename(columns={"average_rating": "rating"}).to_dict(orient="records")
        else:
            return []
    except FileNotFoundError:
        return []

def fetch_books_from_csv(csv_path):
    try:
        return pd.read_csv(csv_path).to_dict(orient="records")
    except FileNotFoundError:
        return []

def clean_dataset(filename="books_dataset.csv"):
    df = pd.read_csv(filename)
    
    df.fillna({
        "authors": "Desconhecido",
        "published_date": "Data não disponível",
        "categories": "Sem categoria",
        "description": "Descrição não disponível"
    }, inplace=True)
    
    df["authors"] = df.groupby("title")["authors"].transform(lambda x: x.fillna(x.mode()[0]) if not x.mode().empty else "Desconhecido")
    
    df.drop_duplicates(subset=["title", "authors"], keep="first", inplace=True)
    
    df.to_csv("books_dataset_cleaned.csv", index=False)
    return df

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
    skoob_books = scrape_skoob_books(pages=15)
    openlibrary_books = fetch_books_from_openlibrary()
    kaggle_books = fetch_books_from_kaggle()
    csv_books = fetch_books_from_csv("books_dataset.csv")
    all_books = google_books + skoob_books + openlibrary_books + kaggle_books + csv_books
    save_to_csv(all_books)
    clean_dataset()

if __name__ == "__main__":
    main()
