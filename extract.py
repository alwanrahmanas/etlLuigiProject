import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm
import requests


def scrape_anime_metadata(year=2024):
    """
    Scrape anime metadata from MyAnimeList for a given year.

    Args:
        year (int): The year for which to scrape anime metadata. Default is 2024.

    Returns:
        list: A list of dictionaries containing anime metadata.
    """
    anime_metadata = []
    list_season = ['winter', 'spring', 'summer', 'fall']

    for season in tqdm(list_season, desc="Processing seasons"):
        try:
            resp = requests.get(f"https://myanimelist.net/anime/season/{year}/{season}")
            soup = BeautifulSoup(resp.content, 'html.parser')

            # List of tags with their corresponding types
            anime_categories = [
                {"show_type": "TV", "class": "js-anime-category-producer seasonal-anime js-seasonal-anime js-anime-type-all js-anime-type-1"},
                {"show_type": "ONA", "class": "js-anime-category-producer seasonal-anime js-seasonal-anime js-anime-type-all js-anime-type-5"},
                {"show_type": "OVA", "class": "js-anime-category-producer seasonal-anime js-seasonal-anime js-anime-type-all js-anime-type-2"},
                {"show_type": "Movie", "class": "js-anime-category-producer seasonal-anime js-seasonal-anime js-anime-type-all js-anime-type-3"}
            ]

            # Dictionary to store the extracted content for each category
            anime_dict = {}

            # Iterate through the anime categories and extract the content
            for category in anime_categories:
                anime_type = category["show_type"]
                anime_class = category["class"]

                # Find all div tags with the specific class
                divs = soup.find_all('div', class_=anime_class)

                # Store the divs in the dictionary
                anime_dict[anime_type] = [div for div in divs]

            for show_type, divs in anime_dict.items():
                for div in tqdm(divs, desc=f"Processing {show_type} anime", leave=False):
                    try:
                        title = div.find('a', class_='link-title').text
                        link = div.find('a', class_='link-title')['href']
                        release_info = div.find('span', class_='item').text  # <span class="item">Jan 11, 2025</span>

                        # Get all info data like release date, anime minutes, and anime episode
                        get_anime_info = div.find("div", class_="info")

                        # Get more detail info
                        get_anime_detail_info = get_anime_info.find_all('span', class_='item')

                        # Get release date
                        release_date = get_anime_detail_info[0].text

                        # Get episode
                        anime_episode = get_anime_detail_info[1].contents[1].text

                        # Get duration
                        anime_duration = get_anime_detail_info[1].contents[3].text

                        # Get genre anime data
                        genre_raw = div.find_all("div", class_="genres-inner js-genre-inner")

                        # Iterate through each <div> and extract genres
                        genres = []
                        for genre_div in genre_raw:
                            genre_links = genre_div.find_all('a')
                            genres = [link.text for link in genre_links]

                        # Get img link
                        img_link = div.find("img").get("src")

                        # Get description
                        description = div.find("p").text

                        # Get studio name
                        get_studio = div.find_all("div", class_="property")[0]
                        studio = get_studio.find("a").text if get_studio.find("a") is not None else ""

                        # Get source
                        get_source = div.find_all("div", class_="property")[1]
                        source = get_source.find("span", class_="item").text

                        # Append the metadata to the list
                        anime_metadata.append({
                            "season": season,
                            "title": title,
                            "link": link,
                            "release_date": release_date,
                            "episodes": anime_episode,
                            "duration": anime_duration,
                            "genres": genres,
                            "image_link": img_link,
                            "description": description,
                            "studio": studio,
                            "source": source,
                            "show_type": show_type
                        })
                    except Exception as e:
                        print(f"An error occurred while processing an anime: {e}")
        except Exception as e:
            print(f"An error occurred while fetching data for season {season}: {e}")

    return anime_metadata

def getMangaByNum(num):
    manga_dict = {}
    try:
        for i in range(1, num + 1):  # Loop from 1 to 'num' inclusive
            resp = requests.get(f"https://api.jikan.moe/v4/manga/{i}/full")  # Use 'i' here
            if resp.status_code == 200:  # Check if the request was successful
                raw_data = resp.json()  # Get the raw response data
                # Construct the manga data dictionary
                manga_data = {
                    "manga_id": raw_data["data"]["mal_id"],
                    "url_manga": raw_data["data"]["url"],
                    "title": raw_data["data"]["title"],
                    "title_english": raw_data["data"].get("title_english", "N/A"),  # Use .get() to handle missing values
                    "title_japanese": raw_data["data"].get("title_japanese", "N/A"),
                    "chapters": raw_data["data"].get("chapters", "N/A"),
                    "volumes": raw_data["data"].get("volumes", "N/A"),
                    "status": raw_data["data"]["status"],
                    "start_published": raw_data["data"]["published"].get("from", "N/A"),
                    "end_published": raw_data["data"]["published"].get("to", "N/A"),
                    "score": raw_data["data"].get("score", "N/A"),
                    "rank": raw_data["data"].get("rank", "N/A"),
                    "authors": [entry["name"] for entry in raw_data["data"]["authors"]],
                    "genres": [entry["name"] for entry in raw_data["data"]["genres"]],
                    "themes": [entry["name"] for entry in raw_data["data"]["themes"]]
                }
                manga_dict[i] = manga_data  # Add manga data to dictionary by 'i'
            else:
                print(f"Failed to fetch manga {i}: {resp.status_code}")
    except Exception as e:
        print(f"An error occurred: {e}")
    
    return manga_dict


# # Example usage
# if __name__ == "__main__":
#     year = 2024
#     anime_data = scrape_anime_metadata(year)
#     print(f"Scraped metadata for {len(anime_data)} anime shows in {year}.")
