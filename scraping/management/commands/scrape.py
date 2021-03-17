from django.core.management.base import BaseCommand
import time
import pymongo
from bs4 import BeautifulSoup
import requests
from tqdm import tqdm
import os

user = os.environ['db_user']
pw = os.environ['db_pw']
# initializing the database connection
client = pymongo.MongoClient(f'mongodb+srv://{user}:{pw}@bc01-muwwi.gcp.mongodb.net/test?retryWrites=true&w=majority')
db = client.BC02
collection = db.artistInfo

#get all links from documents that don't have a location field
all_links = [x['bc_url'] for x in list(collection.find({'location': {'$exists': False}}))]

def location_and_tags(url):
    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'html.parser')
    if soup.find('span', class_='location'):
        location = soup.find('span', class_='location').get_text().lower()
        tags = [x.get_text().lower() for x in soup.find_all('a', class_='tag')]
        return {'location':location, 'genres':tags}
    else:
        return False



class Command(BaseCommand):
    help = "collect jobs"

    def handle(self, *args, **options):    

        for link in tqdm(all_links):
            update = location_and_tags(link)
            # print(link)
            #checks to see if update returned new data
            if update:
                collection.update_one(
                    {'bc_url': link},
                    {'$set': {
                        'location': update['location'],
                        'genres': update['genres']
                    }})
                print(f'Updated {link} location & genres')

            #if no new data then artist page defaults to their discography
            #redirect url to the first release on discography to get location & genres
            else:
                time.sleep(1)
                page = requests.get(link)
                soup = BeautifulSoup(page.text, 'html.parser')
                try:
                    first_release = soup.find_one('li', class_='music-grid-item')
                    url = first_release.find_one('a').get('href')
                    update = location_and_tags(url)
                    collection.update_one(
                        {'bc_url': link},
                        {'$set': {
                            'location': update['location'],
                            'genres': update['genres']
                        }}
                    )
                    print(f'Updated {link} location & genres')
                except:
                    body = soup.find('body')
                    first_container = body.find('span', class_='indexpage_list_cell odd')
                    if not first_container:
                        continue
                    else:
                        release = list(first_container.children)[1]
                        url = release.find('a').get('href')
                        if 'http' not in url:
                            new_link = link+url
                            print(new_link)
                            update = location_and_tags(new_link)
                            collection.update_one(
                                {'bc_url':link},
                                {'$set': {
                                    'location': update['location'],
                                    'genres': update['genres']
                                }}
                            )
                            print(f'Updated {link} location & genres')
                        else:
                            update = location_and_tags(url)
                            collection.update_one(
                                {'bc_url':link},
                                {'$set': {
                                    'location': update['location'],
                                    'genres': update['genres']
                                }}
                            )
                            print(f'Updated {link} location & genres')
            time.sleep(1)
        self.stdout.write('Finished scraping')