import xmltodict
import requests
import os
import csv
import dateparser
from datetime import *
import random
import string
import tqdm
from dotenv import load_dotenv
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool
import chardet

load_dotenv()

apikey = os.getenv('apikey')

# change show xml here
with open('bombcast.xml', 'rb') as f:
    xml_bytes = f.read()
encoding = chardet.detect(xml_bytes)['encoding']
xml_string = xml_bytes.decode(encoding)
beast = xmltodict.parse(xml_string)

# old way
#beast =  xmltodict.parse(read_xml.read())

podcasts = beast['rss']['channel']['item']

upload = []

urls = []
fns = []

folder = 'Giant Bombcast'
curdir = os.getcwd()
dir = os.path.join(curdir,folder)

if not os.path.exists(dir):
     os.makedirs(dir)

# Download function that extracts the tuple to create variables
def download_url(inputs):
    
    url, fn = inputs[0], inputs[1]

    fn.replace('/','-')
    
    # If there's no url, add it to the number of missing show urls
    if url:
   
        # Request the url for download and then write to file
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            
            with open(f'{fn}', 'wb') as f:
                pbar = tqdm.tqdm(total=int(r.headers['Content-Length']),
                            desc=f"Downloading {fn}",
                            unit='MiB',
                            unit_divisor=1024,
                            unit_scale=True,
                            dynamic_ncols=True,
                            colour='#ea0018',
                            mininterval=1
                            )
                
                for chunk in r.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))


# Function that opens the multiple download_url functions
def download_parallel(args):
    cpus = cpu_count()
    ThreadPool(3).map(download_url, args)

def zippy():
    inputs = zip(urls,fns)

# Search through every show and get variables for naming and CSV writing
podbar = tqdm.tqdm(range(len(podcasts)), desc="Gathering download info")
for i in podbar:
    title = podcasts[i].get('title')
    link = podcasts[i].get('link')
    desc = podcasts[i].get('description')
    pubdate = podcasts[i].get('pubDate')
    url = podcasts[i]['media:content'].get('@url')
    guid = podcasts[i]['guid'].get('#text')

    # Translate insane date format        
    trunc_date = pubdate[5:16]
    parse_date = dateparser.parse(trunc_date)
    publish_date = str(datetime.strftime(parse_date, "%Y-%m-%d"))
    
    if '?api_key=' in url:
        url = url[:url.index('?api_key=') + len('?api_key=')]
        url = url + apikey

    filename = (f'{publish_date}' + '-' + f'{title}.mp3').replace(" ", "_").replace(":","")
    filepath = os.path.join(dir, filename)

    urls.append(url)
    fns.append(filepath)

    upload.append({
    'identifier': 'gb-' + guid + '-ID' + ''.join(random.choices(string.ascii_uppercase + string.digits, k=5)),
    'file': filepath,
    'title': title,
    'description': desc,
    'subject[0]': 'Giant Bomb',
    'subject[1]': 'The Giant Beastcast',
    'creator': 'Giant Bomb',
    'date': publish_date,
    'collection': 'opensource_audio',
    'mediatype': 'audio',
    'external-identifier': 'gb-guid:' + guid,
    })

    ## Write CSV for upload to Archive.org
    with open('beastcast_premium.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=upload[0].keys())
        writer.writeheader()
        writer.writerows(upload)
        
    #dl(url, filepath, filename)

inputs = zip(urls,fns)

download_parallel(inputs)





