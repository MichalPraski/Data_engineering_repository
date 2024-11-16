from urllib.request import urlopen
from urllib.error import HTTPError
from bs4 import BeautifulSoup
import pandas as pd
import os
import requests
import pandas_gbq
from google.oauth2 import service_account

import functions_framework
# creating sparksession and giving  
# an app name 
# spark = SparkSession.builder.appName('sparkdf').getOrCreate() 
# from google.cloud import storage

# definicja beautifulsoup
# html = urlopen('https://gratka.pl/nieruchomosci/mieszkania')
# bs = BeautifulSoup(html.read(), 'html.parser')

def get_info_from_current_page(page:int):
    """
    Getting data from gratka.pl related with flats offers
    """
    html = urlopen(f'https://gratka.pl/nieruchomosci/mieszkania?page={page}')
    # creating beautifulsoup object
    bs = BeautifulSoup(html.read(), 'html.parser')

    data = []
    # creating lists

    for advert in list(bs.find_all('div', {'class':'listing__teaserWrapper'})):
        advert_dict = {'miasto': '',
                       'dzielnica': '',
                       'wojewodztwo': '',
                       'metraz': '',
                       'pokoje': '',
                       'pietro': '',
                       'cena': '',
                       'cena_m2': ''
        }
        # names
        flat_name_text = advert.find('span', {'class':'teaserUnified__location'}).get_text(strip=True)
        advert_dict['miasto'] = flat_name_text.strip().replace(' ', '').split(',')[0]
        advert_dict['dzielnica'] = flat_name_text.strip().replace(' ', '').split(',')[1]
        advert_dict['wojewodztwo'] = flat_name_text.strip().replace(' ', '').split(',')[-1]

        # metrics
        metrics = advert.find('li', {'class':'teaserUnified__listItem'})
        if metrics:
            metrics_text = metrics\
                .get_text(strip=True)\
                .lower()
            advert_dict['metraz'] = float(metrics_text.replace(' m2', '').replace(',', '.'))

        siblings = metrics.find_next_siblings('li', {'class':'teaserUnified__listItem'})
        for element in siblings:
            text = element.get_text()
            if 'pok' in text:
                advert_dict['pokoje'] = text.lower().split(' ')[0]
            else:
                advert_dict['pietro'] = text.lower().split(' ')[0].replace('parter', '0')

        # prices
        price_text = advert.find('p', {'class':'teaserUnified__price'}).get_text(strip=True)
        if price_text.startswith('Zapytaj o cenę'):
            advert_dict['cena'] = None
            advert_dict['cena_m2'] = None
        else:
            advert_dict['cena'] = price_text.split('z')[0].replace(' ', '')
            advert_dict['cena_m2'] = price_text.split('zł')[1].replace(' ', '')

        # yielding final dataframe
        data.append(advert_dict)

    df_final = pd.DataFrame(data)

    return df_final

def upload_df_to_gcs(df:pd.DataFrame, bucket_name:str, filepath:str):
    """
    Uploading data to Google big query
    """
    # os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'data-engineering-391216-e689358cbc11.json'
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
        
    bucket.blob(filepath).upload_from_string(df.to_csv(), 'text/csv')

def upload_to_big_query(df:pd.DataFrame, path_to_cred:str):

    credentials = service_account.Credentials.from_service_account_file(
    path_to_cred,
    )
    pandas_gbq.to_gbq(df, 'gratka.ogloszenia', project_id="data-engineering-391216", credentials=credentials, if_exists='append')


def get_info_from_pages(pages:int):
    """
    Collecting data from as many sites as much user would like to
    """
    # creating dataframes list
    dataframes = []
    pages += 1
    all_pages = pages - 1

    # for each page do:
    for page in range(1, pages):
        try:
            # finding page by url
            html = urlopen(f'https://gratka.pl/nieruchomosci/mieszkania?page={page}')
            # definition of beautifulsoup
            bs = BeautifulSoup(html.read(), 'html.parser')

            # run function that scrapps data from chosen page
            print(f'Scrapping page: {page} of {all_pages} pages ...')
            df_temp = get_info_from_current_page(page=page)

            # appending dataframe to list of dataframes
            dataframes.append(df_temp)

        # error handling while limit of pages is reached
        except HTTPError:
            print('Maximum number of pages reached')
            break

    # Concatenation all created dataframes to one final dataframe
    final_df = pd.concat(dataframes, axis=0)
    return final_df

@functions_framework.http
def handler(request):
    html = urlopen('https://gratka.pl/nieruchomosci/mieszkania')
    bs = BeautifulSoup(html.read(), 'html.parser')

    df = get_info_from_pages(pages=2)
    upload_to_big_query(df=df, 
                        path_to_cred='data-engineering-391216-e689358cbc11.json')
    return ('ok', 200)
                    

if __name__ == '__main__':
    html = urlopen('https://gratka.pl/nieruchomosci/mieszkania')
    bs = BeautifulSoup(html.read(), 'html.parser')

    df = get_info_from_pages(pages=2)
    print(df)