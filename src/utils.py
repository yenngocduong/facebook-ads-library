"""
contains the following utility functions:

- facebook_credentials: load Facebook API credentials
- somin_credentials: load SoMin API credentials
- somin_api_versions: load version of different SoMin API
- model_config: load model configurations for feature extraction and modelling
- ads_archive_fields_params: read Fields and Parameters for Ads Archive endpoint
- get_image_url: obtain image_url given ad_url
- download_image: download image from (temporary) image_url onto disk
- read_endpoint: method to read (only) from FB Graph API endpoints
- request: helper method to make http request on FB Graph API endpoint
- save_pickle: helper function to store pickled object
- load_pickle: helper function to load pickled object
- obtain_ads_page_id: helper function to retrieve ads based on page_id
"""
import yaml, os, requests, urllib.request, re, pickle, numpy as np
from bs4 import BeautifulSoup as soup
import matplotlib.pyplot as plt

FIELDS = ['id','ad_creation_time','ad_delivery_start_time','ad_delivery_stop_time',
          'ad_snapshot_url','ad_creative_bodies','ad_creative_link_captions','ad_creative_link_titles','ad_creative_link_descriptions',
          'languages',
          'page_id','page_name','bylines','currency','spend','impressions','estimated_audience_size','publisher_platforms',
          'demographic_distribution','delivery_by_region']
ACCESS_TOKEN = 'EAAEau61SMogBAKXf5ZCiYZC2WkmfCAkZCEz1cQoZBTgg4AdxRAAmG5ZAcH8hrSEsXPDZBPFodOdKr7s49i4ZBecZAZBnnV86z5HMCZAaYp1s2WoUj5P5PzYXTX6Gao2awkZBBudMZBubxwlG3UeRUZBCsBTb5GCeHbAsJFdf0Bal6Rpou4ORNt56cpVpK'


def facebook_credentials(path=''):
    
    if not path:
        path = os.path.abspath('..').replace('\\', '/') + '/configs'
    
    with open(f'{path}/credentials.yaml', 'r') as f:
        creds = yaml.safe_load(f)['facebook_api']
        
    version = creds['version']
    business_id = creds['business_id']
    app_id = creds['app_id']
    app_secret = creds['app_secret']
    user_access_token = creds['user_access_token']
    return version, business_id, app_id, app_secret, user_access_token


def somin_credentials(path=''):
    
    if not path:
        path = os.path.abspath('..').replace('\\', '/') + '/configs'
        
    with open(f'{path}/credentials.yaml', 'r') as f:
        creds = yaml.safe_load(f)['somin_api']['authorization']
    
    version = creds['version']
    username = creds['username']
    password = creds['password']
    
    return version, {'username': username, 'password': password}


def somin_api_versions(path=''):
    
    if not path:
        path = os.path.abspath('..').replace('\\', '/') + '/configs'
        
    with open(f'{path}/credentials.yaml', 'r') as f:
        creds = yaml.safe_load(f)['somin_api']
        versions = {}
        for k in creds.keys():
            versions[k] = creds[k]['version']
    return versions


def model_config(path=''):
    
    if not path:
        path = os.path.abspath('..').replace('\\', '/') + '/configs'
    
    with open(f'{path}/models.yaml', 'r') as f:
        model_config = yaml.safe_load(f)
    return model_config


def ads_archive_fields_params(path=''):
    
    if not path:
        path = os.path.abspath('..').replace('\\', '/') + '/configs'
    
    with open(f'{path}/fields.yaml', 'r') as f:
        fp = yaml.safe_load(f)['ads_archive']
        
    fields = fp['fields']
    parameters = fp['parameters']
    return fields, parameters


def get_image_url(ad_url):
    """
    function to extract image_url from the retreived data from Ad Library API ('/ads_archive' endpoint)
    Why?: the URL (ad_snapshot_url) obtained via the endpoint is not an image_url but instead a url for the entire ad
    """
    source = soup(requests.get(ad_url).content, 'html.parser')
    pattern = r'"resized_image_url":"(?:\\.|[^"\\])*"' # find resized_image_url : <image_url>
    try:
        image_url = re.findall(pattern, str(source))[0].split('"resized_image_url":')[1].replace('\\', '').replace('"', '').replace("'", '')
    except:
        image_url = np.nan
    return image_url
    

def download_image(image_url, filepath):
    """
    download an image from image_url into filepath
    """
    urllib.request.urlretrieve(image_url, filepath)


def read_endpoint(id_, endpoint, access_token, fields=[], params={}, n=1000000):
    """
    read any endpoint for Facebook Graph API
    
    id_: identifier for any of Business, Account, Campaign, Ad Set, Ad, Creative, Ad Image, Ad Library
    endpoint: endpoint for any of the above according to Facebook Graph API
    access_token: required for usage of Facebook Graph API
    fields: fields to be read for the specified endpoint
    params: parameters to be passed for the specified endpoint
    n: number of desired results to be returned
    """
    q = dict({'access_token': access_token, 'fields': ','.join(fields)}, **params)
    if id_ == '':
        response = request(method='GET', resource=endpoint, q=q)
    else:
        response = request(method='GET', resource=f'{id_}/{endpoint}', q=q)
    if endpoint == '':
        return response
    
    data = []
    while True:
        try:
            data.extend(response['data'])
            if 'next' not in response['paging'].keys():
                break
            if len(data) >= n:
                break
            response = request(method='GET', full_url=response['paging']['next'])
        except Exception as e:
            break # not accessible
    return data



def request(method='GET', full_url='', resource='', q={}):
    """
    resource: Endpoint on FB Graph API
    q: Access Token, Parameters and Fields according to FB Graph API documentation for specified endpoint
    """
    response = None
    if method == 'GET':
        if full_url:
            response = requests.get(full_url)
        if resource:
            response = requests.get(f'https://graph.facebook.com/v13.0/{resource}', q)
        if response.status_code==400:
            print(f'Error Status Code 400 for URL: {response.url}')
            return response
    return response.json()


def save_pickle(obj, filepath):
    with open(filepath, 'wb') as f:
        pickle.dump(obj, f)

        
def load_pickle(filepath):
    with open(filepath, 'rb') as f:
        obj = pickle.load(f)
    return obj


def obtain_ads_page_name(search_term, country, fields=FIELDS, access_token=ACCESS_TOKEN):
    """
    a function to obtain ads from Ad Library based on given 'search_page_ids' and 'country'
    """
    params = {'search_terms': search_term,
              'search_type': 'KEYWORD_UNORDERED',
              'ad_active_status': 'ALL',
              'ad_reached_countries': [country]}
    
    ads = read_endpoint('', 'ads_archive', access_token, fields, params, 30000)
    
    for ad in ads: # add additional information
#         ad['search_page_ids'] = page_id 
        ad['topic'] = search_term 
        ad['country'] = country
    return ads

def obtain_ads_page_id(page_id, country, fields=FIELDS, access_token=ACCESS_TOKEN):
    """
    a function to obtain ads from Ad Library based on given 'search_page_ids' and 'country'
    """
    params = {'search_page_ids': page_id,
              'media_type': 'IMAGE',
              'ad_active_status': 'ALL',
              'ad_reached_countries': [country]}
    
    ads = read_endpoint('', 'ads_archive', access_token, fields, params, 30000)
    
    for ad in ads: # add additional information
        ad['search_page_ids'] = page_id 
        ad['country'] = country
    return ads


def get_text_area(source):
    image = cv2.imread(f'{raw_dir}/{source}.png')
    print(plt.imshow(image))
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    print(plt.imshow(gray))
    thresh = cv2.adaptiveThreshold(gray,255,cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY_INV,11,3)
    print(plt.imshow(thresh))
    mask = thresh.copy()
    mask = cv2.merge([mask,mask,mask])
    print(plt.imshow(mask))
    
    picture_threshold = image.shape[0] * image.shape[1] * .01
    cnts = cv2.findContours(thresh, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    cnts = cnts[0] if len(cnts) == 2 else cnts[1]
    for c in cnts:
        area = cv2.contourArea(c)
        if area < picture_threshold:
            cv2.drawContours(mask, [c], -1, (0,0,0), -1)

    mask = cv2.cvtColor(mask, cv2.COLOR_BGR2GRAY)
    result = cv2.bitwise_xor(thresh, mask)

    text_pixels = cv2.countNonZero(result)
    percentage = (text_pixels / (image.shape[0] * image.shape[1])) * 100
    print('Percentage: {:.2f}%'.format(percentage))
    print(plt.imshow(result))
    return percentage
