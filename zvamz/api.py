import requests

def zv_client_access(username, region):
    url = "https://zvdataautomation.com//zvapiauth/"
    payload = {"username": username, "region": region}
    response = requests.post(url, json=payload)
    
    if response.status_code == 200:
        access_token = response.json()['access_token']
        print('Access Token granted 1 hour validity.')
        return access_token
    else:
        return ValueError('Error: Not Authenticated')