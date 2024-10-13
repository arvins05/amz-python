import requests
from .ratelimit import RateLimiter
from datetime import datetime, timedelta
import pandas as pd
from .fcmap import fc_to_country

def zv_client_access(username, region):
    """
    This is authentication process for amazon.
    Only works for ZV Data Automation Clients
    """
    url = "https://zvdataautomation.com//zvapiauth/"
    payload = {"username": username, "region": region}
    response = requests.post(url, json=payload)
    
    if response.status_code == 200:
        access_token = response.json()['access_token']
        print('Access Token granted 1 hour validity.')
        return access_token
    else:
        return ValueError('Error: Not Authenticated')
    
def shipment_status(marketplace_action, access_token, past_days):
    """
    This will pull all shipment and its status for specified marketplace

    Parameter:
    - marketplace_action: the specific marketplace command to pull the data
    - access_token: matching access token of the marketplace
    - past_days: number of days from today's date (UTC)

    return:
    - data frame of the list of shipments and its status
    """
    ShipmentStatusLists =[
        'WORKING', 'READY_TO_SHIP', 'SHIPPED', 'RECEIVING',
        'CANCELLED', 'DELETED', 'CLOSED', 'ERROR',
        'IN_TRANSIT', 'DELIVERED', 'CHECKED_IN'
    ]

    rate_limiter = RateLimiter(tokens_per_second=2, capacity=30)
    NextToken = None
    records = []

    regionUrl, MarketplaceId = marketplace_action()
    endpoint = '/fba/inbound/v0/shipments'
    url = regionUrl + endpoint
    headers = {
        'x-amz-access-token': access_token,
        'Content-Type': 'application/json'
    }

    LastUpdatedAfter = (datetime.utcnow() - timedelta(days=past_days)).isoformat()
    LastUpdatedBefore = datetime.utcnow().isoformat()

    for ShipmentStatusList in ShipmentStatusLists:
        request_params = {
            'MarketplaceId': MarketplaceId,
            'QueryType': 'DATE_RANGE',
            'ShipmentStatusList': ShipmentStatusList,
            'LastUpdatedAfter': LastUpdatedAfter,
            'LastUpdatedBefore': LastUpdatedBefore,
            'NextToken': NextToken
        }

        try:
            response = requests.get(url, headers=headers, params=request_params)
            records.extend(response.json()['payload']['ShipmentData'])

            try:
                NextToken = response.json()['payload']['NextToken']
            except:
                NextToken = None

            while NextToken:
                request_params_next = {
                    'MarketplaceId': MarketplaceId,
                    'QueryType': 'NEXT_TOKEN',
                    'NextToken': NextToken
                }
                response = rate_limiter.send_request(requests.get, url, headers=headers, params=request_params_next)
                records.extend(response.json()['payload']['ShipmentData'])

                try:
                    NextToken = response.json()['payload']['NextToken']
                except:
                    NextToken = None

            print('end of list')

        except Exception as e:            
            print(response.json()['errors'][0]['message'])
            print(response.json()['errors'][0]['details'])

    shipments = []
    for record in records:
        shipments.append({
            'shipment_id': record['ShipmentId'],
            'shipment_name': record['ShipmentName'],
            'shipment_status': record['ShipmentStatus'],
            'destination_fulfillment_center': record['DestinationFulfillmentCenterId']
        })

    df = pd.DataFrame(shipments)
    df['country'] = df['destination_fulfillment_center'].map(fc_to_country)

    return df

def shipment_items(marketplace_action, access_token, past_days):
    """
    This will pull all shipment and items inside it for specified marketplace.
    Together with the quantity shipped vs received

    Parameter:
    - marketplace_action: the specific marketplace command to pull the data
    - access_token: matching access token of the marketplace
    - past_days: number of days from today's date (UTC)

    return:
    - data frame of the list of shipments and items inside it
    """
    rate_limiter = RateLimiter(tokens_per_second=2, capacity=30)
    NextToken = None
    records = []


    regionUrl, marketplace_id = marketplace_action()
    endpoint = f'/fba/inbound/v0/shipmentItems'
    url = regionUrl + endpoint
    headers = {
        'x-amz-access-token': access_token,
        'Content-Type': 'application/json'
    }

    LastUpdatedAfter = (datetime.utcnow() - timedelta(days=past_days)).isoformat()
    LastUpdatedBefore = datetime.utcnow().isoformat()

    request_params = {
        'MarketplaceId': marketplace_id,
        'LastUpdatedAfter': LastUpdatedAfter,
        'LastUpdatedBefore': LastUpdatedBefore,
        'QueryType': 'DATE_RANGE'
    }
    response = requests.get(url, headers=headers, params=request_params)
    try:
        response = requests.get(url, headers=headers, params=request_params)
        records.extend(response.json()['payload']['ItemData'])

        try:
            NextToken = response.json()['payload']['NextToken']
        except:
            NextToken = None

        while NextToken:
            request_params_next = {
                'MarketplaceId': marketplace_id,
                'NextToken': NextToken
            }
            response = rate_limiter.send_request(requests.get, url, headers=headers, params=request_params_next)
            records.extend(response.json()['payload']['ItemData'])

            try:
                NextToken = response.json()['payload']['NextToken']
            except:
                NextToken = None

        print('end of list')

    except Exception as e:
        print(response.json()['errors'][0]['message'])
        print(response.json()['errors'][0]['details'])

    df = []
    for record in records:

        if len(record['PrepDetailsList']) > 0:
            df.append({
                'shipment_id': record['ShipmentId'],
                'sku': record['SellerSKU'],
                'fnsku': record['FulfillmentNetworkSKU'],
                'shipped_qty': record['QuantityShipped'],
                'received_qty': record['QuantityReceived'],
                'case_qty': record['QuantityInCase'],
                'prep_instruction': record['PrepDetailsList'][0]['PrepInstruction'],
                'prep_owner': record['PrepDetailsList'][0]['PrepOwner']
            })
        else:
            df.append({
                'shipment_id': record['ShipmentId'],
                'sku': record['SellerSKU'],
                'fnsku': record['FulfillmentNetworkSKU'],
                'shipped_qty': record['QuantityShipped'],
                'received_qty': record['QuantityReceived'],
                'case_qty': record['QuantityInCase'],
                'prep_instruction': np.nan,
                'prep_owner': np.nan
            })
    shipmentItemsDf = pd.DataFrame(df)
    return shipmentItemsDf 