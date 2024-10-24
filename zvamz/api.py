import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
from .ratelimit import RateLimiter
from .fcmap import fc_to_country
from .marketplaces import marketplaces

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
                'QueryType': 'NEXT_TOKEN',
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

def shipment_summary(marketplace_action, access_token, past_days):
    """
    This will pull all shipment and items inside it for specified marketplace.
    And Summarise the Report

    Parameter:
    - marketplace_action: the specific marketplace command to pull the data
    - access_token: matching access token of the marketplace
    - past_days: number of days from today's date (UTC)

    return:
    - data frame of the report summary
    """
    shipmentDf = shipment_status(marketplace_action, access_token, past_days)
    shipmentItemsDf = shipment_items(marketplace_action, access_token, past_days)

    shipmentSummaryDf = shipmentDf.merge(shipmentItemsDf, how='inner', on='shipment_id')
    shipmentSummaryDf.insert(0,'date',datetime.utcnow().strftime('%F'))

    schema = {
        'date': 'datetime64[ns]',
        'shipment_id': str,
        'shipment_name': str,
        'shipment_status': str,
        'destination_fulfillment_center': str,
        'country': str,
        'sku': str,
        'fnsku': str,
        'shipped_qty': float,
        'received_qty': float,
        'case_qty': float,
        'prep_instruction': str,
        'prep_owner': str
    }
    shipmentSummaryDf = shipmentSummaryDf.astype(schema)

    return shipmentSummaryDf

def narf_eligibility(access_token, file_path_name):
    # Create Report
    regionUrl, marketplace_id = marketplaces.US()
    endpoint = f'/reports/2021-06-30/reports'
    url = regionUrl + endpoint
    headers = {
        'x-amz-access-token': access_token,
        'Content-Type': 'application/json'
    }

    request_params = {
        'marketplaceIds': [marketplace_id],
        'reportType': 'GET_REMOTE_FULFILLMENT_ELIGIBILITY'
    }

    create_response = requests.post(url, headers=headers, json=request_params,)
    report_id = create_response.json()['reportId']

    # Check Report Status
    endpoint = f'/reports/2021-06-30/reports/{report_id}'
    url = regionUrl + endpoint

    while True:
        status_response = requests.get(url, headers=headers)
        status = status_response.json().get("processingStatus")
        
        if status == "DONE":
            print("Report is ready for download!")
            document_id = status_response.json()["reportDocumentId"]
            break
        elif status == "CANCELLED":
            print("Report creation was cancelled.")
            exit()
        elif status == "FAILED":
            print("Report creation failed.")
            exit()
        else:
            print(f"Report status: {status}. Waiting for report to be ready...")
            time.sleep(60)  # Wait before checking again

    # Download Report
    endpoint = f'/reports/2021-06-30/documents/{document_id}'
    url = regionUrl + endpoint
    document_response = requests.get(url, headers=headers)

    if document_response.status_code == 200:
        download_url = document_response.json()["url"]
        report_data = requests.get(download_url)

        with open(file_path_name, "wb") as f:
            f.write(report_data.content)
    else:
        print("Failed to get the report document:", document_response.json())

    #prepare DF
    narfDf = pd.read_excel(file_path_name,sheet_name='Enrollment',skiprows=3)
    narfDf = narfDf.rename(columns=lambda x:x.replace('.1','').replace('.2','').replace('(Yes/No)','')
                                    .replace(' Brazil ','').replace(' Canada ','').replace(' Mexico ','')
                                    .replace('/','_').replace(' ','_')
                                    .lower())

    brNarfDf = narfDf.iloc[:,:6]
    brNarfDf.insert(0,'marketplace','Brazil')

    caNarfDf = pd.concat([narfDf.iloc[:,:3],narfDf.iloc[:,6:9]], axis = 1)
    caNarfDf.insert(0,'marketplace','Canada')

    mxNarfDf = pd.concat([narfDf.iloc[:,:3],narfDf.iloc[:,9:12]], axis = 1)
    mxNarfDf.insert(0,'marketplace','Mexico')

    narfFinalDf = pd.concat([brNarfDf,caNarfDf,mxNarfDf], axis=0, ignore_index=True)
    narfFinalDf.insert(0,'date',datetime.utcnow().strftime('%F'))

    schema = {
        'date': 'datetime64[ns]',
    'marketplace': str,
    'merchant_sku': str,
    'asin': str,
    'product_name': str,
    'offer_status': str,
    'more_details': str,
    'enable_disable': str
    }

    narfFinalDf = narfFinalDf.astype(schema)

    return narfFinalDf