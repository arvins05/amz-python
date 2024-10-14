import pandas as pd
from datetime import datetime


def bgdeldup(dateName:str, minDate: datetime, client: str, bgTable:str):
    """
    Delete the data from the declared minDate to avoid duplicate in the database

    Parameters:
    - dateName: the name of the date column
    - minDate: the start date that will be deleted to avoid duplicate
    - client: the BigQuery client name
    - bgTable: the BigQuery Table address

    Returns:
    - delete the data to avoid duplicate
    """
    delDataQuery = f"""
    DELETE FROM
    `{bgTable}`
    WHERE
    {dateName} >= '{minDate}'
    """

    delData = client.query(delDataQuery)
    delData = delData.result()

    return delData


def bgdeldupf(dateName:str, minDate: datetime, client: str, bgTable:str):
    """
    Delete the data from the declared minDate to avoid duplicate in the database
    (free version of BigQuery)

    Parameters:
    - dateName: the name of the date column
    - minDate: the start date that will be deleted to avoid duplicate
    - client: the BigQuery client name
    - bgTable: the BigQuery Table address

    Returns:
    - delete the data to avoid duplicate
    """
    delDataQuery = f"""
    CREATE OR REPLACE TABLE `{bgTable}` AS

    SELECT *
    FROM `{bgTable}`
    WHERE
    {dateName} < '{minDate}'
    """

    delData = client.query(delDataQuery)
    delData = delData.result()

    return delData


def dfbgcolcheck(df: pd.DataFrame, client: str, bgTable: str):
    """
    This function compares the columns of DataFrame and existing BigQuery Table

    Paremeters:
    - df: Data frame to check
    - client: name of BigQuery client
    - bgTable: the address of BigQuery Table

    Returns:
    - True if it matches
    - ValueError if not
    """
    existingDataQuery = f"""
    SELECT
    *
    FROM
    `{bgTable}`
    LIMIT 10
    """

    existingData = client.query(existingDataQuery).to_dataframe()

    columnsCheck = existingData.columns.equals(df.columns)
    if columnsCheck:
        print("Column names and positions are the same.")
        return True
    else:
        raise ValueError("Error: Column names and positions are not the same.")


def lowfeereport(filePath:str):
    """
    This function process and clean the Amazon Economics Report.
    It extracts the low level inventory data

    Paremeters:
    - filePath: the path where the report is saved

    Returns:
    - DataFrame of the cleaned report
    - Flase if there is no data related to low level inventory fee
    """
    lowFeeDf = pd.read_csv(filePath)

    checkCol = [
        'Low-inventory-level fee per unit',
        'Low-inventory-level fee quantity',
        'Low-inventory-level fee total'
    ]

    colCheck = all(col in lowFeeDf.columns for col in checkCol)

    if colCheck:
        lowFeeDf = lowFeeDf[[
            'Start date',
            'End date',
            'ASIN',
            'MSKU',
            'Low-inventory-level fee per unit',
            'Low-inventory-level fee quantity',
            'Low-inventory-level fee total'
        ]]

        lowFeeDf = lowFeeDf.rename(columns=lambda x:x.replace('-','_').replace(' ','_').lower())
        lowFeeDf['start_date'] = pd.to_datetime(lowFeeDf['start_date'])
        lowFeeDf['end_date'] = pd.to_datetime(lowFeeDf['end_date'])

        schema = {
                'start_date': 'datetime64[ns]',
                'end_date': 'datetime64[ns]',
                'asin': str,
                'msku': str,
                'low_inventory_level_fee_per_unit': float,
                'low_inventory_level_fee_quantity': float,
                'low_inventory_level_fee_total': float
        }

        lowFeeDf = lowFeeDf.astype(schema)
        return lowFeeDf
    else:
        return False
    

def promoreport(filePath:str):
    """
    Clean the raw file downloaded in Amazon Seller Central Promotions Report

    Parameter:
    - filePath: the path where the downloaded Promotions Report is located

    Return:
    - DataFrame of the cleaned report
    """
    promoDf = pd.read_csv(filePath)
    promoDf = promoDf.rename(columns=lambda x:x.replace('?','').replace('"','').replace('-','_').lower())
    promoDf['shipment_date'] = pd.to_datetime(promoDf['shipment_date'], utc=True)

    schema = {
        'shipment_date': 'datetime64[ns, UTC]',
        'currency': str,
        'item_promotion_discount': float,
        'item_promotion_id': str,
        'description': str,
        'promotion_rule_value': str,
        'amazon_order_id': str,
        'shipment_id': str,
        'shipment_item_id': str
    }
    promoDf = promoDf.astype(schema)

    return promoDf
