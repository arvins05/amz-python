#Check current df and existing BigQuery table has the same columns
def dfbgcolcheck(df, client, bgTable):
    # df is the dataframe you want to compare
    # client is your BigQuery Client
    # bgTable is the BigQuery Table address
    existingDataQuery = f"""
    SELECT
    *
    FROM
    `{bgTable}`
    LIMIT 10
    """

    existingData = client.query(existingDataQuery).to_dataframe()

    columnsCheck = existingData.columns.equals(df.columns)

    # Returns True if the column are the same and error if not
    if columnsCheck:
        print("Column names and positions are the same.")
        return True
    else:
        raise ValueError("Error: Column names and positions are not the same.")


#low inventory level fee processing using downloaded report
def lowfeereport(filePath):
    # filePath is the path where the economics report from amazon seller central is located

    import pandas as pd
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

    # Return the cleanned up dataframe for low level inventory fee
    return lowFeeDf