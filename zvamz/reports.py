#low inventory level fee processing using downloaded report
def lowfeereport(filePath,):
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

    return lowFeeDf