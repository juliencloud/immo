
def get():
    return {
        'leboncoin': {
            'start_page': 4020,
            'end_page': 30000
        },
        'threading': {
            'num_threads': 1
        },
        's3': {
            'bucket_thumbs': 'immo-thumbs'
        },
        'postgres': {
            'host': 'immotest.cf4gr5y6qxwb.eu-west-1.rds.amazonaws.com',
            'port': 5432,
            'database': 'immotest',
            'user': 'immotest',
            'password': '1FXacxzsFu3vu51c',
            'table_ads': 'immo.ads',
            'table_sites': 'immo.sites'
        }
    }
