import psycopg2

fields = ['visit_timestamp',
            'ad_id',
            'ad_site',
            'ad_url',
            'ad_title',
            'ad_status',
            'ad_image_urls',
            'ad_text',
            'ad_author',
            'ad_timestamp',
            'ad_price',
            'ad_city',
            'ad_zip_code',
            'ad_latitude',
            'ad_longitude',
            'ad_agency_fees',
            'ad_immo_type',
            'ad_reference',
            'ad_surface',
            'ad_terrain_surface',
            'ad_building_surface',
            'ad_rooms',
            'ad_gaz_emissions',
            'ad_energy_class']

def get_connection(settings):
    s = settings['postgres']
    return psycopg2.connect(host=settings['postgres']['host'],
                            port=settings['postgres']['port'],
                            database=settings['postgres']['database'],
                            user=settings['postgres']['user'],
                            password=settings['postgres']['password'])


def write_ad(connection, table, ad):
    if ad is None:
        return
    has_latlon = ad['ad_longitude'] is not None and ad['ad_latitude'] is not None
    keys = [key for key in ad.keys()]
    values = [ad[key] for key in keys]
    query = 'INSERT INTO %s (' % (table)
    query += ', '.join(keys)
    if has_latlon:
        query += ', ad_location'
    query += ') VALUES ('
    query += ', '.join(['%s' for key in keys])
    if has_latlon:
        query += ', ST_SetSRID(ST_MakePoint(%s, %s), 4326)' % (ad['ad_longitude'], ad['ad_latitude'])
    query += ')'
    connection.cursor().execute(query, values)
    connection.commit()


def write_ads(connection, table, ads):
    cursor = connection.cursor()
    fields_ph = ', '.join(fields)
    value_ph = '(' + ', '.join(['%s' for field in fields]) + ')'
    values = [[ad[field] if field in ad else None for field in fields] for ad in ads if ad is not None]
    values = ', '.join(cursor.mogrify(value_ph, value) for value in values)
    query = 'INSERT INTO %s (%s) VALUES %s' % (table, fields_ph, values)
    cursor.execute(query)
    connection.commit()
