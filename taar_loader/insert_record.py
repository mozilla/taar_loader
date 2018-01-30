#!/bin/sh

def insert_record():
    import boto3
    conn = boto3.resource('dynamodb', region_name='us-west-2')
    table = conn.Table('taar_addon_data')

    with table.batch_writer(overwrite_by_pkeys=['client_id']) as batch:
        item = {"client_id": "some-dummy-client-id",
                "bookmark_count": 5,
                "disabled_addon_ids": ["disabled1", "disabled2"],
                "geo_city": "Toronto",
                "installed_addons": ["active1", "active2"],
                "locale": "en-CA",
                "os": "Mac OSX",
                "profile_age_in_weeks": 5,
                "profile_date": "2018-Jan-08",
                "submission_age_in_weeks": "5",
                "submission_date": "2018-Jan-09",
                "subsession_length": 20,
                "tab_open_count": 10,
                "total_uri": 9,
                "unique_tlds": 5}
        batch.put_item(Item=item)
