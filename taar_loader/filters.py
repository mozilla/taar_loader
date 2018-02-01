# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import boto3


def filterDateAndClientID(row_jstr):
    (row, jstr) = row_jstr
    import dateutil.parser
    try:
        assert row.client_id is not None
        dateutil.parser.parse(row.subsession_start_date)
        return True
    except Exception:
        return False


def list_transformer(row_jsonstr):
    (row, json_str) = row_jsonstr
    """ Run mimimal verification that the clientId and startDate is not
    null"""
    import json
    import dateutil.parser
    client_id = row.client_id
    start_date = dateutil.parser.parse(row.subsession_start_date)
    start_date = start_date.date()
    start_date = start_date.strftime("%Y%m%d")
    jdata = json.loads(json_str)
    jdata['client_id'] = client_id
    jdata['start_date'] = start_date

    # We need to return a 3-tuple of values
    # (numrec_dynamodb_pushed, json_list_length, json_list)

    # These 3-tuples can be reduced in a map/reduce
    return (0, 1, [jdata])


def push_to_dynamo(item_list):
    """
    This connects to DynamoDB and pushes records in `item_list` into
    a table.
    """
    conn = boto3.resource('dynamodb', region_name='us-west-2')
    table = conn.Table('taar_addon_data')
    with table.batch_writer(overwrite_by_pkeys=['client_id']) as batch:
        for item in item_list:
            batch.put_item(Item=item)


def dynamo_reducer(list_a, list_b):

    # If one list is empty, just return the other list
    if list_a[1] == 0:
        return list_b
    if list_b[1] == 0:
        return list_a

    new_list = [list_a[0] + list_b[0],
                list_a[1] + list_b[1],
                list_a[2] + list_b[2]]

    if new_list[1] > 100:
        push_to_dynamo(new_list[2])

        # Update number of records written to dynamo
        new_list[0] += new_list[1]

        # Zero out the number of accumulated records
        new_list[1] = 0

        # Clear out the accumulated JSON records
        new_list[2] = []

    return tuple(new_list)
