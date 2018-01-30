# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


def filterDateAndClientID(row_jstr):
    (row, jstr) = row_jstr
    import dateutil.parser
    try:
        assert row.client_id is not None
        dateutil.parser.parse(row.subsession_start_date)
        return True
    except Exception as inst:
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


def boto3_tuple_reducer(tuple_a, tuple_b):

    if tuple_a[1] == 0:
        return tuple_b
    if tuple_b[1] == 0:
        return tuple_a

    # Both tuples have non-zero length lists, merge them

    working_tuple = [tuple_a[0] + tuple_b[0],
                     tuple_a[1] + tuple_b[1],
                     tuple_a[2] + tuple_b[2]]

    if working_tuple[1] >= 3:
        push_to_dynamo(working_tuple[2])
        working_tuple[0] += working_tuple[1]
        working_tuple[1] = 0
        working_tuple[2] = []

    return tuple(working_tuple)


def push_to_dynamo(json_list):
    import boto3
    conn = boto3.resource('dynamodb', region_name='us-west-2')
    table = conn.Table('taar_addon_data')

    with table.batch_writer(overwrite_by_pkeys=['client_id']) as batch:
        for item in json_list:
            batch.put_item(Item=item)
