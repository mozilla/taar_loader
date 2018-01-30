# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
This module replicates the scala script over at

https://github.com/mozilla/telemetry-batch-view/blob/1c544f65ad2852703883fe31a9fba38c39e75698/src/main/scala/com/mozilla/telemetry/views/HBaseAddonRecommenderView.scala
"""

import argparse
from datetime import date
from datetime import datetime
from datetime import timedelta

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, row_number
from pyspark.sql import Window


def parse_args():
    def valid_date_type(arg_date_str):
        """custom argparse *date* type for user dates values given from the command line"""
        try:
            return datetime.strptime(arg_date_str, "%Y%m%d").date()
        except ValueError:
            msg = "Given Date ({0}) not valid! Expected format, YYYYMMDD!".format(arg_date_str)
            raise argparse.ArgumentTypeError(msg)

    yesterday_str = (date.today() - timedelta(days=1)).strftime("%Y%m%d")

    description = 'Copy data from telemetry S3 parquet files to DynamoDB'
    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('--from',
                        dest='from_date',
                        action='store',
                        type=valid_date_type,
                        default=yesterday_str,
                        required=False,
                        help='Start date for data submission (YYYYMMDD)')

    parser.add_argument('--to',
                        dest='to_date',
                        type=valid_date_type,
                        action='store',
                        default=yesterday_str,
                        required=False,
                        help='End date of the data submission')

    args = parser.parse_args()
    return args


def etl(spark, from_date, to_date, dataFrameFunc):
    # Validation Errors
    resultRDD = None
    for offset in range(0, (to_date - from_date).days + 1):
        currentDate = from_date + timedelta(days=offset)
        currentDateString = currentDate.strftime("%Y%m%d")
        print("Processing %s" % currentDateString)

        # Get the data for the desired date out of the dataframe
        tmp = dataFrameFunc(currentDateString)

        # TODO: Sampling down to 1 in 10,000,000 records to get ~3,
        # possibly 4 records.
        datasetForDate = tmp.sample(False, 0.00000001)

        print ("Dataset is sampled!")

        # Get the most recent (client_id, subsession_start_date) tuple
        # for each client since the main_summary might contain
        # multiple rows per client. We will use it to filter out the
        # full table with all the columns we require.

        # TODO: verify the '$' notation for pyspark vs scala spark
        clientShortList = datasetForDate.select("client_id",
                                                'subsession_start_date',
                                                row_number().over(
                                                    Window.partitionBy('client_id')
                                                    .orderBy(desc('subsession_start_date'))
                                                ).alias('clientid_rank'))
        clientShortList = clientShortList.where('clientid_rank == 1').drop('clientid_rank')

        select_fields = ["client_id",
                         "subsession_start_date",
                         "subsession_length",
                         "city",
                         "locale",
                         "os",
                         "places_bookmarks_count",
                         "scalar_parent_browser_engagement_tab_open_event_count",
                         "scalar_parent_browser_engagement_total_uri_count",
                         "scalar_parent_browser_engagement_unique_domains_count",
                         "active_addons",
                         "disabled_addons_ids"]
        dataSubset = datasetForDate.select(*select_fields)

        # Join the two tables: only the elements in both dataframes
        # will make it through.

        clientsData = dataSubset.join(clientShortList,
                                      ["client_id",
                                       'subsession_start_date'])

        # Convert the DataFrame to JSON and get an RDD out of it.
        subset = clientsData.select("client_id", "subsession_start_date")

        jsonDataRDD = clientsData.select("city",
                                         "subsession_start_date",
                                         "subsession_length",
                                         "locale",
                                         "os",
                                         "places_bookmarks_count",
                                         "scalar_parent_browser_engagement_tab_open_event_count",
                                         "scalar_parent_browser_engagement_total_uri_count",
                                         "scalar_parent_browser_engagement_unique_domains_count",
                                         "active_addons",
                                         "disabled_addons_ids").toJSON()

        def filterDateAndClientID((row, jstr)):
            import dateutil.parser
            try:
                assert row.client_id is not None
                dateutil.parser.parse(row.subsession_start_date)
                return True
            except:
                return False

        def merge_mapper((row, json_str)):
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

        rdd = subset.rdd.zip(jsonDataRDD)
        filtered_rdd = rdd.filter(filterDateAndClientID)
        merged_filtered_rdd = filtered_rdd.map(merge_mapper)

        # TODO: reduce the merged_filtered_rdd into chunks that are
        # pushed into DynamoDB
        resultRDD = merged_filtered_rdd

    # Note that this won't be useful
    return resultRDD


def main(spark, from_date, to_date):
    """
    This script takes 3 command line arguments:
     --from $date
     --to $date
    """

    template = "s3://telemetry-parquet/main_summary/v4/submission_date_s3=%s"

    def load_parquet(dateString):
        return spark.read.parquet(template % dateString)

    return etl(spark,
               from_date,
               to_date,
               load_parquet)


def reducer(tuple_a, tuple_b):
    import boto3
    # If either of the tuples has no elements to push into
    # dynamodb, just return the compliment tuple.
    if tuple_a[1] == 0:
        return tuple_b
    elif tuple_b[1] == 0:
        return tuple_a

    # Merge the lists together
    elem0 = tuple_a[0] + tuple_b[0]
    elem1 = tuple_a[1] + tuple_b[1]
    elem2 = tuple_a[2] + tuple_b[2]

    tuple_data = [elem0, elem1, elem2]
    if elem1 >= 3:
        # push records to dynamodb
        DYNAMO_CONN = boto3.resource('dynamodb', region_name='us-west-2')
        DYNAMO_TABLE = DYNAMO_CONN.Table('taar_addon_data')
        with DYNAMO_TABLE.batch_writer(overwrite_by_pkeys=['client_id']) as batch:
            for jdata in tuple_data[2]:
                batch.put_item(Item=jdata)
            # update counters
            tuple_data[0] += tuple_data[1]
            tuple_data[1] = 0
        tuple_data = tuple(tuple_data)

    return tuple_data


def runme(from_date, to_date):
    # APP_NAME = "HBaseAddonRecommenderView"
    # Configure OPTIONS
    conf = SparkConf()  # .setAppName(APP_NAME)
    # conf = conf.setMaster("local[*]")
    sparkSession = SparkSession.builder.config(conf=conf).getOrCreate()
    print ("My spark session: %s" % sparkSession)
    print ("My spark context: %s" % sparkSession.sparkContext)
    # Execute Main functionality
    return main(sparkSession, from_date, to_date)


if __name__ == "__main__":
    args = parse_args()
    runme(args.from_date, args.to_date)
