# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
This module replicates the scala script over at

https://github.com/mozilla/telemetry-batch-view/blob/1c544f65ad2852703883fe31a9fba38c39e75698/src/main/scala/com/mozilla/telemetry/views/HBaseAddonRecommenderView.scala
"""

from pyspark.sql.functions import desc, row_number
from pyspark.sql import Window

from taar_loader.filters import filterDateAndClientID
from taar_loader.filters import list_transformer


def etl(spark, run_date):
    currentDate = run_date
    currentDateString = currentDate.strftime("%Y%m%d")
    print("Processing %s" % currentDateString)


    print("Dataset is sampled!")
    # Get the data for the desired date out of parquet
    template = "s3://telemetry-parquet/main_summary/v4/submission_date_s3=%s"
    datasetForDate = spark.read.parquet(template % currentDateString)

    # Get the most recent (client_id, subsession_start_date) tuple
    # for each client since the main_summary might contain
    # multiple rows per client. We will use it to filter out the
    # full table with all the columns we require.

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

    rdd = subset.rdd.zip(jsonDataRDD)
    filtered_rdd = rdd.filter(filterDateAndClientID)
    merged_filtered_rdd = filtered_rdd.map(list_transformer)

    return merged_filtered_rdd


def main(spark, run_date):


    rdd = etl(spark, run_date, load_parquet)
    return rdd


    reduction_output = etl(spark, run_date, load_parquet)
    reduction_output = etl(spark, run_date)
    report_data = (reduction_output[0], reduction_output[1])
    print("=" * 40)
    print("%d records inserted to DynamoDB.\n%d records remaining in queue." % report_data)
    print("=" * 40)
    return reduction_output
