#!/usr/bin/env python3

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import argparse

from datetime import date
from datetime import datetime
from datetime import timedelta

from pprint import pprint

from pyspark import SparkConf
from pyspark.sql import SparkSession


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

    parser.add_argument('--date',
                        dest='run_date',
                        action='store',
                        type=valid_date_type,
                        default=yesterday_str,
                        required=False,
                        help='Start date for data submission (YYYYMMDD)')

    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    APP_NAME = "HBaseAddonRecommenderView"
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[*]")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.addPyFile("taar_loader-1.0-py3.5.egg")
    import taar_loader
    import taar_loader.filters
    print(taar_loader)
    print(taar_loader.filters)
    rdd = taar_loader.main(spark, args.run_date)
    reduced_rdd = rdd.reduce(taar_loader.filters.dynamo_reducer)
    pprint(reduced_rdd)

if __name__ == '__main__':
    main()
