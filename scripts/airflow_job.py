#!/usr/bin/env python3

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import argparse

from datetime import date
from datetime import datetime
from datetime import timedelta

from taar_loader import runme


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


if __name__ == "__main__":
    args = parse_args()
    runme(args.run_date)
