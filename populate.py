#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ###
# Copyright (c) 2013, Rice University
# This software is subject to the provisions of the GNU Affero General
# Public License version 3 (AGPLv3).
# See LICENCE.txt for details.
# ###
"""A script that populates a cnx-archive database from a legacy repository."""
import os
import sys
import argparse
import logging
import uuid
import zipfile

import wget


DESCRIPTION = __doc__
here = os.path.abspath(os.path.dirname(__file__))
logger = logging.getLogger('populate')


def acquire_content(id, versions=[], host='http://cnx.org',
                    output_dir=here):
    """Download or use the complete zip for the content at `id` for
    the specified `versons` from `host`."""
    for version in versions:
        download = '{}-{}.complete.zip'.format(id, version)
        directory = '{}_{}_complete'.format(id, version)

        zip_location = os.path.join(output_dir, download)
        output_location = os.path.join(output_dir, directory)
        if os.path.exists(output_location):
            logger.debug("Using found directory, '{}'.".format(directory))
        elif os.path.exists(zip_location):
            logger.debug("Using found complete zip, '{}'".format(download))
        else:
            url = "{}/content/{}/{}/complete".format(host, id, version)
            # Download the complete zip
            zip_file = wget.download(url)
            os.rename(zip_file, zip_location)
            # Unpack it
            with zipfile.ZipFile(zip_file, 'r') as zf:
                zf.extractall(output_dir)

        yield output_location
    raise StopIteration


def main(argv=None):
    """Main commandline interface"""
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument('collection_id', help="(e.g. col11496)")
    parser.add_argument('--versions', nargs='+', default=['latest'],
                        help="a series of version numbers")
    parser.add_argument('-u', '--legacy-url', default='http://cnx.org',
                        help="defaults to http://cnx.org")
    parser.add_argument('-p', '--psycopg-conn-str',
                        help="a psycopg2 connection string")
    parser
    args = parser.parse_args(argv)

    locations = acquire_content(args.collection_id, args.versions,
                                host=args.legacy_url)

    print([l for l in locations])

    # collection_uuid = uuid.uuid4()
    # ident_mappings = {args.collection_id: collection_uuid}
    # for location in locations:
    #     insert_collection(location, ident_mappings)


if __name__ == '__main__':
    main()
