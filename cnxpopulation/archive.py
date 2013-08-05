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
import json
import uuid
import zipfile

import psycopg2
import wget

from .parsers import parse_collection_xml, parse_module_xml


DESCRIPTION = __doc__
DEFAULT_PSYCOPG_CONNECTION_STRING = "dbname=cnxarchive user=cnxarchive " \
                                    "password=cnxarchive host=localhost " \
                                    "port=5432"
here = os.path.abspath(os.path.dirname(__file__))
logger = logging.getLogger('populate')


def unpack(zip_file, output_directory=None):
    """Unpack a zip file. Optionally provide an ``output_directory``"""
    with zipfile.ZipFile(zip_file, 'r') as zf:
        zf.extractall(output_directory)
    # This does not return the extracted files because we can make
    #   certain assumptions about the complete-zip format, specifically
    #   the encapsulating directories name: {col###}_{version}_complete


def acquire_content(id, versions=[], host='http://cnx.org',
                    output_dir=here):
    """Download or use the complete zip for the content at ``id`` for
    the specified ``versions`` from ``host``.
    """
    for version in versions:
        download = '{}-{}.complete.zip'.format(id, version)
        directory = '{}_{}_complete'.format(id, version)

        zip_location = os.path.join(output_dir, download)
        output_location = os.path.join(output_dir, directory)
        if os.path.exists(output_location):
            logger.debug("Using found directory, '{}'.".format(directory))
        elif os.path.exists(zip_location):
            logger.debug("Using found complete zip, '{}'".format(download))
            unpack(zip_location, output_dir)
        else:
            url = "{}/content/{}/{}/complete".format(host, id, version)
            # Download the complete zip
            zip_file = wget.download(url)
            os.rename(zip_file, zip_location)
            # Unpack it
            unpack(zip_location, output_dir)
        yield output_location
    raise StopIteration


def populate_from_completezip(location, ident_mappings, psycopg_conn):
    """Populate the database using an unpacked completezip
    formated collection.
    """
    collection_xml_path = os.path.join(location, 'collection.xml')
    with open(collection_xml_path, 'r') as fp:
        collection_parts = parse_collection_xml(fp)
    abstract, license_url, collection_metadata, contents = collection_parts
    # Fix the uuid value and/or pull it from the ident_mapping
    try:
        collection_uuid = ident_mappings[collection_metadata['moduleid']]
    except (KeyError,):
        collection_uuid = uuid.uuid4()
    collection_metadata['uuid'] = str(collection_uuid)

    with psycopg_conn.cursor() as cursor:
        # Insert the abstract
        cursor.execute("INSERT INTO abstracts (abstract) "
                       "VALUES (%s) "
                       "RETURNING abstractid;", (abstract,))
        abstract_id = cursor.fetchone()[0]
        # Find the license id
        cursor.execute("SELECT licenseid FROM licenses "
                       "WHERE url = %s;", (license_url,))
        license_id = cursor.fetchone()[0]
        # Relate the abstract and license
        collection_metadata['abstractid'] = abstract_id
        collection_metadata['licenseid'] = license_id

        # Insert the collection
        collection_metadata = collection_metadata.items()
        metadata_keys = ', '.join([x for x, y in collection_metadata])
        metadata_value_spaces = ', '.join(['%s'] * len(collection_metadata))
        metadata_values = [y for x, y in collection_metadata]
        cursor.execute("INSERT INTO modules  ({}) "
                       "VALUES ({}) "
                       "RETURNING module_ident;".format(metadata_keys,
                                                        metadata_value_spaces),
                       metadata_values)
        collection_id = cursor.fetchone()[0]

        # And finally insert the original collection.xml file
        with open(collection_xml_path, 'r') as fp:
            cursor.execute("INSERT INTO files (file) VALUES (%s) RETURNING fileid;",
                           (fp.read(),))
        file_id = cursor.fetchone()[0]
        cursor.execute("INSERT INTO module_files "
                       "  (module_ident, fileid, filename, mimetype) "
                       "  VALUES (%s, %s, %s, %s) ",
                       (collection_id, file_id, 'collection.xml', 'text/xml',))

    for module_id in contents:
        content_file_path = os.path.join(location, module_id, 'index.cnxml')
        content_w_metadata_file_path = os.path.join(location, module_id,
                                                   'index_auto_generated.cnxml')
        with open(content_w_metadata_file_path, 'r') as fp:
            abstract, license_url, metadata, resources = parse_module_xml(fp)
        with psycopg_conn.cursor() as cursor:
            if abstract is not None:
                # Insert the abstract
                cursor.execute("INSERT INTO abstracts (abstract) "
                               "VALUES (%s) "
                               "RETURNING abstractid;", (abstract,))
                abstract_id = cursor.fetchone()[0]
                metadata['abstractid'] = abstract_id
            # Find the license id
            cursor.execute("SELECT licenseid FROM licenses "
                           "WHERE url = %s;", (license_url,))
            license_id = cursor.fetchone()[0]
            metadata['licenseid'] = license_id

            # Insert the collection
            metadata = metadata.items()
            metadata_keys = ', '.join([x for x, y in metadata])
            metadata_value_spaces = ', '.join(['%s'] * len(metadata))
            metadata_values = [y for x, y in metadata]
            cursor.execute("INSERT INTO modules  ({}) "
                           "VALUES ({}) "
                           "RETURNING module_ident;".format(
                               metadata_keys,
                               metadata_value_spaces),
                           metadata_values)
            content_id = cursor.fetchone()[0]

            # And finally insert the original collection.xml file
            with open(content_file_path, 'r') as fp:
                cursor.execute("INSERT INTO files (file) VALUES (%s) "
                               "RETURNING fileid;", (fp.read(),))
                file_id = cursor.fetchone()[0]
                cursor.execute("INSERT INTO module_files "
                               "  (module_ident, fileid, filename, mimetype) "
                               "  VALUES (%s, %s, %s, %s) ",
                               (content_id, file_id, 'index.cnxml',
                                'text/xml',))
        for filename, mimetype in resources:
            resource_file_path = os.path.join(location, module_id, filename)
            with psycopg_conn.cursor() as cursor:
                with open(resource_file_path, 'rb') as fp:
                    cursor.execute("INSERT INTO files (file) VALUES (%s) "
                                   "RETURNING fileid;",
                                   (psycopg2.Binary(fp.read()),))
                    file_id = cursor.fetchone()[0]
                cursor.execute("INSERT INTO module_files "
                               "  (module_ident, fileid, filename, mimetype) "
                               "  VALUES (%s, %s, %s, %s) ",
                               (content_id, file_id, filename,
                                mimetype,))


def main(argv=None):
    """Main commandline interface"""
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument('collection_id', help="(e.g. col11496)")
    parser.add_argument('--versions', nargs='+', default=['latest'],
                        help="a series of version numbers")
    parser.add_argument('-u', '--legacy-url', default='http://cnx.org',
                        help="defaults to http://cnx.org")
    parser.add_argument('-p', '--psycopg-conn-str',
                        default=DEFAULT_PSYCOPG_CONNECTION_STRING,
                        help="a psycopg2 connection string")
    args = parser.parse_args(argv)

    locations = acquire_content(args.collection_id, args.versions,
                                host=args.legacy_url)

    collection_uuid = uuid.uuid4()
    ident_mappings = {args.collection_id: collection_uuid}
    for location in locations:
        with psycopg2.connect(args.psycopg_conn_str) as db_connection:
            populate_from_completezip(location,
                                      ident_mappings,
                                      db_connection)
            db_connection.commit()


if __name__ == '__main__':
    main()
