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
import csv
import argparse
import logging
import json
import uuid
import zipfile
from urllib import urlretrieve

import psycopg2

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
            zip_file, headers = urlretrieve(url, filename=zip_location)
            # Unpack it
            unpack(zip_location, output_dir)
        yield output_location
    raise StopIteration


def _insert_abstract(abstract_text, cursor):
    """insert the abstract"""
    cursor.execute("INSERT INTO abstracts (abstract) "
                   "VALUES (%s) "
                   "RETURNING abstractid;", (abstract_text,))
    id = cursor.fetchone()[0]
    return id
def _find_license_id_by_url(url, cursor):
    cursor.execute("SELECT licenseid FROM licenses "
                   "WHERE url = %s;", (url,))
    id = cursor.fetchone()[0]
    return id
def _insert_module(metadata, cursor):
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
    id = cursor.fetchone()[0]
    return id
def _insert_module_file(module_id, filename, mimetype, file, cursor):
    payload = (psycopg2.Binary(file.read()),)
    cursor.execute("INSERT INTO files (file) VALUES (%s) "
                   "RETURNING fileid;", payload)
    file_id = cursor.fetchone()[0]
    cursor.execute("INSERT INTO module_files "
                   "  (module_ident, fileid, filename, "
                   "   mimetype) "
                   "  VALUES (%s, %s, %s, %s) ",
                   (module_id, file_id, filename, mimetype,))
def _insert_subject_for_module(subject_text, module_id, cursor):
    cursor.execute("INSERT INTO moduletags (module_ident, tagid) "
                   "  VALUES (%s, "
                   "          (SELECT tagid FROM tags "
                   "             WHERE tag = %s)"
                   "          );",
                   (module_id, subject_text))
def _insert_keyword_for_module(keyword, module_id, cursor):
    try:
        cursor.execute("SELECT keywordid FROM keywords "
                       "  WHERE word = %s;", (keyword,))
        keyword_id = cursor.fetchone()[0]
    except TypeError:
        cursor.execute("INSERT INTO keywords (word) "
                       "  VALUES (%s) "
                       "  RETURNING keywordid", (keyword,))
        keyword_id = cursor.fetchone()[0]
    cursor.execute("INSERT INTO modulekeywords "
                   "  (module_ident, keywordid) "
                   "  VALUES (%s, %s)",
                   (module_id, keyword_id,))


def populate_from_completezip(location, ident_mappings, psycopg_conn):
    """Populate the database using an unpacked completezip
    formated collection.
    """
    collection_xml_path = os.path.join(location, 'collection.xml')
    with open(collection_xml_path, 'r') as fp:
        collection_parts = parse_collection_xml(fp)
    collection_abstract, collection_license_url, collection_metadata, \
        collection_keywords, collection_subjects, contents = collection_parts
    # Determine the *id values from the ident_mapping
    try:
        collection_mid = collection_metadata['moduleid']
        collection_uuid, collection_ident = ident_mappings[collection_mid]
        collection_metadata['uuid'] = collection_uuid
        collection_metadata['module_ident'] = collection_ident
    except KeyError:
        pass

    for module_mid in contents:
        # Paths to the medatadata/content files.
        content_file_path = os.path.join(location, module_mid, 'index.cnxml')
        content_w_metadata_file_path = os.path.join(location, module_mid,
                                                   'index_auto_generated.cnxml')
        # Read the metadata file.
        with open(content_w_metadata_file_path, 'r') as fp:
            abstract, license_url, metadata, \
                keywords, subjects,resources = parse_module_xml(fp)
        # Determine the *id values from the ident_mapping.
        try:
            module_mid = metadata['moduleid']
            module_uuid, module_ident = ident_mappings[module_mid]
            metadata['uuid'] = module_uuid
            metadata['module_ident'] = module_ident
        except KeyError:
            pass

        with psycopg_conn.cursor() as cursor:
            if abstract is not None:
                metadata['abstractid'] = _insert_abstract(abstract, cursor)
            # Find the license id
            metadata['licenseid'] = _find_license_id_by_url(license_url,
                                                            cursor)
            # Insert the module
            content_id = _insert_module(metadata, cursor)
            # And finally insert the original index.cnxml
            #   and index_auto_generated.cnxml files.
            for file_path in (content_file_path, content_w_metadata_file_path,):
                filename = os.path.basename(file_path)
                with open(file_path, 'r') as fb:
                    _insert_module_file(content_id, filename, 'text/xml', fb,
                                        cursor)
        for filename, mimetype in resources:
            resource_file_path = os.path.join(location, module_mid, filename)
            if not os.path.exists(resource_file_path):
                # FIXME Should at least log this as an error.
                continue
            with psycopg_conn.cursor() as cursor:
                with open(resource_file_path, 'rb') as fb:
                    _insert_module_file(content_id, filename, mimetype, fb,
                                        cursor)
        # Associate the subjects and input the keywords.
        with psycopg_conn.cursor() as cursor:
            for subject in subjects:
                _insert_subject_for_module(subject, content_id, cursor)
            for keyword in keywords:
                _insert_keyword_for_module(keyword, content_id, cursor)
        psycopg_conn.commit()

    with psycopg_conn.cursor() as cursor:
        abstract_id = _insert_abstract(collection_abstract, cursor)
        license_id = _find_license_id_by_url(collection_license_url, cursor)
        collection_metadata['abstractid'] = abstract_id
        collection_metadata['licenseid'] = license_id

        # Insert the collection
        collection_id = _insert_module(collection_metadata, cursor)

        # And finally insert the original collection.xml file
        with open(collection_xml_path, 'r') as fb:
            _insert_module_file(collection_id, 'collection.xml', 'text/xml',
                                fb, cursor)
        # Associate the subjects and input the keywords.
        with psycopg_conn.cursor() as cursor:
            for subject in collection_subjects:
                _insert_subject_for_module(subject, collection_id, cursor)
            for keyword in collection_keywords:
                _insert_keyword_for_module(keyword, collection_id, cursor)
    psycopg_conn.commit()


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
    parser.add_argument('--ids-file', type=argparse.FileType('r'),
                        help="CSV containing moduleid, uuid and module_ident")
    args = parser.parse_args(argv)

    output_dir = os.getcwd()
    locations = acquire_content(args.collection_id, args.versions,
                                host=args.legacy_url,
                                output_dir=output_dir)

    import psycopg2.extras
    psycopg2.extras.register_uuid()
    if args.ids_file:
        ident_mappings = {mid: (uuid.UUID(uid), int(ident),)
                          for mid, uid, ident in csv.reader(args.ids_file)}
    else:
        ident_mappings = {}
    for location in locations:
        with psycopg2.connect(args.psycopg_conn_str) as db_connection:
            populate_from_completezip(location,
                                      ident_mappings,
                                      db_connection)
            db_connection.commit()


if __name__ == '__main__':
    main()
