# -*- coding: utf-8 -*-
# ###
# Copyright (c) 2013, Rice University
# This software is subject to the provisions of the GNU Affero General
# Public License version 3 (AGPLv3).
# See LICENCE.txt for details.
# ###
"""Connexions content parsers"""
import os
import lxml.etree


__all__ = ('parse_collection_xml',)


def parse_collection_xml(fp):
    """Parse into the file into segments that will fit into the database.
    Returns the abstract content, license url, metadata dictionary,
    and a list of content ids that are part of this collection.
    """
    # Parse the document
    tree = lxml.etree.parse(fp)

    nsmap = tree.getroot().nsmap.copy()
    del nsmap[None]
    xpath = lambda xpth: tree.xpath(xpth, namespaces=nsmap)

    # Pull the abstract
    abstract = xpath('//md:abstract/text()')[0]

    # Pull the license
    license = xpath('//md:license/@url')[0]

    # Pull the collection metadata
    metadata = {
        'portal_type': 'Collection',
        'moduleid': xpath('//md:content-id/text()')[0],
        'version': xpath('//md:version/text()')[0],
        'name': xpath('//md:title/text()')[0],
        # FIXME Don't feel like parsing the dates at the moment.
        # 'created': ?,
        # 'revised': ?,
        'doctype': '',  # Can't be null, but appears unused.
        'submitter': '',
        'submitlog': '',
        'language': xpath('//md:language/text()')[0],
        'authors': xpath('//md:roles/md:role[type="author"]/text()')[:],
        'maintainers': xpath('//md:roles/md:role[type="maintainer"]/text()')[:],
        'licensors': xpath('//md:roles/md:role[type="licensor"]/text()')[:],
        # 'parentauthors': None,

        # Related on insert...
        # 'parent': 1,
        # 'stateid': 1,
        # 'licenseid': 1,
        # 'abstractid': 1,
        }

    # Pull the linked content (modules)
    contents = xpath('//col:module/@document')[:]

    return [abstract, license, metadata, contents]
