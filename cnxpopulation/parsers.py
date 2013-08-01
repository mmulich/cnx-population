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


__all__ = ('parse_collection_xml', 'parse_module_xml',)


def _generate_xpath_func(xml_doc, default_namespace_name='base'):
    """Generates an easy to work with xpath function."""
    nsmap = xml_doc.nsmap.copy()
    try:
        nsmap[default_namespace_name] = nsmap.pop(None)
    except KeyError:
        # There isn't a default namespace.
        pass
    return lambda xpth: xml_doc.xpath(xpth, namespaces=nsmap)


def _parse_common_elements(xml_doc):
    """Parse the common elements between a ColXML and CnXML files."""
    xpath = _generate_xpath_func(xml_doc)

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

    return [abstract, license, metadata]


def parse_collection_xml(fp):
    """Parse into the file into segments that will fit into the database.
    Returns the abstract content, license url, metadata dictionary,
    and a list of content ids that are part of this collection.
    """
    # Parse the document
    tree = lxml.etree.parse(fp)
    doc = tree.getroot()
    xpath = _generate_xpath_func(doc, 'colxml')

    data = _parse_common_elements(doc)
    # Pull the linked content (modules)
    contents = xpath('//colxml:module/@document')[:]
    data.append(contents)
    return data


def parse_module_xml(fp):
    """Parse the file into segments that will fit into the database.
    This works against the index_auto_generated.cnxml
    Returns the abstract content, license url, metadata dictionary,
    and a list of resource urls that are in the content.
    """
    # Parse the document
    tree = lxml.etree.parse(fp)
    doc = tree.getroot()
    xpath = _generate_xpath_func(doc, 'cnxml')

    data = _parse_common_elements(doc)
    # Pull the linked content (modules)
    resources = [(e.get('src'), e.get('mime-type'),)
                 for e in xpath('//cnxml:image')]
    data.append(resources)
    return data
