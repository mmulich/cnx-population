# -*- coding: utf-8 -*-
from setuptools import setup, find_packages


install_requires = (
    'lxml',
    'psycopg2',
    'wget',
    )
description = "A set of content population utilities."


setup(
    name='cnx-population',
    version='0.1',
    author='Connexions team',
    author_email='info@cnx.org',
    url="https://github.com/connexions/cnx-population",
    license='LGPL, See aslo LICENSE.txt',
    description=description,
    packages=find_packages(),
    install_requires=install_requires,
    include_package_data=True,
    entry_points="""\
    [console_scripts]
    cnx-archive-populate = cnxpopulation.archive:main
    """,
    # test_suite=''
    )
