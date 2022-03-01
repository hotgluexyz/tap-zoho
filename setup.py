#!/usr/bin/env python
from setuptools import setup

setup(
  name="tap-zoho",
  version="0.1.1",
  description="Revlock tap for extracting data from ZOHO CRM",
  author="Stitch",
  url="http://singer.io",
  classifiers=["Programming Language :: Python :: 3 :: Only"],
  py_modules=["tap_zoho"],
  install_requires=[
    "singer-python==5.3.1",
    'zcrmsdk==2.0.6',
    'python-dateutil==2.8.1',
    'requests==2.21.0'
  ],
  entry_points="""
    [console_scripts]
    tap-zoho=tap_zoho:main
    """,
  packages=["tap_zoho", "tap_zoho.zoho", "tap_zoho.zohoAuthPersistence"],
  package_data={
    "tap_zoho/schemas/": [
      # add schema.json filenames here
    ]
  },
  include_package_data=True,
)
