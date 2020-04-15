#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-pendo",
    version="0.1.0",
    description="Singer.io tap for extracting NPS data from the Pendo API",
    author="Soxhub",
    url="https://github.com/soxhub",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_pendo"],
    install_requires=[
        "singer-python>=5.0.12",
        "requests",
    ],
    entry_points='''
      [console_scripts]
      tap-pendo=tap_pendo:main
    ''',
    packages=["tap_pendo"],
    package_data={
      "schemas": ["schemas/*.json"]
    },
    include_package_data=True,
)
