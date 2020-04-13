#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-pendo",
    version="0.1.0",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="https://app.pendo.io/api/v1/report",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_pendo"],
    install_requires=[
        "singer-python>=5.0.12",
        "requests",
    ],
    entry_points="""
    [console_scripts]
    tap-pendo=tap_pendo:main
    """,
    packages=["tap_pendo"],
    package_data = {
        "schemas": ["tap_pendo/schemas/*.json"]
    },
    include_package_data=True,
)
