#!/usr/bin/env python
from pathlib import Path
import re
from setuptools import setup, find_packages
import shutil
import sys


def read(*parts):
    return Path(__file__).parent.joinpath(*parts).read_text()


def find_version(*parts):
    vers_file = read(*parts)
    match = re.search(r'^__version__ = "(\d+\.\d+\.\d+)"', vers_file, re.M)
    if match is not None:
        return match.group(1)
    raise RuntimeError("Unable to find version string.")


# install pandoc
if not shutil.which('pandoc'):
    import pypandoc
    pypandoc.download_pandoc(version='2.2.3.2', download_folder='/tmp')


setup(
    name="Redmine-zulip",
    version=find_version("redmine_zulip", "__init__.py"),
    author="Thomas Michelat",
    author_email="thomas.michelat@gmail.com",
    maintainer="Thomas Michelat",
    url="",
    description=("Publish Redmine issues to Zulip"),
    long_description=read("README.md"),
    license="BSD-3-Clause",
    entry_points={
        "console_scripts": [
            "redmine-zulip-publisher = redmine_zulip.redmine:main",
        ],
    },

    python_requires='>=3.7',
    install_requires=[
        'atoma',
        'dataset',
        'loguru',
        'pypandoc',
        'python-redmine>=2.3.0',
        'toml',
        'zulip>=0.7.1',
    ],
    packages=find_packages(),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'License :: OSI Approved :: BSD License',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ]
)
