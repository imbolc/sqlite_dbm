#!/usr/bin/env python
import os
import sys
import doctest
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

import sqlite_dbm as mod


DOC = mod.__doc__.strip()

# For first pypi publish use: ./setup.py register
# and then use: ./setup.py publish
open('README.md', 'w').write(DOC)
if sys.argv[-1] == 'publish':
    if not doctest.testfile('README.md', verbose=True).failed:
        os.system('python setup.py sdist upload')
        sys.exit(0)

setup(
    name=mod.__name__,
    url='https://github.com/imbolc/%s' % mod.__name__,
    version=mod.__version__,
    description=DOC.split('===\n')[1].strip().split('\n\n')[0],
    long_description=DOC,

    py_modules=[mod.__name__],

    author='Imbolc',
    author_email='imbolc@imbolc.name',
    license='ISC',

    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: ISC License (ISCL)',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
    ],
)
