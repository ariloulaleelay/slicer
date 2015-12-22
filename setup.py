#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='hiveslicer',
    version='0.1.0',
    description='Hive cube slicer server',
    author='Andrey Proskurnev',
    author_email='andrey@proskurnev.ru',
    packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    install_requires=[
        'pyhocon==0.3.11',
        'sqlalchemy',
        'cherrypy',
        'pyhive[hive]',
    ],
    entry_points={
        'console_scripts': [
            'hiveslicer = hiveslicer.scripts:main',
        ]
    },
)
