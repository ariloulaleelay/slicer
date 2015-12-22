#!/usr/bin/env python
# coding: utf8

import argparse
import logging
import os
import cherrypy
from hiveslicer.config import default_config
from hiveslicer.server import SlicerServer

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s\t%(levelname)s\t%(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger('root')

__all__ = [
    'runserver'
]


def runserver(args, config):
    logger.info("run server")

    cherrypy.config.update({'server.socket_host': config.hiveslicer.http.host()})
    cherrypy.config.update({'server.socket_port': config.hiveslicer.http.port()})

    cherrypy.quickstart(SlicerServer(config), '/')


def main():
    parser = argparse.ArgumentParser(description='hiveslicer server')
    parser.add_argument('--config', default=os.environ.get('SLICER_CONFIG'))

    subparsers = parser.add_subparsers(help='subcommands')
    parser_command = subparsers.add_parser("runserver")
    parser_command.set_defaults(func=runserver)

    args = parser.parse_args()
    config = default_config(args.config)
    args.func(args, config)

if __name__ == '__main__':
    main()
