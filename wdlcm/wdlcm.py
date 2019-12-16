#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#
#    Copyright 2018 Julien Girard
#
#    Licensed under the GNU GENERAL PUBLIC LICENSE, Version 3 ;
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#      http://http://www.gnu.org/licenses/gpl-3.0.html
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#

"""
Simple tool to manage Warp10 Data Life Cycle
"""

import sys
import argparse
import configparser
import re
import requests

def find(configuration, selector):
    """
    This function will find gts matching the specified selector.
    """
    response = requests.get(configuration['find_endpoint'],
                            headers={'X-Warp10-Token': configuration['read_token']},
                            params={'selector': selector,
                                    'sortmeta': 'true',
                                    'showattr': 'true',
                                    'format': 'fulltext'},
                            stream=True)
    response.raise_for_status()
    if response.encoding is None:
        response.encoding = 'utf-8'
    return response.iter_lines()


def fetch(configuration, selector):
    """
    This function will fetch gts matching the specified selector
    with at least 1 point.
    """
    response = requests.get(configuration['fetch_endpoint'],
                            headers={'X-Warp10-Token': configuration['read_token']},
                            params={'selector': selector,
                                    'now': '9223372036854775807',
                                    'timespan': '-1',
                                    'sortmeta': 'true',
                                    'showattr': 'true',
                                    'format': 'fulltext'},
                            stream=True)
    response.raise_for_status()
    if response.encoding is None:
        response.encoding = 'utf-8'
    return response.iter_lines()


def delete_older(configuration, selector, instant):
    """
    This function will delete any datapoint in a serie matching
    the specified selector and older than the specified instant.
    """
    response = requests.get(configuration['delete_endpoint'],
                            headers={'X-Warp10-Token': configuration['write_token']},
                            params={'selector': selector,
                                    'end': instant,
                                    'start': '-9223372036854775808'},
                            stream=True)
    response.raise_for_status()
    if response.encoding is None:
        response.encoding = 'utf-8'
    return response.iter_lines()


def delete_all(configuration, selector):
    """
    This function will delete any serie matching the specified
    selector.
    """
    response = requests.get(configuration['delete_endpoint'],
                            headers={'X-Warp10-Token': configuration['write_token']},
                            params={'selector': selector, 'deleteall': 'true'},
                            stream=True)
    response.raise_for_status()
    if response.encoding is None:
        response.encoding = 'utf-8'
    return response.iter_lines()


def mark_empty(configuration, selector):
    """
    This function will mark with an attribute any empty gts matching
    the specified selector.
    """
    empty = set()

    find_it = find(configuration, selector)
    for line in find_it:
        empty.add(str(line, 'utf-8'))

    fetch_it = fetch(configuration, selector)
    for line in fetch_it:
        empty.remove(str(line.split()[1], 'utf-8'))

    meta = []
    for entry in empty:
        meta.append(re.sub(r'{[^{}]*}$', '{wdlcm=empty}', entry))

    response = requests.post(configuration['meta_endpoint'],
                             headers={'X-Warp10-Token': configuration['write_token']},
                             data='\n'.join(meta))
    response.raise_for_status()
    if response.encoding is None:
        response.encoding = 'utf-8'
    return response.iter_lines()

def delete_empty(configuration):
    """
    This function will delete any gts marked as empty.
    """
    selector = '~.*{wdlcm=empty}'
    # Find to check if empty series are really empty
    fetch_it = fetch(configuration, selector)
    for _ in fetch_it:
        raise requests.exceptions.RequestException("""
                               Failed to delete series marked as empty
                               as some of them still contains datapoints.
                               """)

    return delete_all(configuration, selector)

def launch():
    """
    Function that wrap the main one to be used with when testing and as package
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Simple tool to manage Warp10 Data Life Cycle.')
    parser.add_argument(
        '-c', '--configuration',
        help='Path to the configuration file.',
        dest='configuration_file_path',
        )
    args = parser.parse_args()

    # Parse configuration file.
    configuration = configparser.ConfigParser()
    configuration['DEFAULT'] = {
        'find_endpoint':   'http://127.0.0.1:8080/api/v0/find',
        'fetch_endpoint':  'http://127.0.0.1:8080/api/v0/fetch',
        'update_endpoint': 'http://127.0.0.1:8080/api/v0/update',
        'delete_endpoint': 'http://127.0.0.1:8080/api/v0/delete',
        'meta_endpoint':   'http://127.0.0.1:8080/api/v0/meta'
        }
    if args.configuration_file_path:
        configuration.read(args.configuration_file_path)

    print('-- Please write a command down (Ctrl+D to quit).')
    for line in sys.stdin:
        print('-- Please write a command down (Ctrl+D to quit).')
        arguments = line.split()

        if len(arguments) < 2:
            print('Not enougth arguments.')
            continue

        command = arguments[0]
        application = arguments[1]
        selector = '~.*{}'
        instant = '0'

        if len(arguments) >= 3:
            selector = arguments[2]
        if len(arguments) >= 4:
            instant = arguments[3]

        if application not in configuration:
            print('Application: {} not found.'.format(application))
            continue

        print('command: {}'.format(command))
        print('application: {}'.format(application))
        print('selector: {}'.format(selector))
        print('instant: {}'.format(instant))

        try:
            result_it = None
            if command == 'find':
                result_it = find(configuration[application], selector)
            elif command == 'delete_all':
                result_it = delete_all(configuration[application], selector)
            elif command == 'delete_older':
                result_it = delete_older(configuration[application], selector, instant)
            elif command == 'mark_empty':
                result_it = mark_empty(configuration[application], selector)
            elif command == 'delete_empty':
                result_it = delete_empty(configuration[application])
            else:
                print('invalid commande: {}'.format(arguments[0]))
                continue

            for result in result_it:
                print(result)

        except requests.exceptions.RequestException as exception:
            print(exception)
            sys.exit(1)

if __name__ == '__main__':
    launch()
