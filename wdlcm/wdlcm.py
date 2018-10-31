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

def find(configuration, cell, selector, read_token):
    """
    This function will find gts matching the specified selector.
    """
    result = set()

    response = requests.get(configuration[cell]['find_endpoint'],
                            headers={'X-Warp10-Token': read_token},
                            params={'selector': selector,
                                    'sortmeta': 'true',
                                    'showattr': 'true',
                                    'format': 'fulltext'},
                            stream=True)
    response.raise_for_status()
    for line in response.iter_lines():
        result.add(str(line, 'utf-8'))

    return result


def fetch(configuration, cell, selector, read_token):
    """
    This function will fetch gts matching the specified selector
    with at least 1 point.
    """
    result = set()

    response = requests.get(configuration[cell]['fetch_endpoint'],
                            headers={'X-Warp10-Token': read_token},
                            params={'selector': selector,
                                    'now': '9223372036854775807',
                                    'timespan': '-1',
                                    'sortmeta': 'true',
                                    'showattr': 'true',
                                    'format': 'fulltext'},
                            stream=True)
    response.raise_for_status()
    for line in response.iter_lines():
        result.add(str(line.split()[1], 'utf-8'))

    return result


def delete_older(configuration, cell, selector, write_token, instant):
    """
    This function will delete any datapoint in a serie matching
    the specified selector and older than the specified instant.
    """
    response = requests.get(configuration[cell]['delete_endpoint'],
                            headers={'X-Warp10-Token': write_token},
                            params={'selector': selector,
                                    'end': instant,
                                    'start': '-9223372036854775808'})
    response.raise_for_status()

    return response.text


def delete_all(configuration, cell, selector, write_token):
    """
    This function will delete any serie matching the specified
    selector.
    """
    response = requests.get(configuration[cell]['delete_endpoint'],
                            headers={'X-Warp10-Token': write_token},
                            params={'selector': selector, 'deleteall': 'true'})
    response.raise_for_status()

    return response.text


def mark_empty(configuration, cell, selector, read_token, write_token):
    """
    This function will mark with an attribute any empty gts matching
    the specified selector.
    """
    find_result = find(configuration, cell, selector, read_token)
    fetch_result = fetch(configuration, cell, selector, read_token)

    diff = find_result.difference(fetch_result)

    meta_orders = []
    for entry in diff:
        meta_orders.append(re.sub(r'{[^{}]*}$', '{wdlcm=empty}', entry))

    response = requests.post(configuration[cell]['meta_endpoint'],
                             headers={'X-Warp10-Token': write_token},
                             data='\n'.join(meta_orders))
    response.raise_for_status()

    return

def delete_empty(configuration, cell, read_token, write_token):
    """
    This function will delete any gts marked as empty.
    """
    selector = '~.*{wdlcm=empty}'
    # Find to check if empty series are really empty
    fetch_result = fetch(configuration, cell, selector, read_token)
    if fetch_result:
        raise requests.exceptions.RequestException("""
                               Failed to delete series marked as empty
                               as some of them still contains datapoints.
                               """)

    return delete_all(configuration, cell, selector, write_token)

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

        try:
            if arguments[0] == 'delete_all':
                delete_all(configuration, arguments[1], arguments[2], arguments[3])
            elif arguments[0] == 'delete_older':
                delete_older(configuration, arguments[1], arguments[2], arguments[3], arguments[4])
            elif arguments[0] == 'mark_empty':
                mark_empty(configuration, arguments[1], arguments[2], arguments[3], arguments[4])
            elif arguments[0] == 'delete_empty':
                delete_empty(configuration, arguments[1], arguments[2], arguments[3])
            else:
                print('invalid commande: {}'.format(arguments[0]))
        except requests.exceptions.RequestException as exception:
            print(exception)
            sys.exit(1)

if __name__ == '__main__':
    launch()
