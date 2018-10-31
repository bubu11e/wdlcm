#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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

    r = requests.get(configuration[cell]['find_endpoint'],
                     headers={'X-Warp10-Token': read_token},
                     params={'selector': selector,
                             'sortmeta': 'true',
                             'showattr': 'true',
                             'format': 'fulltext'},
                     stream=True)
    r.raise_for_status()
    for line in r.iter_lines():
        result.add(str(line, 'utf-8'))

    return result


def fetch(configuration, cell, selector, read_token):
    """
    This function will fetch gts matching the specified selector
    with at least 1 point.
    """
    result = set()

    r = requests.get(configuration[cell]['fetch_endpoint'],
                     headers={'X-Warp10-Token': read_token},
                     params={'selector': selector,
                             'now': '9223372036854775807',
                             'timespan': '-1',
                             'sortmeta': 'true',
                             'showattr': 'true',
                             'format': 'fulltext'},
                     stream=True)
    r.raise_for_status()
    for line in r.iter_lines():
        result.add(str(line.split()[1], 'utf-8'))

    return result


def delete_older(configuration, cell, selector, write_token, instant):
    """
    This function will delete any datapoint in a serie matching
    the specified selector and older than the specified instant.
    """
    r = requests.get(configuration[cell]['delete_endpoint'],
                     headers={'X-Warp10-Token': write_token},
                     params={'selector': selector,
                             'end': 'instant',
                             'start': '-9223372036854775808'})
    r.raise_for_status()

    return r.text


def delete_all(configuration, cell, selector, write_token):
    """
    This function will delete any serie matching the specified
    selector.
    """
    r = requests.get(configuration[cell]['delete_endpoint'],
                     headers={'X-Warp10-Token': write_token},
                     params={'selector': selector, 'deleteall': 'true'})
    r.raise_for_status()

    return r.text


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

    r = requests.post(configuration[cell]['meta_endpoint'],
                      headers={'X-Warp10-Token': write_token},
                      data='\n'.join(meta_orders))
    r.raise_for_status()

    return

def delete_empty(configuration, cell, read_token, write_token):
    """
    This function will delete any gts marked as empty.
    """
    selector = '~.*{wdlcm=empty}'
    # Find to check if empty series are really empty
    fetch_result = fetch(configuration, cell, selector, read_token)
    if len(fetch_result) != 0:
        print('failed to delete series marked as empty as the following still contains datapoints.')
        print(fetch_result)
        sys.exit(1)

    return delete_all(configuration, cell, selector, write_token)

if __name__ == '__main__':
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

    configuration.read(args.configuration_file_path)

    for line in sys.stdin:
        arguments = line.split()

        if arguments[0] == 'delete_all':
            try:
                delete_all(configuration, arguments[1], arguments[2], arguments[3])
            except Exception as e:
                print(e)
                sys.exit(1)
        elif arguments[0] == 'delete_older':
            print('toto')
        elif arguments[0] == 'mark_empty':
            print('toto')
        elif arguments[0] == 'mark_empty':
            print('toto')
        else:
            print('invalid commande: {}'.format(arguments[0]))
