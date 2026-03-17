#!/usr/bin/env python

import sys, os
from argparse import ArgumentParser
from .utility import TcpUtility, UtilityException


def checkCorrectAddress(address):
    try:
        host, port = address.rsplit(':', 1)
        port = int(port)
        assert (port > 0 and port < 65536)
        return True
    except:
        return False


def executeAdminCommand(args):
    parser = ArgumentParser()
    parser.add_argument('-conn', action='store', dest='connection', help='address to connect')
    parser.add_argument('-pass', action='store', dest='password', help='cluster\'s password')
    parser.add_argument('-status', action='store_true', help='send command \'status\'')
    parser.add_argument('-add', action='store', dest='add', help='send command \'add\'')
    parser.add_argument('-remove', action='store', dest='remove', help='send command \'remove\'')
    parser.add_argument('-set_version', action='store', dest='version', type=int, help='set cluster code version')

    data = parser.parse_args(args)
    if not checkCorrectAddress(data.connection):
        return 'invalid address to connect'

    if data.status:
        message = ['status']
    elif data.add:
        if not checkCorrectAddress(data.add):
            return 'invalid address to command add'
        message = ['add', data.add]
    elif data.remove:
        if not checkCorrectAddress(data.remove):
            return 'invalid address to command remove'
        message = ['remove', data.remove]
    elif data.version is not None:
        message = ['set_version', data.version]
    else:
        return 'invalid command'

    util = TcpUtility(data.password)
    try:
        result = util.executeCommand(data.connection, message)
    except UtilityException as e:
        return str(e)

    if isinstance(result, str):
        return result
    if isinstance(result, dict):
        return '\n'.join('%s: %s' % (k, v) for k, v in sorted(result.items()))
    return str(result)


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    result = executeAdminCommand(args)
    sys.stdout.write(result)
    sys.stdout.write(os.linesep)


if __name__ == '__main__':
    main()
