# coding: utf-8

version_info = (0, 4, 15)
version = '.'.join([str(x) if isinstance(x, int) else '.' + x + '.'
                    for x in version_info]).replace('..', '')

from _ordereddict import ordereddict, sorteddict
