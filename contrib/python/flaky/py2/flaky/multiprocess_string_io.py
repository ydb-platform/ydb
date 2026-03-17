# coding: utf-8

from __future__ import unicode_literals

import multiprocessing


class MultiprocessingStringIO(object):
    """
    Provide a StringIO-like interface to the multiprocessing ListProxy. The
    multiprocessing ListProxy needs to be instantiated before the flaky plugin
    is configured, so the list is created as a class variable.
    """

    _manager = multiprocessing.Manager()
    proxy = _manager.list()  # pylint:disable=no-member

    def getvalue(self):
        """
        Shadow the StringIO.getvalue method.
        """
        return ''.join(i for i in self.proxy)

    def writelines(self, content_list):
        """
        Shadow the StringIO.writelines method. Ingests a list and
        translates that to a string
        """

        for item in content_list:
            self.write(item)

    def write(self, content):
        """
        Shadow the StringIO.write method.
        """
        content.strip('\n')
        self.proxy.append(content)
