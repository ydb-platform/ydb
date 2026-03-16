# -*- coding: utf-8 -*-
# by yangxufeng.zhao

from ncclient.xml_ import *
from ncclient.operations import util

from ncclient.operations.rpc import RPC


class GetBulk(RPC):
    "The *get-bulk* RPC."

    def request(self, filter=None):
        """Retrieve running configuration and device state information.

        *filter* specifies the portion of the configuration to retrieve (by default entire configuration is retrieved)

        :seealso: :ref:`filter_params`
        """
        node = new_ele("get-bulk")
        if filter is not None:
            node.append(util.build_filter(filter))
        return self._request(node)


class GetBulkConfig(RPC):
    """The *get-bulk-config* RPC."""

    def request(self, source, filter=None):
        """Retrieve all or part of a specified configuration.

        *source* name of the configuration datastore being queried

        *filter* specifies the portion of the configuration to retrieve (by default entire configuration is retrieved)

        :seealso: :ref:`filter_params`"""
        node = new_ele("get-bulk-config")
        node.append(util.datastore_or_url("source", source, self._assert))
        if filter is not None:
            node.append(util.build_filter(filter))
        return self._request(node)


class CLI(RPC):
    def request(self, command=None):
        """command text
        view: Execution user view exec
              Configuration system view exec
        """
        node = new_ele("CLI")
        node.append(validated_element(command))
        # sub_ele(node, view).text = command
        return self._request(node)


class Action(RPC):
    def request(self, action=None):
        node = new_ele("action")
        node.append(validated_element(action))
        return self._request(node)

class Save(RPC):
    def request(self, file=None):
        node = new_ele('save')
        sub_ele(node, 'file').text = file
        return self._request(node)


class Load(RPC):
    def request(self, file=None):
        node = new_ele('load')
        sub_ele(node, 'file').text = file
        return self._request(node)


class Rollback(RPC):
    def request(self, file=None):
        node = new_ele('rollback')
        sub_ele(node, 'file').text = file
        return self._request(node)