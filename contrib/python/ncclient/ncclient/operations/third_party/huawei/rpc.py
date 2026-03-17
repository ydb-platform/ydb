# -*- coding: utf-8 -*-
# by yangxufeng.zhao

from ncclient.xml_ import *

from ncclient.operations.rpc import RPC


class CLI(RPC):
    def request(self, command=None):
        """command text
        view: Execution user view exec
              Configuration system view exec
        """
        # node = new_ele("execute-cli")
        node = new_ele("execute-cli", attrs={"xmlns":HW_PRIVATE_NS})
        node.append(validated_element(command))
        return self._request(node)


class Action(RPC):
    "`execute-action` RPC"
    def request(self, action=None):
        node = new_ele("execute-action", attrs={"xmlns":HW_PRIVATE_NS})
        node.append(validated_element(action))
        return self._request(node)
