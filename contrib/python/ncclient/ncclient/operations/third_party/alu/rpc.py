# by Pasi Sikala <pasi.sikala@elisa.fi>

from ncclient.xml_ import *

from ncclient.operations.rpc import RPC
from ncclient.operations import util


class ShowCLI(RPC):
    def request(self, command=None):
        """Run CLI -show commands
        *command* (show) command to run

        """
        node = new_ele('get')
        filter = sub_ele(node, 'filter')
        block = sub_ele(filter, 'oper-data-format-cli-block')
        sub_ele(block, 'cli-show').text = command

        return self._request(node)


class GetConfiguration(RPC):

    def request(self, content='xml', filter=None, detail=False):
        """Get config from Alu router
        *content*   Content layer. cli or xml
        *filter*    specifies the portion of the configuration to retrieve (by default entire configuration is retrieved)
        *detail*    Show detailed config in CLI -layer"""

        node = new_ele('get-config')
        node.append(util.datastore_or_url('source', 'running', self._assert))

        if filter is not None:

            if content == 'xml':
                node.append(util.build_filter(('subtree', filter)))

            elif content == 'cli':
                rep = new_ele('filter')
                sub_filter = sub_ele(rep, 'config-format-cli-block')

                if filter is not None:
                    for item in filter:
                        if detail:
                            sub_ele(sub_filter, 'cli-info-detail').text = item
                        else:
                            sub_ele(sub_filter, 'cli-info').text = item
                else:
                    if detail:
                        sub_ele(sub_filter, 'cli-info-detail')
                    else:
                        sub_ele(sub_filter, 'cli-info')

                node.append(validated_element(rep))

        return self._request(node)


class LoadConfiguration(RPC):

    def request(self, format='xml', default_operation=None, target='running', config=None):
        node = new_ele('edit-config')
        config_node = new_ele('config')

        if default_operation is not None:
            # TODO: check if it is a valid default-operation
            sub_ele(node, "default-operation").text = default_operation

        if config is not None:
            if format == 'xml':
                node.append(util.datastore_or_url('target', target, self._assert))
                config_node.append(config)

            if format == 'cli':
                node.append(util.datastore_or_url('target', 'running', self._assert))
                sub_ele(config_node, 'config-format-cli-block').text = config

            node.append(config_node)
        return self._request(node)
