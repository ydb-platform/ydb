from ncclient.xml_ import *

from ncclient.operations.rpc import RPC
from ncclient.operations.rpc import RPCReply
from ncclient.operations.rpc import RPCError
from ncclient import NCClientError
import math


class GetConfiguration(RPC):
    def request(self, format='xml', filter=None):
        node = new_ele('get-configuration', {'format':format})
        if filter is not None:
            node.append(filter)
        if format !='xml':
            # The entire config comes as a single text element and this requires huge_tree support for large configs
            self._huge_tree = True
        return self._request(node)

class LoadConfiguration(RPC):
    def request(self, format='xml', action='merge',
            target='candidate', config=None):
        if config is not None:
            if type(config) == list:
                config = '\n'.join(config)
            if action == 'set':
                format = 'text'
            node = new_ele('load-configuration', {'action':action, 'format':format})
            if format == 'xml':
                config_node = sub_ele(node, 'configuration')
                config_node.append(config)
            if format == 'json':
                config_node = sub_ele(node, 'configuration-json').text = config
            if format == 'text' and not action == 'set':
                config_node = sub_ele(node, 'configuration-text').text = config
            if action == 'set' and format == 'text':
                config_node = sub_ele(node, 'configuration-set').text = config
            return self._request(node)


class CompareConfiguration(RPC):
    def request(self, rollback=0, format='text'):
        node = new_ele('get-configuration', {'compare':'rollback', 'format':format, 'rollback':str(rollback)})
        return self._request(node)


class ExecuteRpc(RPC):
    def request(self, rpc, filter_xml=None):
        if isinstance(rpc, str):
            rpc = to_ele(rpc)
        self._filter_xml = filter_xml
        return self._request(rpc)


class Command(RPC):
    def request(self, command=None, format='xml'):
        node = new_ele('command', {'format':format})
        node.text = command
        return self._request(node)


class Reboot(RPC):
    def request(self):
        node = new_ele('request-reboot')
        return self._request(node)


class Halt(RPC):
    def request(self):
        node = new_ele('request-halt')
        return self._request(node)


class Commit(RPC):
    "`commit` RPC. Depends on the `:candidate` capability, and the `:confirmed-commit`."

    DEPENDS = [':candidate']

    def request(self, confirmed=False, timeout=None, comment=None, synchronize=False, at_time=None, check=False):
        """Commit the candidate configuration as the device's new current configuration. Depends on the `:candidate` capability.

        A confirmed commit (i.e. if *confirmed* is `True`) is reverted if there is no followup commit within the *timeout* interval. If no timeout is specified the confirm timeout defaults to 600 seconds (10 minutes). A confirming commit may have the *confirmed* parameter but this is not required. Depends on the `:confirmed-commit` capability.

        *confirmed* whether this is a confirmed commit. Mutually exclusive with at_time.

        *timeout* specifies the confirm timeout in seconds

        *comment* a string to comment the commit with. Review on the device using 'show system commit'

        *synchronize* Whether we should synch this commit across both Routing Engines

        *at_time* Mutually exclusive with confirmed. The time at which the commit should happen. Junos expects either of these two formats:
            A time value of the form hh:mm[:ss] (hours, minutes, and, optionally, seconds)
            A date and time value of the form yyyy-mm-dd hh:mm[:ss] (year, month, date, hours, minutes, and, optionally, seconds)

        *check* Verify the syntactic correctness of the candidate configuration"""
        # NOTE: non netconf standard, Junos specific commit-configuration element, see
        # https://www.juniper.net/documentation/en_US/junos/topics/reference/tag-summary/junos-xml-protocol-commit-configuration.html
        node = new_ele_ns("commit-configuration", "")
        if confirmed and at_time is not None:
            raise NCClientError("'Commit confirmed' and 'commit at' are mutually exclusive.")
        if confirmed:
            self._assert(":confirmed-commit")
            sub_ele(node, "confirmed")
            if timeout is not None:
                # NOTE: For Junos, confirm-timeout has to be given in minutes:
                # https://www.juniper.net/documentation/en_US/junos/topics/reference/tag-summary/junos-xml-protocol-commit-configuration.html
                # but the netconf standard and ncclient library uses seconds:
                # https://tools.ietf.org/html/rfc6241#section-8.4.5
                # http://ncclient.readthedocs.io/en/latest/manager.html#ncclient.manager.Manager.commit
                # so need to convert the value in seconds to minutes.
                timeout_int = int(timeout) if isinstance(timeout, str) else timeout
                sub_ele(node, "confirm-timeout").text = str(int(math.ceil(timeout_int/60.0)))
        elif at_time is not None:
            sub_ele(node, "at-time").text = at_time
        if comment is not None:
            sub_ele(node, "log").text = comment
        if synchronize:
            sub_ele(node, "synchronize")
        if check:
            sub_ele(node, "check")
        return self._request(node)

class Rollback(RPC):
    def request(self, rollback=0):
        node = new_ele('load-configuration', {'rollback':str(rollback)})
        return self._request(node)
