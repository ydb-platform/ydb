import re
from collections import namedtuple

from ppadb.plugins import Plugin

State = namedtuple("TrafficState", [
    'idx',
    'iface',
    'acct_tag_hex',
    'uid_tag_int',
    'cnt_set',
    'rx_bytes',
    'rx_packets',
    'tx_bytes',
    'tx_packets',
    'rx_tcp_bytes',
    'rx_tcp_packets',
    'rx_udp_bytes',
    'rx_udp_packets',
    'rx_other_bytes',
    'rx_other_packets',
    'tx_tcp_bytes',
    'tx_tcp_packets',
    'tx_udp_bytes',
    'tx_udp_packets',
    'tx_other_bytes',
    'tx_other_packets',
])


class Traffic(Plugin):
    def get_traffic(self, package_name):
        cmd = 'dumpsys package {} | grep userId'.format(package_name)
        result = self.shell(cmd).strip()

        pattern = "userId=([\d]+)"

        if result:
            match = re.search(pattern, result)
            uid = match.group(1)
        else:
            # This package is not existing
            return None

        cmd = 'cat /proc/net/xt_qtaguid/stats | grep {}'.format(uid)
        result = self.shell(cmd)

        def convert(token):
            if token.isdigit():
                return int(token)
            else:
                return token

        states = []
        if result:
            for line in result.strip().split('\n'):
                values = map(convert, line.split())
                states.append(State(*values))

            return states
        else:
            return None
