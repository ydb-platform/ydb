# -*- coding: utf-8 -*-
import json
import logging

from six.moves.urllib.request import urlopen
from six.moves.urllib.parse import urlencode

logger = logging.getLogger()
TIMEOUT = 120
DEFAULT_HIVE_ID = 72057594037968897


class HiveClient(object):

    def __init__(self, server, mon_port, hive_id=DEFAULT_HIVE_ID):
        self.__hive_id = hive_id
        self.__url = 'http://{server}:{mon_port}/tablets/app?TabletID={hive_id}'.format(
            server=server,
            mon_port=mon_port,
            hive_id=self.__hive_id
        )

    def __get(self, url, timeout):
        u = urlopen(url, timeout=TIMEOUT)
        u.read()

    def rebalance_all_tablets(self):
        self.__get(
            self.__url + '&page=Rebalance',
            timeout=TIMEOUT
        )

    def change_tablet_group(self, tablet_id, channels=()):
        url_lst = [
            self.__url,
            'page=ReassignTablet',
            'tablet={tablet_id}'.format(tablet_id=tablet_id)
        ]
        if channels:
            url_lst.append('channel=' + ','.join(map(str, channels)))

        self.__get('&'.join(url_lst), timeout=TIMEOUT)

    def change_tablet_group_by_tablet_type(self, tablet_type, percent=None, channels=()):
        url_lst = [
            self.__url,
            'page=ReassignTablet',
            'type={tablet_type}'.format(tablet_type=int(tablet_type))
        ]
        if percent is not None:
            url_lst.append(
                'percent={percent}'.format(percent=percent)
            )
        if channels:
            url_lst.append('channel=' + ','.join(map(str, channels)))
        self.__get('&'.join(url_lst), timeout=TIMEOUT)

    def kick_tablets_from_node(self, node_id):
        self.__get(
            self.__url + "&page=KickNode&node={node}".format(node=node_id),
            timeout=TIMEOUT
        )

    def block_node(self, node_id, block=True):
        block = '1' if block else '0'
        self.__get(
            self.__url + "&page=SetDown&node={node}&down={down}".format(node=node_id, down=block),
            timeout=TIMEOUT
        )

    def unblock_node(self, node_id):
        self.block_node(node_id, block=False)

    def change_tablet_weight(self, tablet_id, tablet_weight):
        self.__get(
            self.__url + "&page=UpdateResources&tablet={tablet}&kv={size}".format(
                tablet=tablet_id,
                size=tablet_weight
            ),
            timeout=TIMEOUT
        )

    def change_tablet_cpu_usage(self, tablet_id, cpu_usage):
        self.__get(
            self.__url + "&page=UpdateResources&tablet={tablet}&cpu={size}".format(
                tablet=tablet_id,
                size=cpu_usage
            ),
            timeout=TIMEOUT
        )

    def set_min_scatter_to_balance(self, min_scatter_to_balance=101):
        """
        min_scatter_to_balance=101 -- effectively disables auto rebalancing
        """
        self.__get(
            self.__url + "&page=Settings&minScatterToBalance={min_scatter_to_balance}".format(
                min_scatter_to_balance=min_scatter_to_balance), timeout=TIMEOUT
        )

    def set_max_scheduled_tablets(self, max_scheduled_tablets=10):
        self.__get(
            self.__url + "&page=Settings&maxScheduledTablets={max_scheduled_tablets}".format(
                max_scheduled_tablets=max_scheduled_tablets
            ),
            timeout=TIMEOUT
        )

    def set_max_boot_batch_size(self, max_boot_batch_size=10):
        self.__get(
            self.__url + "&page=Settings&maxBootBatchSize={max_boot_batch_size}".format(
                max_boot_batch_size=max_boot_batch_size
            ),
            timeout=TIMEOUT
        )


class SwaggerClient(object):
    def __init__(self, host, port, hive_id=DEFAULT_HIVE_ID):
        super(SwaggerClient, self).__init__()
        self.__host = host
        self.__port = port
        self.__timeout = 10 * 1000
        self.__hive_id = hive_id

        self.__debug_enabled = logger.isEnabledFor(logging.DEBUG)
        self.__url = 'http://{server}:{port}/viewer'.format(
            server=host,
            port=port
        )

    def __get(self, url, timeout):
        u = urlopen(url, timeout=TIMEOUT)
        return json.load(u)

    def __http_get_and_parse_json(self, path, **kwargs):
        params = urlencode(kwargs)
        url = self.__url + path + "?" + params
        response = self.__get(url, timeout=TIMEOUT)
        return response

    def hive_info_by_tablet_id(self, tablet_id):
        return self.__hive_info(tablet_id=tablet_id)

    def hive_info_by_tablet_type(self, tablet_type):
        return self.__hive_info(tablet_type=int(tablet_type))

    def hive_info_all(self):
        return self.__hive_info(followers="true")

    def __hive_info(self, **kwargs):
        return self.__http_get_and_parse_json(
            "/json/hiveinfo", hive_id=self.__hive_id, timeout=self.__timeout, **kwargs
        )

    def pdisk_info(self, node_id):
        return self.__http_get_and_parse_json(
            "/json/pdiskinfo", node_id=str(node_id), enums='true', timeout=self.__timeout
        )

    def vdisk_info(self):
        return self.__http_get_and_parse_json(
            "/json/vdiskinfo", timeout=self.__timeout
        )

    def tablet_info(self, tablet_type=None):
        if tablet_type is not None:
            return self.__http_get_and_parse_json(
                "/json/tabletinfo", timeout=self.__timeout, filter='Type={}'.format(tablet_type)
            )
        else:
            return self.__http_get_and_parse_json(
                "/json/tabletinfo", timeout=self.__timeout
            )

    def nodes_info(self):
        return self.__http_get_and_parse_json(
            "/json/nodes", timeout=self.__timeout
        )
