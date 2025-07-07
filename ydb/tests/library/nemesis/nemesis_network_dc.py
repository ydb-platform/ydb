# -*- coding: utf-8 -*-

import random
import time
import collections

from ydb.tests.library.nemesis.nemesis_core import Nemesis, Schedule
from ydb.tests.tools.nemesis.library import base


def validate_multiple_datacenters(cluster, min_datacenters=2):
    yaml_config = cluster._ExternalKiKiMRCluster__yaml_config
    hosts_config = yaml_config.get('hosts', [])

    dc_to_nodes = collections.defaultdict(list)
    for host_config in hosts_config:
        host_name = host_config.get('name', host_config.get('host'))
        location = host_config.get('location', {})
        data_center = location.get('data_center', 'unknown')
        if data_center != 'unknown':
            dc_to_nodes[data_center].append(host_name)

    data_centers = list(dc_to_nodes.keys())
    if len(data_centers) < min_datacenters:
        return None, None
    return dc_to_nodes, data_centers


class DataCenterNetworkNemesis(Nemesis, base.AbstractMonitoredNemesis):
    def __init__(self, cluster, schedule=(300, 600), stop_duration=60):
        super(DataCenterNetworkNemesis, self).__init__(schedule=schedule)
        base.AbstractMonitoredNemesis.__init__(self, scope='datacenter')

        self._cluster = cluster
        self._stop_duration = stop_duration
        self._current_dc = None
        self._stopped_nodes = []
        self._dc_cycle_iterator = None
        self._stop_time = None
        self._dc_to_nodes = collections.defaultdict(list)
        self._data_centers = []

        self._restore_schedule = Schedule.from_tuple_or_int(stop_duration)

    def next_schedule(self):
        if self._stopped_nodes:
            return next(self._restore_schedule)
        return super(DataCenterNetworkNemesis, self).next_schedule()

    def prepare_state(self):
        self.logger.info("Preparing DataCenterNetworkNemesis state...")

        dc_to_nodes, data_centers = validate_multiple_datacenters(self._cluster, min_datacenters=2)
        if dc_to_nodes is None or data_centers is None:
            self.logger.warning("Found insufficient data centers. DataCenter nemesis requires multiple DCs.")
            return

        self._dc_to_nodes = dc_to_nodes
        self._data_centers = data_centers

        self.logger.info("Parsing cluster configuration with %d hosts in %d data centers",
                         sum(len(hosts) for hosts in self._dc_to_nodes.values()), len(self._data_centers))
        self.logger.info("Found %d data centers: %s",
                         len(self._data_centers),
                         list(self._data_centers))

        for dc in self._data_centers:
            self.logger.info("Data center '%s' has %d hosts: %s",
                             dc, len(self._dc_to_nodes[dc]), self._dc_to_nodes[dc])
        self._dc_cycle_iterator = self._create_dc_cycle()

    def _create_dc_cycle(self):
        while True:
            for dc in self._data_centers:
                yield dc

    def inject_fault(self):
        if self._stopped_nodes:
            self._check_and_restore_nodes()
            return

        if self._dc_cycle_iterator is None:
            self._dc_cycle_iterator = self._create_dc_cycle()

        self._current_dc = next(self._dc_cycle_iterator)
        self.logger.info("Starting fault injection in data center: %s", self._current_dc)

        self._stop_datacenter_services(self._current_dc)

        if self._stopped_nodes:
            self._stop_time = time.time()
            self.on_success_inject_fault()

    def _stop_datacenter_services(self, datacenter):
        dc_hosts = self._dc_to_nodes.get(datacenter, [])

        if not dc_hosts:
            self.logger.warning("No hosts found in data center: %s", datacenter)
            return

        self.logger.info("Stopping services in data center '%s' on %d hosts: %s",
                         datacenter, len(dc_hosts), dc_hosts)

        stopped_count = 0

        for node_id, node in self._cluster.nodes.items():
            if node.host in dc_hosts:
                try:
                    self.logger.info("Stopping node %d (%s) in DC %s",
                                     node_id, node.host, datacenter)
                    node.stop()
                    self._stopped_nodes.append((node_id, node))
                    stopped_count += 1

                except Exception as e:
                    self.logger.error("Failed to stop node %d (%s): %s",
                                      node_id, node.host, str(e))

        self.logger.info("Successfully stopped %d nodes in data center '%s'",
                         stopped_count, datacenter)

    def _check_and_restore_nodes(self):
        if not self._stop_time:
            return

        elapsed_time = time.time() - self._stop_time
        if elapsed_time >= self._stop_duration:
            self.logger.info("Stop duration (%d seconds) elapsed. Restoring services in DC '%s'",
                             self._stop_duration, self._current_dc)
            self.extract_fault()

    def extract_fault(self):
        if not self._stopped_nodes:
            return False

        self.logger.info("Restoring %d stopped nodes in data center '%s'",
                         len(self._stopped_nodes), self._current_dc)

        restored_count = 0
        for node_id, node in self._stopped_nodes:
            try:
                self.logger.info("Starting node %d (%s) in DC %s",
                                 node_id, node.host, self._current_dc)
                node.start()
                restored_count += 1

            except Exception as e:
                self.logger.error("Failed to start node %d (%s): %s",
                                  node_id, node.host, str(e))

        self.logger.info("Successfully restored %d nodes in data center '%s'",
                         restored_count, self._current_dc)

        self._stopped_nodes = []
        self._stop_time = None
        self._current_dc = None

        return True


class SingleDataCenterFailureNemesis(DataCenterNetworkNemesis):
    def __init__(self, cluster, schedule=(1200, 2400), stop_duration=3600):
        super(SingleDataCenterFailureNemesis, self).__init__(
            cluster, schedule=schedule, stop_duration=stop_duration)

    def _create_dc_cycle(self):
        while True:
            yield random.choice(self._data_centers)


class DataCenterRouteUnreachableNemesis(Nemesis, base.AbstractMonitoredNemesis):
    def __init__(self, cluster, schedule=(1800, 3600), block_duration=120):
        super(DataCenterRouteUnreachableNemesis, self).__init__(schedule=schedule)
        base.AbstractMonitoredNemesis.__init__(self, scope='datacenter_routes')

        self._cluster = cluster
        self._block_duration = block_duration
        self._current_dc = None
        self._blocked_routes = []  # Список заблокированных маршрутов для восстановления
        self._dc_cycle_iterator = None
        self._block_time = None
        self._dc_to_nodes = collections.defaultdict(list)
        self._data_centers = []

        self._restore_schedule = Schedule.from_tuple_or_int(block_duration)

    def next_schedule(self):
        if self._blocked_routes:
            return next(self._restore_schedule)
        return super(DataCenterRouteUnreachableNemesis, self).next_schedule()

    def prepare_state(self):
        self.logger.info("Preparing DataCenterRouteUnreachableNemesis state...")

        # Используем общую функцию валидации
        dc_to_nodes, data_centers = validate_multiple_datacenters(self._cluster, min_datacenters=2)

        if dc_to_nodes is None or data_centers is None:
            self.logger.warning("Found insufficient data centers. Route unreachable nemesis requires multiple DCs.")
            return

        self._dc_to_nodes = dc_to_nodes
        self._data_centers = data_centers

        self.logger.info("Parsing cluster configuration with %d hosts in %d data centers",
                         sum(len(hosts) for hosts in self._dc_to_nodes.values()), len(self._data_centers))

        self.logger.info("Found %d data centers: %s",
                         len(self._data_centers),
                         list(self._data_centers))

        for dc in self._data_centers:
            self.logger.info("Data center '%s' has %d hosts: %s",
                             dc, len(self._dc_to_nodes[dc]), self._dc_to_nodes[dc])

        # Создаем циклический итератор по ДЦ
        self._dc_cycle_iterator = self._create_dc_cycle()

    def _create_dc_cycle(self):
        while True:
            for dc in self._data_centers:
                yield dc

    def inject_fault(self):
        # Если есть заблокированные маршруты, проверяем не пора ли их восстановить
        if self._blocked_routes:
            self._check_and_restore_routes()
            return

        # Если нет доступных ДЦ для тестирования
        if len(self._data_centers) <= 1:
            self.logger.info("Cannot inject fault. Need at least 2 data centers, found %d",
                             len(self._data_centers))
            return

        # Выбираем следующий ДЦ как current
        if self._dc_cycle_iterator is None:
            self._dc_cycle_iterator = self._create_dc_cycle()

        self._current_dc = next(self._dc_cycle_iterator)
        self.logger.info("Starting route blocking for current DC: %s", self._current_dc)

        # Блокируем маршруты к current ДЦ на всех хостах other ДЦ
        self._block_routes_to_current_dc()

        if self._blocked_routes:
            self._block_time = time.time()
            self.on_success_inject_fault()

    def _block_routes_to_current_dc(self):
        current_dc_hosts = self._dc_to_nodes.get(self._current_dc, [])

        if not current_dc_hosts:
            self.logger.warning("No hosts found in current data center: %s", self._current_dc)
            return

        # Получаем все other ДЦ (все кроме current)
        other_dcs = [dc for dc in self._data_centers if dc != self._current_dc]

        self.logger.info("Blocking routes from other DCs %s to current DC '%s' hosts: %s",
                         other_dcs, self._current_dc, current_dc_hosts)

        # Создаем temporary файл со списком IP current ДЦ
        current_dc_ips_content = '\n'.join(current_dc_hosts)

        for other_dc in other_dcs:
            other_dc_hosts = self._dc_to_nodes.get(other_dc, [])

            for other_host in other_dc_hosts:
                try:
                    # Ищем соответствующую ноду в кластере
                    target_node = self._find_node_by_host(other_host)
                    if not target_node:
                        self.logger.warning("Node not found for host: %s", other_host)
                        continue

                    self.logger.info("Blocking routes on host %s (other DC: %s) to current DC %s",
                                     other_host, other_dc, self._current_dc)

                    # Создаем временный файл с IP адресами current DC на other хосте
                    temp_file = '/tmp/blocked_ips_{}_{}_{}'.format(
                        self._current_dc, other_dc, int(time.time())
                    )

                    # Записываем IP в файл
                    create_file_cmd = 'echo "{}" | sudo tee {}'.format(current_dc_ips_content, temp_file)
                    target_node.ssh_command(create_file_cmd)

                    # Блокируем маршруты к current ДЦ
                    block_cmd = 'for i in `cat {}`; do sudo /usr/bin/ip -6 ro add unreach ${{i}} || sudo /usr/bin/ip ro add unreachable ${{i}}; done'.format(temp_file)
                    target_node.ssh_command(block_cmd, raise_on_error=False)  # Не падаем если маршрут уже есть

                    # Сохраняем информацию для восстановления
                    self._blocked_routes.append({
                        'node': target_node,
                        'host': other_host,
                        'temp_file': temp_file,
                        'other_dc': other_dc
                    })

                except Exception as e:
                    self.logger.error("Failed to block routes on host %s: %s", other_host, str(e))

        self.logger.info("Successfully blocked routes for %d other DC hosts to current DC '%s'",
                         len(self._blocked_routes), self._current_dc)

    def _find_node_by_host(self, hostname):
        for node in self._cluster.nodes.values():
            if node.host == hostname:
                return node
        return None

    def _check_and_restore_routes(self):
        if not self._block_time:
            return

        elapsed_time = time.time() - self._block_time

        if elapsed_time >= self._block_duration:
            self.logger.info("Block duration (%d seconds) elapsed. Restoring routes for DC '%s'",
                             self._block_duration, self._current_dc)
            self.extract_fault()

    def extract_fault(self):
        if not self._blocked_routes:
            return False

        self.logger.info("Restoring %d blocked routes for current DC '%s'",
                         len(self._blocked_routes), self._current_dc)

        restored_count = 0

        for route_info in self._blocked_routes:
            try:
                node = route_info['node']
                host = route_info['host']
                temp_file = route_info['temp_file']

                self.logger.info("Restoring routes on host %s to current DC %s", host, self._current_dc)

                # Выполняем команду восстановления маршрутов
                restore_cmd = 'for i in `cat {}`; do sudo /usr/bin/ip -6 ro del unreach ${{i}} || sudo /usr/bin/ip ro del unreachable ${{i}}; done'.format(temp_file)
                node.ssh_command(restore_cmd, raise_on_error=False)  # Не падаем если маршрутов уже нет

                # Удаляем временный файл
                cleanup_cmd = "sudo rm -f {}".format(temp_file)
                node.ssh_command(cleanup_cmd, raise_on_error=False)

                restored_count += 1

            except Exception as e:
                self.logger.error("Failed to restore routes on host %s: %s",
                                  route_info['host'], str(e))

        self.logger.info("Successfully restored routes for %d hosts in current DC '%s'",
                         restored_count, self._current_dc)

        # Очищаем состояние
        self._blocked_routes = []
        self._block_time = None
        self._current_dc = None

        return True


class DataCenterIptablesBlockPortsNemesis(Nemesis, base.AbstractMonitoredNemesis):
    def __init__(self, cluster, schedule=(1800, 3600), block_duration=120):
        super(DataCenterIptablesBlockPortsNemesis, self).__init__(schedule=schedule)
        base.AbstractMonitoredNemesis.__init__(self, scope='datacenter_iptables')

        self._cluster = cluster
        self._block_duration = block_duration
        self._current_dc = None
        self._blocked_hosts = []  # Список хостов с заблокированными портами
        self._dc_cycle_iterator = None
        self._block_time = None
        self._restore_schedule = Schedule.from_tuple_or_int(block_duration)

        # Инициализируем состояние
        self._dc_to_nodes = {}
        self._data_centers = []

        # Команды для iptables
        self._block_ports_cmd = (
            "sudo /sbin/ip6tables -w -A YDB_FW -p tcp -m multiport "
            "--ports 2135,2136,8765,19001,31000:32000 -j REJECT"
        )
        self._restore_ports_cmd = "sudo ip6tables --flush YDB_FW"

    def next_schedule(self):
        if self._blocked_hosts:
            return next(self._restore_schedule)
        return super(DataCenterIptablesBlockPortsNemesis, self).next_schedule()

    def prepare_state(self):
        self.logger.info("Preparing DataCenterIptablesBlockPortsNemesis state...")

        # Используем общую функцию валидации
        dc_to_nodes, data_centers = validate_multiple_datacenters(self._cluster, min_datacenters=2)

        if dc_to_nodes is None or data_centers is None:
            self.logger.warning("Found insufficient data centers. Iptables ports nemesis requires multiple DCs.")
            return

        self._dc_to_nodes = dc_to_nodes
        self._data_centers = data_centers

        self.logger.info("Parsing cluster configuration with %d hosts in %d data centers",
                         sum(len(hosts) for hosts in self._dc_to_nodes.values()), len(self._data_centers))

        self.logger.info("Found %d data centers: %s",
                         len(self._data_centers),
                         list(self._data_centers))

        for dc in self._data_centers:
            self.logger.info("Data center '%s' has %d hosts: %s",
                             dc, len(self._dc_to_nodes[dc]), self._dc_to_nodes[dc])

        # Создаем циклический итератор по ДЦ
        self._dc_cycle_iterator = self._create_dc_cycle()

    def _create_dc_cycle(self):
        while True:
            for dc in self._data_centers:
                yield dc

    def inject_fault(self):
        # Если есть заблокированные порты, проверяем не пора ли их восстановить
        if self._blocked_hosts:
            self._check_and_restore_ports()
            return

        # Если нет доступных ДЦ для тестирования
        if len(self._data_centers) <= 1:
            self.logger.info("Cannot inject fault. Need at least 2 data centers, found %d",
                             len(self._data_centers))
            return

        # Выбираем следующий ДЦ для блокировки портов
        if self._dc_cycle_iterator is None:
            self._dc_cycle_iterator = self._create_dc_cycle()

        self._current_dc = next(self._dc_cycle_iterator)
        self.logger.info("Starting iptables ports blocking in data center: %s", self._current_dc)

        # Блокируем порты на всех хостах в выбранном ДЦ
        self._block_ports_on_current_dc()

        if self._blocked_hosts:
            self._block_time = time.time()
            self.on_success_inject_fault()

    def _block_ports_on_current_dc(self):
        current_dc_hosts = self._dc_to_nodes.get(self._current_dc, [])

        if not current_dc_hosts:
            self.logger.warning("No hosts found in current data center: %s", self._current_dc)
            return

        self.logger.info("Blocking YDB ports on %d hosts in data center '%s'",
                         len(current_dc_hosts), self._current_dc)

        blocked_count = 0

        for hostname in current_dc_hosts:
            # Находим ноду соответствующую хосту
            node = self._find_node_by_host(hostname)
            if not node:
                self.logger.warning("Cannot find node for host %s", hostname)
                continue

            try:
                self.logger.info("Blocking YDB ports on host %s in DC %s", hostname, self._current_dc)

                # Выполняем команду блокировки портов
                result = node.ssh_command(self._block_ports_cmd)
                exit_code = result.exit_code if hasattr(result, 'exit_code') else result

                if exit_code == 0:
                    # Команда выполнена успешно
                    self._blocked_hosts.append({
                        'node': node,
                        'host': hostname,
                        'dc': self._current_dc
                    })
                    blocked_count += 1
                    self.logger.info("Successfully blocked YDB ports on host %s", hostname)
                else:
                    # Команда не выполнена - вероятно нет цепочки YDB_FW
                    self.logger.warning("Skip block iptables ports on host %s, because we don't see chain YDB_FW",
                                        hostname)

            except Exception as e:
                self.logger.error("Failed to block YDB ports on host %s: %s", hostname, str(e))

        self.logger.info("Successfully blocked YDB ports on %d/%d hosts in data center '%s'",
                         blocked_count, len(current_dc_hosts), self._current_dc)

    def _find_node_by_host(self, hostname):
        for node in self._cluster.nodes.values():
            if node.host == hostname:
                return node
        return None

    def _check_and_restore_ports(self):
        if not self._block_time:
            return

        elapsed_time = time.time() - self._block_time

        if elapsed_time >= self._block_duration:
            self.logger.info("Block duration (%d seconds) elapsed. Restoring YDB ports in DC '%s'",
                             self._block_duration, self._current_dc)
            self.extract_fault()

    def extract_fault(self):
        """
        Восстанавливает заблокированные порты через flush iptables цепочки.
        """
        if not self._blocked_hosts:
            return False

        self.logger.info("Restoring YDB ports on %d hosts in data center '%s'",
                         len(self._blocked_hosts), self._current_dc)

        restored_count = 0

        for host_info in self._blocked_hosts:
            node = host_info['node']
            hostname = host_info['host']

            try:
                self.logger.info("Restoring YDB ports on host %s", hostname)

                result = node.ssh_command(self._restore_ports_cmd)
                exit_code = result.exit_code if hasattr(result, 'exit_code') else result

                if exit_code == 0:
                    restored_count += 1
                    self.logger.info("Successfully restored YDB ports on host %s", hostname)
                else:
                    self.logger.error("Failed to restore YDB ports on host %s (exit code: %s)",
                                      hostname, exit_code)

            except Exception as e:
                self.logger.error("Failed to restore YDB ports on host %s: %s", hostname, str(e))

        self.logger.info("Successfully restored YDB ports on %d/%d hosts in data center '%s'",
                         restored_count, len(self._blocked_hosts), self._current_dc)

        # Очищаем состояние
        self._blocked_hosts = []
        self._block_time = None
        self._current_dc = None

        return True
