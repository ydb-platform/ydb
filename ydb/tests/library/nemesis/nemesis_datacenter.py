# -*- coding: utf-8 -*-

import time
import collections

from ydb.tests.library.nemesis.nemesis_core import Nemesis, Schedule
from ydb.tests.tools.nemesis.library import base


class DataCenterNetworkNemesis(Nemesis, base.AbstractMonitoredNemesis):
    def __init__(self, cluster, schedule=(300, 900), duration=60):
        super(DataCenterNetworkNemesis, self).__init__(schedule=schedule)
        base.AbstractMonitoredNemesis.__init__(self, scope='datacenter')

        self._cluster = cluster
        self._duration = duration
        self._current_dc = None
        self._dc_cycle_iterator = None
        self._dc_to_nodes = None
        self._data_centers = None

        self._restore_schedule = Schedule.from_tuple_or_int(duration)

    def _validate_datacenters(self, min_datacenters=2):
        dc_to_nodes = collections.defaultdict(list)
        for node in self._cluster.nodes.values():
            self.logger.info("Node %s has datacenter %s", node.host, node.datacenter)
            if node.datacenter is not None:
                dc_to_nodes[node.datacenter].append(node)

        data_centers = list(dc_to_nodes.keys())
        if len(data_centers) < min_datacenters:
            return None, None
        return dc_to_nodes, data_centers

    def next_schedule(self):
        return super(DataCenterNetworkNemesis, self).next_schedule()

    def prepare_state(self):
        self.logger.info("Preparing DataCenterNetworkNemesis state...")
        dc_to_nodes, data_centers = self._validate_datacenters(min_datacenters=2)
        if dc_to_nodes is None or data_centers is None:
            self.logger.warning("Found insufficient data centers. DataCenter nemesis requires multiple DCs.")
            return

        self._dc_to_nodes = dc_to_nodes
        self._data_centers = data_centers
        self._dc_cycle_iterator = self._create_dc_cycle()

    def _create_dc_cycle(self):
        while True:
            for dc in self._data_centers:
                yield dc


class DataCenterStopNodesNemesis(DataCenterNetworkNemesis):
    def __init__(self, cluster, schedule=(300, 900), duration=120):
        super(DataCenterStopNodesNemesis, self).__init__(
            cluster, schedule=schedule, duration=duration)

        self._stopped_nodes = []
        self._stop_time = None

    def _check_and_restore_nodes(self):
        if not self._stop_time:
            return

        elapsed_time = time.time() - self._stop_time
        if elapsed_time >= self._duration:
            self.logger.info("Duration (%d seconds) elapsed. Restoring services in DC '%s'", self._duration, self._current_dc)
            self.extract_fault()

    def _stop_datacenter_services(self, datacenter):
        current_dc_hosts = self._dc_to_nodes.get(datacenter, [])

        if not current_dc_hosts:
            self.logger.warning("No hosts found in data center: %s", datacenter)
            return

        self.logger.info("Stopping services in data center '%s' on %d hosts: %s", datacenter, len(current_dc_hosts), current_dc_hosts)

        stopped_count = 0
        for node_id, node in self._cluster.nodes.items():
            if node.host in current_dc_hosts:
                try:
                    self.logger.info("Stopping node %d (%s) in DC %s", node_id, node.host, datacenter)
                    node.stop()
                    self._stopped_nodes.append((node_id, node))
                    stopped_count += 1

                except Exception as e:
                    self.logger.error("Failed to stop node %d (%s): %s", node_id, node.host, str(e))

        self.logger.info("Successfully stopped %d nodes in data center '%s'", stopped_count, datacenter)

    def next_schedule(self):
        if self._stopped_nodes:
            return next(self._restore_schedule)
        return super(DataCenterStopNodesNemesis, self).next_schedule()

    def inject_fault(self):
        if self._stopped_nodes:
            self._check_and_restore_nodes()
            return

        # If preparation failed, skip fault injection
        if self._dc_to_nodes is None or self._data_centers is None:
            self.logger.warning("Skipping fault injection - nemesis not properly prepared")
            return

        if self._dc_cycle_iterator is None:
            self._dc_cycle_iterator = self._create_dc_cycle()

        self._current_dc = next(self._dc_cycle_iterator)
        self.logger.info("Starting fault injection in data center: %s", self._current_dc)

        self._stop_datacenter_services(self._current_dc)

        if self._stopped_nodes:
            self._stop_time = time.time()
            self.on_success_inject_fault()

    def extract_fault(self):
        if not self._stopped_nodes:
            return False

        self.logger.info("Restoring %d stopped nodes in data center '%s'", len(self._stopped_nodes), self._current_dc)
        restored_count = 0
        for node_id, node in self._stopped_nodes:
            try:
                self.logger.info("Starting node %d (%s) in DC %s", node_id, node.host, self._current_dc)
                node.start()
                restored_count += 1

            except Exception as e:
                self.logger.error("Failed to start node %d (%s): %s", node_id, node.host, str(e))

        self.logger.info("Successfully restored %d nodes in data center '%s'", restored_count, self._current_dc)

        self._stopped_nodes = []
        self._stop_time = None
        self._current_dc = None

        return True


class DataCenterRouteUnreachableNemesis(DataCenterNetworkNemesis):
    def __init__(self, cluster, schedule=(300, 900), duration=120):
        super(DataCenterRouteUnreachableNemesis, self).__init__(
            cluster, schedule=schedule, duration=duration)

        self._blocked_routes = []
        self._block_time = None

    def _check_and_restore_routes(self):
        if not self._block_time:
            return

        elapsed_time = time.time() - self._block_time

        if elapsed_time >= self._duration:
            self.logger.info("Block duration (%d seconds) elapsed. Restoring routes for DC '%s'",
                             self._duration, self._current_dc)
            self.extract_fault()

    def _block_routes(self, datacenter):
        current_dc_hosts = self._dc_to_nodes.get(datacenter, [])

        if not current_dc_hosts:
            self.logger.warning("No hosts found in current data center: %s", self._current_dc)
            return

        current_dc_content = '\n'.join(current_dc_hosts.get('host', ''))

        other_dcs = [dc for dc in self._data_centers if dc != self._current_dc]
        self.logger.info("Blocking routes from other DCs %s to current DC '%s' hosts: %s", other_dcs, self._current_dc, current_dc_hosts)

        for other_dc in other_dcs:
            other_dc_nodes = self._dc_to_nodes.get(other_dc, [])

            for node in other_dc_nodes:
                try:
                    self.logger.info("Blocking routes on host %s (other DC: %s) to current DC %s", node.host, other_dc, self._current_dc)
                    temp_file = '/tmp/blocked_ips_{}_{}_{}'.format(
                        self._current_dc, other_dc, int(time.time())
                    )

                    create_file_cmd = 'echo "{}" | sudo tee {}'.format(current_dc_content, temp_file)
                    node.ssh_command(create_file_cmd)

                    block_cmd = (
                        'for i in `cat {}`; do sudo /usr/bin/ip -6 ro add unreach ${{i}} || '
                        'sudo /usr/bin/ip ro add unreachable ${{i}}; done'
                    ).format(temp_file)
                    node.ssh_command(block_cmd, raise_on_error=False)
                    self._blocked_routes.append({
                        'node': node,
                        'temp_file': temp_file,
                    })

                except Exception as e:
                    self.logger.error("Failed to block routes on host %s: %s", node.host, str(e))

        self.logger.info("Successfully blocked routes for %d other DC hosts to current DC '%s'", len(self._blocked_routes), self._current_dc)

    def next_schedule(self):
        if self._blocked_routes:
            return next(self._restore_schedule)
        return super(DataCenterRouteUnreachableNemesis, self).next_schedule()

    def inject_fault(self):
        if self._blocked_routes:
            self._check_and_restore_routes()
            return

        # If preparation failed, skip fault injection
        if self._dc_to_nodes is None or self._data_centers is None:
            self.logger.warning("Skipping fault injection - nemesis not properly prepared")
            return

        if self._dc_cycle_iterator is None:
            self._dc_cycle_iterator = self._create_dc_cycle()

        self._current_dc = next(self._dc_cycle_iterator)
        self.logger.info("Starting route blocking for current DC: %s", self._current_dc)

        self._block_routes(self._current_dc)

        if self._blocked_routes:
            self._block_time = time.time()
            self.on_success_inject_fault()

    def extract_fault(self):
        if not self._blocked_routes:
            return False

        self.logger.info("Restoring %d blocked routes for current DC '%s'", len(self._blocked_routes), self._current_dc)
        restored_count = 0
        for route_info in self._blocked_routes:
            try:
                node = route_info['node']
                temp_file = route_info['temp_file']

                self.logger.info("Restoring routes on host %s to current DC %s", node.host, self._current_dc)
                restore_cmd = (
                    'for i in `cat {}`; do sudo /usr/bin/ip -6 ro del unreach ${{i}} || '
                    'sudo /usr/bin/ip ro del unreachable ${{i}}; done'
                ).format(temp_file)
                node.ssh_command(restore_cmd, raise_on_error=False)
                cleanup_cmd = "sudo rm -f {}".format(temp_file)
                node.ssh_command(cleanup_cmd, raise_on_error=False)

                restored_count += 1

            except Exception as e:
                self.logger.error("Failed to restore routes on host %s: %s", route_info['host'], str(e))

        self.logger.info("Successfully restored routes for %d hosts in current DC '%s'", restored_count, self._current_dc)

        self._blocked_routes = []
        self._block_time = None
        self._current_dc = None

        return True


class DataCenterIptablesBlockPortsNemesis(DataCenterNetworkNemesis):
    def __init__(self, cluster, schedule=(300, 900), duration=120):
        super(DataCenterIptablesBlockPortsNemesis, self).__init__(
            cluster, schedule=schedule, duration=duration)

        self._blocked_hosts = []
        self._block_time = None

        self._block_ports_cmd = (
            "sudo /sbin/ip6tables -w -A YDB_FW -p tcp -m multiport "
            "--ports 2135,2136,8765,19001,31000:32000 -j REJECT"
        )
        self._restore_ports_cmd = "sudo ip6tables --flush YDB_FW"

    def _block_ports_on_current_dc(self):
        current_dc_nodes = self._dc_to_nodes.get(self._current_dc, [])
        if not current_dc_nodes:
            self.logger.warning("No hosts found in current data center: %s", self._current_dc)
            return

        self.logger.info("Blocking YDB ports on %d hosts in data center '%s'", len(current_dc_nodes), self._current_dc)
        blocked_count = 0
        for node in current_dc_nodes:
            try:
                self.logger.info("Blocking YDB ports on host %s in DC %s", node.host, self._current_dc)

                result = node.ssh_command(self._block_ports_cmd)
                exit_code = result.exit_code if hasattr(result, 'exit_code') else result

                if exit_code == 0:
                    blocked_count += 1
                    self._blocked_hosts.append(node)

                    self.logger.info("Successfully blocked YDB ports on host %s", node.host)
                else:
                    self.logger.warning("Skip block iptables ports on host %s, because we don't see chain YDB_FW", node.host)

            except Exception as e:
                self.logger.error("Failed to block YDB ports on host %s: %s", node.host, str(e))

        self.logger.info("Successfully blocked YDB ports on %d/%d hosts in data center '%s'", blocked_count, len(current_dc_nodes), self._current_dc)

    def _check_and_restore_ports(self):
        if not self._block_time:
            return

        elapsed_time = time.time() - self._block_time
        if elapsed_time >= self._duration:
            self.logger.info("Duration (%d seconds) elapsed. Restoring YDB ports in DC '%s'", self._duration, self._current_dc)
            self.extract_fault()

    def next_schedule(self):
        if self._blocked_hosts:
            return next(self._restore_schedule)
        return super(DataCenterIptablesBlockPortsNemesis, self).next_schedule()

    def inject_fault(self):
        if self._blocked_hosts:
            self._check_and_restore_ports()
            return

        # If preparation failed, skip fault injection
        if self._dc_to_nodes is None or self._data_centers is None:
            self.logger.warning("Skipping fault injection - nemesis not properly prepared")
            return

        if self._dc_cycle_iterator is None:
            self._dc_cycle_iterator = self._create_dc_cycle()

        self._current_dc = next(self._dc_cycle_iterator)
        self.logger.info("Starting iptables ports blocking in data center: %s", self._current_dc)

        self._block_ports_on_current_dc()

        if self._blocked_hosts:
            self._block_time = time.time()
            self.on_success_inject_fault()

    def extract_fault(self):
        if not self._blocked_hosts:
            return False

        self.logger.info("Restoring YDB ports on %d hosts in data center '%s'", len(self._blocked_hosts), self._current_dc)
        restored_count = 0
        for node in self._blocked_hosts:
            try:
                self.logger.info("Restoring YDB ports on host %s", node.host)

                result = node.ssh_command(self._restore_ports_cmd)
                exit_code = result.exit_code if hasattr(result, 'exit_code') else result

                if exit_code == 0:
                    restored_count += 1
                    self.logger.info("Successfully restored YDB ports on host %s", node.host)
                else:
                    self.logger.error("Failed to restore YDB ports on host %s (exit code: %s)", node.host, exit_code)

            except Exception as e:
                self.logger.error("Failed to restore YDB ports on host %s: %s", node.host, str(e))

        self.logger.info("Successfully restored YDB ports on %d/%d hosts in data center '%s'", restored_count, len(self._blocked_hosts), self._current_dc)

        self._blocked_hosts = []
        self._block_time = None
        self._current_dc = None

        return True
