# -*- coding: utf-8 -*-
import collections
import time
import asyncio
import abc
import socket
from ydb.tests.library.nemesis.nemesis_core import Nemesis, Schedule
from ydb.tests.tools.nemesis.library import base

class AbstractDataCenterNemesis(Nemesis, base.AbstractMonitoredNemesis):
    def __init__(self, cluster, schedule=(300, 900), duration=60):
        super(AbstractDataCenterNemesis, self).__init__(schedule=schedule)
        base.AbstractMonitoredNemesis.__init__(self, scope='datacenter')

        self._cluster = cluster
        self._duration = duration

        self._current_dc = None
        self._current_nodes = None

        self._interval_schedule = Schedule.from_tuple_or_int(duration)

    def next_schedule(self):
        if self._current_dc is not None:
            return next(self._interval_schedule)
        return super(AbstractDataCenterNemesis, self).next_schedule()

    def _create_dc_cycle(self):
        while True:
            for dc in self._data_centers:
                yield dc

    def _validate_datacenters(self, cluster):
         dc_to_nodes = collections.defaultdict(list)
         for node in cluster.nodes.values():
             if node.datacenter is not None:
                 dc_to_nodes[node.datacenter].append(node)

         data_centers = list(dc_to_nodes.keys())
         return dc_to_nodes, data_centers

    def prepare_state(self):
        self._dc_to_nodes, self._data_centers = self._validate_datacenters(self._cluster)
        if len(self._data_centers) < 2:
            self.logger.error("No datacenters found in cluster or only one datacenter found")
            raise Exception("No datacenters found in cluster or only one datacenter found")
        
        self._dc_cycle_iterator = self._create_dc_cycle()

    def inject_fault(self):
        if self.extract_fault():
            return

        self.start_inject_fault()
        self._current_dc = next(self._dc_cycle_iterator)
        self._current_nodes = self._dc_to_nodes.get(self._current_dc, [])
        self.logger.info("Selected dc %s with %d nodes for %s", self._current_dc, len(self._current_nodes), self.__class__.__name__)

        self.logger.info("=== INJECTING SPECIFIC FAULT ===")
        self.logger.info("Injecting specific fault for %s on dc %s", self.__class__.__name__, self._current_dc)
        try:
            self._inject_specific_fault()
            self.logger.info("=== SPECIFIC FAULT INJECTED ===")
        except Exception as e:
            self.logger.error("Failed to inject specific fault for %s: %s", self.__class__.__name__, e)
            return
        self.logger.info("=== WAITING FOR %d SECONDS ===", self._duration)
        time.sleep(self._duration)
        self.logger.info("=== EXTRACTING SPECIFIC FAULT ===")
        try:
            self._extract_specific_fault()
            self.logger.info("=== SPECIFIC FAULT EXTRACTED ===")
        except Exception as e:
            self.logger.error("Failed to extract specific fault for %s: %s", self.__class__.__name__, e)
            return
        
        self.on_success_inject_fault()

    @abc.abstractmethod
    def _inject_specific_fault(self):
        pass

    def extract_fault(self):
        if self._current_dc is not None:
            try:
                self._extract_specific_fault()
            except Exception as e:
                self.logger.error("Exception during extraction for %s: %s", self.__class__.__name__, e)
            finally:
                self._current_dc = None
                self._current_nodes = None
            
            self.on_success_extract_fault()
            return True
            
        return False
    
    @abc.abstractmethod
    def _extract_specific_fault(self):
        pass


class DataCenterStopNodesNemesis(AbstractDataCenterNemesis):
    def __init__(self, cluster, schedule=(300, 900), duration=60):
        super(DataCenterStopNodesNemesis, self).__init__(
            cluster, schedule=schedule, duration=duration)
    
    def _inject_specific_fault(self):
        async def _async_stop_nodes():
            try:
                stop_tasks = []
                for slot in self._cluster.slots.values():
                    if slot.datacenter == self._current_dc:
                        self.logger.info("Stopping slot %d on host %s", slot.ic_port, slot.host)
                        task = asyncio.create_task(asyncio.to_thread(slot.ssh_command, "sudo systemctl stop kikimr-multi@%d" % slot.ic_port, raise_on_error=True))
                        stop_tasks.append(task)

                slot_results = await asyncio.gather(*stop_tasks, return_exceptions=True)
                slot_success_count = 0
                for slot, result in zip(self._cluster.slots, slot_results):
                    if isinstance(result, Exception):
                        self.logger.error("Exception stopping slot %d on host %s: %s", slot.ic_port, slot.host, result)
                        # Skip slot failure
                        continue
                    self.logger.info("Successfully stopped slot %d on host %s", slot.ic_port, slot.host)
                    slot_success_count += 1

                self.logger.info("Stopped %d/%d dynamic slots in dc %s", slot_success_count, len(stop_tasks), self._current_dc)

                stop_tasks = []
                for node in self._current_nodes:
                    self.logger.info("Stopping storage node %d on host %s", node.node_id, node.host)
                    task = asyncio.create_task(asyncio.to_thread(node.ssh_command, "sudo systemctl stop kikimr", raise_on_error=True))
                    stop_tasks.append(task)
                
                node_results = await asyncio.gather(*stop_tasks, return_exceptions=True)
                node_success_count = 0
                for i, result in enumerate(node_results):
                    if isinstance(result, Exception):
                        self.logger.error("Exception stopping storage node_id %d on host %s: %s",  node.node_id, node.host, result)
                        raise result
                    self.logger.info("Successfully stopped node %d on host %s", node.node_id, node.host)
                    node_success_count += 1

                self.logger.info("Stopped %d/%d storage nodes in dc %s", node_success_count, len(stop_tasks), self._current_dc)

            except Exception as e:
                self.logger.error("Failed to stop nodes: %s", str(e))
                raise

        with asyncio.Runner() as runner:
            runner.run(_async_stop_nodes())

    def _extract_specific_fault(self):
        async def _async_start_nodes():
            start_tasks = []
            for node in self._current_nodes:
                self.logger.info("Starting storage node %d on host %s", node.node_id, node.host)
                task = asyncio.create_task(asyncio.to_thread(node.ssh_command, "sudo systemctl start kikimr", raise_on_error=True))
                start_tasks.append(task)

            node_results = await asyncio.gather(*start_tasks, return_exceptions=True)
            node_success_count = 0
            for i, result in enumerate(node_results):
                if isinstance(result, Exception):
                    self.logger.error("Exception starting node: %s", result)
                    continue
                self.logger.info("Successfully started node %d on host %s", node.node_id, node.host)
                node_success_count += 1

            self.logger.info("Started %d/%d storage nodes in dc %s", node_success_count, len(start_tasks), self._current_dc)

            start_tasks = []
            for slot in self._cluster.slots.values():
                if slot.datacenter == self._current_dc:
                    self.logger.info("Starting slot %d on host %s", slot.ic_port, slot.host)
                    task = asyncio.create_task(asyncio.to_thread(slot.ssh_command, "sudo systemctl start kikimr-multi@%d" % slot.ic_port, raise_on_error=True))
                    start_tasks.append(task)

            slot_results = await asyncio.gather(*start_tasks, return_exceptions=True)
            slot_success_count = 0
            for slot, result in zip(self._cluster.slots, slot_results):
                if isinstance(result, Exception):
                    self.logger.error("Exception starting slot %d on host %s: %s", slot.ic_port, slot.host, result)
                    continue
                self.logger.info("Successfully started slot %d on host %s", slot.ic_port, slot.host)
                slot_success_count += 1

            self.logger.info("Started %d/%d dynamic slots in dc %s", slot_success_count, len(start_tasks), self._current_dc)

        with asyncio.Runner() as runner:
            runner.run(_async_start_nodes())


class DataCenterRouteUnreachableNemesis(AbstractDataCenterNemesis):
    def __init__(self, cluster, schedule=(300, 900), duration=60):
        super(DataCenterRouteUnreachableNemesis, self).__init__(
            cluster, schedule=schedule, duration=duration)

        self._block_ports_cmd = (
            "sudo /sbin/ip6tables -w -A YDB_FW -p tcp -m multiport "
            "--ports 2135,2136,8765,19001,31000:32000 -j REJECT"
        )
        self._restore_ports_cmd = (
            "sudo /sbin/ip6tables -w -F YDB_FW"
        )

    def _inject_specific_fault(self):
        async def _async_block_ports():
            try:
                block_tasks = []
                for node in self._current_nodes:
                    self.logger.info("Blocking YDB ports on host %s", node.host)
                    task = asyncio.create_task(asyncio.to_thread(node.ssh_command, self._block_ports_cmd, raise_on_error=True))
                    block_tasks.append(task)
                
                results = await asyncio.gather(*block_tasks, return_exceptions=True)
                success_count = 0
                for node, result in zip(self._current_nodes, results):
                    if isinstance(result, Exception):
                        self.logger.error("Exception blocking YDB ports on host %s: %s", node.host, result)
                        raise result
                    self.logger.info("Successfully blocked YDB ports on host %s", node.host)
                    success_count += 1
                
                self.logger.info("Blocked YDB ports on %d/%d nodes in dc %s", success_count, len(self._current_nodes), self._current_dc)

            except Exception as e:
                self.logger.error("Failed to block YDB ports: %s", str(e))
                raise

        with asyncio.Runner() as runner:
            runner.run(_async_block_ports())

    def _extract_specific_fault(self):
        async def _async_restore_ports():
            restore_tasks = []
            for node in self._current_nodes:
                self.logger.info("Restoring YDB ports on host %s", node.host)
                task = asyncio.create_task(asyncio.to_thread(node.ssh_command, self._restore_ports_cmd, raise_on_error=True))
                restore_tasks.append(task)

            results = await asyncio.gather(*restore_tasks, return_exceptions=True)
            success_count = 0
            for node, result in zip(self._current_nodes, results):
                if isinstance(result, Exception):
                    self.logger.error("Exception restoring YDB ports on host %s: %s", node.host, result)
                    continue
                self.logger.info("Successfully restored YDB ports on host %s", node.host)
                success_count += 1

            self.logger.info("Restored YDB ports on %d/%d nodes in dc %s", success_count, len(self._current_nodes), self._current_dc)

        with asyncio.Runner() as runner:
            runner.run(_async_restore_ports())


class DataCenterIptablesBlockPortsNemesis(AbstractDataCenterNemesis):
    def __init__(self, cluster, schedule=(300, 900), duration=60):
        super(DataCenterIptablesBlockPortsNemesis, self).__init__(
            cluster, schedule=schedule, duration=duration)

        self._block_cmd_template = (
            'sudo /usr/bin/ip -6 ro replace unreach {} || sudo /usr/bin/ip -6 ro add unreach {}'
        )

        self._restore_cmd_template = (
            'sudo /usr/bin/ip -6 ro del unreach {}'
        )

    def _resolve_hostname_to_ip(self, hostname):
        """Resolve hostname to IP address."""
        try:
            result = socket.getaddrinfo(hostname, None, socket.AF_INET6)
            if result:
                return result[0][4][0]
        except socket.gaierror:
            pass
    
        return None

    def _inject_specific_fault(self):
        async def _async_block_routes():
            try:
                other_nodes = []
                for dc, nodes in self._dc_to_nodes.items():
                    if dc != self._current_dc:
                        other_nodes.extend(nodes)

                unreach_tasks = []
                for other_node in other_nodes:
                    self.logger.info("Blocking routes on host %s to current dc %s", other_node.host, self._current_dc)
                    for node in self._current_nodes:
                        ip = self._resolve_hostname_to_ip(node.host)
                        if ip is None:
                            self.logger.error("Failed to resolve hostname %s to IP address", node.host)
                            raise Exception("Failed to resolve hostname to IP address")
                        block_cmd = self._block_cmd_template.format(ip, ip)
                        task = asyncio.create_task(asyncio.to_thread(other_node.ssh_command, block_cmd, raise_on_error=True))
                        unreach_tasks.append(task)

                results = await asyncio.gather(*unreach_tasks, return_exceptions=True)
                success_count = 0
                for result in results:
                    if isinstance(result, Exception):
                        self.logger.error("Exception blocking route to current dc %s: %s", self._current_dc, result)
                        raise result
                    self.logger.info("Successfully blocked route to current dc %s", self._current_dc)
                    success_count += 1
                
                self.logger.info("Blocked routes to dc %s: %d/%d operations successful", self._current_dc, success_count, len(results))

            except Exception as e:
                self.logger.error("Failed to block routes to current dc %s: %s", self._current_dc, str(e))
                raise

        with asyncio.Runner() as runner:
            runner.run(_async_block_routes())

    def _extract_specific_fault(self):
        async def _async_restore_routes():
            other_nodes = []
            for dc, nodes in self._dc_to_nodes.items():
                if dc != self._current_dc:
                    other_nodes.extend(nodes)

            restore_tasks = []
            for other_node in other_nodes:
                self.logger.info("Restoring routes on host %s to current dc %s", other_node.host, self._current_dc)
                for node in self._current_nodes:
                    ip = self._resolve_hostname_to_ip(node.host)
                    if ip is None:
                        self.logger.error("Failed to resolve hostname %s to IP address", node.host)
                        continue
                    restore_cmd = self._restore_cmd_template.format(ip)
                    task = asyncio.create_task(asyncio.to_thread(other_node.ssh_command, restore_cmd, raise_on_error=True))
                    restore_tasks.append(task)

            results = await asyncio.gather(*restore_tasks, return_exceptions=True)
            success_count = 0
            for result in results:
                if isinstance(result, Exception):
                    self.logger.error("Exception restoring route to current dc %s: %s", self._current_dc, result)
                    continue
                self.logger.info("Successfully restored route to current dc %s", self._current_dc)
                success_count += 1
            
            self.logger.info("Restored routes to dc %s: %d/%d operations successful", self._current_dc, success_count, len(results))


        with asyncio.Runner() as runner:
            runner.run(_async_restore_routes())


def datacenter_nemesis_list(cluster):
    return [
        DataCenterStopNodesNemesis(cluster),
        DataCenterRouteUnreachableNemesis(cluster),
        DataCenterIptablesBlockPortsNemesis(cluster)
    ]