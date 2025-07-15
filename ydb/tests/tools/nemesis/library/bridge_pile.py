# -*- coding: utf-8 -*-

import asyncio
import collections
import logging
import random
import time
import abc
import threading
import socket
from ydb.tests.library.clients.kikimr_bridge_client import bridge_client_factory
from ydb.tests.tools.nemesis.library.base import AbstractMonitoredNemesis
from ydb.tests.library.nemesis.nemesis_core import Nemesis, Schedule


class AbstractBridgePileNemesis(Nemesis, AbstractMonitoredNemesis):
    """
    Bridge-aware nemesis that performs master bridge pile switching scenarios.
    This implements the specific scenario requested for switching off master.
    """

    def __init__(self, cluster, schedule=(300, 900), duration=60):
        super(AbstractBridgePileNemesis, self).__init__(schedule)
        AbstractMonitoredNemesis.__init__(self, 'bridge_pile')

        self._cluster = cluster
        self._duration = duration

        self._current_pile_id = None
        self._current_nodes = None

        self._interval_schedule = Schedule.from_tuple_or_int(duration)

    def next_schedule(self):
        if self._current_pile_id is not None:
            return next(self._interval_schedule)
        return super(AbstractBridgePileNemesis, self).next_schedule()

    def _validate_bridge_piles(self, cluster):
        bridge_pile_to_nodes = collections.defaultdict(list)
        for node in cluster.nodes.values():
            if node.bridge_pile_id is not None:
                bridge_pile_to_nodes[node.bridge_pile_id].append(node)

        bridge_piles = list(bridge_pile_to_nodes.keys())
        return bridge_pile_to_nodes, bridge_piles

    def _create_bridge_pile_cycle(self):
        while True:
            selected_pile = random.choice(self._bridge_piles)
            self.logger.info("Randomly selected pile %d from available piles %s", selected_pile, self._bridge_piles)
            yield selected_pile

    def _create_bridge_clients(self):
        bridge_clients = {}
        for pile in self._bridge_piles:
            node = self._bridge_pile_to_nodes[pile][0]
            bridge_clients[pile] = bridge_client_factory(
                node.host, node.port, cluster=self._cluster, retry_count=3, timeout=5
            )
            bridge_clients[pile].set_auth_token('root@builtin')
        return bridge_clients

    def prepare_state(self):
        self._bridge_pile_to_nodes, self._bridge_piles = self._validate_bridge_piles(self._cluster)
        if len(self._bridge_piles) < 2:
            self.logger.error("No bridge piles found in cluster or only one bridge pile found")
            raise Exception("No bridge piles found in cluster or only one bridge pile found")
        
        self._bridge_pile_cycle = self._create_bridge_pile_cycle()
        self._bridge_clients = self._create_bridge_clients()

    def next_schedule(self):
        if self._current_pile_id is not None:
            return next(self._interval_schedule)
        return super(AbstractBridgePileNemesis, self).next_schedule()

    def inject_fault(self):
        if self.extract_fault():
            return

        self.start_inject_fault()
        self._current_pile_id = next(self._bridge_pile_cycle)
        self._current_nodes = self._bridge_pile_to_nodes.get(self._current_pile_id, [])
        self.logger.info("Selected pile %s with %d nodes for %s", self._current_pile_id, len(self._current_nodes), self.__class__.__name__)

        self.logger.info("=== INJECTING SPECIFIC FAULT ===")
        self.logger.info("Injecting specific fault for %s on pile %s", self.__class__.__name__, self._current_pile_id)
        try:
            self._inject_specific_fault()
            self.logger.info("=== SPECIFIC FAULT INJECTED ===")
        except Exception as e:
            self.logger.error("Failed to inject specific fault for %s: %s", self.__class__.__name__, e)
            self.extract_fault()
            raise
        
        self.logger.info("=== MANAGING BRIDGE STATE ===")
        try:
            self._manage_bridge_state()
        except Exception as e:
            self.logger.error("Failed to manage bridge state for %s: %s", self.__class__.__name__, e)
            self.extract_fault()
            raise

        self.logger.info("=== WAITING FOR %d SECONDS ===", self._duration)
        time.sleep(self._duration)

        self.logger.info("=== EXTRACTING SPECIFIC FAULT ===")
        try:
            self._extract_specific_fault()
            self.logger.info("=== SPECIFIC FAULT EXTRACTED ===")
        except Exception as e:
            self.logger.error("Failed to extract specific fault for %s: %s", self.__class__.__name__, e)
            return

        self.logger.info("=== RESTORING BRIDGE STATE ===")
        try:
            self._restore_bridge_state()
            self.logger.info("=== BRIDGE STATE RESTORED ===")
        except Exception as e:
            self.logger.error("Failed to restore bridge state: %s", e)
            return
        
        self.on_success_inject_fault()

    @abc.abstractmethod
    def _inject_specific_fault(self):
        """Subclass-specific fault injection - to be overridden by subclasses."""
        pass
    
    def _manage_bridge_state(self):
        """Handle bridge state changes common to all bridge pile nemesis."""
        self.logger.info("Current pile being affected: %d", self._current_pile_id)
        
        another_pile_id = None
        for pile_id in self._bridge_piles:
            if pile_id != self._current_pile_id:
                another_pile_id = pile_id
                break

        if another_pile_id is None:
            self.logger.error("Could not find another pile for bridge operations")
            raise
        
        self.logger.info("Current bridge state: %s", self._bridge_clients[another_pile_id].get_cluster_state_result())
        is_current_primary = self._bridge_clients[another_pile_id].is_primary_pile(self._current_pile_id)
        if is_current_primary:
            self.logger.info("OPERATION: failover_scenario (%d -> %d)", self._current_pile_id, another_pile_id)
            result = self._bridge_clients[another_pile_id].failover_scenario(self._current_pile_id, another_pile_id)
        else:
            self.logger.info("OPERATION: switchover_scenario (%d)", self._current_pile_id)
            result = self._bridge_clients[another_pile_id].switchover_scenario(self._current_pile_id)

        if not result:
            raise Exception("Failed to manage bridge state")
        
        self.logger.info("Bridge state managed successfully")


    def extract_fault(self):
        if self._current_pile_id is not None:
            try:
                self._extract_specific_fault()
            except Exception as e:
                self.logger.error("Exception during extraction for %s: %s", self.__class__.__name__, e)

            try:
                self._restore_bridge_state()
            except Exception as e:
                self.logger.error("Failed to restore bridge state: %s", e)
            
            self._current_pile_id = None
            self._current_nodes = None
            
            self.on_success_extract_fault()
            return True
            
        return False

    @abc.abstractmethod
    def _extract_specific_fault(self):
        """Subclass-specific fault extraction - to be overridden by subclasses."""
        pass
    
    def _restore_bridge_state(self):
        """Handle bridge state restoration common to all bridge pile nemesis."""
        self.logger.info("Restoring state for pile: %d", self._current_pile_id)

        another_pile_id = None
        for pile_id in self._bridge_piles:
            if pile_id != self._current_pile_id:
                another_pile_id = pile_id
                break

        if another_pile_id is None:
            self.logger.error("Could not find another pile for bridge restoration operations")
            raise
        
        self.logger.info("Current bridge state: %s", self._bridge_clients[another_pile_id].get_cluster_state_result())
        self.logger.info("OPERATION: restore_scenario for pile %d", self._current_pile_id)
        result = self._bridge_clients[another_pile_id].restore_scenario(self._current_pile_id, another_pile_id)
        if not result:
            raise Exception("Failed to restore pile %d", self._current_pile_id)

        self.logger.info("Bridge state restored successfully")

class BridgePileStopNodesNemesis(AbstractBridgePileNemesis):
    def __init__(self, cluster, schedule=(0, 60), duration=60):
        super(BridgePileStopNodesNemesis, self).__init__(
            cluster, schedule=schedule, duration=duration)

    def _inject_specific_fault(self):
        """Stop nodes and slots in the current pile."""
        async def _async_stop_nodes():
            try:
                # Stop slots in the current pile
                stop_tasks = []
                for slot in self._cluster.slots.values():
                    if slot.bridge_pile_id == self._current_pile_id:
                        self.logger.info("Stopping slot %d on host %s", slot.ic_port, slot.host)
                        task = asyncio.create_task(asyncio.to_thread(slot.ssh_command, "sudo systemctl stop kikimr-multi@%d" % slot.ic_port))
                        stop_tasks.append(task)

                slot_results = await asyncio.gather(*stop_tasks)
                self.logger.info("Successfully stopped %d dynamic slots in pile %d", len(stop_tasks), self._current_pile_id)

                # Run node.stop() operations in parallel
                stop_tasks = []
                for node in self._current_nodes:
                    self.logger.info("Stopping storage node %d on host %s", node.node_id, node.host)
                    # Wrap the potentially blocking node.stop() call in asyncio.to_thread
                    task = asyncio.create_task(asyncio.to_thread(node.stop))
                    stop_tasks.append(task)
                
                # Wait for all stop operations to complete
                node_results = await asyncio.gather(*stop_tasks)
                self.logger.info("Successfully stopped %d storage nodes in pile %d", len(stop_tasks), self._current_pile_id)

            except Exception as e:
                self.logger.error("Failed to stop nodes: %s", str(e))
                raise

        # Run the async operations synchronously
        asyncio.run(_async_stop_nodes())

    def _extract_specific_fault(self):
        """Start nodes and slots in the current pile."""
        async def _async_start_nodes():
            try:
                # Start storage nodes in current pile
                start_tasks = []
                for node in self._current_nodes:
                    self.logger.info("Starting storage node %d on host %s", node.node_id, node.host)
                    # Wrap the potentially blocking node.start() call in asyncio.to_thread
                    task = asyncio.create_task(asyncio.to_thread(node.start))
                    start_tasks.append(task)
                
                # Wait for all start operations to complete
                node_results = await asyncio.gather(*start_tasks)
                self.logger.info("Successfully started %d storage nodes in pile %d", len(start_tasks), self._current_pile_id)

                # Start dynamic slots in the current pile
                start_tasks = []
                for slot in self._cluster.slots.values():
                    if slot.bridge_pile_id == self._current_pile_id:
                        self.logger.info("Starting slot %d on host %s", slot.ic_port, slot.host)
                        task = asyncio.create_task(asyncio.to_thread(slot.ssh_command, "sudo systemctl start kikimr-multi@%d" % slot.ic_port))
                        start_tasks.append(task)

                # Wait for all start operations to complete
                slot_results = await asyncio.gather(*start_tasks)
                self.logger.info("Successfully started %d dynamic slots in pile %d", len(start_tasks), self._current_pile_id)

            except Exception as e:
                self.logger.error("Failed to start nodes: %s", str(e))
                raise

        # Run the async operations synchronously
        asyncio.run(_async_start_nodes())


class BridgePileIptablesBlockPortsNemesis(AbstractBridgePileNemesis):
    def __init__(self, cluster, schedule=(0, 60), duration=60):
        super(BridgePileIptablesBlockPortsNemesis, self).__init__(
            cluster, schedule=schedule, duration=duration)

        self._block_ports_cmd = (
            "sudo /sbin/ip6tables -w -A YDB_FW -p tcp -m multiport "
            "--ports 2135,2136,8765,19001,31000:32000 -j REJECT"
        )
        self._restore_ports_cmd = (
            "sudo /sbin/ip6tables -w -F YDB_FW"
        )

    def _inject_specific_fault(self):
        """Block YDB ports using iptables on nodes in the current pile."""
        async def _async_block_ports():
            try:
                # Run block_ports_cmd operations in parallel
                block_tasks = []
                for node in self._current_nodes:
                    self.logger.info("Blocking YDB ports on host %s", node.host)
                    task = asyncio.create_task(asyncio.to_thread(node.ssh_command, self._block_ports_cmd))
                    block_tasks.append(task)
                
                # Wait for all block_ports_cmd operations to complete
                results = await asyncio.gather(*block_tasks)
                success_count = 0
                for node, result in zip(self._current_nodes, results):
                    # ssh_command returns bytes on success, None on failure
                    if result is not None:
                        self.logger.info("Successfully blocked YDB ports on host %s", node.host)
                        success_count += 1
                    else:
                        self.logger.warning("Error blocking YDB ports on host %s: %s", node.host, result)
                
                self.logger.info("Blocked YDB ports on %d/%d nodes in pile %d", success_count, len(self._current_nodes), self._current_pile_id)

            except Exception as e:
                self.logger.error("Failed to block YDB ports: %s", str(e))
                raise

        # Run the async operations synchronously
        asyncio.run(_async_block_ports())

    def _extract_specific_fault(self):
        """Restore YDB ports using iptables on nodes in the current pile."""
        async def _async_restore_ports():
            try:
                # Run restore_ports_cmd operations in parallel
                restore_tasks = []
                for node in self._current_nodes:
                    self.logger.info("Restoring YDB ports on host %s", node.host)
                    task = asyncio.create_task(asyncio.to_thread(node.ssh_command, self._restore_ports_cmd))
                    restore_tasks.append(task)

                # Wait for all restore_ports_cmd operations to complete
                results = await asyncio.gather(*restore_tasks)
                success_count = 0
                for node, result in zip(self._current_nodes, results):
                    # ssh_command returns bytes on success, None on failure
                    if result is not None:
                        self.logger.info("Successfully restored YDB ports on host %s", node.host)
                        success_count += 1
                    else:
                        self.logger.error("Failed to restore YDB ports on host %s", node.host)

                self.logger.info("Restored YDB ports on %d/%d nodes in pile %d", success_count, len(self._current_nodes), self._current_pile_id)

            except Exception as e:
                self.logger.error("Failed to restore YDB ports: %s", str(e))
                raise

        # Run the async operations synchronously
        asyncio.run(_async_restore_ports())


class BridgePileRouteUnreachableNemesis(AbstractBridgePileNemesis):
    def __init__(self, cluster, schedule=(0, 60), duration=60):
        super(BridgePileRouteUnreachableNemesis, self).__init__(
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
            # Try IPv6 first
            result = socket.getaddrinfo(hostname, None, socket.AF_INET6)
            if result:
                return result[0][4][0]
        except socket.gaierror:
            pass
    
        return None

    def _inject_specific_fault(self):
        """Block network routes from other piles to the current pile using ip route."""
        async def _async_block_routes():
            try:
                other_nodes = []
                for pile_id, nodes in self._bridge_pile_to_nodes.items():
                    if pile_id != self._current_pile_id:
                        other_nodes.extend(nodes)

                unreach_tasks = []
                for other_node in other_nodes:
                    self.logger.info("Blocking routes on host %s to current pile %d", other_node.host, self._current_pile_id)
                    for node in self._current_nodes:
                        ip = self._resolve_hostname_to_ip(node.host)
                        if ip is None:
                            self.logger.error("Failed to resolve hostname %s to IP address", node.host)
                            raise
                        block_cmd = self._block_cmd_template.format(ip, ip)
                        task = asyncio.create_task(asyncio.to_thread(other_node.ssh_command, block_cmd))
                        unreach_tasks.append(task)

                # Wait for all unreach_tasks operations to complete
                results = await asyncio.gather(*unreach_tasks)
                success_count = 0
                for result in results:
                    # ssh_command returns bytes on success, None on failure
                    if result is not None:
                        self.logger.info("Successfully blocked routes on host %s to current pile %d", other_node.host, self._current_pile_id)
                        success_count += 1
                    else:
                        self.logger.error("Failed to block routes on host %s to current pile %d", other_node.host, self._current_pile_id)
                
                self.logger.info("Blocked routes to pile %d: %d/%d operations successful", self._current_pile_id, success_count, len(results))

            except Exception as e:
                self.logger.error("Failed to block routes to current pile %d: %s", self._current_pile_id, str(e))
                raise

        # Run the async operations synchronously
        asyncio.run(_async_block_routes())

    def _extract_specific_fault(self):
        """Restore network routes from other piles to the current pile using ip route."""
        async def _async_restore_routes():
            try:
                other_nodes = []
                for pile_id, nodes in self._bridge_pile_to_nodes.items():
                    if pile_id != self._current_pile_id:
                        other_nodes.extend(nodes)

                restore_tasks = []
                for other_node in other_nodes:
                    self.logger.info("Restoring routes on host %s to current pile %d", other_node.host, self._current_pile_id)
                    for node in self._current_nodes:
                        ip = self._resolve_hostname_to_ip(node.host)
                        if ip is None:
                            self.logger.error("Failed to resolve hostname %s to IP address", node.host)
                            raise
                        restore_cmd = self._restore_cmd_template.format(ip)
                        task = asyncio.create_task(asyncio.to_thread(other_node.ssh_command, restore_cmd))
                        restore_tasks.append(task)

                # Wait for all restore_tasks operations to complete
                results = await asyncio.gather(*restore_tasks)
                success_count = 0
                for result in results:
                    # ssh_command returns bytes on success, None on failure
                    if result is not None:
                        self.logger.info("Successfully restored routes on host %s to current pile %d", other_node.host, self._current_pile_id)
                        success_count += 1
                    else:
                        self.logger.error("Failed to restore routes on host %s to current pile %d", other_node.host, self._current_pile_id)
                
                self.logger.info("Restored routes to pile %d: %d/%d operations successful", self._current_pile_id, success_count, len(results))

            except Exception as e:
                self.logger.error("Failed to restore routes to current pile %d: %s", self._current_pile_id, str(e))
                raise

        # Run the async operations synchronously
        asyncio.run(_async_restore_routes())

def bridge_pile_nemesis_list(cluster):
    return [
        BridgePileStopNodesNemesis(cluster),
        BridgePileRouteUnreachableNemesis(cluster),
        BridgePileIptablesBlockPortsNemesis(cluster),
    ]
