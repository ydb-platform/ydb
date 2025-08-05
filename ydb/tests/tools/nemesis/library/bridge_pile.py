# -*- coding: utf-8 -*-

import asyncio
import logging
import collections
import random
import time
import abc
import socket
from ydb.tests.library.clients.kikimr_bridge_client import bridge_client_factory
from ydb.tests.tools.nemesis.library.base import AbstractMonitoredNemesis
from ydb.tests.library.nemesis.nemesis_core import Nemesis, Schedule


logger = logging.getLogger("bridge_pile")
logger.info("=== BRIDGE_PILE.PY LOADED ===")

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
        self.logger.info("Validating bridge piles for %d nodes", len(cluster.nodes))

        for node_id, node in cluster.nodes.items():
            self.logger.info("Node %d: host=%s, bridge_pile_name=%s, bridge_pile_id=%s",
                             node_id, node.host, node.bridge_pile_name, node.bridge_pile_id)
            if node.bridge_pile_id is not None:
                bridge_pile_to_nodes[node.bridge_pile_id].append(node)

        bridge_piles = list(bridge_pile_to_nodes.keys())
        self.logger.info("Found bridge piles: %s", bridge_piles)
        self.logger.info("Bridge pile to nodes mapping: %s",
                         {pile_id: [node.host for node in nodes] for pile_id, nodes in bridge_pile_to_nodes.items()})
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
            self.logger.error("Bridge piles: %s", self._bridge_piles)
            self.logger.error("No bridge piles found in cluster or only one bridge pile found")
            raise Exception("No bridge piles found in cluster or only one bridge pile found")

        self._bridge_pile_cycle = self._create_bridge_pile_cycle()
        self._bridge_clients = self._create_bridge_clients()

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
            raise e

        self.logger.info("=== MANAGING BRIDGE STATE ===")
        try:
            self._manage_bridge_state()
        except Exception as e:
            self.logger.error("Failed to manage bridge state for %s: %s", self.__class__.__name__, e)
            self.extract_fault()
            raise e

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
            raise Exception("Could not find another pile for bridge operations")

        self.logger.info("Current bridge state: %s", self._bridge_clients[another_pile_id].per_pile_state)
        self.logger.info("OPERATION: failover (pile_id=%d)", self._current_pile_id)
        result = self._bridge_clients[another_pile_id].failover(self._current_pile_id)

        if not result:
            self.logger.error("Failed to failover pile %d", self._current_pile_id)
            raise Exception("Failed to failover pile")

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
            raise Exception("Could not find another pile for bridge restoration operations")

        self.logger.info("Current bridge state: %s", self._bridge_clients[another_pile_id].per_pile_state)
        self.logger.info("OPERATION: rejoin for pile %d", self._current_pile_id)
        result = self._bridge_clients[another_pile_id].rejoin(self._current_pile_id)
        if not result:
            self.logger.error("Failed to rejoin pile %d", self._current_pile_id)
            raise Exception("Failed to rejoin pile")

        self.logger.info("Bridge state restored successfully")


class BridgePileStopNodesNemesis(AbstractBridgePileNemesis):
    def __init__(self, cluster, schedule=(300, 900), duration=60):
        super(BridgePileStopNodesNemesis, self).__init__(
            cluster, schedule=schedule, duration=duration)

    def _inject_specific_fault(self):
        """Stop nodes and slots in the current pile."""
        async def _async_stop_nodes():
            try:
                stop_tasks = []
                slots_to_stop = []
                for slot in self._cluster.slots.values():
                    if slot.bridge_pile_id == self._current_pile_id:
                        self.logger.info("Stopping slot %d on host %s", slot.ic_port, slot.host)
                        task = asyncio.create_task(asyncio.to_thread(slot.ssh_command, "sudo systemctl stop kikimr-multi@%d" % slot.ic_port, raise_on_error=True))
                        stop_tasks.append(task)
                        slots_to_stop.append(slot)

                slot_results = await asyncio.gather(*stop_tasks, return_exceptions=True)
                slot_success_count = 0
                for slot, result in zip(slots_to_stop, slot_results):
                    if isinstance(result, Exception):
                        self.logger.error("Exception stopping slot %d on host %s: %s", slot.ic_port, slot.host, result)
                        # Skip slot failure
                        continue
                    self.logger.info("Successfully stopped slot %d on host %s", slot.ic_port, slot.host)
                    slot_success_count += 1

                self.logger.info("Stopped %d/%d dynamic slots in pile %d", slot_success_count, len(stop_tasks), self._current_pile_id)

                stop_tasks = []
                for node in self._current_nodes:
                    self.logger.info("Stopping storage node %d on host %s", node.node_id, node.host)
                    task = asyncio.create_task(asyncio.to_thread(node.ssh_command, "sudo systemctl stop kikimr", raise_on_error=True))
                    stop_tasks.append(task)

                node_results = await asyncio.gather(*stop_tasks, return_exceptions=True)
                node_success_count = 0
                for i, result in enumerate(node_results):
                    if isinstance(result, Exception):
                        self.logger.error("Exception stopping node_id %d on host %s: %s", self._current_nodes[i].node_id, self._current_nodes[i].host, result)
                        raise result
                    self.logger.info("Successfully stopped node %d on host %s", self._current_nodes[i].node_id, self._current_nodes[i].host)
                    node_success_count += 1

                self.logger.info("Stopped %d/%d storage nodes in pile %d", node_success_count, len(stop_tasks), self._current_pile_id)

            except Exception as e:
                self.logger.error("Failed to stop nodes: %s", str(e))
                raise e

        with asyncio.Runner() as runner:
            runner.run(_async_stop_nodes())

    def _extract_specific_fault(self):
        """Start nodes and slots in the current pile."""
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
                self.logger.info("Successfully started node %d on host %s", self._current_nodes[i].node_id, self._current_nodes[i].host)
                node_success_count += 1

            self.logger.info("Started %d/%d storage nodes in pile %d", node_success_count, len(start_tasks), self._current_pile_id)

            start_tasks = []
            slots_to_start = []
            for slot in self._cluster.slots.values():
                if slot.bridge_pile_id == self._current_pile_id:
                    self.logger.info("Starting slot %d on host %s", slot.ic_port, slot.host)
                    task = asyncio.create_task(asyncio.to_thread(slot.ssh_command, "sudo systemctl start kikimr-multi@%d" % slot.ic_port, raise_on_error=True))
                    start_tasks.append(task)
                    slots_to_start.append(slot)

            slot_results = await asyncio.gather(*start_tasks, return_exceptions=True)
            slot_success_count = 0
            for slot, result in zip(slots_to_start, slot_results):
                if isinstance(result, Exception):
                    self.logger.error("Exception starting slot %d on host %s: %s", slot.ic_port, slot.host, result)
                    continue
                self.logger.info("Successfully started slot %d on host %s", slot.ic_port, slot.host)
                slot_success_count += 1

            self.logger.info("Started %d/%d dynamic slots in pile %d", slot_success_count, len(start_tasks), self._current_pile_id)

        with asyncio.Runner() as runner:
            runner.run(_async_start_nodes())


class BridgePileIptablesBlockPortsNemesis(AbstractBridgePileNemesis):
    def __init__(self, cluster, schedule=(300, 900), duration=60):
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
                block_tasks = []
                for node in self._current_nodes:
                    self.logger.info("Blocking YDB ports on host %s", node.host)
                    task = asyncio.create_task(asyncio.to_thread(node.ssh_command, self._block_ports_cmd, raise_on_error=True))
                    block_tasks.append(task)

                results = await asyncio.gather(*block_tasks, return_exceptions=True)
                success_count = 0
                for node, result in zip(self._current_nodes, results):
                    if isinstance(result, Exception):
                        self.logger.error("Error blocking YDB ports on host %s: %s", node.host, result)
                        raise result
                    self.logger.info("Successfully blocked YDB ports on host %s", node.host)
                    success_count += 1

                self.logger.info("Blocked YDB ports on %d/%d nodes in pile %d", success_count, len(self._current_nodes), self._current_pile_id)
            except Exception as e:
                self.logger.error("Failed to block YDB ports: %s", str(e))
                raise e

        with asyncio.Runner() as runner:
            runner.run(_async_block_ports())

    def _extract_specific_fault(self):
        """Restore YDB ports using iptables on nodes in the current pile."""
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

            self.logger.info("Restored YDB ports on %d/%d nodes in pile %d", success_count, len(self._current_nodes), self._current_pile_id)

        with asyncio.Runner() as runner:
            runner.run(_async_restore_ports())


class BridgePileRouteUnreachableNemesis(AbstractBridgePileNemesis):
    def __init__(self, cluster, schedule=(300, 900), duration=60):
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

                # Schedule recovery and block routes in one pass
                recovery_tasks = []
                unreach_tasks = []

                for other_node in other_nodes:
                    self.logger.info("Processing host %s for current pile %d", other_node.host, self._current_pile_id)
                    for node in self._current_nodes:
                        ip = self._resolve_hostname_to_ip(node.host)
                        if ip is None:
                            self.logger.error("Failed to resolve hostname %s to IP address", node.host)
                            raise Exception("Failed to resolve hostname to IP address")

                        # Schedule automatic recovery first
                        recovery_cmd = f"sudo /usr/bin/ip -6 ro del unreach {ip}"
                        at_cmd = f"echo '{recovery_cmd}' | sudo at now + {self._duration} seconds"
                        try:
                            at_task = asyncio.create_task(asyncio.to_thread(other_node.ssh_command, at_cmd, raise_on_error=False))
                            self.logger.info("Scheduled automatic recovery for IP %s on host %s", ip, other_node.host)
                            recovery_tasks.append(at_task)
                        except Exception as e:
                            self.logger.warning("Failed to schedule automatic recovery for IP %s on host %s: %s", ip, other_node.host, str(e))

                        # Then block the route
                        block_cmd = self._block_cmd_template.format(ip, ip)
                        block_task = asyncio.create_task(asyncio.to_thread(other_node.ssh_command, block_cmd, raise_on_error=True))
                        unreach_tasks.append(block_task)

                # Wait for recovery scheduling to complete first
                if recovery_tasks:
                    await asyncio.gather(*recovery_tasks, return_exceptions=True)

                # Then wait for blocking operations to complete
                await asyncio.gather(*unreach_tasks, return_exceptions=True)

                self.logger.info("Blocked routes to pile %d and scheduled automatic recovery", self._current_pile_id)

            except Exception as e:
                self.logger.error("Failed to block routes: %s", str(e))
                raise e

        with asyncio.Runner() as runner:
            runner.run(_async_block_routes())

    def _extract_specific_fault(self):
        """Network routes are automatically restored via 'at' command scheduled during injection."""
        self.logger.info("Skipping manual route restoration - automatic recovery via 'at' command is scheduled")


def bridge_pile_nemesis_list(cluster):
    # Для функций модуля используем глобальный логгер
    logger = logging.getLogger("bridge_pile")
    logger.info("=== BRIDGE_PILE_NEMESIS_LIST CALLED ===")
    logger.info("Creating bridge pile nemesis list")
    logger.info("Cluster: %s", cluster)

    try:
        bridge_nemesis_list = [
            BridgePileStopNodesNemesis(cluster),
            BridgePileRouteUnreachableNemesis(cluster),
            BridgePileIptablesBlockPortsNemesis(cluster),
        ]
        logger.info("Successfully created %d bridge pile nemesis", len(bridge_nemesis_list))
        return bridge_nemesis_list
    except Exception as e:
        logger.error("Failed to create bridge pile nemesis list: %s", e)
        raise
