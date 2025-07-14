# -*- coding: utf-8 -*-

import collections
import time
import asyncio
from ydb.tests.library.nemesis.nemesis_core import Nemesis, Schedule
from ydb.tests.tools.nemesis.library import base
from ydb.tests.tools.nemesis.library.state import NemesisState

datacenter_state = NemesisState()

class AbstractDataCenterNemesis(Nemesis, base.AbstractMonitoredNemesis):
    def __init__(self, cluster, schedule=(300, 900), duration=120):
        super(AbstractDataCenterNemesis, self).__init__(schedule=schedule)
        base.AbstractMonitoredNemesis.__init__(self, scope='datacenter')

        self._cluster = cluster
        self._duration = duration

        self._current_dc = None
        self._current_nodes = None

        self._interval_schedule = Schedule.from_tuple_or_int(duration)

    def prepare_state(self):
        try:
            self._dc_to_nodes, self._data_centers = self._validate_datacenters()
            if self._dc_to_nodes is None:
                self.logger.error("Not enough datacenters to run nemesis")
                raise
            self._dc_cycle_iterator = self._create_dc_cycle()
        except Exception as e:
            self.logger.error("Failed to prepare datacenter nemesis: %s", e)
            raise

    def next_schedule(self):
        # If this nemesis instance is active and has a duration set, schedule more frequently 
        # to check for duration expiry
        if self._current_dc is not None:
            # Check if duration has expired and we need to extract
            if datacenter_state.is_duration_expired(self):
                return 1  # Schedule immediately to extract fault
            
            # Verify that this instance is still the active nemesis
            if not datacenter_state.is_active(self):
                self.logger.warning("SCHEDULE: %s (instance %s) lost lock, scheduling immediate extraction", self.__class__.__name__, id(self))
                return 1  # Schedule immediately to extract fault
            
            # Otherwise use the interval schedule for ongoing checks
            return next(self._interval_schedule)
        return super(AbstractDataCenterNemesis, self).next_schedule()

    def inject_fault(self):
        """Inject fault with global coordination to ensure mutual exclusion."""
        self.logger.info("=== INJECT_FAULT CALLED for %s (instance %s) ===", self.__class__.__name__, id(self))
        self.logger.info("Current local state: dc=%s, nodes=%s", self._current_dc, len(self._current_nodes) if self._current_nodes else 0)
        
        # Log current global state for debugging
        debug_state = datacenter_state.debug_state()
        self.logger.info("Current global state: %s", debug_state)
        
        # First check: if this instance is active but duration expired, extract fault
        if self._current_dc is not None:
            self.logger.info("Nemesis has active dc=%s, checking extraction conditions...", self._current_dc)
            
            is_duration_expired = datacenter_state.is_duration_expired(self)
            self.logger.info("Duration expired check: %s", is_duration_expired)
            if is_duration_expired:
                self.logger.info("Duration expired for active %s (instance %s), extracting fault", self.__class__.__name__, id(self))
                self.extract_fault()
                return
                
            is_active = datacenter_state.is_active(self)
            self.logger.info("Global state check: is_active=%s", is_active)
            if not is_active:
                self.logger.warning("Instance %s (instance %s) lost lock, extracting fault", self.__class__.__name__, id(self))
                self.extract_fault()
                return

            # Still active and not expired - this is a duplicate injection attempt
            self.logger.warning("DUPLICATE INJECTION BLOCKED: %s (instance %s) already has active fault (dc %s)", self.__class__.__name__, id(self), self._current_dc)
            return
        
        self.logger.info("No active dc, attempting to acquire new lock...")
        can_start, active_info = datacenter_state.try_acquire_lock(self)
        if not can_start:
            self.logger.info("Failed to acquire lock for %s (instance %s), active nemesis: %s", self.__class__.__name__, id(self), active_info)
            return
        
        try:
            self.logger.info("Acquired lock for %s, starting fault injection", self.__class__.__name__)
            self.logger.info("NEMESIS CONTEXT: dc=%s, nodes=%s, duration=%d", self._current_dc, len(self._current_nodes) if self._current_nodes else 0, self._duration)
            self._do_inject_fault()
        except Exception as e:
            self.logger.error("Failed to inject fault in %s: %s", self.__class__.__name__, e)
            datacenter_state.end_execution(self)

    def _do_inject_fault(self):
        """Actual inject fault implementation - sets up state and calls subclass implementation."""
        if self._current_dc is None:
            if not self._data_centers:
                self.logger.error("No datacenters available for fault injection")
                datacenter_state.end_execution(self)
                return
            
            self._current_dc = next(self._dc_cycle_iterator)  # Get randomly selected dc
            self._current_nodes = self._dc_to_nodes.get(self._current_dc, [])
            self.logger.info("Selected dc %s with %d nodes for %s", self._current_dc, len(self._current_nodes), self.__class__.__name__)

            if not self._current_nodes:
                self.logger.error("No nodes found for dc %s", self._current_dc)
                datacenter_state.end_execution(self)
                return

        self.logger.info("=== STEP 1: INJECTING SPECIFIC FAULT ===")
        self.logger.info("Injecting specific fault for %s on dc %s", self.__class__.__name__, self._current_dc)

        try:
            self._inject_specific_fault()
            self.logger.info("=== STEP 1 COMPLETED: SPECIFIC FAULT INJECTED ===")
        except Exception as e:
            self.logger.error("Failed to inject specific fault for %s: %s", self.__class__.__name__, e)
            datacenter_state.end_execution(self)
            return

        datacenter_state.start_duration(self, self._duration)

    def _inject_specific_fault(self):
        """Subclass-specific fault injection - to be overridden by subclasses."""
        pass

    def extract_fault(self):
        self.logger.info("=== EXTRACTION REQUEST for %s (instance %s) ===", self.__class__.__name__, id(self))
        self.logger.info("Current state: dc=%s, nodes=%s", self._current_dc, len(self._current_nodes) if self._current_nodes else 0)
        
        try:
            self.logger.info("Extracting fault for %s", self.__class__.__name__)
            self._do_extract_fault()
            self.logger.info("=== EXTRACTION COMPLETED SUCCESSFULLY for %s ===", self.__class__.__name__)
        except Exception as e:
            self.logger.error("Exception during extraction for %s: %s", self.__class__.__name__, e)
        finally:
            # Always end execution in global state to prevent lock issues
            datacenter_state.end_execution(self)  # Called while state is still available
            self._current_dc = None           # Clear local state after global cleanup
            self._current_nodes = None

    def _do_extract_fault(self):
        if self._current_dc is None:
            self.logger.info("DataCenterNemesis is not in progress, nothing to extract")
            raise

        self.logger.info("=== EXTRACTION CONTEXT: dc=%s, nodes=%d ===", self._current_dc, len(self._current_nodes) if self._current_nodes else 0)
        self.logger.info("=== STEP 1: EXTRACTING SPECIFIC FAULT ===")
        try:
            self._extract_specific_fault()
            self.logger.info("=== STEP 1 COMPLETED: SPECIFIC FAULT EXTRACTED ===")
        except Exception as e:
            self.logger.error("Failed to extract specific fault: %s", e)
            raise
        
        self._current_dc = None
        self._current_nodes = None

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

    def _create_dc_cycle(self):
        while True:
            for dc in self._data_centers:
                yield dc


class DataCenterStopNodesNemesis(AbstractDataCenterNemesis):
    def __init__(self, cluster, schedule=(300, 900), duration=120):
        super(DataCenterStopNodesNemesis, self).__init__(cluster, schedule=schedule, duration=duration)
    
    def _inject_specific_fault(self):
        async def _async_stop_nodes():
            try:
                stop_tasks = []
                for slot in self._cluster.slots.values():
                    if slot.datacenter == self._current_dc:
                        self.logger.info("Stopping slot %d on host %s", slot.ic_port, slot.host)
                        task = asyncio.create_task(asyncio.to_thread(slot.ssh_command, "sudo systemctl stop kikimr-multi@%d" % slot.ic_port))
                        stop_tasks.append(task)

                slot_results = await asyncio.gather(*stop_tasks)
                self.logger.info("Successfully stopped %d dynamic slots in dc %s", len(stop_tasks), self._current_dc)

                stop_tasks = []
                for node in self._current_nodes:
                    self.logger.info("Stopping storage node %d on host %s", node.node_id, node.host)
                    task = asyncio.create_task(asyncio.to_thread(node.stop))
                    stop_tasks.append(task)
                
                node_results = await asyncio.gather(*stop_tasks)
                self.logger.info("Successfully stopped %d storage nodes in dc %s", len(stop_tasks), self._current_dc)

            except Exception as e:
                self.logger.error("Failed to stop nodes: %s", str(e))
                raise

        # Run the async operations synchronously
        asyncio.run(_async_stop_nodes())

    def _extract_specific_fault(self):
        async def _async_start_nodes():
            try:
                start_tasks = []
                for node in self._current_nodes:
                    self.logger.info("Starting storage node %d on host %s", node.node_id, node.host)
                    task = asyncio.create_task(asyncio.to_thread(node.start))
                    start_tasks.append(task)

                node_results = await asyncio.gather(*start_tasks)
                self.logger.info("Successfully started %d storage nodes in dc %s", len(start_tasks), self._current_dc)

                start_tasks = []
                for slot in self._cluster.slots.values():
                    if slot.datacenter == self._current_dc:
                        self.logger.info("Starting slot %d on host %s", slot.ic_port, slot.host)
                        task = asyncio.create_task(asyncio.to_thread(slot.ssh_command, "sudo systemctl start kikimr-multi@%d" % slot.ic_port))
                        start_tasks.append(task)

                slot_results = await asyncio.gather(*start_tasks)
                self.logger.info("Successfully started %d dynamic slots in dc %s", len(start_tasks), self._current_dc)

            except Exception as e:
                self.logger.error("Failed to start nodes: %s", str(e))
                raise


class DataCenterRouteUnreachableNemesis(AbstractDataCenterNemesis):
    def __init__(self, cluster, schedule=(300, 900), duration=120):
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
                
                self.logger.info("Blocked YDB ports on %d/%d nodes in dc %s", success_count, len(self._current_nodes), self._current_dc)

            except Exception as e:
                self.logger.error("Failed to block YDB ports: %s", str(e))
                raise

        # Run the async operations synchronously
        asyncio.run(_async_block_ports())

    def _extract_specific_fault(self):
        async def _async_restore_ports():
            try:
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

                self.logger.info("Restored YDB ports on %d/%d nodes in dc %s", success_count, len(self._current_nodes), self._current_dc)

            except Exception as e:
                self.logger.error("Failed to restore YDB ports: %s", str(e))
                raise

        # Run the async operations synchronously
        asyncio.run(_async_restore_ports())


class DataCenterIptablesBlockPortsNemesis(AbstractDataCenterNemesis):
    def __init__(self, cluster, schedule=(300, 900), duration=120):
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
            # Try IPv6 first
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
                        self.logger.info("Successfully blocked routes on host %s to current dc %s", other_node.host, self._current_dc)
                        success_count += 1
                    else:
                        self.logger.error("Failed to block routes on host %s to current dc %s", other_node.host, self._current_dc)
                
                self.logger.info("Blocked routes to dc %s: %d/%d operations successful", self._current_dc, success_count, len(results))

            except Exception as e:
                self.logger.error("Failed to block routes to current dc %s: %s", self._current_dc, str(e))
                raise

        # Run the async operations synchronously
        asyncio.run(_async_block_routes())

    def _extract_specific_fault(self):
        async def _async_restore_routes():
            try:
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
                        self.logger.info("Successfully restored routes on host %s to current dc %s", other_node.host, self._current_dc)
                        success_count += 1
                    else:
                        self.logger.error("Failed to restore routes on host %s to current dc %s", other_node.host, self._current_dc)
                
                self.logger.info("Restored routes to dc %s: %d/%d operations successful", self._current_dc, success_count, len(results))

            except Exception as e:
                self.logger.error("Failed to restore routes to current dc %s: %s", self._current_dc, str(e))
                raise

        # Run the async operations synchronously
        asyncio.run(_async_restore_routes())


def datacenter_nemesis_list(cluster):
    return [
        DataCenterStopNodesNemesis(cluster),
        DataCenterRouteUnreachableNemesis(cluster),
        DataCenterIptablesBlockPortsNemesis(cluster)
    ]