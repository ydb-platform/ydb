# -*- coding: utf-8 -*-
import random
import signal
import abc
import six
import itertools
import collections

from ydb.tests.library.nemesis.nemesis_core import Nemesis, Schedule
from ydb.tests.tools.nemesis.library import base


@six.add_metaclass(abc.ABCMeta)
class AbstractKillDaemonNemesis(Nemesis, base.AbstractMonitoredNemesis):
    def __init__(self, cluster, schedule):
        base.AbstractMonitoredNemesis.__init__(self, scope='node')
        Nemesis.__init__(self, schedule=schedule)
        self.cluster = cluster

    def prepare_state(self):
        self.logger.info("Daemons to kill = {}".format(str(self.daemons)))

    @property
    @abc.abstractmethod
    def daemons(self):
        pass

    def extract_fault(self):
        pass

    def inject_fault(self):
        self.logger.info("=== INJECT_FAULT START: %s ===", str(self))
        self.logger.info("Available daemons: %s (count: %d)", str(self.daemons), len(self.daemons))
        
        if len(self.daemons) == 0:
            self.logger.warning("Cannot inject the fault. List of daemons is empty, %s", str(self.daemons))
            self.logger.info("=== INJECT_FAULT SKIPPED (no daemons): %s ===", str(self))
            return

        daemon = random.choice(self.daemons)
        self.logger.info("Selected daemon to kill: %s", str(daemon))
        self.logger.info("Kill daemon %s", str(daemon))
        
        try:
            daemon.kill()
            self.logger.info("Successfully killed daemon: %s", str(daemon))
            self.on_success_inject_fault()
            self.logger.info("=== INJECT_FAULT SUCCESS: %s ===", str(self))
        except Exception as e:
            self.logger.error("Failed to kill daemon %s: %s", str(daemon), str(e))
            self.logger.info("=== INJECT_FAULT FAILED: %s ===", str(self))


class KillSlotNemesis(AbstractKillDaemonNemesis):
    def __init__(self, cluster, schedule=(60, 180)):
        super(KillSlotNemesis, self).__init__(cluster, schedule=schedule)

    @property
    def daemons(self):
        return list(self.cluster.slots.values())


class KillNodeNemesis(AbstractKillDaemonNemesis):
    def __init__(self, cluster, schedule=(120, 180)):
        super(KillNodeNemesis, self).__init__(cluster, schedule=schedule)

    @property
    def daemons(self):
        return list(self.cluster.nodes.values())


@six.add_metaclass(abc.ABCMeta)
class AbstractSerialDaemonKillNemesis(Nemesis, base.AbstractMonitoredNemesis):
    def __init__(self, cluster, schedule, schedule_between_kills):
        base.AbstractMonitoredNemesis.__init__(self, scope='node')
        Nemesis.__init__(self, schedule=schedule)
        self.cluster = cluster
        self.target = None
        self.target_cnt = 0
        self.schedule_between_kills = Schedule.from_tuple_or_int(schedule_between_kills)

    def next_schedule(self):
        if self.target is not None:
            return next(self.schedule_between_kills)
        else:
            return super(AbstractSerialDaemonKillNemesis, self).next_schedule()

    def prepare_state(self):
        self.logger.info("Daemons to kill = {}".format(str(self.daemons)))

    @property
    @abc.abstractmethod
    def daemons(self):
        pass

    def extract_fault(self):
        pass

    def inject_fault(self):
        if len(self.daemons) == 0:
            return self.logger.info(
                "Cannot inject the fault. List of daemons is empty, %s", str(
                    self.daemons
                )
            )

        if self.target is None:
            self.target = random.choice(self.daemons)
            self.target_cnt = random.randint(1, 4)

        self.kill_target_daemon()
        self.on_success_inject_fault()

    def kill_target_daemon(self):
        self.logger.info("Killing node = " + str(self.target))
        self.target.kill()
        self.target_cnt -= 1
        if self.target_cnt <= 0:
            self.target = None


class SerialKillNodeNemesis(AbstractSerialDaemonKillNemesis):
    def __init__(self, cluster, schedule=(240, 300), schedule_between_kills=(30, 60)):
        super(SerialKillNodeNemesis, self).__init__(cluster, schedule, schedule_between_kills)

    @property
    def daemons(self):
        return list(self.cluster.nodes.values())


class SerialKillSlotsNemesis(AbstractSerialDaemonKillNemesis):
    def __init__(self, cluster, schedule=(240, 300), schedule_between_kills=(30, 60)):
        super(SerialKillSlotsNemesis, self).__init__(cluster, schedule, schedule_between_kills)

    @property
    def daemons(self):
        return list(self.cluster.slots.values())


def nodes_nemesis_list(cluster):
    scale_per_cluster = max(1, int(len(cluster.nodes.values()) / 8))
    nemesis_list = [
        KillNodeNemesis(cluster),

        SerialKillNodeNemesis(cluster),
        SerialKillSlotsNemesis(cluster),

        RollingUpdateClusterNemesis(cluster),
    ]

    for _ in range(scale_per_cluster):
        nemesis_list.extend([
            KillSlotNemesis(cluster),
        ])
    return nemesis_list


class StopStartNodeNemesis(Nemesis, base.AbstractMonitoredNemesis):
    def __init__(self, cluster, schedule=(300, 600)):
        super(StopStartNodeNemesis, self).__init__(schedule=schedule)
        base.AbstractMonitoredNemesis.__init__(self, scope='node')
        self._cluster = cluster
        self._processes = self._cluster.nodes.values()
        self._current_process = None
        self.__stop_interval_schedule = Schedule.from_tuple_or_int(30)
        self._can_stop = len(self._cluster.nodes.values()) >= 8

    def next_schedule(self):
        if self._current_process is not None:
            return next(self.__stop_interval_schedule)
        return super(StopStartNodeNemesis, self).next_schedule()

    def prepare_state(self):
        self.logger.info("Nodes to stop/start = " + str(self._processes))

    def inject_fault(self):
        if not self._can_stop:
            self.logger.info("Node stop is prohibited.")
            return

        # we keep exactly one process stopped
        if self.extract_fault():
            return

        self.start_inject_fault()
        self._current_process = random.choice(self._processes)
        self.logger.info("Stopping node = %s", str(self._current_process))
        self._current_process.stop()
        self.on_success_inject_fault()

    def extract_fault(self):
        if self._current_process is not None:
            self.logger.info("Starting node = %s", str(self._current_process))
            self._current_process.start()
            self._current_process = None
            self.on_success_extract_fault()
            return True
        return False


class SuspendNodeNemesis(Nemesis, base.AbstractMonitoredNemesis):
    def __init__(self, cluster, schedule=(600, 1200)):
        super(SuspendNodeNemesis, self).__init__(schedule=schedule)
        base.AbstractMonitoredNemesis.__init__(self, scope='node')
        self._cluster = cluster
        self._processes = self._cluster.nodes.values() + self._cluster.slots.values()
        self._process_to_wake_up = None
        self.__stop_time_schedule = Schedule.from_tuple_or_int((10, 30))
        self._can_suspend = True

    def next_schedule(self):
        if self._process_to_wake_up is not None:
            return next(self.__stop_time_schedule)
        return super(SuspendNodeNemesis, self).next_schedule()

    def prepare_state(self):
        self.logger.info("Nodes to suspend = " + str(self._processes))
        self._can_suspend = len(self._cluster.nodes.values()) >= 8

    def inject_fault(self):
        if not self._can_suspend:
            self.logger.info("Node suspend is prohibited")
            return

        if self.extract_fault():
            return

        self.start_inject_fault()
        self._process_to_wake_up = random.choice(self._processes)
        self.logger.info("Suspending node = " + str(self._process_to_wake_up))
        self._process_to_wake_up.send_signal(signal.SIGSTOP)
        self.on_success_inject_fault()

    def extract_fault(self):
        if self._process_to_wake_up is not None:
            self.logger.info("Continuing node = " + str(self._process_to_wake_up))
            self._process_to_wake_up.send_signal(signal.SIGCONT)
            self._process_to_wake_up = None
            self.on_success_extract_fault()
            return True
        return False


class RollingUpdateClusterNemesis(Nemesis, base.AbstractMonitoredNemesis):
    def __init__(self, cluster, schedule=(60, 70)):
        super(RollingUpdateClusterNemesis, self).__init__(schedule=schedule)
        base.AbstractMonitoredNemesis.__init__(self, scope='node')
        self.cluster = cluster
        self.buckets = {0: collections.deque(self.cluster.nodes.values()), 1: collections.deque()}
        self.step_id = itertools.count(start=1)
        self.slots_by_host = collections.defaultdict(list)

    def prepare_state(self):
        self.logger.info("Initializing rolling update nemesis....")
        self.logger.info("Nodes to update = %s", str(self.cluster.nodes.values()))
        self.logger.info("Slots to update = %s", str(self.cluster.slots.values()))

        for slot in self.cluster.slots.values():
            self.logger.info("Adding slot %s (host: %s) to slots_by_host", str(slot), slot.host)
            self.slots_by_host[slot.host].append(slot)
        
        # Log the final mapping
        for host, slots in self.slots_by_host.items():
            self.logger.info("Host %s has %d slots: %s", host, len(slots), [str(slot) for slot in slots])

    def extract_fault(self):
        pass

    def inject_fault(self):
        self.logger.info("=== INJECT_FAULT START: RollingUpdateClusterNemesis ===")
        self.logger.info("Starting next (%d-th) iteration of rolling update process...." % next(self.step_id))
        
        # Log current state
        self.logger.info("Current cluster state - nodes: %d, slots: %d", 
                        len(self.cluster.nodes), len(self.cluster.slots))
        self.logger.info("Bucket 0 size: %d, Bucket 1 size: %d", 
                        len(self.buckets[0]), len(self.buckets[1]))
        
        bucket_id = 0 if len(self.buckets[0]) >= len(self.buckets[1]) else 1
        self.logger.info("Selected bucket_id: %d (bucket[0] size: %d, bucket[1] size: %d)", 
                        bucket_id, len(self.buckets[0]), len(self.buckets[1]))

        node = self.buckets[bucket_id].popleft()
        self.buckets[bucket_id ^ 1].append(node)
        self.logger.info("Update nodes on host %s, direction is %s -> %s" % (node.host, bucket_id, bucket_id ^ 1))
        
        try:
            self.logger.info("Calling node.switch_version() for host: %s", node.host)
            node.switch_version()
            self.logger.info("Successfully switched version for host: %s", node.host)
            
            self.logger.info("Calling node.kill() for host: %s", node.host)
            node.kill()
            self.logger.info("Successfully killed node for host: %s", node.host)

            self.logger.info("Successfully updated version on host %s" % node.host)
            
            slots_for_host = self.slots_by_host.get(node.host, [])
            self.logger.info("Killing %d slots for host: %s", len(slots_for_host), node.host)
            for i, slot in enumerate(slots_for_host):
                try:
                    self.logger.info("Killing slot %d/%d: %s (type: %s)", i+1, len(slots_for_host), str(slot), type(slot).__name__)
                    slot.kill()
                    self.logger.info("Successfully killed slot: %s", str(slot))
                except Exception as e:
                    self.logger.error("Failed to kill slot %s: %s", str(slot), str(e))
                    self.logger.exception("Exception details for slot %s:", str(slot))

            self.on_success_inject_fault()
            self.logger.info("=== INJECT_FAULT SUCCESS: RollingUpdateClusterNemesis ===")
            
        except Exception as e:
            self.logger.error("Failed to perform rolling update for host %s: %s", node.host, str(e))
            self.logger.info("=== INJECT_FAULT FAILED: RollingUpdateClusterNemesis ===")
