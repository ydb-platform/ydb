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
        if len(self.daemons) == 0:
            return self.logger.info("Cannot inject the fault. List of daemons is empty, %s", str(self.daemons))

        daemon = random.choice(self.daemons)
        self.logger.info("Kill daemon %s", str(daemon))
        daemon.kill()
        self.on_success_inject_fault()


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


class KillBlockStoreNodeNemesis(AbstractKillDaemonNemesis):
    def __init__(self, cluster, schedule=(120, 180)):
        super(KillBlockStoreNodeNemesis, self).__init__(cluster, schedule=schedule)

    @property
    def daemons(self):
        return list(self.cluster.nbs.values())


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
            KillBlockStoreNodeNemesis(cluster),
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
            self.slots_by_host[slot.host].append(slot)

    def extract_fault(self):
        pass

    def inject_fault(self):
        self.logger.info("Starting next (%d-th) iteration of rolling update process...." % next(self.step_id))
        bucket_id = 0 if len(self.buckets[0]) >= len(self.buckets[1]) else 1

        node = self.buckets[bucket_id].popleft()
        self.buckets[bucket_id ^ 1].append(node)
        self.logger.info("Update nodes on host %s, direction is %s -> %s" % (node.host, bucket_id, bucket_id ^ 1))
        node.switch_version()
        node.kill()

        self.logger.info("Successfully updated version on host %s" % node.host)
        for slot in self.slots_by_host.get(node.host, []):
            slot.kill()

        self.on_success_inject_fault()
