# -*- coding: utf-8 -*-
import abc
import heapq
import itertools
import logging
import random
import time
import threading
import six.moves
from six.moves import collections_abc
from ydb.tests.library.nemesis import remote_execution


def wrap_in_list(item):
    if isinstance(item, list):
        return item
    else:
        return [item]


logger = logging.getLogger(__name__)


class RunUnderNemesisContext(object):
    def __init__(self, nemesis_process):
        self.__nemesis = nemesis_process

    def __enter__(self):
        if self.__nemesis is not None:
            self.__nemesis.start()

    def __exit__(self, type, value, traceback):
        if self.__nemesis is not None:
            self.__nemesis.stop()
        return False


class NemesisProcess(threading.Thread):
    def __init__(self, nemesis_factory, initial_sleep=10):
        super(NemesisProcess, self).__init__(name='Nemesis')
        self.__nemesis_factory = nemesis_factory
        self.__private_nemesis_list = wrap_in_list(nemesis_factory)
        self.__pq = PriorityQueue()
        self.__is_running = threading.Event()
        self.__finished_running = threading.Event()
        self.__initial_sleep = initial_sleep

        self.daemon = True
        self.__logger = logger.getChild(self.__class__.__name__)

    @property
    def __nemesis_list(self):
        if self.__private_nemesis_list is None:
            self.__private_nemesis_list = wrap_in_list(self.__nemesis_factory())
        return self.__private_nemesis_list

    def stop(self):
        if self.__is_running.is_set():
            self.__is_running.clear()
        else:
            return

        start_time = time.time()
        self.__logger.info("Stopping Nemesis")

        # wait for Nemesis to stop
        finish_time = time.time() + 480
        while not self.__finished_running.is_set() and time.time() < finish_time:
            time.sleep(1)
        finish_time = time.time()
        self.__logger.info(
            "Stopped Nemesis successfully in {} seconds".format(
                int(finish_time - start_time)
            )
        )

    def run(self):
        try:
            self.__run()
        except Exception:
            self.__logger.exception("Some exception in NemesisProcess")
            raise
        finally:
            self.__stop_nemesis()

    def __run(self):
        random.seed()
        self.__pq.clear()
        self.__is_running.set()
        self.__finished_running.clear()

        time.sleep(self.__initial_sleep)
        self.__init_pq()

        while self.__is_running.is_set() and self.__pq:
            while self.__is_running.is_set() and time.time() < self.__pq.peek_priority():
                time.sleep(1)
            if not self.__is_running.is_set():
                break

            nemesis = self.__pq.pop()
            try:
                nemesis.inject_fault()
            except Exception:
                self.__logger.exception(
                    'Inject fault for nemesis = {nemesis} failed.'.format(
                        nemesis=nemesis,
                    )
                )

            priority = time.time() + nemesis.next_schedule()
            self.__pq.add_task(task=nemesis, priority=priority)

    def __init_pq(self):
        self.__logger.info("NemesisProcess started")
        self.__logger.info("self.nemesis_list = " + str(self.__nemesis_list))

        # noinspection PyTypeChecker
        for nemesis in self.__nemesis_list:
            prepared = False
            while not prepared:
                try:
                    self.__logger.info("Preparing nemesis = " + str(nemesis))
                    nemesis.prepare_state()
                    prepared = True
                    self.__logger.info("Preparation succeeded nemesis = " + str(nemesis))
                except Exception:
                    self.__logger.exception("Preparation failed for nemesis = " + str(nemesis))
                    time.sleep(1)

        # noinspection PyTypeChecker
        for nemesis in self.__nemesis_list:
            priority = time.time() + nemesis.next_schedule()
            self.__pq.add_task(nemesis, priority=priority)

        self.__logger.debug("Initial PriorityQueue = " + str(self.__pq))

    def __stop_nemesis(self):
        # Stopping Nemesis
        self.__logger.info("Stopping Nemesis in run()")
        # noinspection PyTypeChecker
        for nemesis in self.__nemesis_list:
            self.__logger.info("Extracting fault for Nemesis = " + str(nemesis))
            try:
                nemesis.extract_fault()
            except Exception:
                logger.exception('Nemesis = {nemesis} extract_fault() failed with exception = '.format(nemesis=nemesis))

        self.__finished_running.set()
        self.__logger.info("Stopped Nemesis successfully in run()")


class Nemesis(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, schedule):
        self.__schedule = Schedule.from_tuple_or_int(schedule)
        self.__logger = logging.getLogger(self.__class__.__name__)

    def next_schedule(self):
        """
        Return amount of second from current time this Nemesis should be called with `inject_fault()`

        :return: amount of seconds to schedule this Nemesis next time
        """
        return next(self.__schedule)

    @abc.abstractmethod
    def prepare_state(self):
        """
        Prepare state of your Nemesis. Called only once on start.

        :return: not specified
        """
        pass

    @abc.abstractmethod
    def inject_fault(self):
        """
        Inject some fault into running cluster.

        :return: not specified
        """
        pass

    @abc.abstractmethod
    def extract_fault(self):
        """
        Cancel all injected fault if this is possible. Some faults can't be canceled (e.g. node formatting).
        Usually called at the end of the Nemesis run.

        :return: not specified
        """
        pass

    @property
    def logger(self):
        """
        Logger for current Nemesis.

        :return: logger
        """
        return self.__logger

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return '{ntype}'.format(ntype=type(self))


class AbstractNemesisNodeTerrorist(Nemesis):
    __metaclass__ = abc.ABCMeta

    def __init__(self, node_list, act_interval, ssh_user=None, remote_sudo_user=None, timeout=60):
        super(AbstractNemesisNodeTerrorist, self).__init__(act_interval)
        self._remote_exec_method = remote_execution.execute_command
        self.__nodes = node_list
        self.__ssh_user = ssh_user
        self.__sudo_user = remote_sudo_user

        self.__timeout = timeout

    def _get_victim_node(self):
        return random.choice(self.__nodes)

    @property
    def timeout(self):
        return self.__timeout

    @abc.abstractmethod
    def _commands_on_node(self, node=None):
        pass

    def _pre_inject_fault(self, node, commands):
        pass

    def _post_inject_fault(self, node, commands):
        pass

    def prepare_state(self):
        pass

    def extract_fault(self):
        pass

    def inject_fault(self):
        success = True
        node = random.choice(self.__nodes)
        node_commands = wrap_in_list(self._commands_on_node(node))

        self._pre_inject_fault(node, node_commands)

        for command in node_commands:
            cmd_result = self._remote_exec_method(self._full_command(node, command))
            success = cmd_result == 0 and success

        self._post_inject_fault(node, node_commands)

        return success

    def _full_command(self, node, node_command):
        if self.__ssh_user is not None:
            address = "%s@%s" % (self.__ssh_user, node)
        else:
            address = node
        if self.__sudo_user is not None:
            if self.__sudo_user == 'root':
                return ["ssh", address, "sudo"] + list(node_command)
            else:
                return [
                    "ssh", address,
                    "sudo", "-u", self.__sudo_user
                ] + list(node_command)
        else:
            return ["ssh", address] + list(node_command)


class Schedule(collections_abc.Iterator):
    def __init__(self, schedule_iterator):
        self.__iterator = schedule_iterator

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.__iterator)

    def next(self):
        return next(self.__iterator)

    @staticmethod
    def combine(list_of_iterators):
        return Schedule(itertools.chain.from_iterable(list_of_iterators))

    @staticmethod
    def cycle(factory_of_list_of_iterators):
        def iterable():
            while True:
                list_of_iterators = factory_of_list_of_iterators()
                for item in list_of_iterators:
                    yield item

        return Schedule(itertools.chain.from_iterable(iterable()))

    @staticmethod
    def from_tuple_or_int(value):
        if isinstance(value, Schedule):
            return value
        elif isinstance(value, int):
            return Schedule(itertools.repeat(value))
        else:
            a, b = value
            return Schedule(six.moves.map(lambda x: random.randint(a, b), itertools.count()))


class RunOnceSchedule(Schedule):
    def __init__(self, time_at):
        super(RunOnceSchedule, self).__init__(iter([time_at, 2**50]))


class PriorityQueue(object):
    def __init__(self):
        super(PriorityQueue, self).__init__()
        self.__heap = []
        self.__count = itertools.count()

    def add_task(self, task, priority):
        entry = priority, next(self.__count), task
        heapq.heappush(self.__heap, entry)

    def pop(self):
        _, _, task = heapq.heappop(self.__heap)
        return task

    def peek(self):
        _, _, task = self.__heap[0]
        return task

    def peek_priority(self):
        priority, _, _ = self.__heap[0]
        return priority

    def clear(self):
        self.__heap = []

    def __len__(self):
        return len(self.__heap)

    def __str__(self):
        # return str(self.__heap)
        return '\n'.join(map(str, sorted([(entry[0], entry[1]) for entry in self.__heap])))


class FakeNemesis(Nemesis):
    """
    >>> n = FakeNemesis(None)
    >>> print(n.logger.name)
    FakeNemesis
    """

    def __init__(self, out_queue, schedule=1):
        super(FakeNemesis, self).__init__(schedule=schedule)
        self.__out_queue = out_queue

    def inject_fault(self):
        self.__out_queue.put('inject_fault')

    def prepare_state(self):
        self.__out_queue.put('prepare_state')

    def extract_fault(self):
        self.__out_queue.put('extract_fault')

    def next_schedule(self):
        n = super(FakeNemesis, self).next_schedule()
        self.__out_queue.put('next_schedule = ' + str(n))
        return n


class ExceptionThrowingNemesis(Nemesis):

    def __init__(self, out_queue, schedule=1):
        super(ExceptionThrowingNemesis, self).__init__(schedule=schedule)
        self.__out_queue = out_queue

    def inject_fault(self):
        # raise AssertionError("ExceptionThrowingNemesis")
        raise RuntimeError("ExceptionThrowingNemesis")

    def prepare_state(self):
        self.__out_queue.put('prepare_state')

    def extract_fault(self):
        self.__out_queue.put('extract_fault')

    def next_schedule(self):
        n = super(ExceptionThrowingNemesis, self).next_schedule()
        self.__out_queue.put('next_schedule = ' + str(n))
        return n


def test_priority_queue():
    """
    >>> pq = PriorityQueue()
    >>> pq.add_task('last', time.time() + 10)
    >>> pq.add_task('first', time.time())
    >>> pq.add_task('task 4', time.time() + 4)
    >>> pq.add_task('task 3', time.time() + 3)
    >>> pq.add_task('task 2', time.time() + 2)
    >>> pq.pop()
    'first'
    >>> pq.peek()
    'task 2'
    >>> len(pq)
    4
    >>> pq.pop()
    'task 2'
    >>> len(pq)
    3
    >>> bool(pq)
    True
    >>> pq.add_task('first', time.time())
    >>> pq.pop()
    'first'
    >>> pq.pop()
    'task 3'
    >>> pq.pop()
    'task 4'
    >>> pq.pop()
    'last'
    >>> bool(pq)
    False
    """


def test_nemesis_process():
    """
    >>> from multiprocessing import Queue
    >>> timeout = 10
    >>> out_queue = Queue()
    >>> np = NemesisProcess(FakeNemesis(out_queue, schedule=1), initial_sleep=0)
    >>> np.start()
    >>> time.sleep(1)
    >>> out_queue.get(timeout=timeout)
    'prepare_state'
    >>> out_queue.get(timeout=timeout)
    'next_schedule = 1'
    >>> out_queue.get(timeout=timeout)
    'inject_fault'
    >>> out_queue.get(timeout=timeout)
    'next_schedule = 1'
    >>> out_queue.get(timeout=timeout)
    'inject_fault'
    >>> out_queue.get(timeout=timeout)
    'next_schedule = 1'

    >>> np.stop()
    >>> out_queue.get(timeout=timeout)
    'extract_fault'

    >>> np = NemesisProcess(ExceptionThrowingNemesis(out_queue, schedule=1), initial_sleep=0)
    >>> np.start()
    >>> time.sleep(3)
    >>> np.stop()
    """


def test_schedule():
    """
    >>> random.seed(123)
    >>> s = Schedule.from_tuple_or_int((10, 20))
    >>> [next(s) for _ in range(10)]
    [10, 14, 11, 16, 14, 11, 10, 16, 18, 18]
    >>> s = Schedule.from_tuple_or_int(15)
    >>> [next(s) for _ in range(10)]
    [15, 15, 15, 15, 15, 15, 15, 15, 15, 15]
    >>> q = Schedule.combine(iter([[1, 2], Schedule.from_tuple_or_int(5)]))
    >>> [next(q) for _ in range(10)]
    [1, 2, 5, 5, 5, 5, 5, 5, 5, 5]
    """
