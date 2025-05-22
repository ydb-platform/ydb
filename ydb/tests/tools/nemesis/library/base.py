# -*- coding: utf-8 -*-
import abc

import six
from ydb.tests.tools.nemesis.library import monitor


@six.add_metaclass(abc.ABCMeta)
class AbstractMonitoredNemesis(object):
    def __init__(self, scope=None):
        self.inject_completed = None
        self.inject_in_flight = None
        self.inject_in_flight_value = 0
        self.extract_completed = None
        self.registry = monitor.monitor()
        self.register_counters(scope)

    @property
    def name(self):
        return self.__class__.__name__

    def register_counters(self, scope=None):
        labels = {'nemesis': self.name}
        if scope is not None:
            labels.update({'scope': scope})
        self.inject_completed = self.registry.rate('InjectCompleted', labels)
        self.inject_in_flight = self.registry.int_gauge('InjectInFlight', labels)
        self.extract_completed = self.registry.rate('ExtractCompleted', labels)

    def start_inject_fault(self):
        self.inject_in_flight_value += 1
        self.inject_in_flight.set(self.inject_in_flight_value)

    def on_success_extract_fault(self):
        self.extract_completed.inc()

    def on_success_inject_fault(self):
        if self.inject_in_flight_value > 0:
            self.inject_in_flight_value -= 1
            self.inject_in_flight.set(self.inject_in_flight_value)
        self.inject_completed.inc()
