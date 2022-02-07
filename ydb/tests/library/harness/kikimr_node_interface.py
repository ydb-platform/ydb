# -*- coding: utf-8 -*-
import abc
import signal

from ydb.tests.library.harness import kikimr_monitoring as monitoring


class NodeInterface(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        pass

    @abc.abstractmethod
    def stop(self):
        pass

    @abc.abstractmethod
    def kill(self):
        pass

    @abc.abstractmethod
    def send_signal(self, signal):
        pass

    @abc.abstractmethod
    def start(self):
        pass

    @abc.abstractmethod
    def is_alive(self):
        pass

    def freeze(self):
        self.send_signal(signal.SIGSTOP)
        return self

    def unfreeze(self):
        self.send_signal(signal.SIGCONT)
        return self

    @property
    def monitor(self):
        return monitoring.KikimrMonitor(self.host, self.mon_port)

    @abc.abstractproperty
    def cwd(self):
        pass

    @abc.abstractproperty
    def host(self):
        pass

    @abc.abstractproperty
    def pid(self):
        pass

    def __repr__(self):
        return '{host}:{port}/{node_id}'.format(
            host=self.host,
            port=self.grpc_port,
            node_id=self.node_id
        )

    def __str__(self):
        return '{host}:{port}/{node_id}'.format(
            host=self.host,
            port=self.grpc_port,
            node_id=self.node_id
        )

    @abc.abstractmethod
    def format_pdisk(self, pdisk_id):
        pass

    @property
    def grpc_endpoint(self):
        return "grpc://{}:{}".format(self.host, self.grpc_port)
