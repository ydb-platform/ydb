# -*- coding: utf-8 -*-
import abc
import six

from . import daemon
from . import param_constants


@six.add_metaclass(abc.ABCMeta)
class BlockStoreNode(object):
    @abc.abstractmethod
    def start(self):
        pass

    @abc.abstractmethod
    def stop(self):
        pass

    @abc.abstractmethod
    def prepare_artifacts(self):
        pass


class ExternalNetworkBlockStoreNode(daemon.ExternalNodeDaemon):
    def __init__(self, host):
        super(ExternalNetworkBlockStoreNode, self).__init__(host)
        self.ic_port = 29010

    @property
    def logs_directory(self):
        return "/Berkanavt/nbs-server/logs/"

    def start(self):
        return self.ssh_command(
            "sudo service nbs-server start"
        )

    def stop(self):
        return self.ssh_command(
            "sudo service nbs-server stop"
        )

    def prepare_artifacts(self, cluster_yml):
        self.ssh_command(
            param_constants.generate_configs_cmd(
                "--nbs", param_constants.blockstore_configs_deploy_path
            )
        )
