import functools
import importlib
import inspect
from contextlib import contextmanager


class BaseManager:
    """A BaseManager class for handling collection operations."""

    @property
    def manager_for(self):  # pragma: no cover
        raise AttributeError("Manager class requires 'manager_for' attribute")

    def __init__(self, *args, **kwargs):
        manager_for = self.manager_for
        module = ".".join(manager_for.split(".")[0:-1])
        obj = manager_for.split(".")[-1]
        target_module = importlib.import_module(module)
        target = getattr(target_module, obj)

        methods = inspect.getmembers(target, predicate=inspect.ismethod)
        for name, method in methods:
            func = functools.partial(method, *args, **kwargs)
            setattr(self, name, func)
        return super().__init__()


class CertificateManager(BaseManager):
    manager_for = "pylxd.models.Certificate"


class InstanceManager(BaseManager):
    manager_for = "pylxd.models.Instance"


class ContainerManager(BaseManager):
    manager_for = "pylxd.models.Container"


class VirtualMachineManager(BaseManager):
    manager_for = "pylxd.models.VirtualMachine"


class ImageManager(BaseManager):
    manager_for = "pylxd.models.Image"


class NetworkManager(BaseManager):
    manager_for = "pylxd.models.Network"


class NetworkForwardManager(BaseManager):
    manager_for = "pylxd.models.NetworkForward"


class OperationManager(BaseManager):
    manager_for = "pylxd.models.Operation"


class ProfileManager(BaseManager):
    manager_for = "pylxd.models.Profile"


class ProjectManager(BaseManager):
    manager_for = "pylxd.models.Project"


class SnapshotManager(BaseManager):
    manager_for = "pylxd.models.Snapshot"


class StoragePoolManager(BaseManager):
    manager_for = "pylxd.models.StoragePool"


class StorageResourcesManager(BaseManager):
    manager_for = "pylxd.models.StorageResources"


class StorageVolumeManager(BaseManager):
    manager_for = "pylxd.models.StorageVolume"


class StorageVolumeSnapshotManager(BaseManager):
    manager_for = "pylxd.models.StorageVolumeSnapshot"


class ClusterMemberManager(BaseManager):
    manager_for = "pylxd.models.ClusterMember"


class ClusterCertificateManager(BaseManager):
    manager_for = "pylxd.models.ClusterCertificate"


class ClusterManager(BaseManager):
    manager_for = "pylxd.models.Cluster"

    def __init__(self, client, *args, **kwargs):
        super().__init__(client, *args, **kwargs)
        self._client = client
        self.members = ClusterMemberManager(client)
        self.certificate = ClusterCertificateManager(client)


@contextmanager
def web_socket_manager(manager):
    try:
        yield manager
    finally:
        manager.stop()
