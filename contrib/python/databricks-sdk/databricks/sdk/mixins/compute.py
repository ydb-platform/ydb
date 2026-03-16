import datetime
import logging
import re
import time
from dataclasses import dataclass
from typing import Optional

from databricks.sdk.core import DatabricksError
from databricks.sdk.errors import OperationFailed
from databricks.sdk.service import compute

_LOG = logging.getLogger("databricks.sdk")


@dataclass
class SemVer:
    major: int
    minor: int
    patch: int
    pre_release: Optional[str] = None
    build: Optional[str] = None

    # official https://semver.org/ recommendation: https://regex101.com/r/Ly7O1x/
    # with addition of "x" wildcards for minor/patch versions. Also, patch version may be omitted.
    _pattern = re.compile(
        r"^"
        r"(?P<major>0|[1-9]\d*)\.(?P<minor>x|0|[1-9]\d*)(\.(?P<patch>x|0|[1-9x]\d*))?"
        r"(?:-(?P<pre_release>(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)"
        r"(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?"
        r"(?:\+(?P<build>[0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$"
    )

    @classmethod
    def parse(cls, v: str) -> "SemVer":
        if not v:
            raise ValueError(f"Not a valid SemVer: {v}")
        if v[0] != "v":
            v = f"v{v}"
        m = cls._pattern.match(v[1:])
        if not m:
            raise ValueError(f"Not a valid SemVer: {v}")
        # patch and/or minor versions may be wildcards.
        # for now, we're converting wildcards to zeroes.
        minor = m.group("minor")
        try:
            patch = m.group("patch")
        except IndexError:
            patch = 0
        return SemVer(
            major=int(m.group("major")),
            minor=0 if minor == "x" else int(minor),
            patch=0 if patch == "x" or patch is None else int(patch),
            pre_release=m.group("pre_release"),
            build=m.group("build"),
        )

    def __lt__(self, other: "SemVer"):
        if not other:
            return False
        if self.major != other.major:
            return self.major < other.major
        if self.minor != other.minor:
            return self.minor < other.minor
        if self.patch != other.patch:
            return self.patch < other.patch
        if self.pre_release != other.pre_release:
            return self.pre_release < other.pre_release
        if self.build != other.build:
            return self.build < other.build
        return False


class ClustersExt(compute.ClustersAPI):
    __doc__ = compute.ClustersAPI.__doc__

    def select_spark_version(
        self,
        long_term_support: bool = False,
        beta: bool = False,
        latest: bool = True,
        ml: bool = False,
        genomics: bool = False,
        gpu: bool = False,
        scala: str = "2.12",
        spark_version: str = None,
        photon: bool = False,
        graviton: bool = False,
    ) -> str:
        """Selects the latest Databricks Runtime Version.

        :param long_term_support: bool
        :param beta: bool
        :param latest: bool
        :param ml: bool
        :param genomics: bool
        :param gpu: bool
        :param scala: str
        :param spark_version: str
        :param photon: bool
        :param graviton: bool

        :returns: `spark_version` compatible string
        """
        # Logic ported from https://github.com/databricks/databricks-sdk-go/blob/main/service/compute/spark_version.go
        versions = []
        sv = self.spark_versions()
        for version in sv.versions:
            if "-scala" + scala not in version.key:
                continue
            matches = (
                ("apache-spark-" not in version.key)
                and (("-ml-" in version.key) == ml)
                and (("-hls-" in version.key) == genomics)
                and (("-gpu-" in version.key) == gpu)
                and (("-photon-" in version.key) == photon)
                and (("-aarch64-" in version.key) == graviton)
                and (("Beta" in version.name) == beta)
            )
            if matches and long_term_support:
                matches = matches and (("LTS" in version.name) or ("-esr-" in version.key))
            if matches and spark_version:
                matches = matches and ("Apache Spark " + spark_version in version.name)
            if matches:
                versions.append(version.key)
        if len(versions) < 1:
            raise ValueError("spark versions query returned no results")
        if len(versions) > 1:
            if not latest:
                raise ValueError("spark versions query returned multiple results")
            versions = sorted(versions, key=SemVer.parse, reverse=True)
        return versions[0]

    @staticmethod
    def _node_sorting_tuple(item: compute.NodeType) -> tuple:
        local_disks = local_disk_size_gb = local_nvme_disk = local_nvme_disk_size_gb = 0
        if item.node_instance_type is not None:
            local_disks = item.node_instance_type.local_disks
            local_nvme_disk = item.node_instance_type.local_nvme_disks
            local_disk_size_gb = item.node_instance_type.local_disk_size_gb
            local_nvme_disk_size_gb = item.node_instance_type.local_nvme_disk_size_gb
        return (
            item.is_deprecated,
            item.num_cores,
            item.memory_mb,
            local_disks,
            local_disk_size_gb,
            local_nvme_disk,
            local_nvme_disk_size_gb,
            item.num_gpus,
            item.instance_type_id,
        )

    @staticmethod
    def _should_node_be_skipped(nt: compute.NodeType) -> bool:
        if not nt.node_info:
            return False
        if not nt.node_info.status:
            return False
        val = compute.CloudProviderNodeStatus
        for st in nt.node_info.status:
            if st in (
                val.NOT_AVAILABLE_IN_REGION,
                val.NOT_ENABLED_ON_SUBSCRIPTION,
            ):
                return True
        return False

    def select_node_type(
        self,
        min_memory_gb: int = None,
        gb_per_core: int = None,
        min_cores: int = None,
        min_gpus: int = None,
        local_disk: bool = None,
        local_disk_min_size: int = None,
        category: str = None,
        photon_worker_capable: bool = None,
        photon_driver_capable: bool = None,
        graviton: bool = None,
        is_io_cache_enabled: bool = None,
        support_port_forwarding: bool = None,
        fleet: str = None,
    ) -> str:
        """Selects smallest available node type given the conditions.

        :param min_memory_gb: int
        :param gb_per_core: int
        :param min_cores: int
        :param min_gpus: int
        :param local_disk: bool
        :param local_disk_min_size: bool
        :param category: bool
        :param photon_worker_capable: bool
        :param photon_driver_capable: bool
        :param graviton: bool
        :param is_io_cache_enabled: bool
        :param support_port_forwarding: bool
        :param fleet: bool

        :returns: `node_type` compatible string
        """
        # Logic ported from https://github.com/databricks/databricks-sdk-go/blob/main/service/clusters/node_type.go
        res = self.list_node_types()
        types = sorted(res.node_types, key=self._node_sorting_tuple)
        for nt in types:
            if self._should_node_be_skipped(nt):
                continue
            gbs = nt.memory_mb // 1024
            if fleet is not None and fleet not in nt.node_type_id:
                continue
            if min_memory_gb is not None and gbs < min_memory_gb:
                continue
            if gb_per_core is not None and gbs // nt.num_cores < gb_per_core:
                continue
            if min_cores is not None and nt.num_cores < min_cores:
                continue
            if (min_gpus is not None and nt.num_gpus < min_gpus) or (min_gpus == 0 and nt.num_gpus > 0):
                continue
            if local_disk or local_disk_min_size is not None:
                instance_type = nt.node_instance_type
                local_disks = int(instance_type.local_disks) if instance_type.local_disks else 0
                local_nvme_disks = int(instance_type.local_nvme_disks) if instance_type.local_nvme_disks else 0
                if instance_type is None or (local_disks < 1 and local_nvme_disks < 1):
                    continue
                local_disk_size_gb = instance_type.local_disk_size_gb if instance_type.local_disk_size_gb else 0
                local_nvme_disk_size_gb = (
                    instance_type.local_nvme_disk_size_gb if instance_type.local_nvme_disk_size_gb else 0
                )
                all_disks_size = local_disk_size_gb + local_nvme_disk_size_gb
                if local_disk_min_size is not None and all_disks_size < local_disk_min_size:
                    continue
            if category is not None and not nt.category.lower() == category.lower():
                continue
            if is_io_cache_enabled and not nt.is_io_cache_enabled:
                continue
            if support_port_forwarding and not nt.support_port_forwarding:
                continue
            if photon_driver_capable and not nt.photon_driver_capable:
                continue
            if photon_worker_capable and not nt.photon_worker_capable:
                continue
            if graviton and nt.is_graviton != graviton:
                continue
            return nt.node_type_id
        raise ValueError("cannot determine smallest node type")

    def ensure_cluster_is_running(self, cluster_id: str) -> None:
        """Ensures that given cluster is running, regardless of the current state"""
        timeout = datetime.timedelta(minutes=20)
        deadline = time.time() + timeout.total_seconds()
        while time.time() < deadline:
            try:
                state = compute.State
                info = self.get(cluster_id)
                if info.state == state.RUNNING:
                    return
                elif info.state == state.TERMINATED:
                    self.start(cluster_id).result()
                    return
                elif info.state == state.TERMINATING:
                    self.wait_get_cluster_terminated(cluster_id)
                    self.start(cluster_id).result()
                    return
                elif info.state in (
                    state.PENDING,
                    state.RESIZING,
                    state.RESTARTING,
                ):
                    self.wait_get_cluster_running(cluster_id)
                    return
                elif info.state in (state.ERROR, state.UNKNOWN):
                    raise RuntimeError(f"Cluster {info.cluster_name} is {info.state}: {info.state_message}")
            except DatabricksError as e:
                if e.error_code == "INVALID_STATE":
                    _LOG.debug(f"Cluster was started by other process: {e} Retrying.")
                    continue
                raise e
            except OperationFailed as e:
                _LOG.debug("Operation failed, retrying", exc_info=e)
        raise TimeoutError(f"timed out after {timeout}")
