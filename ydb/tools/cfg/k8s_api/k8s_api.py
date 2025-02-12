import logging
import threading
import hashlib

from kubernetes import client, config

from ydb.tools.cfg.walle import HostsInformationProvider

logger = logging.getLogger()


class K8sApiHostsInformationProvider(HostsInformationProvider):
    def __init__(self, kubeconfig):
        self._kubeconfig = kubeconfig
        self._cache = {}
        self._timeout_seconds = 5
        self._retry_count = 10
        self._lock = threading.Lock()
        self._k8s_rack_label = None
        self._k8s_dc_label = None

    def _init_k8s_labels(self, rack_label, dc_label):
        self._k8s_rack_label = rack_label
        self._k8s_dc_label = dc_label
        logger.info(f"initialized rack with {rack_label}, dc with {dc_label}")
        self._populate_cache()

    def _populate_cache(self):
        try:
            config.load_kube_config(config_file=self._kubeconfig)
            with client.ApiClient() as api_client:
                v1 = client.CoreV1Api(api_client)
                nodes = v1.list_node().items

            with self._lock:
                for node in nodes:
                    hostname = node.metadata.name
                    self._cache[hostname] = node.metadata.labels
        except client.exceptions.ApiException as e:
            print(f"Failed to fetch node labels: {e}")

    def get_rack(self, hostname):
        if self._k8s_rack_label is None:
            return "defaultRack"

        labels = self._cache.get(hostname)
        if labels and self._k8s_rack_label in labels:
            return labels[self._k8s_rack_label]
        logging.info(f"rack not found for hostname {hostname}")
        return ""

    def get_datacenter(self, hostname):
        if self._k8s_dc_label is None:
            return "defaultDC"

        labels = self._cache.get(hostname)
        if labels and self._k8s_dc_label in labels:
            return labels[self._k8s_dc_label]
        return ""

    def get_body(self, hostname):
        # Just something for now, please present better ideas
        hex_digest = hashlib.md5(hostname.encode()).hexdigest()
        decimal_value = int(hex_digest, 16) % (1 << 31)
        return decimal_value
