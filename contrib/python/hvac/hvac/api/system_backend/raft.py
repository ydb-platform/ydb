#!/usr/bin/env python
"""Raft methods module."""
from hvac.api.system_backend.system_backend_mixin import SystemBackendMixin
from hvac import utils, adapters


class Raft(SystemBackendMixin):
    """Raft cluster-related system backend methods.

    When using Shamir seal, as soon as the Vault server is brought up, this API should be invoked
    instead of sys/init. This API completes in 2 phases. Once this is invoked, the joining node
    will receive a challenge from the Raft's leader node. This challenge can be answered by the
    joining node only after a successful unseal. Hence, the joining node should be unsealed using
    the unseal keys of the Raft's leader node.

    Reference: https://www.vaultproject.io/api-docs/system/storage/raft
    """

    def join_raft_cluster(
        self,
        leader_api_addr,
        retry=False,
        leader_ca_cert=None,
        leader_client_cert=None,
        leader_client_key=None,
    ):
        """Join a new server node to the Raft cluster.

        When using Shamir seal, as soon as the Vault server is brought up, this API should be invoked
        instead of sys/init. This API completes in 2 phases. Once this is invoked, the joining node will
        receive a challenge from the Raft's leader node. This challenge can be answered by the joining
        node only after a successful unseal. Hence, the joining node should be unsealed using the unseal
        keys of the Raft's leader node.

        Supported methods:
            POST: /sys/storage/raft/join.

        :param leader_api_addr: Address of the leader node in the Raft cluster to which this node is trying to join.
        :type leader_api_addr: str | unicode
        :param retry: Retry joining the Raft cluster in case of failures.
        :type retry: bool
        :param leader_ca_cert: CA certificate used to communicate with Raft's leader node.
        :type leader_ca_cert: str | unicode
        :param leader_client_cert: Client certificate used to communicate with Raft's leader node.
        :type leader_client_cert: str | unicode
        :param leader_client_key: Client key used to communicate with Raft's leader node.
        :type leader_client_key: str | unicode
        :return: The response of the join_raft_cluster request.
        :rtype: requests.Response
        """
        params = utils.remove_nones(
            {
                "leader_api_addr": leader_api_addr,
                "retry": retry,
                "leader_ca_cert": leader_ca_cert,
                "leader_client_cert": leader_client_cert,
                "leader_client_key": leader_client_key,
            }
        )
        api_path = "/v1/sys/storage/raft/join"
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def read_raft_config(self):
        """Read the details of all the nodes in the raft cluster.

        Supported methods:
            GET: /sys/storage/raft/configuration.

        :return: The response of the read_raft_config request.
        :rtype: requests.Response
        """
        api_path = "/v1/sys/storage/raft/configuration"
        return self._adapter.get(
            url=api_path,
        )

    def remove_raft_node(self, server_id):
        """Remove a node from the raft cluster.

        Supported methods:
            POST: /sys/storage/raft/remove-peer.

        :param server_id: The ID of the node to remove.
        :type server_id: str
        :return: The response of the remove_raft_node request.
        :rtype: requests.Response
        """
        params = {
            "server_id": server_id,
        }
        api_path = "/v1/sys/storage/raft/remove-peer"
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def take_raft_snapshot(self):
        """Returns a snapshot of the current state of the raft cluster.

        The snapshot is returned as binary data and should be redirected to a file.

        This endpoint will ignore your chosen adapter and always uses a RawAdapter.

        Supported methods:
            GET: /sys/storage/raft/snapshot.

        :return: The response of the snapshot request.
        :rtype: requests.Response
        """
        api_path = "/v1/sys/storage/raft/snapshot"
        raw_adapter = adapters.RawAdapter.from_adapter(self._adapter)
        return raw_adapter.get(
            url=api_path,
            stream=True,
        )

    def restore_raft_snapshot(self, snapshot):
        """Install the provided snapshot, returning the cluster to the state defined in it.

        Supported methods:
            POST: /sys/storage/raft/snapshot.

        :param snapshot: Previously created raft snapshot / binary data.
        :type snapshot: bytes
        :return: The response of the restore_raft_snapshot request.
        :rtype: requests.Response
        """
        api_path = "/v1/sys/storage/raft/snapshot"
        return self._adapter.post(
            url=api_path,
            data=snapshot,
        )

    def force_restore_raft_snapshot(self, snapshot):
        """Installs the provided snapshot, returning the cluster to the state defined in it.

        This is same as writing to /sys/storage/raft/snapshot except that this bypasses checks
        ensuring the Autounseal or shamir keys are consistent with the snapshot data.

        Supported methods:
            POST: /sys/storage/raft/snapshot-force.

        :param snapshot: Previously created raft snapshot / binary data.
        :type snapshot: bytes
        :return: The response of the force_restore_raft_snapshot request.
        :rtype: requests.Response
        """
        api_path = "/v1/sys/storage/raft/snapshot-force"
        return self._adapter.post(
            url=api_path,
            data=snapshot,
        )

    def read_raft_auto_snapshot_status(self, name):
        """Read the status of the raft auto snapshot.

        Supported methods:
            GET: /sys/storage/raft/snapshot-auto/status/:name. Produces: 200 application/json

        :param name: The name of the snapshot configuration.
        :type name: str
        :return: The response of the read_raft_auto_snapshot_status request.
        :rtype: requests.Response
        """
        api_path = f"/v1/sys/storage/raft/snapshot-auto/status/{name}"
        return self._adapter.get(
            url=api_path,
        )

    def read_raft_auto_snapshot_config(self, name):
        """Read the configuration of the raft auto snapshot.

        Supported methods:
            GET: /sys/storage/raft/snapshot-auto/config/:name. Produces: 200 application/json

        :param name: The name of the snapshot configuration.
        :type name: str
        :return: The response of the read_raft_auto_snapshot_config request.
        :rtype: requests.Response
        """
        api_path = f"/v1/sys/storage/raft/snapshot-auto/config/{name}"
        return self._adapter.get(
            url=api_path,
        )

    def list_raft_auto_snapshot_configs(self):
        """List the configurations of the raft auto snapshot.

        Supported methods:
            LIST: /sys/storage/raft/snapshot-auto/config. Produces: 200 application/json

        :return: The response of the list_raft_auto_snapshot_configs request.
        :rtype: requests.Response
        """
        api_path = "/v1/sys/storage/raft/snapshot-auto/config"
        return self._adapter.list(
            url=api_path,
        )

    def create_or_update_raft_auto_snapshot_config(
        self, name, interval, storage_type, retain=1, **kwargs
    ):
        """Create or update the configuration of the raft auto snapshot.

        Supported methods:
            POST: /sys/storage/raft/snapshot-auto/config/:name. Produces: 204 application/json

        :param name: The name of the snapshot configuration.
        :type name: str
        :param interval: The interval at which snapshots should be taken.
        :type interval: str
        :param storage_type: The type of storage to use for the snapshot.
        :type storage_type: str
        :param retain: The number of snapshots to retain. Default is 1
        :type retain: int
        :param kwargs: Additional parameters to send in the request. Should be params specific to the storage type.
        :type kwargs: dict
        :return: The response of the create_or_update_raft_auto_snapshot_config request.
        :rtype: requests.Response
        """
        params = utils.remove_nones(
            {
                "interval": interval,
                "storage_type": storage_type,
                "retain": retain,
                **kwargs,
            }
        )

        api_path = f"/v1/sys/storage/raft/snapshot-auto/config/{name}"
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def delete_raft_auto_snapshot_config(self, name):
        """Delete the configuration of the raft auto snapshot.

        Supported methods:
            DELETE: /sys/storage/raft/snapshot-auto/config/:name. Produces: 204 application/json

        :param name: The name of the snapshot configuration.
        :type name: str
        :return: The response of the delete_raft_auto_snapshot_config request.
        :rtype: requests.Response
        """
        api_path = f"/v1/sys/storage/raft/snapshot-auto/config/{name}"
        return self._adapter.delete(
            url=api_path,
        )
