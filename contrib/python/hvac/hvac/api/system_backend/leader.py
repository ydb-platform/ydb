from hvac.api.system_backend.system_backend_mixin import SystemBackendMixin


class Leader(SystemBackendMixin):
    def read_leader_status(self):
        """Read the high availability status and current leader instance of Vault.

        Supported methods:
            GET: /sys/leader. Produces: 200 application/json

        :return: The JSON response of the request.
        :rtype: dict
        """
        api_path = "/v1/sys/leader"
        return self._adapter.get(
            url=api_path,
        )

    def step_down(self):
        """Force the node to give up active status.

        When executed against a non-active node, i.e. a standby or performance
        standby node, the request will be forwarded to the active node.
        Note that the node will sleep for ten seconds before attempting to grab
        the active lock again, but if no standby nodes grab the active lock in
        the interim, the same node may become the active node again. Requires a
        token with root policy or sudo capability on the path.

        :return: The JSON response of the request.
        :rtype: dict
        """
        api_path = "/v1/sys/step-down"
        return self._adapter.put(
            url=api_path,
        )
