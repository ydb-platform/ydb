import json

from hvac import utils
from hvac.api.system_backend.system_backend_mixin import SystemBackendMixin


class Policies(SystemBackendMixin):
    def list_acl_policies(self):
        """List all configured acl policies.

        Supported methods:
            GET: /sys/policies/acl. Produces: 200 application/json

        :return: The JSON response of the request.
        :rtype: dict
        """
        api_path = "/v1/sys/policies/acl"
        return self._adapter.list(
            url=api_path,
        )

    def read_acl_policy(self, name):
        """Retrieve the policy body for the named acl policy.

        Supported methods:
            GET: /sys/policies/acl/{name}. Produces: 200 application/json

        :param name: The name of the acl policy to retrieve.
        :type name: str | unicode
        :return: The response of the request
        :rtype: dict
        """
        api_path = utils.format_url("/v1/sys/policies/acl/{name}", name=name)
        return self._adapter.get(
            url=api_path,
        )

    def create_or_update_acl_policy(self, name, policy, pretty_print=True):
        """Add a new or update an existing acl policy.

        Once a policy is updated, it takes effect immediately to all associated users.

        Supported methods:
            PUT: /sys/policies/acl/{name}. Produces: 204 (empty body)

        :param name: Specifies the name of the policy to create.
        :type name: str | unicode
        :param policy: Specifies the policy to create or update.
        :type policy: str | unicode | dict
        :param pretty_print: If True, and provided a dict for the policy argument, send the policy JSON to Vault with
            "pretty" formatting.
        :type pretty_print: bool
        :return: The response of the request.
        :rtype: requests.Response
        """
        if isinstance(policy, dict):
            if pretty_print:
                policy = json.dumps(policy, indent=4, sort_keys=True)
            else:
                policy = json.dumps(policy)
        params = {
            "policy": policy,
        }
        api_path = utils.format_url(f"/v1/sys/policies/acl/{name}", name=name)
        return self._adapter.put(
            url=api_path,
            json=params,
        )

    def delete_acl_policy(self, name):
        """Delete the acl policy with the given name.

        This will immediately affect all users associated with this policy.

        Supported methods:
            DELETE: /sys/policies/acl/{name}. Produces: 204 (empty body)

        :param name: Specifies the name of the policy to delete.
        :type name: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url("/v1/sys/policies/acl/{name}", name=name)
        return self._adapter.delete(
            url=api_path,
        )

    def list_rgp_policies(self):
        """List all configured rgp policies.

        Supported methods:
            GET: /sys/policies/rgp. Produces: 200 application/json

        :return: The JSON response of the request.
        :rtype: dict
        """
        api_path = "/v1/sys/policies/rgp"
        return self._adapter.list(
            url=api_path,
        )

    def read_rgp_policy(self, name):
        """Retrieve the policy body for the named rgp policy.

        Supported methods:
            GET: /sys/policies/rgp/{name}. Produces: 200 application/json

        :param name: The name of the rgp policy to retrieve.
        :type name: str | unicode
        :return: The response of the request
        :rtype: dict
        """
        api_path = utils.format_url("/v1/sys/policies/rgp/{name}", name=name)
        return self._adapter.get(
            url=api_path,
        )

    def create_or_update_rgp_policy(self, name, policy, enforcement_level):
        """Add a new or update an existing rgp policy.

        Once a policy is updated, it takes effect immediately to all associated users.

        Supported methods:
            PUT: /sys/policies/rgp/{name}. Produces: 204 (empty body)

        :param name: Specifies the name of the policy to create.
        :type name: str | unicode
        :param policy: Specifies the policy to create or update.
        :type policy: str | unicode
        :param enforcement_level: Specifies the enforcement level to use. This must be one of advisory, soft-mandatory, or hard-mandatory
        :type enforcement_level: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """
        params = {"policy": policy, "enforcement_level": enforcement_level}
        api_path = utils.format_url(f"/v1/sys/policies/rgp/{name}", name=name)
        return self._adapter.put(
            url=api_path,
            json=params,
        )

    def delete_rgp_policy(self, name):
        """Delete the rgp policy with the given name.

        This will immediately affect all users associated with this policy.

        Supported methods:
            DELETE: /sys/policies/rgp/{name}. Produces: 204 (empty body)

        :param name: Specifies the name of the policy to delete.
        :type name: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url("/v1/sys/policies/rgp/{name}", name=name)
        return self._adapter.delete(
            url=api_path,
        )

    def list_egp_policies(self):
        """List all configured egp policies.

        Supported methods:
            GET: /sys/policies/egp. Produces: 200 application/json

        :return: The JSON response of the request.
        :rtype: dict
        """
        api_path = "/v1/sys/policies/egp"
        return self._adapter.list(
            url=api_path,
        )

    def read_egp_policy(self, name):
        """Retrieve the policy body for the named egp policy.

        Supported methods:
            GET: /sys/policies/egp/{name}. Produces: 200 application/json

        :param name: The name of the egp policy to retrieve.
        :type name: str | unicode
        :return: The response of the request
        :rtype: dict
        """
        api_path = utils.format_url("/v1/sys/policies/egp/{name}", name=name)
        return self._adapter.get(
            url=api_path,
        )

    def create_or_update_egp_policy(self, name, policy, enforcement_level, paths):
        """Add a new or update an existing egp policy.

        Once a policy is updated, it takes effect immediately to all associated users.

        Supported methods:
            PUT: /sys/policies/egp/{name}. Produces: 204 (empty body)

        :param name: Specifies the name of the policy to create.
        :type name: str | unicode
        :param policy: Specifies the policy to create or update.
        :type policy: str | unicode
        :param enforcement_level: Specifies the enforcement level to use. This must be one of advisory, soft-mandatory, or hard-mandatory
        :type enforcement_level: str | unicode
        :param paths: Specifies the paths on which this EGP should be applied.
        :type paths: list
        :return: The response of the request.
        :rtype: requests.Response
        """
        params = {
            "policy": policy,
            "enforcement_level": enforcement_level,
            "paths": paths,
        }
        api_path = utils.format_url(f"/v1/sys/policies/egp/{name}", name=name)
        return self._adapter.put(
            url=api_path,
            json=params,
        )

    def delete_egp_policy(self, name):
        """Delete the egp policy with the given name.

        This will immediately affect all users associated with this policy.

        Supported methods:
            DELETE: /sys/policies/egp/{name}. Produces: 204 (empty body)

        :param name: Specifies the name of the policy to delete.
        :type name: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url("/v1/sys/policies/egp/{name}", name=name)
        return self._adapter.delete(
            url=api_path,
        )
