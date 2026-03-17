# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.


from typing import Any

from ..client.utils import SKIP_IN_PATH, NamespacedClient, _make_path, query_params


class IndexManagementClient(NamespacedClient):
    @query_params()
    async def put_policy(
        self, policy: Any, body: Any = None, params: Any = None, headers: Any = None
    ) -> Any:
        """
        Creates, or updates, a policy.

        :arg policy: The name of the policy
        """
        if policy in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'policy'.")

        return await self.transport.perform_request(
            "PUT",
            _make_path("_plugins", "_ism", "policies", policy),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params()
    async def add_policy(
        self, index: Any, body: Any = None, params: Any = None, headers: Any = None
    ) -> Any:
        """
        Adds a policy to an index. This operation does not change the policy if the index already has one.

        :arg index: The name of the index to add policy on
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'index'.")

        return await self.transport.perform_request(
            "POST",
            _make_path("_plugins", "_ism", "add", index),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params()
    async def get_policy(
        self, policy: Any = None, params: Any = None, headers: Any = None
    ) -> Any:
        """
        Gets the policy by `policy_id`; returns all policies if no policy_id is provided.

        :arg policy: The name of the policy
        """

        return await self.transport.perform_request(
            "GET",
            _make_path("_plugins", "_ism", "policies", policy),
            params=params,
            headers=headers,
        )

    @query_params()
    async def remove_policy_from_index(
        self, index: Any, params: Any = None, headers: Any = None
    ) -> Any:
        """
        Removes any ISM policy from the index.

        :arg index: The name of the index to remove policy on
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'index'.")

        return await self.transport.perform_request(
            "POST",
            _make_path("_plugins", "_ism", "remove", index),
            params=params,
            headers=headers,
        )

    @query_params()
    async def change_policy(
        self, index: Any, body: Any = None, params: Any = None, headers: Any = None
    ) -> Any:
        """
        Updates the managed index policy to a new policy (or to a new version of the policy).

        :arg index: The name of the index to change policy on
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'index'.")

        return await self.transport.perform_request(
            "POST",
            _make_path("_plugins", "_ism", "change_policy", index),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params()
    async def retry(
        self, index: Any, body: Any = None, params: Any = None, headers: Any = None
    ) -> Any:
        """
        Retries the failed action for an index.

        :arg index: The name of the index whose is in a failed state
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'index'.")

        return await self.transport.perform_request(
            "POST",
            _make_path("_plugins", "_ism", "retry", index),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params("show_policy")
    async def explain_index(
        self, index: Any, params: Any = None, headers: Any = None
    ) -> Any:
        """
        Gets the current state of the index.

        :arg index: The name of the index to explain
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'index'.")

        return await self.transport.perform_request(
            "GET",
            _make_path("_plugins", "_ism", "explain", index),
            params=params,
            headers=headers,
        )

    @query_params()
    async def delete_policy(
        self, policy: Any, params: Any = None, headers: Any = None
    ) -> Any:
        """
        Deletes the policy by `policy_id`.

        :arg policy: The name of the policy to delete
        """
        if policy in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'policy'.")

        return await self.transport.perform_request(
            "DELETE",
            _make_path("_plugins", "_ism", "policies", policy),
            params=params,
            headers=headers,
        )
