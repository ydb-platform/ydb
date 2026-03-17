# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.


from typing import Any

from ..client.utils import NamespacedClient, _make_path, query_params


class AlertingClient(NamespacedClient):
    @query_params()
    def search_monitor(self, body: Any, params: Any = None, headers: Any = None) -> Any:
        """
        Returns the search result for a monitor.

        :arg monitor_id: The configuration for the monitor we are trying to search
        """
        return self.transport.perform_request(
            "GET",
            _make_path("_plugins", "_alerting", "monitors", "_search"),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params()
    def get_monitor(
        self, monitor_id: Any, params: Any = None, headers: Any = None
    ) -> Any:
        """
        Returns the details of a specific monitor.

        :arg monitor_id: The id of the monitor we are trying to fetch
        """
        return self.transport.perform_request(
            "GET",
            _make_path("_plugins", "_alerting", "monitors", monitor_id),
            params=params,
            headers=headers,
        )

    @query_params("dryrun")
    def run_monitor(
        self, monitor_id: Any, params: Any = None, headers: Any = None
    ) -> Any:
        """
        Runs/Executes a specific monitor.

        :arg monitor_id: The id of the monitor we are trying to execute
        :arg dryrun: Shows the results of a run without actions sending any message
        """
        return self.transport.perform_request(
            "POST",
            _make_path("_plugins", "_alerting", "monitors", monitor_id, "_execute"),
            params=params,
            headers=headers,
        )

    @query_params()
    def create_monitor(
        self, body: Any = None, params: Any = None, headers: Any = None
    ) -> Any:
        """
        Creates a monitor with inputs, triggers, and actions.

        :arg body: The configuration for the monitor (`inputs`, `triggers`, and `actions`)
        """
        return self.transport.perform_request(
            "POST",
            _make_path("_plugins", "_alerting", "monitors"),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params()
    def update_monitor(
        self, monitor_id: Any, body: Any = None, params: Any = None, headers: Any = None
    ) -> Any:
        """
        Updates a monitor's inputs, triggers, and actions.

        :arg monitor_id: The id of the monitor we are trying to update
        :arg body: The configuration for the monitor (`inputs`, `triggers`, and `actions`)
        """
        return self.transport.perform_request(
            "PUT",
            _make_path("_plugins", "_alerting", "monitors", monitor_id),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params()
    def delete_monitor(
        self, monitor_id: Any, params: Any = None, headers: Any = None
    ) -> Any:
        """
        Deletes a specific monitor.

        :arg monitor_id: The id of the monitor we are trying to delete
        """
        return self.transport.perform_request(
            "DELETE",
            _make_path("_plugins", "_alerting", "monitors", monitor_id),
            params=params,
            headers=headers,
        )

    @query_params()
    def get_destination(
        self, destination_id: Any = None, params: Any = None, headers: Any = None
    ) -> Any:
        """
        Returns the details of a specific destination.

        :arg destination_id: The id of the destination we are trying to fetch. If None, returns all destinations
        """
        return self.transport.perform_request(
            "GET",
            (
                _make_path("_plugins", "_alerting", "destinations", destination_id)
                if destination_id
                else _make_path("_plugins", "_alerting", "destinations")
            ),
            params=params,
            headers=headers,
        )

    @query_params()
    def create_destination(
        self, body: Any = None, params: Any = None, headers: Any = None
    ) -> Any:
        """
        Creates a destination for slack, mail, or custom-webhook.

        :arg body: The configuration for the destination
        """
        return self.transport.perform_request(
            "POST",
            _make_path("_plugins", "_alerting", "destinations"),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params()
    def update_destination(
        self,
        destination_id: Any,
        body: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Updates a destination's inputs, triggers, and actions.

        :arg destination_id: The id of the destination we are trying to update
        :arg body: The configuration for the destination
        """
        return self.transport.perform_request(
            "PUT",
            _make_path("_plugins", "_alerting", "destinations", destination_id),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params()
    def delete_destination(
        self, destination_id: Any, params: Any = None, headers: Any = None
    ) -> Any:
        """
        Deletes a specific destination.

        :arg destination_id: The id of the destination we are trying to delete
        """
        return self.transport.perform_request(
            "DELETE",
            _make_path("_plugins", "_alerting", "destinations", destination_id),
            params=params,
            headers=headers,
        )

    @query_params()
    def get_alerts(self, params: Any = None, headers: Any = None) -> Any:
        """
        Returns all alerts.

        """
        return self.transport.perform_request(
            "GET",
            _make_path("_plugins", "_alerting", "monitors", "alerts"),
            params=params,
            headers=headers,
        )

    @query_params()
    def acknowledge_alert(
        self, monitor_id: Any, body: Any = None, params: Any = None, headers: Any = None
    ) -> Any:
        """
        Acknowledges an alert.

        :arg monitor_id: The id of the monitor, the alert belongs to
        :arg body: The alerts to be acknowledged
        """
        return self.transport.perform_request(
            "POST",
            _make_path(
                "_plugins",
                "_alerting",
                "monitors",
                monitor_id,
                "_acknowledge",
                "alerts",
            ),
            params=params,
            headers=headers,
            body=body,
        )
