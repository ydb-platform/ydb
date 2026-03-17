# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

# ------------------------------------------------------------------------------------------
# THIS CODE IS AUTOMATICALLY GENERATED AND MANUAL EDITS WILL BE LOST
#
# To contribute, kindly make modifications in the opensearch-py client generator
# or in the OpenSearch API specification, and run `nox -rs generate`. See DEVELOPER_GUIDE.md
# and https://github.com/opensearch-project/opensearch-api-specification for details.
# -----------------------------------------------------------------------------------------+


from typing import Any

from ..client.utils import SKIP_IN_PATH, NamespacedClient, _make_path, query_params


class NotificationsClient(NamespacedClient):
    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    async def create_config(
        self,
        *,
        body: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Create channel configuration.


        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        if body in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'body'.")

        return await self.transport.perform_request(
            "POST",
            "/_plugins/_notifications/configs",
            params=params,
            headers=headers,
            body=body,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    async def delete_config(
        self,
        *,
        config_id: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Delete a channel configuration.


        :arg config_id: The ID of the channel configuration to delete.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        if config_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'config_id'.")

        return await self.transport.perform_request(
            "DELETE",
            _make_path("_plugins", "_notifications", "configs", config_id),
            params=params,
            headers=headers,
        )

    @query_params(
        "config_id",
        "config_id_list",
        "error_trace",
        "filter_path",
        "human",
        "pretty",
        "source",
    )
    async def delete_configs(
        self,
        *,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Delete multiple channel configurations.


        :arg config_id: The ID of the channel configuration to delete.
        :arg config_id_list: A comma-separated list of channel IDs to
            delete.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        return await self.transport.perform_request(
            "DELETE", "/_plugins/_notifications/configs", params=params, headers=headers
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    async def get_config(
        self,
        *,
        config_id: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Get a specific channel configuration.


        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        if config_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'config_id'.")

        return await self.transport.perform_request(
            "GET",
            _make_path("_plugins", "_notifications", "configs", config_id),
            params=params,
            headers=headers,
        )

    @query_params(
        "chime.url",
        "chime.url.keyword",
        "config_id",
        "config_id_list",
        "config_type",
        "created_time_ms",
        "description",
        "description.keyword",
        "email.email_account_id",
        "email.email_group_id_list",
        "email.recipient_list.recipient",
        "email.recipient_list.recipient.keyword",
        "email_group.recipient_list.recipient",
        "email_group.recipient_list.recipient.keyword",
        "error_trace",
        "filter_path",
        "human",
        "is_enabled",
        "last_updated_time_ms",
        "microsoft_teams.url",
        "microsoft_teams.url.keyword",
        "name",
        "name.keyword",
        "pretty",
        "query",
        "ses_account.from_address",
        "ses_account.from_address.keyword",
        "ses_account.region",
        "ses_account.role_arn",
        "ses_account.role_arn.keyword",
        "slack.url",
        "slack.url.keyword",
        "smtp_account.from_address",
        "smtp_account.from_address.keyword",
        "smtp_account.host",
        "smtp_account.host.keyword",
        "smtp_account.method",
        "sns.role_arn",
        "sns.role_arn.keyword",
        "sns.topic_arn",
        "sns.topic_arn.keyword",
        "source",
        "text_query",
        "webhook.url",
        "webhook.url.keyword",
    )
    async def get_configs(
        self,
        *,
        body: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Get multiple channel configurations with filtering.


        :arg config_id: Notification configuration ID.
        :arg config_id_list: Notification configuration IDs.
        :arg config_type: Type of notification configuration. Valid
            choices are chime, email, email_group, microsoft_teams, ses_account,
            slack, smtp_account, sns, webhook.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        return await self.transport.perform_request(
            "GET",
            "/_plugins/_notifications/configs",
            params=params,
            headers=headers,
            body=body,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    async def list_features(
        self,
        *,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        List supported channel configurations.


        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        return await self.transport.perform_request(
            "GET", "/_plugins/_notifications/features", params=params, headers=headers
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    async def send_test(
        self,
        *,
        config_id: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Send a test notification.


        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        if config_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'config_id'.")

        return await self.transport.perform_request(
            "GET",
            _make_path("_plugins", "_notifications", "feature", "test", config_id),
            params=params,
            headers=headers,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    async def update_config(
        self,
        *,
        config_id: Any,
        body: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Update channel configuration.


        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        for param in (config_id, body):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        return await self.transport.perform_request(
            "PUT",
            _make_path("_plugins", "_notifications", "configs", config_id),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    async def list_channels(
        self,
        *,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        List created notification channels.


        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        return await self.transport.perform_request(
            "GET", "/_plugins/_notifications/channels", params=params, headers=headers
        )
