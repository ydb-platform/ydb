#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Author: Jialiang Shi
from gerrit.config.caches import Caches
from gerrit.config.tasks import Tasks


class GerritConfig:
    def __init__(self, gerrit):
        self.gerrit = gerrit
        self.endpoint = "/config/server"

    def get_version(self):
        """
        get the version of the Gerrit server.

        :return:
        """
        return self.gerrit.get(self.endpoint + "/version")

    def get_server_info(self):
        """
        get the information about the Gerrit server configuration.

        :return:
        """
        return self.gerrit.get(self.endpoint + "/info")

    def check_consistency(self, input_):
        """
        Runs consistency checks and returns detected problems.

        .. code-block:: python

            input_ = {
                "check_accounts": {},
                "check_account_external_ids": {}
            }
            result = client.config.check_consistency(input_)

        :param input_: the ConsistencyCheckInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-config.html#consistency-check-input
        :return:
        """
        return self.gerrit.post(
            self.endpoint + "/check.consistency",
            json=input_,
            headers=self.gerrit.default_headers,
        )

    def reload_config(self):
        """
        Reloads the gerrit.config configuration.

        :return:
        """
        return self.gerrit.post(self.endpoint + "/reload")

    def confirm_email(self, input_):
        """
        Confirms that the user owns an email address.
        If the token is invalid or if it's the token of another user the request fails and the
        response is '422 Unprocessable Entity'.

        .. code-block:: python

            input_ = {
                "token": "Enim+QNbAo6TV8Hur8WwoUypI6apG7qBPvF+bw==$MTAwMDAwNDp0ZXN0QHRlc3QuZGU="
            }
            result = client.config.confirm_email(input_)

        :param input_: the EmailConfirmationInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-config.html#email-confirmation-input
        :return:
        """
        self.gerrit.put(
            self.endpoint + "/email.confirm",
            json=input_,
            headers=self.gerrit.default_headers,
        )

    @property
    def caches(self):
        return Caches(gerrit=self.gerrit)

    def get_summary(self, option=None):
        """
        Retrieves a summary of the current server state.

        :param option: query option.such as jvm or gc
        :return:
        """
        endpoint = self.endpoint + "/summary"
        if option is not None:
            endpoint += f"?{option}"
        return self.gerrit.get(endpoint)

    def list_capabilities(self):
        """
        Lists the capabilities that are available in the system.
        There are two kinds of capabilities: core and plugin-owned capabilities.

        :return:
        """
        return self.gerrit.get(self.endpoint + "/capabilities")

    @property
    def tasks(self):
        return Tasks(gerrit=self.gerrit)

    def get_top_menus(self):
        """
        Returns the list of additional top menu entries.

        :return:
        """
        return self.gerrit.get(self.endpoint + "/top-menus")

    def get_default_user_preferences(self):
        """
        Returns the default user preferences for the server.

        :return:
        """
        return self.gerrit.get(self.endpoint + "/preferences")

    def set_default_user_preferences(self, input_):
        """
        Sets the default user preferences for the server.

        .. code-block:: python

            input_ = {
                "changes_per_page": 50
            }
            result = client.config.set_default_user_preferences(input_)

        :param input_: the PreferencesInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-accounts.html#preferences-input
        :return:
        """
        return self.gerrit.put(
            self.endpoint + "/preferences",
            json=input_,
            headers=self.gerrit.default_headers,
        )

    def get_default_diff_preferences(self):
        """
        Returns the default diff preferences for the server.

        :return:
        """
        return self.gerrit.get(self.endpoint + "/preferences.diff")

    def set_default_diff_preferences(self, input_):
        """
        Sets the default diff preferences for the server.

        .. code-block:: python

            input_ = {
                "context": 10,
                "tab_size": 8,
                "line_length": 80,
                "cursor_blink_rate": 0,
                "intraline_difference": true,
                "show_line_endings": true,
                "show_tabs": true,
                "show_whitespace_errors": true,
                "syntax_highlighting": true,
                "auto_hide_diff_table_header": true,
                "theme": "DEFAULT",
                "ignore_whitespace": "IGNORE_NONE"
            }
            result = client.config.set_default_diff_preferences(input_)

        :param input_: the DiffPreferencesInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-accounts.html#diff-preferences-input
        :return:
        """
        return self.gerrit.put(
            self.endpoint + "/preferences.diff",
            json=input_,
            headers=self.gerrit.default_headers,
        )

    def get_default_edit_preferences(self):
        """
        Returns the default edit preferences for the server.

        :return:
        """
        return self.gerrit.get(self.endpoint + "/preferences.edit")

    def set_default_edit_preferences(self, input_):
        """
        Sets the default edit preferences for the server.

        .. code-block:: python

            input_ = {
                "tab_size": 8,
                "line_length": 80,
                "indent_unit": 2,
                "cursor_blink_rate": 0,
                "show_tabs": true,
                "syntax_highlighting": true,
                "match_brackets": true,
                "auto_close_brackets": true,
                "theme": "DEFAULT",
                "key_map_type": "DEFAULT"
            }
            result = client.config.set_default_edit_preferences(input_)

        :param input_: the EditPreferencesInfo entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-accounts.html#edit-preferences-input
        :return:
        """
        return self.gerrit.put(
            self.endpoint + "/preferences.edit",
            json=input_,
            headers=self.gerrit.default_headers,
        )

    def index_changes(self, input_):
        """
        Index a set of changes

        .. code-block:: python

            input_ = {changes: ["foo~101", "bar~202"]}
            gerrit.config.index_changes(input_)

        :param input_: the IndexChangesInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-config.html#index-changes-input
        :return:
        """
        self.gerrit.post(
            self.endpoint + "/index.changes",
            json=input_,
            headers=self.gerrit.default_headers,
        )
