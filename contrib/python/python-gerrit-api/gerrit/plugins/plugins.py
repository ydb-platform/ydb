#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Author: Jialiang Shi
from gerrit.utils.common import params_creator


class GerritPlugin:
    def __init__(self, id_: str, gerrit):
        self.id_ = id_
        self.gerrit = gerrit
        self.endpoint = f"/plugins/{self.id_}"

    def enable(self):
        """
        Enables a plugin on the Gerrit server.

        :return:
        """
        result = self.gerrit.post(self.endpoint + "/gerrit~enable")
        return result

    def disable(self):
        """
        Disables a plugin on the Gerrit server.

        :return:
        """
        result = self.gerrit.post(self.endpoint + "/gerrit~disable")
        return result

    def reload(self):
        """
        Reloads a plugin on the Gerrit server.

        :return:
        """
        result = self.gerrit.post(self.endpoint + "/gerrit~reload")
        return result


class GerritPlugins:
    def __init__(self, gerrit):
        self.gerrit = gerrit
        self.endpoint = "/plugins"

    def list(
        self,
        is_all: bool = False,
        limit: int = 25,
        skip: int = 0,
        pattern_dispatcher=None,
    ):
        """
        Lists the plugins installed on the Gerrit server.

        :param is_all: boolean value, if True then all plugins (including
                       hidden ones) will be added to the results
        :param limit: Int value that allows to limit the number of plugins
                      to be included in the output results
        :param skip: Int value that allows to skip the given
                     number of plugins from the beginning of the list
        :param pattern_dispatcher: Dict of pattern type with respective
                     pattern value: {('prefix'|'match'|'regex') : value}
        :return:
        """
        params = params_creator(
            (("n", limit), ("S", skip)),
            {"prefix": "p", "match": "m", "regex": "r"},
            pattern_dispatcher,
        )
        params["all"] = int(is_all)

        return self.gerrit.get(self.endpoint + "/", params=params)

    def get(self, id_):
        """

        :param id_: plugin id
        :return:
        """
        result = self.gerrit.get(self.endpoint + f"/{id_}/gerrit~status")

        plugin_id = result.get("id")
        return GerritPlugin(id_=plugin_id, gerrit=self.gerrit)

    def install(self, id_, input_):
        """
        Installs a new plugin on the Gerrit server.

        .. code-block:: python

            input_ = {
                "url": "file:///gerrit/plugins/delete-project/delete-project-2.8.jar"
            }

            plugin = client.plugins.install(input_)

        :param id_: plugin id
        :param input_: the PluginInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-plugins.html#plugin-input
        :return:
        """
        result = self.gerrit.put(
            self.endpoint + f"/{id_}.jar",
            json=input_,
            headers=self.gerrit.default_headers,
        )
        return result
