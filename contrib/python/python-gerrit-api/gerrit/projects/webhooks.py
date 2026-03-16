#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Author: Jialiang Shi
from gerrit.utils.gerritbase import GerritBase


class GerritProjectWebHook(GerritBase):
    def __init__(self, name: str, project: str, gerrit):
        self.name = name
        self.project = project
        self.gerrit = gerrit
        self.endpoint = (
            f"/config/server/webhooks~projects/{self.project}/remotes/{self.name}"
        )
        super().__init__(self)

    def __str__(self):
        return self.name

    def delete(self):
        """
        Delete a webhook for a project.

        :return:
        """
        self.gerrit.delete(self.endpoint)


class GerritProjectWebHooks:
    def __init__(self, project, gerrit):
        self.project = project
        self.gerrit = gerrit
        self.endpoint = f"/config/server/webhooks~projects/{self.project}/remotes"

    def list(self):
        """
        List existing webhooks for a project.

        :return:
        """
        result = self.gerrit.get(self.endpoint + "/")

        return result

    def create(self, name, input_):
        """
        Create or update a webhook for a project.

        .. code-block:: python

            input_ = {
                "url": "https://foo.org/gerrit-events",
                "maxTries": "3",
                "sslVerify": "true"
            }

            project = client.projects.get('myproject')
            new_webhook = project.webhooks.create('test', input_)

        :param name: the webhook name
        :param input_: the RemoteInfo entity
        :return:
        """
        result = self.gerrit.put(
            self.endpoint + f"/{name}", json=input_, headers=self.gerrit.default_headers
        )
        return result

    def get(self, name):
        """
        Get information about one webhook.

        :param name: the webhook name
        :return:
        """
        result = self.gerrit.get(self.endpoint + f"/{name}")
        name = result.get("name")
        return GerritProjectWebHook(name=name, project=self.project, gerrit=self.gerrit)

    def delete(self, name):
        """
        Delete a webhook for a project.

        :param name: the webhook name
        :return:
        """
        self.gerrit.delete(self.endpoint + f"/{name}")
