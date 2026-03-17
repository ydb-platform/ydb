#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Author: Jialiang Shi
from gerrit.utils.gerritbase import GerritBase


class GerritChangeMessage(GerritBase):
    def __init__(self, id: str, change: str, gerrit):
        self.id = id
        self.change = change
        self.gerrit = gerrit
        self.endpoint = f"/changes/{self.change}/messages/{self.id}"
        super().__init__(self)

    def __str__(self):
        return self.id

    def delete(self, input_=None):
        """
        Deletes a change message.
        Note that only users with the Administrate Server global capability are permitted to
        delete a change message.

        .. code-block:: python

            input_ = {
                "reason": "spam"
            }
            change = client.changes.get('Project~stable~I10394472cbd17dd12454f229e4f6de00b143a444')
            message = change.messages.get("babf4c5dd53d7a11080696efa78830d0a07762e6")
            result = message.delete(input_)
            # or
            result = message.delete()

        :param input_: the DeleteChangeMessageInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-changes.html#delete-change-message-input
        :return:
        """
        if input_ is None:
            self.gerrit.delete(self.endpoint)
        else:
            self.gerrit.post(
                self.endpoint + "/delete",
                json=input_,
                headers=self.gerrit.default_headers,
            )


class GerritChangeMessages:
    def __init__(self, change, gerrit):
        self.change = change
        self.gerrit = gerrit
        self.endpoint = f"/changes/{self.change}/messages"

    def list(self):
        """
        Lists all the messages of a change including detailed account information.

        :return:
        """
        result = self.gerrit.get(self.endpoint)
        return result

    def get(self, id_):
        """
        Retrieves a change message including detailed account information.

        :param id_: change message id
        :return:
        """
        result = self.gerrit.get(self.endpoint + f"/{id_}")
        id_ = result.get("id")
        return GerritChangeMessage(id=id_, change=self.change, gerrit=self.gerrit)
