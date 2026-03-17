#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Author: Jialiang Shi
from urllib.parse import quote_plus
from gerrit.utils.gerritbase import GerritBase


class GerritChangeEdit(GerritBase):
    def __init__(self, change: str, gerrit):
        self.change = change
        self.gerrit = gerrit
        self.endpoint = f"/changes/{self.change}/edit"

        super().__init__(self)

    def __str__(self):
        return f"change {self.change} edit"

    def get_change_file_content(self, file):
        """
        Retrieves content of a file from a change edit.
        The content of the file is returned as text encoded inside base64.

        :param file: the file path
        :return:
        """
        return self.gerrit.get(self.endpoint + f"/{quote_plus(file)}")

    def get_file_meta_data(self, file):
        """
        Retrieves meta data of a file from a change edit.

        :param file: the file path
        :return:
        """
        return self.gerrit.get(self.endpoint + f"/{quote_plus(file)}/meta")

    def put_change_file_content(self, file, file_content):
        """
        Put content of a file to a change edit.

        :param file: the file path
        :param file_content: the content of the file need to change
        :return:
        """
        self.gerrit.put(
            self.endpoint + f"/{quote_plus(file)}",
            data=file_content,
            headers={"Content-Type": "plain/text"},
        )

    def restore_file_content(self, file):
        """
        restores file content

        :param file: Path to file to restore.
        :return:
        """
        input_ = {"restore_path": file}
        self.gerrit.post(
            self.endpoint, json=input_, headers=self.gerrit.default_headers
        )

    def rename_file(self, old_path, new_path):
        """
        rename file

        :param old_path: Old path to file to rename.
        :param new_path: New path to file to rename.
        :return:
        """
        input_ = {"old_path": old_path, "new_path": new_path}
        self.gerrit.post(
            self.endpoint, json=input_, headers=self.gerrit.default_headers
        )

    def delete_file(self, file):
        """
        Deletes a file from a change edit.

        :param file: Path to file to delete.
        :return:
        """
        self.gerrit.delete(self.endpoint + f"/{quote_plus(file)}")

    def change_commit_message(self, input_):
        """
        Modify commit message.

        .. code-block:: python

            input_ = {
                "message": "New commit message\\n\\n
                Change-Id: I10394472cbd17dd12454f229e4f6de00b143a444"
            }

            change = client.changes.get('Project~stable~I10394472cbd17dd12454f229e4f6de00b143a444')
            edit = change.get_edit()
            edit.change_commit_message(input_)

        :param input_: the ChangeEditMessageInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-changes.html#change-edit-message-input
        :return:
        """
        self.gerrit.put(
            self.endpoint + ":message", json=input_, headers=self.gerrit.default_headers
        )

    def get_commit_message(self):
        """
        Retrieves commit message from change edit.
        The commit message is returned as base64 encoded string.

        :return:
        """
        return self.gerrit.get(self.endpoint + ":message")

    def publish(self, input_):
        """
        Promotes change edit to a regular patch set.

        .. code-block:: python

            input_ = {
                "notify": "NONE"
            }

            change = client.changes.get('Project~stable~I10394472cbd17dd12454f229e4f6de00b143a444')
            edit = change.get_edit()
            edit.publish(input_)

        :param input_: the PublishChangeEditInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-changes.html#publish-change-edit-input
        :return:
        """
        self.gerrit.post(
            self.endpoint + ":publish", json=input_, headers=self.gerrit.default_headers
        )

    def rebase(self):
        """
        Rebase change edit on top of the latest patch set.
        When change was rebased on top of the latest patch set, response '204 No Content'
        is returned.
        When change edit is already based on top of the latest patch set,
        the response '409 Conflict' is returned.

        :return:
        """
        self.gerrit.post(self.endpoint + ":rebase")

    def delete(self):
        """
        Deletes change edit.

        :return:
        """
        self.gerrit.delete(self.endpoint)
