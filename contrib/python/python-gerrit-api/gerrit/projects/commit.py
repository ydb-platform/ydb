#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Author: Jialiang Shi
from base64 import b64decode
from urllib.parse import quote_plus
from gerrit.utils.gerritbase import GerritBase


class GerritProjectCommit(GerritBase):
    def __init__(self, commit: str, project: str, gerrit):
        self.commit = commit
        self.project = project
        self.gerrit = gerrit
        self.endpoint = f"/projects/{self.project}/commits/{self.commit}"
        super().__init__(self)

    def __str__(self):
        return self.commit

    def get_include_in(self):
        """
        Retrieves the branches and tags in which a change is included.

        :return:
        """
        return self.gerrit.get(self.endpoint + "/in")

    def get_file_content(self, file, decode=False):
        """
        Gets the content of a file from a certain commit.

        :param file: the file path
        :param decode: Decode bas64 to plain text.
        :return:
        """
        result = self.gerrit.get(self.endpoint + f"/files/{quote_plus(file)}/content")
        if decode:
            return b64decode(result).decode("utf-8")
        return result

    def cherry_pick(self, input_):
        """
        Cherry-picks a commit of a project to a destination branch.

        .. code-block:: python

            input_ = {
                "message": "Implementing Feature X",
                "destination": "release-branch"
            }
            result = commit.cherry_pick(input_)

        :param input_: the CherryPickInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-changes.html#cherrypick-input
        :return:  the resulting cherry-picked change
        """
        result = self.gerrit.post(
            self.endpoint + "/cherrypick",
            json=input_,
            headers=self.gerrit.default_headers,
        )
        return result

    def list_change_files(self):
        """
        Lists the files that were modified, added or deleted in a commit.

        :return:
        """
        return self.gerrit.get(self.endpoint + "/files/")
