#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Author: Jialiang Shi
import logging
from typing import Optional
from base64 import b64decode
from urllib.parse import quote_plus
import requests
from gerrit.utils.exceptions import (
    UnknownFile,
    FileContentNotFoundError,
    GerritAPIException,
)


logger = logging.getLogger(__name__)


class GerritChangeRevisionFile:
    def __init__(self, path: str, json: dict, change: str, revision: str, gerrit):
        self.path = path
        self.json = json
        self.change = change
        self.revision = revision
        self.gerrit = gerrit
        self.endpoint = (
            f"/changes/{self.change}/revisions/{self.revision}"
            f"/files/{quote_plus(self.path)}"
        )

    def __repr__(self):
        return f"<{self.__class__.__module__}.{self.__class__.__name__} {str(self)}>"

    def __str__(self):
        return self.path

    def to_dict(self):
        return self.json

    def get_content(self, decode=False):
        """
        Gets the content of a file from a certain revision.
        The content is returned as base64 encoded string.

        :param decode: Decode bas64 to plain text.
        :return:
        """
        try:
            result = self.gerrit.get(self.endpoint + "/content")
            if decode:
                return b64decode(result).decode("utf-8")
            return result
        except requests.exceptions.HTTPError as error:
            if error.response.status_code == 404:
                message = f"Revision File {self.path} content does not exist"
                logger.error(message)
                raise FileContentNotFoundError(message)
            raise GerritAPIException from error

    def download_content(self):
        """
        Downloads the content of a file from a certain revision, in a safe format that poses no
        risk for inadvertent execution of untrusted code.

        If the content type is defined as safe, the binary file content is returned verbatim.
        If the content type is not safe, the file is stored inside a ZIP file, containing a
        single entry with a random, unpredictable name having the same base and suffix as the
        true filename. The ZIP file is returned in verbatim binary form.

        :return:
        """
        return self.gerrit.get(self.endpoint + "/download")

    def get_diff(self, intraline=False):
        """
        Gets the diff of a file from a certain revision.

        :param intraline: If the intraline parameter is specified, intraline differences are
        included in the diff.
        :return:
        """
        endpoint = self.endpoint + "/diff"
        if intraline:
            endpoint += endpoint + "?intraline"

        return self.gerrit.get(endpoint)

    def get_blame(self):
        """
        Gets the blame of a file from a certain revision.

        :return:
        """
        return self.gerrit.get(self.endpoint + "/blame")

    def set_reviewed(self):
        """
        Marks a file of a revision as reviewed by the calling user.

        :return:
        """
        self.gerrit.put(self.endpoint + "/reviewed")

    def delete_reviewed(self):
        """
        Deletes the reviewed flag of the calling user from a file of a revision.

        :return:
        """
        self.gerrit.delete(self.endpoint + "/reviewed")


class GerritChangeRevisionFiles:
    def __init__(self, change, revision, gerrit):
        self.change = change
        self.revision = revision
        self.gerrit = gerrit
        self._data = []
        self.endpoint = f"/changes/{self.change}/revisions/{self.revision}/files"

    def search(
        self,
        reviewed: Optional[bool] = None,
        base: Optional[int] = None,
        q: Optional[str] = None,
        parent: Optional[int] = None,
    ):
        """
        Lists the files that were modified, added or deleted in a revision.
        The reviewed, base, q, and parent are mutually exclusive. That is, only one of them may be used at a time.

        :param reviewed: return a list of the paths the caller has marked as reviewed
        :param base: return a map of the files which are different in this commit compared to the given revision.
          The revision must correspond to a patch set in the change.
        :param q: return a list of all files (modified or unmodified) that contain that substring in the path name.
        :param parent: For merge commits only, the integer-valued request parameter changes the response to
          return a map of the files which are different in this commit compared to the given parent commit.
        :return:
        """
        params = {
            k: v
            for k, v in (("base", base), ("q", q), ("parent", parent))
            if v is not None
        }
        if reviewed:
            params["reviewed"] = int(reviewed)

        result = self.gerrit.get(self.endpoint, params=params)

        return result

    def poll(self):
        """

        :return:
        """
        result = self.search()
        files = []
        for key, value in result.items():
            file = value
            file.update({"path": key})
            files.append(file)

        return files

    def iterkeys(self):
        """
        Iterate over the paths of all files

        :return:
        """
        if not self._data:
            self._data = self.poll()

        for file in self._data:
            yield file["path"]

    def keys(self):
        """
        Return a list of the file paths

        :return:
        """
        return list(self.iterkeys())

    def __len__(self):
        """

        :return:
        """
        return len(self.keys())

    def __contains__(self, ref):
        """

        :param ref:
        :return:
        """
        return ref in self.keys()

    def __iter__(self):
        """

        :return:
        """
        if not self._data:
            self._data = self.poll()

        for file in self._data:
            yield GerritChangeRevisionFile(
                path=file["path"],
                json=file,
                change=self.change,
                revision=self.revision,
                gerrit=self.gerrit,
            )

    def __getitem__(self, path):
        """
        get a file by path

        :param path: file path
        :return:
        """
        if not self._data:
            self._data = self.poll()

        result = [file for file in self._data if file["path"] == path]
        if result:
            file = result[0]
            return GerritChangeRevisionFile(
                path=file["path"],
                json=file,
                change=self.change,
                revision=self.revision,
                gerrit=self.gerrit,
            )
        else:
            raise UnknownFile(path)

    def get(self, path):
        """
        get a file by path

        :param path: file path
        :return:
        """
        return self[path]
