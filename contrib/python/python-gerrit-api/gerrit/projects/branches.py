#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Author: Jialiang Shi
import logging
from base64 import b64decode
from urllib.parse import quote_plus, unquote_plus
import requests
from gerrit.utils.common import params_creator
from gerrit.utils.gerritbase import GerritBase
from gerrit.utils.exceptions import (
    BranchNotFoundError,
    BranchAlreadyExistsError,
    GerritAPIException,
)

logger = logging.getLogger(__name__)


class GerritProjectBranch(GerritBase):
    def __init__(self, name: str, project: str, gerrit):
        self.name = name
        self.project = project
        self.gerrit = gerrit
        self.endpoint = f"/projects/{self.project}/branches/{quote_plus(self.name)}"
        super().__init__(self)

    def __str__(self):
        return self.name

    def get_file_content(self, file, decode=False):
        """
        Gets the content of a file from the HEAD revision of a certain branch.
        The content is returned as base64 encoded string.

        :param file: the file path
        :param decode: Decode bas64 to plain text.
        :return:
        """
        result = self.gerrit.get(self.endpoint + f"/files/{quote_plus(file)}/content")
        if decode:
            return b64decode(result).decode("utf-8")
        return result

    def is_mergeable(self, input_):
        """
        Gets whether the source is mergeable with the target branch.

        .. code-block:: python

            input_ = {
                'source': 'testbranch',
                'strategy': 'recursive'
            }
            result = stable.is_mergeable(input_)

        :param input_: the MergeInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-changes.html#merge-input
        :return:
        """
        source = input_.get("source")
        try:
            project = self.gerrit.projects.get(unquote_plus(self.project))
            project.branches.get(name=source)
        except requests.exceptions.HTTPError as error:
            if error.response.status_code == 404:
                message = f"Source Branch {source} does not exist"
                logger.error(message)
                raise BranchNotFoundError(message)

        return self.gerrit.get(self.endpoint + "/mergeable", params=input_)

    def get_reflog(self):
        """
        Gets the reflog of a certain branch.

        :return:
        """
        return self.gerrit.get(self.endpoint + "/reflog")

    def delete(self):
        """
        Delete a branch.

        :return:
        """
        self.gerrit.delete(self.endpoint)


class GerritProjectBranches:
    branch_prefix = "refs/heads/"

    def __init__(self, project, gerrit):
        self.project = project
        self.gerrit = gerrit
        self.endpoint = f"/projects/{self.project}/branches"

    def list(self, pattern_dispatcher=None, limit: int = 25, skip: int = 0):
        """
        List the branches of a project.

        :param pattern_dispatcher: Dict of pattern type with respective
               pattern value: {('match'|'regex') : value}
        :param limit: Limit the number of branches to be included in the results.
        :param skip: Skip the given number of branches from the beginning of the list.
        :return:
        """
        params = params_creator(
            (("n", limit), ("s", skip)),
            {"match": "m", "regex": "r"},
            pattern_dispatcher,
        )

        return self.gerrit.get(self.endpoint + "/", params=params)

    def get(self, name):
        """
        get a branch by ref

        :param name: branch ref name
        :return:
        """
        try:
            result = self.gerrit.get(self.endpoint + f"/{quote_plus(name)}")

            ref = result.get("ref")
            name = ref.replace(self.branch_prefix, "")
            return GerritProjectBranch(
                name=name, project=self.project, gerrit=self.gerrit
            )
        except requests.exceptions.HTTPError as error:
            if error.response.status_code == 404:
                message = f"Branch {name} does not exist"
                raise BranchNotFoundError(message)
            raise GerritAPIException from error

    def create(self, name, input_):
        """
        Creates a new branch.

        .. code-block:: python

            input_ = {
                'revision': '76016386a0d8ecc7b6be212424978bb45959d668'
            }
            project = client.projects.get('myproject')
            new_branch = project.branches.create('stable', input_)


        :param name: the branch name
        :param input_: the BranchInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-projects.html#branch-info
        :return:
        """
        try:
            self.get(name)
            message = f"Branch {name} already exists"
            logger.error(message)
            raise BranchAlreadyExistsError(message)
        except BranchNotFoundError:
            self.gerrit.put(
                self.endpoint + f"/{quote_plus(name)}",
                json=input_,
                headers=self.gerrit.default_headers,
            )

            return self.get(name)

    def delete(self, name):
        """
        Delete a branch.

        :param name: branch ref name
        :return:
        """
        self.get(name)
        self.gerrit.delete(self.endpoint + f"/{quote_plus(name)}")
