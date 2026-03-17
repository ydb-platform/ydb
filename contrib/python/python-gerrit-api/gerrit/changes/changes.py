#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Author: Jialiang Shi
import logging
import requests
from gerrit.changes.change import GerritChange
from gerrit.utils.exceptions import ChangeNotFoundError, GerritAPIException


logger = logging.getLogger(__name__)


class GerritChanges:
    def __init__(self, gerrit):
        self.gerrit = gerrit
        self.endpoint = "/changes"

    def search(self, query: str, options=None, limit: int = 25, skip: int = 0):
        """
        Queries changes visible to the caller.

        .. code-block:: python

            query = "is:open+owner:self+is:mergeable"
            result = client.changes.search(query=query, options=["LABELS"])

        :param query: Query string, it can contain multiple search operators
                      concatenated by '+' character
        :param options: List of options to fetch additional data about changes
        :param limit: Int value that allows to limit the number of changes
                      to be included in the output results
        :param skip: Int value that allows to skip the given number of
                     changes from the beginning of the list
        :return:
        """
        params = {
            k: v
            for k, v in (("o", options), ("n", limit), ("S", skip))
            if v is not None
        }

        return self.gerrit.get(self.endpoint + f"/?q={query}", params=params)

    def get(self, id_):
        """
        Retrieves a change.

        :param id_: change id
        :return:
        """
        try:
            endpoint = self.endpoint + f"/{id_}/"
            result = self.gerrit.get(endpoint)

            id = result.get("id")
            return GerritChange(id=id, gerrit=self.gerrit)
        except requests.exceptions.HTTPError as error:
            if error.response.status_code == 404:
                message = f"Change {id_} does not exist"
                if isinstance(id_, str) and id_.startswith("I"):
                    res = self.search(query=f"change: {id_}")
                    if len(res) > 0:
                        change_ids = [item.get("id") for item in res]
                        message = f"Change {id_} query multiple changes: {', '.join(change_ids)}, which one do you want?"

                logger.error(message)
                raise ChangeNotFoundError(message)
            raise GerritAPIException from error

    def create(self, input_):
        """
        create a change

        .. code-block:: python

            input_ = {
                "project": "myProject",
                "subject": "Let's support 100% Gerrit workflow direct in browser",
                "branch": "stable",
                "topic": "create-change-in-browser",
                "status": "NEW"
            }
            result = client.changes.create(input_)

        :param input_: the ChangeInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-changes.html#change-input
        :return:
        """
        project_name = input_.get("project")
        branch_name = input_.get("branch")
        project = self.gerrit.projects.get(project_name)
        project.branches.get(branch_name)

        result = self.gerrit.post(
            self.endpoint + "/", json=input_, headers=self.gerrit.default_headers
        )
        return result

    def delete(self, id_):
        """
        Deletes a change.

        :param id_: change id
        :return:
        """
        self.get(id_)
        self.gerrit.delete(self.endpoint + f"/{id_}")
