#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Author: Jialiang Shi
import logging
import requests
from gerrit.groups.group import GerritGroup
from gerrit.utils.common import params_creator
from gerrit.utils.exceptions import (
    GroupNotFoundError,
    GroupAlreadyExistsError,
    GerritAPIException,
)

logger = logging.getLogger(__name__)


class GerritGroups:
    def __init__(self, gerrit):
        self.gerrit = gerrit
        self.endpoint = "/groups/"

    def list(
        self, pattern_dispatcher=None, options=None, limit: int = 25, skip: int = 0
    ):
        """
        Lists the groups accessible by the caller.

        :param pattern_dispatcher: Dict of pattern type with respective
                     pattern value: {('match'|'regex') : value}
        :param options: Additional fields can be obtained by adding o parameters,
                        each option requires more lookups and slows down the query response time to
                        the client so they are generally disabled by default. Optional fields are:
                          INCLUDES: include list of direct subgroups.
                          MEMBERS: include list of direct group members.
        :param limit: Int value that allows to limit the number of groups
                      to be included in the output results
        :param skip: Int value that allows to skip the given
                     number of groups from the beginning of the list
        :return:
        """
        params = params_creator(
            (("o", options), ("n", limit), ("S", skip)),
            {"match": "m", "regex": "r"},
            pattern_dispatcher,
        )

        return self.gerrit.get(self.endpoint, params=params)

    def search(self, query, options=None, limit: int = 25, skip: int = 0):
        """
        Query Groups

        :param query:
        :param options: Additional fields can be obtained by adding o parameters,
                        each option requires more lookups and slows down the query response time to
                        the client so they are generally disabled by default. Optional fields are:
                          INCLUDES: include list of direct subgroups.
                          MEMBERS: include list of direct group members.
        :param limit: Int value that allows to limit the number of groups
                      to be included in the output results
        :param skip: Int value that allows to skip the given
                     number of groups from the beginning of the list
        :return:
        """
        endpoint = self.endpoint + f"/?query={query}"

        params = {
            k: v
            for k, v in (("o", options), ("limit", limit), ("start", skip))
            if v is not None
        }

        return self.gerrit.get(endpoint, params=params)

    def get(self, id_):
        """
        Retrieves a group.

        :param id_: group id, or group_id, or group name
        :param detailed:
        :return:
        """
        try:
            endpoint = self.endpoint + f"/{id_}/"
            res = self.gerrit.get(endpoint)

            group_id = res.get("id")
            return GerritGroup(group_id=group_id, gerrit=self.gerrit)
        except requests.exceptions.HTTPError as error:
            if error.response.status_code == 404:
                message = f"Group {id_} does not exist"
                raise GroupNotFoundError(message)
            raise GerritAPIException from error

    def create(self, name, input_):
        """
        Creates a new Gerrit internal group.

        .. code-block:: python

            input_ = {
                "description": "contains all committers for MyProject2",
                "visible_to_all": 'true',
                "owner": "Administrators",
                "owner_id": "af01a8cb8cbd8ee7be072b98b1ee882867c0cf06"
            }
            new_group = client.groups.create('My-Project2-Committers', input_)

        :param name: group name
        :param input_: the GroupInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-groups.html#group-input
        :return:
        """
        try:
            self.get(name)
            message = f"Group {name} already exists"
            logger.error(message)
            raise GroupAlreadyExistsError(message)
        except GroupNotFoundError:
            self.gerrit.put(
                self.endpoint + f"/{name}",
                json=input_,
                headers=self.gerrit.default_headers,
            )
            return self.get(name)
