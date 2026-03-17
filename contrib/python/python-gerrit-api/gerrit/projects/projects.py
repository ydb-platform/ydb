#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Author: Jialiang Shi
import logging
from typing import Any, Union, Dict, List, Optional
from urllib.parse import quote_plus
import requests
from gerrit.projects.project import GerritProject
from gerrit.utils.common import params_creator
from gerrit.utils.exceptions import (
    ProjectNotFoundError,
    ProjectAlreadyExistsError,
    GerritAPIException,
)


logger = logging.getLogger(__name__)


class GerritProjects:
    def __init__(self, gerrit):
        self.gerrit = gerrit
        self.endpoint = "/projects"

    def list(
        self,
        is_all: bool = False,
        limit: int = 25,
        skip: int = 0,
        pattern_dispatcher: Union[Dict, None] = None,
        project_type: Optional[str] = None,
        description: bool = False,
        branch: Optional[str] = None,
        state: Optional[str] = None,
    ) -> List:
        """
        Get list of all available projects accessible by the caller.

        :param is_all: boolean value, if True then all projects (including
                       hidden ones) will be added to the results.
                       May not be used together with the state option.
        :param limit: Int value that allows to limit the number of projects
                      to be included in the output results
        :param skip: Int value that allows to skip the given
                     number of projects from the beginning of the list
        :param pattern_dispatcher: Dict of pattern type with respective
                     pattern value: {('prefix'|'match'|'regex') : value}
        :param project_type: string value for type of projects to be fetched
                            ('code'|'permissions'|'all')
        :param description: boolean value, if True then description will be
                            added to the output result
        :param branch: Limit the results to the projects having the specified branch
                       and include the sha1 of the branch in the results.
        :param state: Get all projects with the given state. May not be used together with the all
        option.

        :return:
        """
        if is_all and state:
            raise ValueError("is_all can not be used together with the state option.")

        pattern_types = {"prefix": "p", "match": "m", "regex": "r"}
        tuples = (
            ("n", limit),
            ("S", skip),
            ("type", project_type),
            ("b", branch),
            ("state", state),
        )
        params = params_creator(tuples, pattern_types, pattern_dispatcher)
        if is_all:
            params.clear()
            params["all"] = int(is_all)
        params["d"] = int(description)

        return self.gerrit.get(self.endpoint + "/", params=params)

    def search(self, query: str, limit: int = 25, skip: int = 0) -> List:
        """
        Queries projects visible to the caller. The query string must be provided by the
        query parameter.
        The start and limit parameters can be used to skip/limit results.

        query parameter
          * name:'NAME' Matches projects that have exactly the name 'NAME'.
          * parent:'PARENT' Matches projects that have 'PARENT' as parent project.
          * inname:'NAME' Matches projects that a name part that starts with
           'NAME' (case insensitive).
          * description:'DESCRIPTION' Matches projects whose description contains 'DESCRIPTION',
          using a full-text search.
          * state:'STATE' Matches project’s state. Can be either 'active' or 'read-only'.

        :param query:
        :param limit: Int value that allows to limit the number of accounts
                      to be included in the output results
        :param skip: Int value that allows to skip the given
                     number of accounts from the beginning of the list
        :return:
        """
        params = {k: v for k, v in (("limit", limit), ("start", skip)) if v is not None}

        return self.gerrit.get(self.endpoint + f"/?query={query}", params=params)

    def get(self, name: str):
        """
        Retrieves a project.

        :param name: the name of the project
        :return:
        """
        try:
            res = self.gerrit.get(self.endpoint + f"/{quote_plus(name)}")
            project_id = res.get("id")
            return GerritProject(project_id=project_id, gerrit=self.gerrit)
        except requests.exceptions.HTTPError as error:
            if error.response.status_code == 404:
                message = f"Project {name} does not exist"
                raise ProjectNotFoundError(message)
            raise GerritAPIException from error

    def create(self, project_name: str, input_: Dict[str, Any]):
        """
        Creates a new project.

        .. code-block:: python

            input_ = {
                "description": "This is a demo project.",
                "submit_type": "INHERIT",
                "owners": [
                  "MyProject-Owners"
                ]
            }
            project = client.projects.create('MyProject', input_)

        :param project_name: the name of the project
        :param input_: the ProjectInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-projects.html#project-input

        :return:
        """
        try:
            self.get(project_name)
            message = f"Project {project_name} already exists"
            logger.error(message)
            raise ProjectAlreadyExistsError(message)
        except ProjectNotFoundError:
            self.gerrit.put(
                self.endpoint + f"/{quote_plus(project_name)}",
                json=input_,
                headers=self.gerrit.default_headers,
            )
            return self.get(project_name)

    def delete(self, project_name: str):
        """
        Delete the project, requires delete-project plugin

        :param project_name: project name
        :return:
        """
        self.get(project_name)
        self.gerrit.post(
            self.endpoint + f"/{quote_plus(project_name)}/delete-project~delete"
        )
