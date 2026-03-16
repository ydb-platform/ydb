#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Author: Jialiang Shi
import logging
from urllib.parse import quote_plus
import requests
from gerrit.utils.common import params_creator
from gerrit.utils.gerritbase import GerritBase
from gerrit.utils.exceptions import (
    TagNotFoundError,
    TagAlreadyExistsError,
    GerritAPIException,
)


logger = logging.getLogger(__name__)


class GerritProjectTag(GerritBase):
    def __init__(self, name: str, project: str, gerrit):
        self.name = name
        self.project = project
        self.gerrit = gerrit
        self.endpoint = f"/projects/{self.project}/tags/{quote_plus(self.name)}"
        super().__init__(self)

    def __str__(self):
        return self.name

    def delete(self):
        """
        Delete a tag.

        :return:
        """
        self.gerrit.delete(self.endpoint)


class GerritProjectTags:
    tag_prefix = "refs/tags/"

    def __init__(self, project, gerrit):
        self.project = project
        self.gerrit = gerrit
        self.endpoint = f"/projects/{self.project}/tags"

    def list(self, pattern_dispatcher=None, limit: int = 25, skip: int = 0):
        """
        List the tags of a project.

        :param pattern_dispatcher: Dict of pattern type with respective
               pattern value: {('match'|'regex') : value}
        :param limit: Limit the number of tags to be included in the results.
        :param skip: Skip the given number of tags from the beginning of the list.
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
        get a tag by ref

        :param name: the tag ref
        :return:
        """
        try:
            result = self.gerrit.get(self.endpoint + f"/{quote_plus(name)}")

            ref = result.get("ref")
            name = ref.replace(self.tag_prefix, "")
            return GerritProjectTag(name=name, project=self.project, gerrit=self.gerrit)
        except requests.exceptions.HTTPError as error:
            if error.response.status_code == 404:
                message = f"Tag {name} does not exist"
                raise TagNotFoundError(message)
            raise GerritAPIException from error

    def create(self, name, input_):
        """
        Creates a new tag on the project.

        .. code-block:: python

            input_ = {
                "message": "annotation",
                'revision': 'c83117624b5b5d8a7f86093824e2f9c1ed309d63'
            }

            project = client.projects.get('myproject')
            new_tag = project.tags.create('1.1.8', input_)

        :param name: the tag name
        :param input_: the TagInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-projects.html#tag-input
        :return:
        """
        try:
            self.get(name)
            message = f"Tag {name} already exists"
            logger.error(message)
            raise TagAlreadyExistsError(message)
        except TagNotFoundError:
            self.gerrit.put(
                self.endpoint + f"/{quote_plus(name)}",
                json=input_,
                headers=self.gerrit.default_headers,
            )

            return self.get(name)

    def delete(self, name):
        """
        Delete a tag.

        :param name: the tag ref
        :return:
        """
        self.get(name)
        self.gerrit.delete(self.endpoint + f"/{quote_plus(name)}")
