#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Author: Jialiang Shi
import logging
import requests

from gerrit.utils.exceptions import (
    GroupMemberNotFoundError,
    GroupMemberAlreadyExistsError,
    GerritAPIException,
)

logger = logging.getLogger(__name__)


class GerritGroupMembers:
    def __init__(self, group_id, gerrit):
        self.id = group_id
        self.gerrit = gerrit
        self.endpoint = f"/groups/{self.id}/members"

    def list(self):
        """
        Lists the direct members of a Gerrit internal group.
        This endpoint is only allowed for Gerrit internal groups;
        attempting to call on a non-internal group will return 405 Method Not Allowed.

        :return:
        """
        result = self.gerrit.get(self.endpoint)

        accounts = []
        for item in result:
            account_id = item.get("_account_id")
            accounts.append(self.gerrit.accounts.get(account_id))

        return accounts

    def get(self, account):
        """
        Retrieves a group member.
        This endpoint is only allowed for Gerrit internal groups;
        attempting to call on a non-internal group will return 405 Method Not Allowed.

        :param account: account username or id
        :return:
        """
        try:
            result = self.gerrit.get(self.endpoint + f"/{account}")
            account_id = result.get("_account_id")
            return self.gerrit.accounts.get(account_id)
        except requests.exceptions.HTTPError as error:
            if error.response.status_code == 404:
                message = f"Group member {account} does not exist"
                raise GroupMemberNotFoundError(message)
            raise GerritAPIException from error

    def add(self, account):
        """
        Adds a user as member to a Gerrit internal group.
        This endpoint is only allowed for Gerrit internal groups;
        attempting to call on a non-internal group will return 405 Method Not Allowed.

        :param account: account username or id
        :return:
        """
        try:
            self.get(account)
            message = f"Group member {account} already exists"
            logger.error(message)
            raise GroupMemberAlreadyExistsError(message)
        except GroupMemberNotFoundError:
            self.gerrit.put(self.endpoint + f"/{account}")
            return self.get(account)

    def remove(self, account):
        """
        Removes a user from a Gerrit internal group.
        This endpoint is only allowed for Gerrit internal groups;
        attempting to call on a non-internal group will return 405 Method Not Allowed.

        :param account: account username or id
        :return:
        """
        self.get(account)
        self.gerrit.delete(self.endpoint + f"/{account}")
