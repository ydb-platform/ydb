#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Author: Jialiang Shi
import logging
import requests
from gerrit.accounts.account import GerritAccount
from gerrit.utils.exceptions import (
    AccountNotFoundError,
    AccountAlreadyExistsError,
    GerritAPIException,
)


logger = logging.getLogger(__name__)


class GerritAccounts:
    def __init__(self, gerrit):
        self.gerrit = gerrit
        self.endpoint = "/accounts"

    def search(
        self,
        query: str,
        limit: int = 25,
        skip: int = 0,
        detailed: bool = False,
        suggested: bool = False,
        all_emails: bool = False,
    ):
        """
        Queries accounts visible to the caller.

        :param query: Query string
        :param limit: Int value that allows to limit the number of accounts
                      to be included in the output results
        :param skip: Int value that allows to skip the given
                     number of accounts from the beginning of the list
        :param detailed: boolean value, if True then full name,
                         preferred email, username and avatars for each account
                         will be added to the output result
        :param suggested: boolean value, if True get account suggestions
                          based on query string. If a result limit n is not
                          specified, then the default 10 is used.
        :param all_emails: boolean value, if True then all registered emails
                           for each account will be added to the output result
        :return:
        """
        option = filter(
            None,
            ["DETAILS" if detailed else None, "ALL_EMAILS" if all_emails else None],
        )
        # option = None if not option else option
        params = {
            k: v for k, v in (("n", limit), ("S", skip), ("o", option)) if v is not None
        }

        endpoint = self.endpoint + "/?"
        if suggested:
            endpoint += "suggest&"
        endpoint += f"q={query}"

        return self.gerrit.get(endpoint, params=params)

    def get(self, account):
        """
        Returns an account

        :param account: username or email or _account_id or 'self'
        :return:
        """
        try:
            endpoint = self.endpoint + f"/{account}/"
            result = self.gerrit.get(endpoint)

            account_ = result.get("_account_id")
            return GerritAccount(account=account_, gerrit=self.gerrit)
        except requests.exceptions.HTTPError as error:
            if error.response.status_code == 404:
                message = f"Account {account} does not exist"
                raise AccountNotFoundError(message)
            raise GerritAPIException from error

    def create(self, username, input_):
        """
        Creates a new account.

        .. code-block:: python

            input_ = {
                "name": "John Doe",
                "email": "john.doe@example.com",
                "ssh_key": "ssh-rsa AAAAB3NzaC1yc2EAAAABIwAAAQEA0T...YImydZAw==",
                "http_password": "19D9aIn7zePb",
                "groups": [
                  "MyProject-Owners"
                ]
            }
            new_account = client.accounts.create('john.doe', input_)

        :param username: account username
        :param input_: the AccountInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-accounts.html#account-input
        :return:
        """
        try:
            self.get(username)
            message = f"Account {username} already exists"
            logger.error(message)
            raise AccountAlreadyExistsError(message)
        except AccountNotFoundError:
            self.gerrit.put(
                self.endpoint + f"/{username}",
                json=input_,
                headers=self.gerrit.default_headers,
            )
            return self.get(username)
