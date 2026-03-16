#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Author: Jialiang Shi
import logging
import requests
from gerrit.utils.gerritbase import GerritBase
from gerrit.utils.exceptions import (
    AccountEmailNotFoundError,
    AccountEmailAlreadyExistsError,
    GerritAPIException,
)


logger = logging.getLogger(__name__)


class GerritAccountEmail(GerritBase):
    def __init__(self, email, account, gerrit):
        self.email = email
        self.account = account
        self.gerrit = gerrit
        self.endpoint = f"/accounts/{self.account}/emails/{self.email}"
        super().__init__(self)

    def __str__(self):
        return self.email

    def delete(self):
        """
        Deletes an email address of an account.

        :return:
        """
        self.gerrit.delete(self.endpoint)

    def set_preferred(self):
        """
        Sets an email address as preferred email address for an account.

        :return:
        """
        self.gerrit.put(self.endpoint + "/preferred")


class GerritAccountEmails:
    def __init__(self, account, gerrit):
        self.account = account
        self.gerrit = gerrit
        self.endpoint = f"/accounts/{self.account}/emails"

    def list(self):
        """
        Returns the email addresses that are configured for the specified user.

        :return:
        """
        result = self.gerrit.get(self.endpoint)
        return result

    def create(self, email):
        """
        Registers a new email address for the user.

        :return:
        """
        try:
            self.get(email)
            message = f"Account Email {email} already register"
            logger.error(message)
            raise AccountEmailAlreadyExistsError(message)
        except AccountEmailNotFoundError:
            self.gerrit.put(self.endpoint + f"/{email}")
            return self.get(email)

    def get(self, email):
        """
        Retrieves an email address of a user.

        :return:
        """
        try:
            result = self.gerrit.get(self.endpoint + f"/{email}")

            email_ = result.get("email")
            return GerritAccountEmail(
                email=email_, account=self.account, gerrit=self.gerrit
            )
        except requests.exceptions.HTTPError as error:
            if error.response.status_code == 404:
                message = f"Account Email {email} does not exist"
                raise AccountEmailNotFoundError(message)
            raise GerritAPIException from error

    def set_preferred(self, email):
        """
        Sets an email address as preferred email address for an account.

        :param email: account email
        :return:
        """
        self.get(email)
        self.gerrit.put(self.endpoint + f"/{email}/preferred")

    def delete(self, email):
        """
        Deletes an email address of an account.

        :param email: account email
        :return:
        """
        self.get(email)
        self.gerrit.delete(self.endpoint + f"/{email}")
