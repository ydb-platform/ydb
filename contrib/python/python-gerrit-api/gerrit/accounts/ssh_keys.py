#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Author: Jialiang Shi
import logging
import requests
from gerrit.utils.gerritbase import GerritBase
from gerrit.utils.exceptions import SSHKeyNotFoundError, GerritAPIException

logger = logging.getLogger(__name__)


class GerritAccountSSHKey(GerritBase):
    def __init__(self, seq, account, gerrit):
        self.seq = seq
        self.account = account
        self.gerrit = gerrit
        self.endpoint = f"/accounts/{self.account}/sshkeys"
        super().__init__(self)

    def __str__(self):
        return str(self.seq)

    def delete(self):
        """
        Deletes an SSH key of a user.

        :return:
        """
        self.gerrit.delete(self.endpoint + f"/{str(self.seq)}")


class GerritAccountSSHKeys:
    def __init__(self, account, gerrit):
        self.account = account
        self.gerrit = gerrit
        self.endpoint = f"/accounts/{self.account}/sshkeys"

    def list(self):
        """
        Returns the SSH keys of an account.

        :return:
        """
        result = self.gerrit.get(self.endpoint)
        return result

    def get(self, seq):
        """
        Retrieves an SSH key of a user.

        :param seq: SSH key id
        :return:
        """
        try:
            result = self.gerrit.get(self.endpoint + f"/{str(seq)}")

            seq = result.get("seq")
            return GerritAccountSSHKey(
                seq=seq, account=self.account, gerrit=self.gerrit
            )
        except requests.exceptions.HTTPError as error:
            if error.response.status_code == 404:
                message = f"SSH key {seq} does not exist"
                raise SSHKeyNotFoundError(message)
            raise GerritAPIException from error

    def add(self, ssh_key):
        """
        Adds an SSH key for a user.
        The SSH public key must be provided as raw content in the request body.

        :param ssh_key: SSH key raw content
        :return:
        """
        result = self.gerrit.post(
            self.endpoint, data=ssh_key, headers={"Content-Type": "plain/text"}
        )
        return result

    def delete(self, seq):
        """
        Deletes an SSH key of a user.

        :param seq: SSH key id
        :return:
        """
        self.get(seq)
        self.gerrit.delete(self.endpoint + f"/{str(seq)}")
