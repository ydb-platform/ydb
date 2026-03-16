#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Author: Jialiang Shi
import logging
import requests
from gerrit.utils.gerritbase import GerritBase
from gerrit.utils.exceptions import GPGKeyNotFoundError, GerritAPIException

logger = logging.getLogger(__name__)


class GerritAccountGPGKey(GerritBase):
    def __init__(self, id, account, gerrit):
        self.id = id
        self.account = account
        self.gerrit = gerrit
        self.endpoint = f"/accounts/{self.account}/gpgkeys/{self.id}"
        super().__init__(self)

    def __str__(self):
        return self.id

    def delete(self):
        """
        Deletes a GPG key of a user.

        :return:
        """
        self.gerrit.delete(self.endpoint)


class GerritAccountGPGKeys:
    def __init__(self, account, gerrit):
        self.account = account
        self.gerrit = gerrit
        self.endpoint = f"/accounts/{self.account}/gpgkeys"

    def list(self):
        """
        Returns the GPG keys of an account.

        :return:
        """
        result = self.gerrit.get(self.endpoint)
        keys = []
        for key, value in result.items():
            gpg_key = value
            gpg_key.update({"id": key})
            keys.append(gpg_key)

        return keys

    def get(self, id_):
        """
        Retrieves a GPG key of a user.

        :param id_: GPG key id
        :return:
        """
        try:
            result = self.gerrit.get(self.endpoint + f"/{id_}")

            id = result.get("id")
            return GerritAccountGPGKey(id=id, account=self.account, gerrit=self.gerrit)
        except requests.exceptions.HTTPError as error:
            if error.response.status_code == 404:
                message = f"GPG key {id_} does not exist"
                raise GPGKeyNotFoundError(message)
            raise GerritAPIException from error

    def modify(self, input_):
        """
        Add or delete one or more GPG keys for a user.

        .. code-block:: python

            input_ = {
                "add": [
                  "-----BEGIN PGP PUBLIC KEY BLOCK-----\\n
                  Version: GnuPG v1\\n\\n
                  mQENBFXUpNcBCACv4paCiyKxZ0EcKy8VaWVNkJlNebRBiyw9WxU85wPOq5Gz/3GT\\n
                  RQwKqeY0SxVdQT8VNBw2sBe2m6eqcfZ2iKmesSlbXMe15DA7k8Bg4zEpQ0tXNG1L\\n
                  hceZDVQ1Xk06T2sgkunaiPsXi82nwN3UWYtDXxX4is5e6xBNL48Jgz4lbqo6+8D5\\n
                  vsVYiYMx4AwRkJyt/oA3IZAtSlY8Yd445nY14VPcnsGRwGWTLyZv9gxKHRUppVhQ\\n
                  E3o6ePXKEVgmONnQ4CjqmkGwWZvjMF2EPtAxvQLAuFa8Hqtkq5cgfgVkv/Vrcln4\\n
                  nQZVoMm3a3f5ODii2tQzNh6+7LL1bpqAmVEtABEBAAG0H0pvaG4gRG9lIDxqb2hu\\n
                  LmRvZUBleGFtcGxlLmNvbT6JATgEEwECACIFAlXUpNcCGwMGCwkIBwMCBhUIAgkK\\n
                  CwQWAgMBAh4BAheAAAoJEJNQnkuvyKSbfjoH/2OcSQOu1kJ20ndjhgY2yNChm7gd\\n
                  tU7TEBbB0TsLeazkrrLtKvrpW5+CRe07ZAG9HOtp3DikwAyrhSxhlYgVsQDhgB8q\\n
                  G0tYiZtQ88YyYrncCQ4hwknrcWXVW9bK3V4ZauxzPv3ADSloyR9tMURw5iHCIeL5\\n
                  fIw/pLvA3RjPMx4Sfow/bqRCUELua39prGw5Tv8a2ZRFbj2sgP5j8lUFegyJPQ4z\\n
                  tJhe6zZvKOzvIyxHO8llLmdrImsXRL9eqroWGs0VYqe6baQpY6xpSjbYK0J5HYcg\\n
                  TO+/u80JI+ROTMHE6unGp5Pgh/xIz6Wd34E0lWL1eOyNfGiPLyRWn1d0yZO5AQ0E\\n
                  VdSk1wEIALUycrH2HK9zQYdR/KJo1yJJuaextLWsYYn881yDQo/p06U5vXOZ28lG\\n
                  Aq/Xs96woVZPbgME6FyQzhf20Z2sbr+5bNo3OcEKaKX3Eo/sWwSJ7bXbGLDxMf4S\\n
                  etfY1WDC+4rTqE30JuC++nQviPRdCcZf0AEgM6TxVhYEMVYwV787YO1IH62EBICM\\n
                  SkIONOfnusNZ4Skgjq9OzakOOpROZ4tki5cH/5oSDgdcaGPy1CFDpL9fG6er2zzk\\n
                  sw3qCbraqZrrlgpinWcAduiao67U/dV18O6OjYzrt33fTKZ0+bXhk1h1gloC21MQ\\n
                  ya0CXlnfR/FOQhvuK0RlbR3cMfhZQscAEQEAAYkBHwQYAQIACQUCVdSk1wIbDAAK\\n
                  CRCTUJ5Lr8ikm8+QB/4uE+AlvFQFh9W8koPdfk7CJF7wdgZZ2NDtktvLL71WuMK8\\n
                  POmf9f5JtcLCX4iJxGzcWogAR5ed20NgUoHUg7jn9Xm3fvP+kiqL6WqPhjazd89h\\n
                  k06v9hPE65kp4wb0fQqDrtWfP1lFGuh77rQgISt3Y4QutDl49vXS183JAfGPxFxx\\n
                  8FgGcfNwL2LVObvqCA0WLqeIrQVbniBPFGocE3yA/0W9BB/xtolpKfgMMsqGRMeu\\n
                  9oIsNxB2oE61OsqjUtGsnKQi8k5CZbhJaql4S89vwS+efK0R+mo+0N55b0XxRlCS\\n
                  faURgAcjarQzJnG0hUps2GNO/+nM7UyyJAGfHlh5\\n
                  =EdXO\\n
                  -----END PGP PUBLIC KEY BLOCK-----\\n"
                ],
                "delete": [
                  "DEADBEEF",
                ]
            }
            account = client.accounts.get('kevin.shi')
            result = account.gpg_keys.modify(input_)

        :param input_: the GpgKeysInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-accounts.html#gpg-keys-input
        :return:
        """
        return self.gerrit.post(
            self.endpoint, json=input_, headers=self.gerrit.default_headers
        )

    def delete(self, id_):
        """
        Deletes a GPG key of a user.

        :param id_: GPG key id
        :return:
        """
        self.get(id_)
        self.gerrit.delete(self.endpoint + f"/{id_}")
