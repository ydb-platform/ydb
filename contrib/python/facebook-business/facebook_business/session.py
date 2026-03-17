# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

"""
The purpose of the session module is to encapsulate authentication classes and
utilities.
"""
import facebook_business.certs

import hashlib
import hmac
import requests


class FacebookSession(object):
    """
    FacebookSession manages the the Graph API authentication and https
    connection.

    Attributes:
        GRAPH (class): The graph url without an ending forward-slash.
        app_id: The application id.
        app_secret: The application secret.
        access_token: The access token.
        appsecret_proof: The application secret proof.
        proxies: Object containing proxies for 'http' and 'https'
        requests: The python requests object through which calls to the api can
            be made.
    """
    GRAPH = 'https://graph.facebook.com'

    def __init__(self, app_id=None, app_secret=None, access_token=None,
                 proxies=None, timeout=None, debug=False):
        """
        Initializes and populates the instance attributes with app_id,
        app_secret, access_token, appsecret_proof, proxies, timeout and requests
        given arguments app_id, app_secret, access_token, proxies and timeout.
        """
        self.app_id = app_id
        self.app_secret = app_secret
        self.access_token = access_token
        self.proxies = proxies
        self.timeout = timeout
        self.debug = debug
        self.requests = requests.Session()
        self.requests.verify = facebook_business.certs.cert_manager.get_cert_file()
        params = {
            'access_token': self.access_token
        }
        if app_secret:
            params['appsecret_proof'] = self._gen_appsecret_proof()
        self.requests.params.update(params)

        if self.proxies:
            self.requests.proxies.update(self.proxies)

    def _gen_appsecret_proof(self):
        h = hmac.new(
            self.app_secret.encode('utf-8'),
            msg=self.access_token.encode('utf-8'),
            digestmod=hashlib.sha256
        )

        self.appsecret_proof = h.hexdigest()
        return self.appsecret_proof

__all__ = ['FacebookSession']
