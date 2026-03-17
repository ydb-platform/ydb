# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

import sys
import os

this_dir = os.path.dirname(__file__)
repo_dir = os.path.join(this_dir, os.pardir)
sys.path.insert(1, repo_dir)

import json
from facebook_business.session import FacebookSession
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects import *
from facebook_business.exceptions import FacebookError


class Authentication():
    """
        DON'T USE THIS CLASS DIRECTLY. USE `auth()` function from this module
        Helper class to authenticate using config.json config file from this
        repository. This is useful in two cases:
            - Testing environment
            - Interactive exploration in REPL
        This class shouldn't be used in production.
        It's intended for development. Use FacebookAdsApi.init in production
    """

    _api = FacebookAdsApi.get_default_api()
    if _api:
        _is_authenticated = True
    else:
        _is_authenticated = False

    @property
    def is_authenticated(cls):
        return cls._is_authenticated

    @classmethod
    def load_config(cls):
        with open(os.path.join(repo_dir, 'config.json')) as config_file:
            config = json.load(config_file)
        return config

    @classmethod
    def auth(cls):
        """
            Prepare for Ads API calls and return a tuple with act_id
            and page_id. page_id can be None but act_id is always set.
        """
        config = cls.load_config()

        if cls._is_authenticated:
            return config['act_id'], config.get('page_id', None)

        if config['app_id'] and config['app_secret'] \
           and config['act_id'] and config['access_token']:

            FacebookAdsApi.init(
                config['app_id'],
                config['app_secret'],
                config['access_token'],
                config['act_id'],
            )

            cls._is_authenticated = True

            return config['act_id'], config.get('page_id', None)

        else:
            required_fields = set(
                ('app_id', 'app_secret', 'act_id', 'access_token')
            )

            missing_fields = required_fields - set(config.keys())
            raise FacebookError(
                '\n\tFile config.json needs to have the following fields: {}\n'
                '\tMissing fields: {}\n'.format(
                    ', '.join(required_fields),
                    ', '.join(missing_fields),
                )
            )


def auth():
    return Authentication.auth()

if sys.flags.interactive:
    auth()
