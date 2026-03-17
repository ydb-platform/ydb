# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

import sys
import traceback
import inspect
import json
import logging

from enum import Enum

from facebook_business.api import FacebookRequest
from facebook_business.session import FacebookSession
from facebook_business.api import FacebookAdsApi
from facebook_business.exceptions import FacebookError
from facebook_business.exceptions import FacebookRequestError


class Reasons(Enum):
    API = 'API'
    SDK = 'SDK'

class CrashReporter(object):

    reporter_instance = None
    logger = None

    def __init__(self, app_id, excepthook):
        self.__app_id = app_id
        self.__excepthook = excepthook

    @classmethod
    def enable(cls):
        if cls.reporter_instance is None:
            api = FacebookAdsApi.get_default_api()
            cls.reporter_instance = cls(api._session.app_id, sys.excepthook)
            sys.excepthook = cls.reporter_instance.__exception_handler
            cls.logging('Enabled')

    @classmethod
    def disable(cls):
        if cls.reporter_instance != None:
            # Restore the original excepthook
            sys.excepthook = cls.reporter_instance.__excepthook
            cls.reporter_instance = None
            cls.logging('Disabled')

    @classmethod
    def enableLogging(cls):
        if cls.logger is None:
            logging.basicConfig()
            cls.logger = logging.getLogger(__name__)
            cls.logger.setLevel(logging.INFO)

    @classmethod
    def disableLogging(cls):
        if cls.logger != None:
            cls.logger = None

    @classmethod
    def logging(cls, info):
        if cls.logger != None:
            cls.logger.info(info)

    def __exception_handler(self, etype, evalue, tb):
        params = self.__build_param(etype, tb)
        if params:
            CrashReporter.logging('Crash detected!')
            self.__send_report(params)
        else:
            CrashReporter.logging('No crash detected.')

        self.__forward_exception(etype, evalue, tb)


    def __forward_exception(self, etype, evalue, tb):
        self.__excepthook(etype, evalue, tb)

    def __build_param(self, etype, tb):
        if not etype:
            return None
        fb_request_errors = [cls.__name__ for cls in FacebookError.__subclasses__()]
        reason = None

        if etype.__name__ == FacebookRequestError.__name__:
            reason = Reasons.API
        elif etype.__name__ in fb_request_errors:
            reason = Reasons.SDK

        if reason is None:
            extracted_tb = traceback.extract_tb(tb, limit=100)
            for ii, (filename, line, funcname, code) in enumerate(extracted_tb):
                if filename.find('facebook_business') != -1:
                    reason = Reasons.SDK

        if reason is None:
            return None

        return {
            'reason': "{} : {}".format(reason.value, etype.__name__),
            'callstack': traceback.format_tb(tb),
            'platform': sys.version
        };

    def __send_report(self, payload):
        try:
            anonymous = FacebookSession()
            api = FacebookAdsApi(anonymous)
            request = FacebookRequest(
                node_id=self.__app_id,
                method='POST',
                endpoint='/instruments',
                api=api,
            )
            request.add_params({'bizsdk_crash_report':payload})
            request.execute()
            CrashReporter.logging('Succeed to Send Crash Report.')
        except Exception as e:
            CrashReporter.logging('Fail to Send Crash Report.')
