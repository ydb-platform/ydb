# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Base classes for working with xmlrpc APIs
"""

from typing import Dict, Type

from libcloud.utils.py3 import httplib, xmlrpclib
from libcloud.common.base import Response, Connection
from libcloud.common.types import LibcloudError


class ProtocolError(Exception):
    pass


class ErrorCodeMixin:
    """
    This is a helper for API's that have a well defined collection of error
    codes that are easily parsed out of error messages. It acts as a factory:
    it finds the right exception for the error code, fetches any parameters it
    needs from the context and raises it.
    """

    exceptions = {}  # type: Dict[str, Type[LibcloudError]]

    def raise_exception_for_error(self, error_code, message):
        exceptionCls = self.exceptions.get(error_code, None)
        if exceptionCls is None:
            return
        context = self.connection.context
        driver = self.connection.driver
        params = {}
        if hasattr(exceptionCls, "kwargs"):
            for key in exceptionCls.kwargs:
                if key in context:
                    params[key] = context[key]
        raise exceptionCls(value=message, driver=driver, **params)


class XMLRPCResponse(ErrorCodeMixin, Response):
    defaultExceptionCls = Exception  # type: Type[Exception]

    def success(self):
        return self.status == httplib.OK

    def parse_body(self):
        try:
            params, methodname = xmlrpclib.loads(self.body)
            if len(params) == 1:
                params = params[0]
            return params
        except xmlrpclib.Fault as e:
            self.raise_exception_for_error(e.faultCode, e.faultString)
            error_string = "{}: {}".format(e.faultCode, e.faultString)
            raise self.defaultExceptionCls(error_string)

    def parse_error(self):
        msg = "Server returned an invalid xmlrpc response (%d)" % (self.status)
        raise ProtocolError(msg)


class XMLRPCConnection(Connection):
    """
    Connection class which can call XMLRPC based API's.

    This class uses the xmlrpclib marshalling and demarshalling code but uses
    the http transports provided by libcloud giving it better certificate
    validation and debugging helpers than the core client library.
    """

    responseCls = XMLRPCResponse
    endpoint = None  # type: str

    def add_default_headers(self, headers):
        headers["Content-Type"] = "text/xml"
        return headers

    def request(self, method_name, *args, **kwargs):
        """
        Call a given `method_name`.

        :type method_name: ``str``
        :param method_name: A method exposed by the xmlrpc endpoint that you
            are connecting to.

        :type args: ``tuple``
        :param args: Arguments to invoke with method with.
        """
        endpoint = kwargs.get("endpoint", self.endpoint)
        data = xmlrpclib.dumps(args, methodname=method_name, allow_none=True)
        return super().request(endpoint, data=data, method="POST")
