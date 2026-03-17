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

try:
    import simplejson as json
except ImportError:
    import json  # type: ignore

import time

from libcloud.http import LibcloudConnection
from libcloud.utils.py3 import urlparse, urlencode, basestring
from libcloud.common.base import BaseDriver, RawResponse, JsonResponse, ConnectionUserAndKey


class AzureBaseDriver(BaseDriver):
    name = "Microsoft Azure Resource Management API"


class AzureJsonResponse(JsonResponse):
    def parse_error(self):
        b = self.parse_body()

        if isinstance(b, basestring):
            return b
        elif isinstance(b, dict) and "error" in b:
            return "[{}] {}".format(b["error"].get("code"), b["error"].get("message"))
        else:
            return str(b)


class AzureAuthJsonResponse(JsonResponse):
    def parse_error(self):
        b = self.parse_body()

        if isinstance(b, basestring):
            return b
        elif isinstance(b, dict) and "error_description" in b:
            return b["error_description"]
        else:
            return str(b)


# Based on
# https://github.com/Azure/azure-xplat-cli/blob/master/lib/util/profile/environment.js
publicEnvironments = {
    "default": {
        "name": "default",
        "portalUrl": "http://go.microsoft.com/fwlink/?LinkId=254433",
        "publishingProfileUrl": "http://go.microsoft.com/fwlink/?LinkId=254432",
        "managementEndpointUrl": "https://management.core.windows.net",
        "resourceManagerEndpointUrl": "https://management.azure.com/",
        "sqlManagementEndpointUrl": "https://management.core.windows.net:8443/",
        "sqlServerHostnameSuffix": ".database.windows.net",
        "galleryEndpointUrl": "https://gallery.azure.com/",
        "activeDirectoryEndpointUrl": "https://login.microsoftonline.com",
        "activeDirectoryResourceId": "https://management.core.windows.net/",
        "activeDirectoryGraphResourceId": "https://graph.windows.net/",
        "activeDirectoryGraphApiVersion": "2013-04-05",
        "storageEndpointSuffix": ".core.windows.net",
        "keyVaultDnsSuffix": ".vault.azure.net",
        "azureDataLakeStoreFileSystemEndpointSuffix": "azuredatalakestore.net",
        "azureDataLakeAnalyticsCatalogAndJobEndpointSuffix": "azuredatalakeanalytics.net",
    },
    "AzureChinaCloud": {
        "name": "AzureChinaCloud",
        "portalUrl": "http://go.microsoft.com/fwlink/?LinkId=301902",
        "publishingProfileUrl": "http://go.microsoft.com/fwlink/?LinkID=301774",
        "managementEndpointUrl": "https://management.core.chinacloudapi.cn",
        "resourceManagerEndpointUrl": "https://management.chinacloudapi.cn",
        "sqlManagementEndpointUrl": "https://management.core.chinacloudapi.cn:8443/",
        "sqlServerHostnameSuffix": ".database.chinacloudapi.cn",
        "galleryEndpointUrl": "https://gallery.chinacloudapi.cn/",
        "activeDirectoryEndpointUrl": "https://login.chinacloudapi.cn",
        "activeDirectoryResourceId": "https://management.core.chinacloudapi.cn/",
        "activeDirectoryGraphResourceId": "https://graph.chinacloudapi.cn/",
        "activeDirectoryGraphApiVersion": "2013-04-05",
        "storageEndpointSuffix": ".core.chinacloudapi.cn",
        "keyVaultDnsSuffix": ".vault.azure.cn",
        "azureDataLakeStoreFileSystemEndpointSuffix": "N/A",
        "azureDataLakeAnalyticsCatalogAndJobEndpointSuffix": "N/A",
    },
    "AzureUSGovernment": {
        "name": "AzureUSGovernment",
        "portalUrl": "https://manage.windowsazure.us",
        "publishingProfileUrl": "https://manage.windowsazure.us/publishsettings/index",
        "managementEndpointUrl": "https://management.core.usgovcloudapi.net",
        "resourceManagerEndpointUrl": "https://management.usgovcloudapi.net",
        "sqlManagementEndpointUrl": "https://management.core.usgovcloudapi.net:8443/",
        "sqlServerHostnameSuffix": ".database.usgovcloudapi.net",
        "galleryEndpointUrl": "https://gallery.usgovcloudapi.net/",
        "activeDirectoryEndpointUrl": "https://login-us.microsoftonline.com",
        "activeDirectoryResourceId": "https://management.core.usgovcloudapi.net/",
        "activeDirectoryGraphResourceId": "https://graph.windows.net/",
        "activeDirectoryGraphApiVersion": "2013-04-05",
        "storageEndpointSuffix": ".core.usgovcloudapi.net",
        "keyVaultDnsSuffix": ".vault.usgovcloudapi.net",
        "azureDataLakeStoreFileSystemEndpointSuffix": "N/A",
        "azureDataLakeAnalyticsCatalogAndJobEndpointSuffix": "N/A",
    },
    "AzureGermanCloud": {
        "name": "AzureGermanCloud",
        "portalUrl": "http://portal.microsoftazure.de/",
        "publishingProfileUrl": "https://manage.microsoftazure.de/publishsettings/index",
        "managementEndpointUrl": "https://management.core.cloudapi.de",
        "resourceManagerEndpointUrl": "https://management.microsoftazure.de",
        "sqlManagementEndpointUrl": "https://management.core.cloudapi.de:8443/",
        "sqlServerHostnameSuffix": ".database.cloudapi.de",
        "galleryEndpointUrl": "https://gallery.cloudapi.de/",
        "activeDirectoryEndpointUrl": "https://login.microsoftonline.de",
        "activeDirectoryResourceId": "https://management.core.cloudapi.de/",
        "activeDirectoryGraphResourceId": "https://graph.cloudapi.de/",
        "activeDirectoryGraphApiVersion": "2013-04-05",
        "storageEndpointSuffix": ".core.cloudapi.de",
        "keyVaultDnsSuffix": ".vault.microsoftazure.de",
        "azureDataLakeStoreFileSystemEndpointSuffix": "N/A",
        "azureDataLakeAnalyticsCatalogAndJobEndpointSuffix": "N/A",
    },
}


class AzureResourceManagementConnection(ConnectionUserAndKey):
    """
    Represents a single connection to Azure
    """

    conn_class = LibcloudConnection
    driver = AzureBaseDriver
    name = "Azure AD Auth"
    responseCls = AzureJsonResponse
    rawResponseCls = RawResponse

    def __init__(
        self,
        key,
        secret,
        secure=True,
        tenant_id=None,
        subscription_id=None,
        cloud_environment=None,
        **kwargs,
    ):
        super().__init__(key, secret, **kwargs)
        if not cloud_environment:
            cloud_environment = "default"
        if isinstance(cloud_environment, basestring):
            cloud_environment = publicEnvironments[cloud_environment]
        if not isinstance(cloud_environment, dict):
            raise Exception(
                "cloud_environment must be one of '%s' or a dict "
                "containing keys 'resourceManagerEndpointUrl', "
                "'activeDirectoryEndpointUrl', "
                "'activeDirectoryResourceId', "
                "'storageEndpointSuffix'" % ("', '".join(publicEnvironments.keys()))
            )
        self.host = urlparse.urlparse(cloud_environment["resourceManagerEndpointUrl"]).hostname
        self.login_host = urlparse.urlparse(
            cloud_environment["activeDirectoryEndpointUrl"]
        ).hostname
        self.login_resource = cloud_environment["activeDirectoryResourceId"]
        self.storage_suffix = cloud_environment["storageEndpointSuffix"]
        self.tenant_id = tenant_id
        self.subscription_id = subscription_id

    def add_default_headers(self, headers):
        headers["Content-Type"] = "application/json"
        headers["Authorization"] = "Bearer %s" % self.access_token
        return headers

    def encode_data(self, data):
        """Encode data to JSON"""
        return json.dumps(data)

    def get_token_from_credentials(self):
        """
        Log in and get bearer token used to authorize API requests.
        """

        conn = self.conn_class(self.login_host, 443, timeout=self.timeout)
        conn.connect()
        params = urlencode(
            {
                "grant_type": "client_credentials",
                "client_id": self.user_id,
                "client_secret": self.key,
                "resource": self.login_resource,
            }
        )
        headers = {"Content-type": "application/x-www-form-urlencoded"}
        conn.request("POST", "/%s/oauth2/token" % self.tenant_id, params, headers)
        js = AzureAuthJsonResponse(conn.getresponse(), conn)
        self.access_token = js.object["access_token"]
        self.expires_on = js.object["expires_on"]

    def connect(self, **kwargs):
        self.get_token_from_credentials()
        return super().connect(**kwargs)

    def request(self, action, params=None, data=None, headers=None, method="GET", raw=False):
        # Log in again if the token has expired or is going to expire soon
        # (next 5 minutes).
        if (time.time() + 300) >= int(self.expires_on):
            self.get_token_from_credentials()

        return super().request(
            action, params=params, data=data, headers=headers, method=method, raw=raw
        )
