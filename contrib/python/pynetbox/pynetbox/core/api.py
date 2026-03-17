"""
(c) 2017 DigitalOcean

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import contextlib

import requests

from pynetbox.core.app import App, PluginsApp
from pynetbox.core.query import Request
from pynetbox.core.response import Record


class Api:
    """The API object is the point of entry to pynetbox.

    After instantiating the Api() with the appropriate named arguments
    you can specify which app and endpoint you wish to interact with.

    Valid attributes currently are:

    * circuits
    * core (NetBox 3.5+)
    * dcim
    * extras
    * ipam
    * tenancy
    * users
    * virtualization
    * vpn (NetBox 3.7+)
    * wireless

    Calling any of these attributes will return an `App` object which exposes endpoints as attributes.

    ## Additional Attributes

    * **http_session(requests.Session)**: Override the default session with your own. This is used to control
      a number of HTTP behaviors such as SSL verification, custom headers,
      retires, and timeouts.
      See [custom sessions](advanced.md#custom-sessions) for more info.

    ## Parameters

    * **url** (str): The base URL to the instance of NetBox you wish to connect to.
    * **token** (str): Your NetBox token.
    * **threading** (bool, optional): Set to True to use threading in `.all()` and `.filter()` requests.

    ## Raises

    * **AttributeError**: If app doesn't exist.

    ## Examples

    ```python
    import pynetbox
    nb = pynetbox.api(
        'http://localhost:8000',
        token='d6f4e314a5b5fefd164995169f28ae32d987704f'
    )
    list(nb.dcim.devices.all())
    # [test1-leaf1, test1-leaf2, test1-leaf3]
    ```
    """

    def __init__(
        self,
        url,
        token=None,
        threading=False,
        strict_filters=False,
    ):
        """Initialize the API client.

        Args:
            url (str): The base URL to the instance of NetBox you wish to connect to.
            token (str, optional): Your NetBox API token. If not provided, authentication will be required for each request.
            threading (bool, optional): Set to True to use threading in `.all()` and `.filter()` requests, defaults to False.
            strict_filters (bool, optional): Set to True to check GET call filters against OpenAPI specifications (intentionally not done in NetBox API), defaults to False.
        """
        base_url = "{}/api".format(url if url[-1] != "/" else url[:-1])
        self.token = token
        self.base_url = base_url
        self.http_session = requests.Session()
        self.threading = threading
        self.strict_filters = strict_filters

        # Initialize NetBox apps
        self.circuits = App(self, "circuits")
        self.core = App(self, "core")
        self.dcim = App(self, "dcim")
        self.extras = App(self, "extras")
        self.ipam = App(self, "ipam")
        self.tenancy = App(self, "tenancy")
        self.users = App(self, "users")
        self.virtualization = App(self, "virtualization")
        self.vpn = App(self, "vpn")
        self.wireless = App(self, "wireless")
        self.plugins = PluginsApp(self)

    @property
    def version(self):
        """Gets the API version of NetBox.

        Can be used to check the NetBox API version if there are
        version-dependent features or syntaxes in the API.

        ## Returns
        Version number as a string.

        ## Example

        ```python
        import pynetbox
        nb = pynetbox.api(
            'http://localhost:8000',
            token='d6f4e314a5b5fefd164995169f28ae32d987704f'
        )
        nb.version
        # '3.1'
        ```
        """
        version = Request(
            base=self.base_url,
            token=self.token,
            http_session=self.http_session,
        ).get_version()
        return version

    def openapi(self):
        """Returns the OpenAPI spec.

        Quick helper function to pull down the entire OpenAPI spec.
        It is stored in memory to avoid repeated calls on NetBox API.

        ## Returns
        dict: The OpenAPI specification as a dictionary.

        ## Example

        ```python
        import pynetbox
        nb = pynetbox.api(
            'http://localhost:8000',
            token='d6f4e314a5b5fefd164995169f28ae32d987704f'
        )
        nb.openapi()
        # {...}
        ```
        """
        if not (openapi := getattr(self, "_openapi", None)):
            openapi = self._openapi = Request(
                base=self.base_url,
                http_session=self.http_session,
            ).get_openapi()

        return openapi

    def status(self):
        """Gets the status information from NetBox.

        ## Returns
        Dictionary containing NetBox status information.

        ## Raises
        `RequestError`: If the request is not successful.

        ## Example

        ```python
        from pprint import pprint
        pprint(nb.status())
        {
            'django-version': '3.1.3',
            'installed-apps': {
                'cacheops': '5.0.1',
                'debug_toolbar': '3.1.1',
                'django_filters': '2.4.0',
                'django_prometheus': '2.1.0',
                'django_rq': '2.4.0',
                'django_tables2': '2.3.3',
                'drf_yasg': '1.20.0',
                'mptt': '0.11.0',
                'rest_framework': '3.12.2',
                'taggit': '1.3.0',
                'timezone_field': '4.0'
            },
            'netbox-version': '2.10.2',
            'plugins': {},
            'python-version': '3.7.3',
            'rq-workers-running': 1
        }
        ```
        """
        status = Request(
            base=self.base_url,
            token=self.token,
            http_session=self.http_session,
        ).get_status()
        return status

    def create_token(self, username, password):
        """Creates an API token using a valid NetBox username and password.
        Saves the created token automatically in the API object.

        ## Parameters
        * **username** (str): NetBox username
        * **password** (str): NetBox password

        ## Returns
        `Record`: The token as a Record object.

        ## Raises
        `RequestError`: If the request is not successful.

        ## Example

        ```python
        import pynetbox
        nb = pynetbox.api("https://netbox-server")
        token = nb.create_token("admin", "netboxpassword")
        nb.token
        # '96d02e13e3f1fdcd8b4c089094c0191dcb045bef'

        from pprint import pprint
        pprint(dict(token))
        {
            'created': '2021-11-27T11:26:49.360185+02:00',
            'description': '',
            'display': '045bef (admin)',
            'expires': None,
            'id': 2,
            'key': '96d02e13e3f1fdcd8b4c089094c0191dcb045bef',
            'url': 'https://netbox-server/api/users/tokens/2/',
            'user': {
                'display': 'admin',
                'id': 1,
                'url': 'https://netbox-server/api/users/users/1/',
                'username': 'admin'
            },
            'write_enabled': True
        }
        ```
        """
        resp = Request(
            base="{}/users/tokens/provision/".format(self.base_url),
            http_session=self.http_session,
        ).post(data={"username": username, "password": password})
        # Save the newly created API token, otherwise populating the Record
        # object details will fail
        self.token = resp["key"]
        return Record(resp, self, None)

    @contextlib.contextmanager
    def activate_branch(self, branch):
        """Context manager to activate the branch by setting the schema ID in the headers.

        **Note**: The NetBox branching plugin must be installed and enabled in your NetBox instance for this functionality to work.

        ## Parameters
        * **branch** (Record): The NetBox branch to activate

        ## Raises
        `ValueError`: If the branch is not a valid NetBox branch.

        ## Example

        ```python
        import pynetbox
        nb = pynetbox.api("https://netbox-server")
        branch = nb.plugins.branching.branches.create(name="testbranch")
        with nb.activate_branch(branch):
            sites = nb.dcim.sites.all()
            # All operations within this block will use the branch's schema
        ```
        """
        if not isinstance(branch, Record) or "schema_id" not in dict(branch):
            raise ValueError(
                f"The specified branch is not a valid NetBox branch: {branch}."
            )

        self.http_session.headers["X-NetBox-Branch"] = branch.schema_id

        try:
            yield
        finally:
            self.http_session.headers.pop("X-NetBox-Branch", None)
