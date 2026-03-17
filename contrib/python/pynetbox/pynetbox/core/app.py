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

from pynetbox.core.endpoint import Endpoint
from pynetbox.core.query import Request
from pynetbox.models import (
    circuits,
    core,
    dcim,
    extras,
    ipam,
    users,
    virtualization,
    wireless,
)


class App:
    """Represents apps in NetBox.

    Calls to attributes are returned as Endpoint objects.

    ## Returns
    Endpoint matching requested attribute.

    ## Raises
    RequestError if requested endpoint doesn't exist.
    """

    def __init__(self, api, name):
        self.api = api
        self.name = name
        self._setmodel()

    models = {
        "circuits": circuits,
        "core": core,
        "dcim": dcim,
        "extras": extras,
        "ipam": ipam,
        "users": users,
        "virtualization": virtualization,
        "wireless": wireless,
    }

    def _setmodel(self):
        self.model = App.models[self.name] if self.name in App.models else None

    def __getstate__(self):
        return {"api": self.api, "name": self.name}

    def __setstate__(self, d):
        self.__dict__.update(d)
        self._setmodel()

    def __getattr__(self, name):
        return Endpoint(self.api, self, name, model=self.model)

    def config(self):
        """Returns config response from app.

        ## Returns
        Raw response from NetBox's config endpoint.

        ## Raises
        RequestError if called for an invalid endpoint.

        ## Examples

        ```python
        pprint.pprint(nb.users.config())
        {
            'tables': {
                'DeviceTable': {
                    'columns': [
                        'name',
                        'status',
                        'tenant',
                        'role',
                        'site',
                        'primary_ip',
                        'tags'
                    ]
                }
            }
        }
        ```
        """
        config = Request(
            base="{}/{}/config/".format(
                self.api.base_url,
                self.name,
            ),
            token=self.api.token,
            http_session=self.api.http_session,
        ).get()
        return config


class PluginsApp:
    """Basically valid plugins api could be handled by same App class,
    but you need to add plugins to request url path.

    ## Returns
    App with added plugins into path.
    """

    def __init__(self, api):
        self.api = api

    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, d):
        self.__dict__.update(d)

    def __getattr__(self, name):
        return App(self.api, "plugins/{}".format(name.replace("_", "-")))

    def installed_plugins(self):
        """Returns raw response with installed plugins.

        ## Returns
        Raw response NetBox's installed plugins.

        ## Examples

        ```python
        nb.plugins.installed_plugins()
        [
            {
                'name': 'test_plugin',
                'package': 'test_plugin',
                'author': 'Dmitry',
                'description': 'Netbox test plugin',
                'verison': '0.10'
            }
        ]
        ```
        """
        installed_plugins = Request(
            base="{}/plugins/installed-plugins".format(
                self.api.base_url,
            ),
            token=self.api.token,
            http_session=self.api.http_session,
        ).get()
        return installed_plugins
