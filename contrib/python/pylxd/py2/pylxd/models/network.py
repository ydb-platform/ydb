# Copyright (c) 2016 Canonical Ltd
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
import json

from pylxd.models import _model as model


class Network(model.Model):
    """Model representing a LXD network."""
    name = model.Attribute()
    description = model.Attribute()
    type = model.Attribute()
    config = model.Attribute()
    status = model.Attribute(readonly=True)
    locations = model.Attribute(readonly=True)
    managed = model.Attribute(readonly=True)
    used_by = model.Attribute(readonly=True)

    @classmethod
    def exists(cls, client, name):
        """
        Determine whether network with provided name exists.

        :param client: client instance
        :type client: :class:`~pylxd.client.Client`
        :param name: name of the network
        :type name: str
        :returns: `True` if network exists, `False` otherwise
        :rtype: bool
        """
        try:
            client.networks.get(name)
            return True
        except cls.NotFound:
            return False

    @classmethod
    def get(cls, client, name):
        """
        Get a network by name.

        :param client: client instance
        :type client: :class:`~pylxd.client.Client`
        :param name: name of the network
        :type name: str
        :returns: network instance (if exists)
        :rtype: :class:`Network`
        :raises: :class:`~pylxd.exceptions.NotFound` if network does not exist
        """
        response = client.api.networks[name].get()

        return cls(client, **response.json()['metadata'])

    @classmethod
    def all(cls, client):
        """
        Get all networks.

        :param client: client instance
        :type client: :class:`~pylxd.client.Client`
        :rtype: list[:class:`Network`]
        """
        response = client.api.networks.get()

        networks = []
        for url in response.json()['metadata']:
            name = url.split('/')[-1]
            networks.append(cls(client, name=name))
        return networks

    @classmethod
    def create(cls, client, name, description=None, type=None, config=None):
        """
        Create a network.

        :param client: client instance
        :type client: :class:`~pylxd.client.Client`
        :param name: name of the network
        :type name: str
        :param description: description of the network
        :type description: str
        :param type: type of the network
        :type type: str
        :param config: additional configuration
        :type config: dict
        """
        client.assert_has_api_extension('network')

        network = {'name': name}
        if description is not None:
            network['description'] = description
        if type is not None:
            network['type'] = type
        if config is not None:
            network['config'] = config
        client.api.networks.post(json=network)
        return cls.get(client, name)

    def rename(self, new_name):
        """
        Rename a network.

        :param new_name: new name of the network
        :type new_name: str
        :return: Renamed network instance
        :rtype: :class:`Network`
        """
        self.client.assert_has_api_extension('network')
        self.client.api.networks.post(json={'name': new_name})
        return Network.get(self.client, new_name)

    def save(self, *args, **kwargs):
        self.client.assert_has_api_extension('network')
        super(Network, self).save(*args, **kwargs)

    @property
    def api(self):
        return self.client.api.networks[self.name]

    def __str__(self):
        return json.dumps(self.marshall(skip_readonly=False), indent=2)

    def __repr__(self):
        attrs = []
        for attribute, value in self.marshall().items():
            attrs.append('{}={}'.format(attribute,
                                        json.dumps(value, sort_keys=True)))

        return '{}({})'.format(self.__class__.__name__,
                               ', '.join(sorted(attrs)))
