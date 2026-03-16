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
from pylxd.models import _model as model


class Profile(model.Model):
    """A LXD profile."""

    config = model.Attribute()
    description = model.Attribute()
    devices = model.Attribute()
    name = model.Attribute(readonly=True)
    used_by = model.Attribute(readonly=True)

    @classmethod
    def exists(cls, client, name):
        """Determine whether a profile exists."""
        try:
            client.profiles.get(name)
            return True
        except cls.NotFound:
            return False

    @classmethod
    def get(cls, client, name):
        """Get a profile."""
        response = client.api.profiles[name].get()
        return cls(client, **response.json()['metadata'])

    @classmethod
    def all(cls, client):
        """Get all profiles."""
        response = client.api.profiles.get()

        profiles = []
        for url in response.json()['metadata']:
            name = url.split('/')[-1]
            profiles.append(cls(client, name=name))
        return profiles

    @classmethod
    def create(cls, client, name, config=None, devices=None):
        """Create a profile."""
        profile = {'name': name}
        if config is not None:
            profile['config'] = config
        if devices is not None:
            profile['devices'] = devices
        client.api.profiles.post(json=profile)
        return cls.get(client, name)

    @property
    def api(self):
        return self.client.api.profiles[self.name]

    def rename(self, new_name):
        """Rename the profile."""
        self.api.post(json={'name': new_name})

        return Profile.get(self.client, new_name)
