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
        return cls(client, **response.json()["metadata"])

    @classmethod
    def all(cls, client):
        """Get all profiles."""
        response = client.api.profiles.get()

        profiles = []
        for url in response.json()["metadata"]:
            name = url.split("/")[-1]
            name = name.split("?")[0]
            profiles.append(cls(client, name=name))
        return profiles

    @classmethod
    def create(
        cls, client, name, config=None, devices=None, description=None, wait=False
    ):
        """Create a profile.

        :param client: The pylxd client object
        :type client: :class:`pylxd.client.Client`
        :param name: The name of the profile to create
        :type name: str
        :param config: Configuration dictionary for the profile
        :type config: dict
        :param devices: Devices dictionary for the profile
        :type devices: dict
        :param description: Description of the profile
        :type description: str
        :param wait: Whether to wait for async operations to complete
        :type wait: bool
        :returns: The created profile
        :rtype: :class:`pylxd.models.profile.Profile`
        :raises: :class:`pylxd.exceptions.LXDAPIException` if the profile
            couldn't be created.
        """
        profile = {"name": name}
        if config is not None:
            profile["config"] = config
        if devices is not None:
            profile["devices"] = devices
        if description is not None:
            profile["description"] = description

        response = client.api.profiles.post(json=profile)
        # Handle async response if needed
        cls._handle_async_response_for_client(client, response, wait)
        return cls.get(client, name)

    @property
    def api(self):
        return self.client.api.profiles[self.name]

    def rename(self, new_name, wait=False):
        """Rename the profile.

        :param new_name: The new name for the profile
        :type new_name: str
        :param wait: Whether to wait for async operations to complete
        :type wait: bool
        :returns: The renamed profile
        :rtype: :class:`pylxd.models.profile.Profile`
        :raises: :class:`pylxd.exceptions.LXDAPIException` if the profile
            couldn't be renamed.
        """
        response = self.api.post(json={"name": new_name})

        # Handle async response if needed
        self._handle_async_response(response, wait)
        return Profile.get(self.client, new_name)

    def save(self, wait=False):
        """Save the profile using PUT back to the LXD server.

        Implements PUT /1.0/profiles/<self.name>

        The fields affected are: `config`, `description`, and `devices`.
        Note that they are replaced in their *entirety*. If finer grained
        control is required, please use the
        :meth:`~pylxd.models.profile.Profile.patch` method directly.

        :param wait: Whether to wait for async operations to complete.
        :type wait: bool
        :raises: :class:`pylxd.exceptions.LXDAPIException` if the profile
            couldn't be saved.
        """
        # Note: This overrides the base save method to add documentation
        # but uses the parent's implementation which now handles async responses
        super().save(wait=wait)

    def put(self, put_object, wait=False):
        """Put the profile.

        Implements PUT /1.0/profiles/<self.name>

        Putting to a profile may fail if the new configuration is
        incompatible. See the LXD documentation for further details.

        Note that the object is refreshed with a `sync` if the PUT is
        successful. If this is *not* desired, then the raw API on the client
        should be used.

        :param put_object: A dictionary containing profile configuration.
        :type put_object: dict
        :param wait: Whether to wait for async operations to complete.
        :type wait: bool
        :raises: :class:`pylxd.exceptions.LXDAPIException` if the profile
            couldn't be modified.
        """
        # Note: This overrides the base put method to add documentation
        # but uses the parent's implementation which now handles async responses
        super().put(put_object, wait)

    def patch(self, patch_object, wait=False):
        """Patch the profile.

        Implements PATCH /1.0/profiles/<self.name>

        Patching the object allows for more fine grained changes to the config.
        The object is refreshed if the PATCH is successful. If this is *not*
        required, then use the client api directly.

        :param patch_object: A dictionary containing profile configuration updates.
        :type patch_object: dict
        :param wait: Whether to wait for async operations to complete.
        :type wait: bool
        :raises: :class:`pylxd.exceptions.LXDAPIException` if the profile
            couldn't be modified.
        """
        # Note: This overrides the base patch method to add documentation
        # but uses the parent's implementation which now handles async responses
        super().patch(patch_object, wait)

    def delete(self, wait=False):
        """Delete the profile.

        Implements DELETE /1.0/profiles/<self.name>

        Deleting a profile may fail if it is still in use.

        :param wait: Whether to wait for async operations to complete.
        :type wait: bool
        :raises: :class:`pylxd.exceptions.LXDAPIException` if the profile
            couldn't be deleted.
        """
        # Note: This overrides the base delete method to add documentation
        # but uses the parent's implementation which now handles async responses
        super().delete(wait=wait)
