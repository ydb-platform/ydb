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
from pylxd import managers


class StoragePool(model.Model):
    """An LXD storage_pool.

    This corresponds to the LXD endpoint at
    /1.0/storage-pools

    api_extension: 'storage'
    """
    name = model.Attribute(readonly=True)
    driver = model.Attribute(readonly=True)
    used_by = model.Attribute(readonly=True)
    config = model.Attribute()
    managed = model.Attribute(readonly=True)
    description = model.Attribute()
    status = model.Attribute(readonly=True)
    locations = model.Attribute(readonly=True)

    resources = model.Manager()
    volumes = model.Manager()

    def __init__(self, *args, **kwargs):
        super(StoragePool, self).__init__(*args, **kwargs)

        self.resources = StorageResourcesManager(self)
        self.volumes = StorageVolumeManager(self)

    @classmethod
    def get(cls, client, name):
        """Get a storage_pool by name.

        Implements GET /1.0/storage-pools/<name>

        :param client: The pylxd client object
        :type client: :class:`pylxd.client.Client`
        :param name: the name of the storage pool to get
        :type name: str
        :returns: a storage pool if successful, raises NotFound if not found
        :rtype: :class:`pylxd.models.storage_pool.StoragePool`
        :raises: :class:`pylxd.exceptions.NotFound`
        :raises: :class:`pylxd.exceptions.LXDAPIExtensionNotAvailable` if the
            'storage' api extension is missing.
        """
        client.assert_has_api_extension('storage')
        response = client.api.storage_pools[name].get()

        storage_pool = cls(client, **response.json()['metadata'])
        return storage_pool

    @classmethod
    def all(cls, client):
        """Get all storage_pools.

        Implements GET /1.0/storage-pools

        Note that the returned list is 'sparse' in that only the name of the
        pool is populated.  If any of the attributes are used, then the `sync`
        function is called to populate the object fully.

        :param client: The pylxd client object
        :type client: :class:`pylxd.client.Client`
        :returns: a storage pool if successful, raises NotFound if not found
        :rtype: [:class:`pylxd.models.storage_pool.StoragePool`]
        :raises: :class:`pylxd.exceptions.LXDAPIExtensionNotAvailable` if the
            'storage' api extension is missing.
        """
        client.assert_has_api_extension('storage')
        response = client.api.storage_pools.get()

        storage_pools = []
        for url in response.json()['metadata']:
            name = url.split('/')[-1]
            storage_pools.append(cls(client, name=name))
        return storage_pools

    @classmethod
    def create(cls, client, definition):
        """Create a storage_pool from config.

        Implements POST /1.0/storage-pools

        The `definition` parameter defines what the storage pool will be.  An
        example config for the zfs driver is:

            {
                "config": {
                    "size": "10GB"
                },
                "driver": "zfs",
                "name": "pool1"
            }

        Note that **all** fields in the `definition` parameter are strings.

        For further details on the storage pool types see:
        https://lxd.readthedocs.io/en/latest/storage/

        The function returns the a `StoragePool` instance, if it is
        successfully created, otherwise an Exception is raised.

        :param client: The pylxd client object
        :type client: :class:`pylxd.client.Client`
        :param definition: the fields to pass to the LXD API endpoint
        :type definition: dict
        :returns: a storage pool if successful, raises NotFound if not found
        :rtype: :class:`pylxd.models.storage_pool.StoragePool`
        :raises: :class:`pylxd.exceptions.LXDAPIExtensionNotAvailable` if the
            'storage' api extension is missing.
        :raises: :class:`pylxd.exceptions.LXDAPIException` if the storage pool
            couldn't be created.
        """
        client.assert_has_api_extension('storage')
        client.api.storage_pools.post(json=definition)

        storage_pool = cls.get(client, definition['name'])
        return storage_pool

    @classmethod
    def exists(cls, client, name):
        """Determine whether a storage pool exists.

        A convenience method to determine a pool exists.  However, it is better
        to try to fetch it and catch the :class:`pylxd.exceptions.NotFound`
        exception, as otherwise the calling code is like to fetch the pool
        twice.  Only use this if the calling code doesn't *need* the actual
        storage pool information.

        :param client: The pylxd client object
        :type client: :class:`pylxd.client.Client`
        :param name: the name of the storage pool to get
        :type name: str
        :returns: True if the storage pool exists, False if it doesn't.
        :rtype: bool
        :raises: :class:`pylxd.exceptions.LXDAPIExtensionNotAvailable` if the
            'storage' api extension is missing.
        """
        try:
            cls.get(client, name)
            return True
        except cls.NotFound:
            return False

    @property
    def api(self):
        """Provides an object with the endpoint:

        /1.0/storage-pools/<self.name>

        Used internally to construct endpoints.

        :returns: an API node with the named endpoint
        :rtype: :class:`pylxd.client._APINode`
        """
        return self.client.api.storage_pools[self.name]

    def save(self, wait=False):
        """Save the model using PUT back to the LXD server.

        Implements PUT /1.0/storage-pools/<self.name> *automagically*

        The fields affected are: `description` and `config`.  Note that they
        are replaced in their *entirety*.  If finer grained control is
        required, please use the
        :meth:`~pylxd.models.storage_pool.StoragePool.patch` method directly.

        Updating a storage pool may fail if the config is not acceptable to
        LXD. An :class:`~pylxd.exceptions.LXDAPIException` will be generated in
        that case.

        :raises: :class:`pylxd.exceptions.LXDAPIException` if the storage pool
            can't be deleted.
        """
        # Note this method exists so that it is documented via sphinx.
        super(StoragePool, self).save(wait=wait)

    def delete(self):
        """Delete the storage pool.

        Implements DELETE /1.0/storage-pools/<self.name>

        Deleting a storage pool may fail if it is being used.  See the LXD
        documentation for further details.

        :raises: :class:`pylxd.exceptions.LXDAPIException` if the storage pool
            can't be deleted.
        """
        # Note this method exists so that it is documented via sphinx.
        super(StoragePool, self).delete()

    def put(self, put_object, wait=False):
        """Put the storage pool.

        Implements PUT /1.0/storage-pools/<self.name>

        Putting to a storage pool may fail if the new configuration is
        incompatible with the pool.  See the LXD documentation for further
        details.

        Note that the object is refreshed with a `sync` if the PUT is
        successful.  If this is *not* desired, then the raw API on the client
        should be used.

        :param put_object: A dictionary.  The most useful key will be the
            `config` key.
        :type put_object: dict
        :param wait: Whether to wait for async operations to complete.
        :type wait: bool
        :raises: :class:`pylxd.exceptions.LXDAPIException` if the storage pool
            can't be modified.
        """
        # Note this method exists so that it is documented via sphinx.
        super(StoragePool, self).put(put_object, wait)

    def patch(self, patch_object, wait=False):
        """Patch the storage pool.

        Implements PATCH /1.0/storage-pools/<self.name>

        Patching the object allows for more fine grained changes to the config.
        The object is refreshed if the PATCH is successful.  If this is *not*
        required, then use the client api directly.

        :param patch_object: A dictionary.  The most useful key will be the
            `config` key.
        :type patch_object: dict
        :param wait: Whether to wait for async operations to complete.
        :type wait: bool
        :raises: :class:`pylxd.exceptions.LXDAPIException` if the storage pool
            can't be modified.
        """
        # Note this method exists so that it is documented via sphinx.
        super(StoragePool, self).patch(patch_object, wait)


class StorageResourcesManager(managers.BaseManager):
    manager_for = 'pylxd.models.StorageResources'


class StorageResources(model.Model):
    """An LXD Storage Resources model.

    This corresponds to the LXD endpoing at
    /1.0/storage-pools/<pool>/resources

    At present, this is read-only model.

    api_extension: 'resources'
    """
    space = model.Attribute(readonly=True)
    inodes = model.Attribute(readonly=True)

    @classmethod
    def get(cls, storage_pool):
        """Get a storage_pool resource for a named pool

        Implements GET /1.0/storage-pools/<pool>/resources

        Needs the 'resources' api extension in the LXD server.

        :param storage_pool: a storage pool object on which to fetch resources
        :type storage_pool: :class:`pylxd.models.storage_pool.StoragePool`
        :returns: A storage resources object
        :rtype: :class:`pylxd.models.storage_pool.StorageResources`
        :raises: :class:`pylxd.exceptions.LXDAPIExtensionNotAvailable` if the
            'resources' api extension is missing.
        """
        storage_pool.client.assert_has_api_extension('resources')
        response = storage_pool.api.resources.get()
        resources = cls(storage_pool.client, **response.json()['metadata'])
        return resources


class StorageVolumeManager(managers.BaseManager):
    manager_for = 'pylxd.models.StorageVolume'


class StorageVolume(model.Model):
    """An LXD Storage volume.

    This corresponds to the LXD endpoing at
    /1.0/storage-pools/<pool>/volumes

    api_extension: 'storage'
    """
    name = model.Attribute(readonly=True)
    type = model.Attribute(readonly=True)
    description = model.Attribute(readonly=True)
    config = model.Attribute()
    used_by = model.Attribute(readonly=True)
    location = model.Attribute(readonly=True)

    storage_pool = model.Parent()

    @property
    def api(self):
        """Provides an object with the endpoint:

        /1.0/storage-pools/<storage_pool.name>/volumes/<self.type>/<self.name>

        Used internally to construct endpoints.

        :returns: an API node with the named endpoint
        :rtype: :class:`pylxd.client._APINode`
        """
        return self.storage_pool.api.volumes[self.type][self.name]

    @classmethod
    def all(cls, storage_pool):
        """Get all the volumnes for this storage pool.

        Implements GET /1.0/storage-pools/<name>/volumes

        Volumes returned from this method will only have the name
        set, as that is the only property returned from LXD. If more
        information is needed, `StorageVolume.sync` is the method call
        that should be used.

        Note that the storage volume types are 'container', 'image' and
        'custom', and these maps to the names 'containers', 'images' and
        everything else is mapped to 'custom'.

        :param storage_pool: a storage pool object on which to fetch resources
        :type storage_pool: :class:`pylxd.models.storage_pool.StoragePool`
        :returns: a list storage volume if successful
        :rtype: [:class:`pylxd.models.storage_pool.StorageVolume`]
        :raises: :class:`pylxd.exceptions.LXDAPIExtensionNotAvailable` if the
            'storage' api extension is missing.
        """
        storage_pool.client.assert_has_api_extension('storage')
        response = storage_pool.api.volumes.get()

        volumes = []
        for volume in response.json()['metadata']:
            (_type, name) = volume.split('/')[-2:]
            # for each type, convert to the string that will work with GET
            if _type == 'containers':
                _type = 'container'
            elif _type == 'virtual-machines':
                _type = 'virtual-machine'
            elif _type == 'instances':
                _type = 'instance'
            elif _type == 'images':
                _type = 'image'
            else:
                _type = 'custom'
            volumes.append(
                cls(storage_pool.client,
                    name=name,
                    type=_type,
                    storage_pool=storage_pool))
        return volumes

    @classmethod
    def get(cls, storage_pool, _type, name):
        """Get a StorageVolume by type and name.

        Implements GET /1.0/storage-pools/<pool>/volumes/<type>/<name>

        The `_type` param can only (currently) be one of 'container', 'image'
        or 'custom'.  This was determined by read the LXD source.

        :param storage_pool: a storage pool object on which to fetch resources
        :type storage_pool: :class:`pylxd.models.storage_pool.StoragePool`
        :param _type: the type; one of 'container', 'image', 'custom'
        :type _type: str
        :param name: the name of the storage volume to get
        :type name: str
        :returns: a storage pool if successful, raises NotFound if not found
        :rtype: :class:`pylxd.models.storage_pool.StorageVolume`
        :raises: :class:`pylxd.exceptions.NotFound`
        :raises: :class:`pylxd.exceptions.LXDAPIExtensionNotAvailable` if the
            'storage' api extension is missing.
        """
        storage_pool.client.assert_has_api_extension('storage')
        response = storage_pool.api.volumes[_type][name].get()

        volume = cls(
            storage_pool.client, storage_pool=storage_pool,
            **response.json()['metadata'])
        return volume

    @classmethod
    # def create(cls, storage_pool, definition, wait=True, *args):
    def create(cls, storage_pool, *args, **kwargs):
        """Create a 'custom' Storage Volume in the associated storage pool.

        Implements POST /1.0/storage-pools/<pool>/volumes/custom

        See https://github.com/lxc/lxd/blob/master/doc/rest-api.md#post-19 for
        more details on what the `definition` parameter dictionary should
        contain for various volume creation.

        At the moment the only type of volume that can be created is 'custom',
        and this is currently hardwired into the function.

        The function signature 'hides' that the first parameter to the function
        is the definition.  The function should be called as:

        >>> a_storage_pool.volumes.create(definition_dict, wait=<bool>)

        where `definition_dict` is mandatory, and `wait` defaults to True,
        which makes the default a synchronous function call.

        Note that **all** fields in the `definition` parameter are strings.

        If the caller doesn't wan't to wait for an async operation, then it
        MUST be passed as a keyword argument, and not as a positional
        substitute.

        The function returns the a
        :class:`~pylxd.models.storage_pool.StoragePool` instance, if it is
        successfully created, otherwise an Exception is raised.

        :param storage_pool: a storage pool object on which to fetch resources
        :type storage_pool: :class:`pylxd.models.storage_pool.StoragePool`
        :param definition: the fields to pass to the LXD API endpoint
        :type definition: dict
        :param wait: wait until an async action has completed (default True)
        :type wait: bool
        :returns: a storage pool volume if successful, raises NotFound if not
            found
        :rtype: :class:`pylxd.models.storage_pool.StorageVolume`
        :raises: :class:`pylxd.exceptions.LXDAPIExtensionNotAvailable` if the
            'storage' api extension is missing.
        :raises: :class:`pylxd.exceptions.LXDAPIException` if the storage pool
            volume couldn't be created.
        """
        # This is really awkward, but the implementation details mean,
        # depending on how this function is called, we can't know whether the
        # 2nd positional argument will be definition or a client object. This
        # is an difficulty with how BaseManager is implemented, and to get the
        # convenience of being able to call this 'naturally' off of a
        # storage_pool.  So we have to jump through some hurdles to get the
        # right positional parameters.
        storage_pool.client.assert_has_api_extension('storage')
        wait = kwargs.get('wait', True)
        definition = args[-1]
        assert isinstance(definition, dict)
        assert 'name' in definition
        response = storage_pool.api.volumes.custom.post(json=definition)

        if response.json()['type'] == 'async' and wait:
            storage_pool.client.operations.wait_for_operation(
                response.json()['operation'])

        volume = cls.get(storage_pool,
                         'custom',
                         definition['name'])
        return volume

    def rename(self, _input, wait=False):
        """Rename a storage volume

        This requires api_extension: 'storage_api_volume_rename'.

        Implements: POST /1.0/storage-pools/<pool>/volumes/<type>/<name>

        This operation is either sync or async (when moving to a different
        pool).

        This command also allows the migration across instances, and therefore
        it returns the metadata section (as a dictionary) from the call.  The
        caller should assume that the object is stale and refetch it after any
        migrations are done.  However, the 'name' attribute of the object is
        updated for consistency.

        Unlike :meth:`~pylxd.models.storage_pool.StorageVolume.create`, this
        method does not override any items in the input definition, although it
        does check that the 'name' and 'pool' parameters are set.

        Please see: https://github.com/lxc/lxd/blob/master/doc/rest-api.md
        #10storage-poolspoolvolumestypename
        for more details.

        :param _input: The `input` specification for the rename.
        :type _input: dict
        :param wait: Wait for an async operation, if it is async.
        :type wait: bool
        :returns:  The dictionary from the metadata section of the response if
            successful.
        :rtype: dict[str, str]
        :raises: :class:`pylxd.exceptions.LXDAPIExtensionNotAvailable` if the
            'storage_api_volume_rename' api extension is missing.
        :raises: :class:`pylxd.exceptions.LXDAPIException` if the storage pool
            volume couldn't be renamed.
        """
        assert isinstance(_input, dict)
        assert 'name' in _input
        assert 'pool' in _input
        response = self.api.post(json=_input)

        response_json = response.json()
        if wait:
            self.client.operations.wait_for_operation(
                response_json['operation'])
        self.name = _input['name']
        return response_json['metadata']

    def put(self, put_object, wait=False):
        """Put the storage volume.

        Implements: PUT /1.0/storage-pools/<pool>/volumes/<type>/<name>

        Note that this is functionality equivalent to
        :meth:`~pyxld.models.storage_pool.StorageVolume.save` but by using a
        new object (`put_object`) rather than modifying the object and then
        calling :meth:`~pyxld.models.storage_pool.StorageVolume.save`.

        Putting to a storage volume may fail if the new configuration is
        incompatible with the pool.  See the LXD documentation for further
        details.

        Note that the object is refreshed with a `sync` if the PUT is
        successful.  If this is *not* desired, then the raw API on the client
        should be used.

        :param put_object: A dictionary.  The most useful key will be the
            `config` key.
        :type put_object: dict
        :param wait: Whether to wait for async operations to complete.
        :type wait: bool
        :raises: :class:`pylxd.exceptions.LXDAPIExtensionNotAvailable` if the
            'storage' api extension is missing.
        :raises: :class:`pylxd.exceptions.LXDAPIException` if the storage pool
            can't be modified.
        """
        # Note this method exists so that it is documented via sphinx.
        super(StorageVolume, self).put(put_object, wait)

    def patch(self, patch_object, wait=False):
        """Patch the storage volume.

        Implements: PATCH /1.0/storage-pools/<pool>/volumes/<type>/<name>

        Patching the object allows for more fine grained changes to the config.
        The object is refreshed if the PATCH is successful.  If this is *not*
        required, then use the client api directly.

        :param patch_object: A dictionary.  The most useful key will be the
            `config` key.
        :type patch_object: dict
        :param wait: Whether to wait for async operations to complete.
        :type wait: bool
        :raises: :class:`pylxd.exceptions.LXDAPIExtensionNotAvailable` if the
            'storage' api extension is missing.
        :raises: :class:`pylxd.exceptions.LXDAPIException` if the storage
            volume can't be modified.
        """
        # Note this method exists so that it is documented via sphinx.
        super(StorageVolume, self).patch(patch_object, wait)

    def save(self, wait=False):
        """Save the model using PUT back to the LXD server.

        Implements: PUT /1.0/storage-pools/<pool>/volumes/<type>/<name>
        *automagically*.

        The field affected is `config`.  Note that it is replaced *entirety*.
        If finer grained control is required, please use the
        :meth:`~pylxd.models.storage_pool.StorageVolume.patch` method directly.

        Updating a storage volume may fail if the config is not acceptable to
        LXD. An :class:`~pylxd.exceptions.LXDAPIException` will be generated in
        that case.

        :param wait: Whether to wait for async operations to complete.
        :type wait: bool
        :raises: :class:`pylxd.exceptions.LXDAPIExtensionNotAvailable` if the
            'storage' api extension is missing.
        :raises: :class:`pylxd.exceptions.LXDAPIException` if the storage
            volume can't be deleted.
        """
        # Note this method exists so that it is documented via sphinx.
        super(StorageVolume, self).save(wait=wait)

    def delete(self):
        """Delete the storage pool.

        Implements: DELETE /1.0/storage-pools/<pool>/volumes/<type>/<name>

        Deleting a storage volume may fail if it is being used.  See the LXD
        documentation for further details.

        :raises: :class:`pylxd.exceptions.LXDAPIExtensionNotAvailable` if the
            'storage' api extension is missing.
        :raises: :class:`pylxd.exceptions.LXDAPIException` if the storage pool
            can't be deleted.
        """
        # Note this method exists so that it is documented via sphinx.
        super(StorageVolume, self).delete()
