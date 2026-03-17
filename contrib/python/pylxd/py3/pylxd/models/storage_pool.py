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
from pylxd import managers
from pylxd.models import _model as model


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
        super().__init__(*args, **kwargs)

        self.resources = managers.StorageResourcesManager(self)
        self.volumes = managers.StorageVolumeManager(self)

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
        client.assert_has_api_extension("storage")
        response = client.api.storage_pools[name].get()

        storage_pool = cls(client, **response.json()["metadata"])
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
        client.assert_has_api_extension("storage")
        response = client.api.storage_pools.get()

        storage_pools = []
        for url in response.json()["metadata"]:
            name = url.split("/")[-1]
            storage_pools.append(cls(client, name=name))
        return storage_pools

    @classmethod
    def create(cls, client, definition, wait=True):
        """Create a storage_pool from config.

        Implements POST /1.0/storage-pools

        The `definition` parameter defines what the storage pool will be.
        An example config for the zfs driver is::

            {
                "config": {
                    "size": "10GB"
                },
                "driver": "zfs",
                "name": "pool1"
            }

        Note that **all** fields in the `definition` parameter are strings.

        For further details on the storage pool types see:
        https://documentation.ubuntu.com/lxd/en/latest/explanation/storage/

        The function returns the a `StoragePool` instance, if it is
        successfully created, otherwise an Exception is raised.

        :param client: The pylxd client object
        :type client: :class:`pylxd.client.Client`
        :param definition: the fields to pass to the LXD API endpoint
        :type definition: dict
        :param wait: Whether to wait for async operations to complete.
        :type wait: bool
        :returns: a storage pool if successful, raises NotFound if not found
        :rtype: :class:`pylxd.models.storage_pool.StoragePool`
        :raises: :class:`pylxd.exceptions.LXDAPIExtensionNotAvailable` if the
            'storage' api extension is missing.
        :raises: :class:`pylxd.exceptions.LXDAPIException` if the storage pool
            couldn't be created.
        """
        client.assert_has_api_extension("storage")
        response = client.api.storage_pools.post(json=definition)

        # Use helper method for async handling
        cls._handle_async_response_for_client(client, response, wait)
        storage_pool = cls.get(client, definition["name"])
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

        :param wait: Whether to wait for async operations to complete.
        :type wait: bool
        :raises: :class:`pylxd.exceptions.LXDAPIException` if the storage pool
            can't be deleted.
        """
        # Note this method exists so that it is documented via sphinx.
        super().save(wait=wait)

    def delete(self, wait=False):
        """Delete the storage pool.

        Implements DELETE /1.0/storage-pools/<self.name>

        Deleting a storage pool may fail if it is being used.  See the LXD
        documentation for further details.

        :param wait: Whether to wait for async operations to complete.
        :raises: :class:`pylxd.exceptions.LXDAPIException` if the storage pool
            can't be deleted.
        """
        # Note this method exists so that it is documented via sphinx.
        super().delete(wait=wait)

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
        super().put(put_object, wait)

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
        super().patch(patch_object, wait)


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
        storage_pool.client.assert_has_api_extension("resources")
        response = storage_pool.api.resources.get()
        resources = cls(storage_pool.client, **response.json()["metadata"])
        return resources


class StorageVolume(model.Model):
    """An LXD Storage volume.

    This corresponds to the LXD endpoing at
    /1.0/storage-pools/<pool>/volumes

    api_extension: 'storage'
    """

    name = model.Attribute(readonly=True)
    type = model.Attribute(readonly=True)
    content_type = model.Attribute(readonly=True)
    description = model.Attribute(readonly=True)
    config = model.Attribute()
    used_by = model.Attribute(readonly=True)
    location = model.Attribute(readonly=True)
    # Date strings follow the ISO 8601 pattern
    created_at = model.Attribute(readonly=True)
    pool = model.Attribute(readonly=True)
    project = model.Attribute(readonly=True)

    snapshots = model.Manager()

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

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.snapshots = managers.StorageVolumeSnapshotManager(self)

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
        storage_pool.client.assert_has_api_extension("storage")
        response = storage_pool.api.volumes.get()

        volumes = []
        for volume in response.json()["metadata"]:
            _type, name = volume.split("/")[-2:]
            # for each type, convert to the string that will work with GET
            if _type == "container":
                _type = "container"
            elif _type == "virtual-machine":
                _type = "virtual-machine"
            elif _type == "instance":
                _type = "instance"
            elif _type == "image":
                _type = "image"
            else:
                _type = "custom"
            volumes.append(
                cls(
                    storage_pool.client,
                    name=name,
                    type=_type,
                    storage_pool=storage_pool,
                )
            )
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
        storage_pool.client.assert_has_api_extension("storage")
        response = storage_pool.api.volumes[_type][name].get()

        volume = cls(
            storage_pool.client,
            storage_pool=storage_pool,
            **response.json()["metadata"],
        )
        return volume

    @classmethod
    # def create(cls, storage_pool, definition, wait=True, *args):
    def create(cls, storage_pool, *args, **kwargs):
        """Create a 'custom' Storage Volume in the associated storage pool.

        Implements POST /1.0/storage-pools/<pool>/volumes/custom

        See https://documentation.ubuntu.com/lxd/en/latest/api/#/storage/storage_pool_volumes_type_post
        for more details on what the `definition` parameter dictionary should
        contain for various volume creation.

        At the moment the only type of volume that can be created is 'custom',
        and this is currently hardwired into the function.

        The function signature 'hides' that the first parameter to the function
        is the definition.  The function should be called as:

        >> a_storage_pool.volumes.create(definition_dict, wait=<bool>)

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
        storage_pool.client.assert_has_api_extension("storage")
        wait = kwargs.get("wait", True)
        definition = args[-1]
        assert isinstance(definition, dict)
        assert "name" in definition
        response = storage_pool.api.volumes.custom.post(json=definition)

        # Use class method helper for async handling
        cls._handle_async_response_for_client(storage_pool.client, response, wait)

        volume = cls.get(storage_pool, "custom", definition["name"])
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

        Please see: https://documentation.ubuntu.com/lxd/en/latest/api/#/storage/storage_pool_volume_type_post
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
        assert "name" in _input
        assert "pool" in _input
        response = self.api.post(json=_input)

        # Use instance method helper for async handling
        self._handle_async_response(response, wait)

        self.name = _input["name"]
        return response.json()["metadata"]

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
        super().put(put_object, wait)

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
        super().patch(patch_object, wait)

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
            volume can't be saved.
        """
        # Note this method exists so that it is documented via sphinx.
        super().save(wait=wait)

    def delete(self, wait=False):
        """Delete the storage pool.

        Implements: DELETE /1.0/storage-pools/<pool>/volumes/<type>/<name>

        Deleting a storage volume may fail if it is being used.
        See https://documentation.ubuntu.com/lxd/en/latest/explanation/storage/#storage-volumes
        for further details.

        :param wait: Whether to wait for async operations to complete.
        :type wait: bool
        :raises: :class:`pylxd.exceptions.LXDAPIExtensionNotAvailable` if the
            'storage' api extension is missing.
        :raises: :class:`pylxd.exceptions.LXDAPIException` if the storage pool
            can't be deleted.
        """
        # Note this method exists so that it is documented via sphinx.
        super().delete(wait=wait)

    def restore_from(self, snapshot_name, wait=False):
        """Restore this volume from a snapshot using its name.

        Attempts to restore a volume using a snapshot identified by its name.

        Implements POST /1.0/storage-pools/<pool>/volumes/custom/<volume_name>/snapshot/<name>

        :param snapshot_name: the name of the snapshot to restore from
        :type snapshot_name: str
        :param wait: wait until the operation is completed.
        :type wait: boolean
        :raises: :class:`pylxd.exceptions.LXDAPIExtensionNotAvailable` if the
            'storage' or 'storage_api_volume_snapshots' api extension is missing.
        :raises: :class:`pylxd.exceptions.LXDAPIException` if the the operation fails.
        :returns: the original response from the restore operation (not the
            operation result)
        :rtype: :class:`requests.Response`
        """
        response = self.api.put(json={"restore": snapshot_name})

        # Handle both sync and async responses for endpoint changing from sync to async
        if wait:
            response_json = response.json()
            if response_json["type"] == "async":
                self.client.operations.wait_for_operation(response_json["operation"])
        return response


class StorageVolumeSnapshot(model.Model):
    """A storage volume snapshot.

    This corresponds to the LXD endpoing at
    /1.0/storage-pools/<pool>/volumes/<type>/<volume>/snapshots

    api_extension: 'storage_api_volume_snapshots'
    """

    name = model.Attribute(readonly=True)
    description = model.Attribute()
    config = model.Attribute()
    content_type = model.Attribute(readonly=True)
    # Date strings follow the ISO 8601 pattern
    created_at = model.Attribute(readonly=True)
    expires_at = model.Attribute()

    _endpoint = "snapshots"

    volume = model.Parent()

    @property
    def api(self):
        """Provides an object with the endpoint:

        /1.0/storage-pools/<volume.storage_pool.name>/volumes/<volume.type>/<volume.name>/snapshots/<self.name>

        Used internally to construct endpoints.

        :returns: an API node with the named endpoint
        :rtype: :class:`pylxd.client._APINode`
        """
        return self.volume.api[self._endpoint][self.name]

    @classmethod
    def __parse_snapshot_json(cls, volume, snapshot_json):
        snapshot_object = cls(volume.client, volume=volume, **snapshot_json)

        # Snapshot names are namespaced in LXD, as volume-name/snapshot-name.
        # We hide that implementation detail.
        snapshot_object.name = snapshot_object.name.split("/")[-1]

        # If response does not include expires_at sync the object to get that attribute.
        if not snapshot_json.get("expires_at"):
            snapshot_object.sync()

        # Getting '0001-01-01T00:00:00Z' means that the volume does not have an expiration time set.
        if snapshot_object.expires_at == "0001-01-01T00:00:00Z":
            snapshot_object.expires_at = None

        # Overriding default value for when created_at is not present on the response due to using LXD 4.0.
        # Also having created_at as "0001-01-01T00:00:00Z" means this information is not available.
        # This could be because the information was lost or the snapshot was created using an older LXD version.
        if (
            not volume.client.has_api_extension("storage_volumes_created_at")
            or snapshot_json["created_at"] == "0001-01-01T00:00:00Z"
        ):
            snapshot_object.created_at = None

        # This field is may empty so derive it from its volume.
        if not snapshot_object.content_type:
            snapshot_object.content_type = volume.content_type

        return snapshot_object

    @classmethod
    def get(cls, volume, name):
        """Get a :class:`pylxd.models.StorageVolumeSnapshot` by its name.

        Implements GET /1.0/storage-pools/<pool>/volumes/custom/<volume_name>/snapshots/<name>

        :param client: a storage pool object on which to fetch resources
        :type storage_pool: :class:`pylxd.models.storage_pool.StoragePool`
        :param _type: the volume type; one of 'container', 'image', 'custom'
        :type _type: str
        :param volume: the name of the storage volume snapshot to get
        :type volume: pylxd.models.StorageVolume
        :returns: a storage pool if successful, raises NotFound if not found
        :rtype: :class:`pylxd.models.storage_pool.StorageVolume`
        :raises: :class:`pylxd.exceptions.NotFound`
        :raises: :class:`pylxd.exceptions.LXDAPIExtensionNotAvailable` if the
            'storage' or 'storage_api_volume_snapshots' api extension is missing.
        :raises: :class:`pylxd.exceptions.LXDAPIException` if the the operation fails.
        """
        volume.client.assert_has_api_extension("storage_api_volume_snapshots")

        response = volume.api.snapshots[name].get()

        return cls.__parse_snapshot_json(volume, response.json()["metadata"])

    @classmethod
    def all(cls, volume, use_recursion=False):
        """Get all :class:`pylxd.models.StorageVolumeSnapshot` objects related to a certain volume.
        If use_recursion is unset or set to False, a list of snapshot names is returned.
        If use_recursion is set to True, a list of :class:`pylxd.models.StorageVolumeSnapshot` objects is returned
        containing additional information for each snapshot.

        Implements GET /1.0/storage-pools/<pool>/volumes/custom/<volume_name>/snapshots

        :param volume: The storage volume snapshot to get snapshots from
        :type volume: pylxd.models.StorageVolume
        :param use_recursion: Specifies whether 'recursion=1' should be used on the request.
        :type use_recursion: bool
        :returns: A list of storage volume snapshot names if use_recursion is False, otherwise
            returns a list of :class:`pylxd.models.StorageVolumeSnapshot`
        :rtype: list
        :raises: :class:`pylxd.exceptions.LXDAPIExtensionNotAvailable` if the
            'storage' or 'storage_api_volume_snapshots' api extension is missing.
        :raises: :class:`pylxd.exceptions.LXDAPIException` if the the operation fails.
        """
        volume.client.assert_has_api_extension("storage_api_volume_snapshots")

        if use_recursion:
            # Using recursion so returning list of StorageVolumeSnapshot objects.
            response = volume.api.snapshots.get(params={"recursion": 1})

            return [
                cls.__parse_snapshot_json(volume, snapshot)
                for snapshot in response.json()["metadata"]
            ]

        response = volume.api.snapshots.get()

        return [
            snapshot_name.split("/")[-1]
            for snapshot_name in response.json()["metadata"]
        ]

    @classmethod
    def create(cls, volume, name=None, expires_at=None, wait=True):
        """Create new :class:`pylxd.models.StorageVolumeSnapshot` object from the current volume state using the given attributes.

        Implements POST /1.0/storage-pools/<pool>/volumes/custom/<volume_name>/snapshots

        :param volume: :class:`pylxd.models.StorageVolume` object that represents the target volume to take the snapshot from
        :type volume: :class:`pylxd.models.StorageVolume`
        :param name: Optional parameter. Name of the created snapshot. The snapshot will be called "snap{index}" by default.
        :type name: str
        :param expires_at: Optional parameter. Expiration time for the created snapshot in ISO 8601 format. No expiration date by default.
        :type expires_at: str
        :param wait: Whether to wait for async operations to complete.
        :type wait: bool
        :returns: a storage volume snapshot if successful, raises an exception otherwise.
        :rtype: :class:`pylxd.models.StorageVolume`
        :raises: :class:`pylxd.exceptions.LXDAPIExtensionNotAvailable` if the
            'storage_api_volume_snapshots' api extension is missing.
        :raises: :class:`pylxd.exceptions.LXDAPIException` if the the operation fails.
        """
        volume.client.assert_has_api_extension("storage_api_volume_snapshots")

        response = volume.api.snapshots.post(
            json={"name": name, "expires_at": expires_at}
        )

        operation = None

        # Only parse JSON if we need to wait for async responses
        if wait:
            response_json = response.json()

            # Handle both sync and async responses
            if response_json["type"] == "async":
                operation = volume.client.operations.wait_for_operation(
                    response_json["operation"]
                )
            else:
                # Return the snapshot immediately without waiting for completion
                return volume.snapshots.get(name)

        # Extract the snapshot name from the response JSON in case it was not provided
        if not name:
            if operation and "storage_volume_snapshots" in operation.resources:
                name = operation.resources["storage_volume_snapshots"][0].split("/")[-1]
            else:
                # If using LXD 4.0, the snapshot name isn't provided on the request response
                # so grab the latest snapshot name instead.
                name = volume.snapshots.all()[-1].split("/")[-1]

        snapshot = volume.snapshots.get(name)
        return snapshot

    @classmethod
    def exists(cls, volume, name):
        """Determine whether a volume snapshot exists in LXD.

        :param name: Name of the desired snapshot.
        :type name: str
        :returns: True if a snapshot with the given name exists, returns False otherwise.
        :rtype: bool
        :raises: `pylxd.exceptions.LXDAPIException` if the the operation fails.
        """
        try:
            volume.snapshots.get(name)
            return True
        except cls.NotFound:
            return False

    def rename(self, new_name, wait=True):
        """Rename a storage volume snapshot.

        Implements POST /1.0/storage-pools/<pool>/volumes/custom/<volume_name>/snapshot/<name>

        Renames a storage volume snapshot, changing the endpoints that reference it.

        :param new_name: The new name for the snapshot
        :type new_name: str
        :param wait: Whether to wait for async operations to complete
        :type wait: bool
        :raises: :class:`pylxd.exceptions.LXDAPIException` if the the operation fails.
        """
        response = self.api.post(json={"name": new_name})

        # Use instance method helper for async handling
        self._handle_async_response(response, wait)

        self.name = new_name

    def restore(self, wait=False):
        """Restore the volume from this snapshot.

        Attempts to restore a custom volume using this snapshot.
        Equivalent to pylxd.models.StorageVolume.restore_from(this_snapshot).

        :param wait: wait until the operation is completed.
        :type wait: boolean
        :raises: :class:`pylxd.exceptions.LXDAPIException` if the the operation fails.
        """
        self.volume.restore_from(self.name, wait)

    def delete(self, wait=False):
        """Delete this storage pool snapshot.

        Implements: DELETE /1.0/storage-pools/<pool>/volumes/custom/<volume_name>/snapshot/<name>

        :raises: :class:`pylxd.exceptions.LXDAPIException` if the storage pool
            can't be deleted.
        """
        super().delete(wait=wait)
