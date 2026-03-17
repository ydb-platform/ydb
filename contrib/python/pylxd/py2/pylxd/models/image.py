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
import collections
import contextlib
import tempfile
import warnings
from requests_toolbelt import MultipartEncoder

from pylxd.models import _model as model


def _image_create_from_config(client, config, wait=False):
    """ Create an image from the given configuration.

    See: https://github.com/lxc/lxd/blob/master/doc/rest-api.md#post-6
    """
    response = client.api.images.post(json=config)
    if wait:
        return client.operations.wait_for_operation(
            response.json()['operation'])
    return response.json()['operation']


class Image(model.Model):
    """A LXD Image."""
    aliases = model.Attribute(readonly=True)
    auto_update = model.Attribute(optional=True)
    architecture = model.Attribute(readonly=True)
    cached = model.Attribute(readonly=True)
    created_at = model.Attribute(readonly=True)
    expires_at = model.Attribute(readonly=True)
    filename = model.Attribute(readonly=True)
    fingerprint = model.Attribute(readonly=True)
    last_used_at = model.Attribute(readonly=True)
    properties = model.Attribute()
    public = model.Attribute()
    size = model.Attribute(readonly=True)
    uploaded_at = model.Attribute(readonly=True)
    update_source = model.Attribute(readonly=True)

    @property
    def api(self):
        return self.client.api.images[self.fingerprint]

    @classmethod
    def exists(cls, client, fingerprint, alias=False):
        """Determine whether an image exists.

        If `alias` is True, look up the image by its alias,
        rather than its fingerprint.
        """
        try:
            if alias:
                client.images.get_by_alias(fingerprint)
            else:
                client.images.get(fingerprint)
            return True
        except cls.NotFound:
            return False

    @classmethod
    def get(cls, client, fingerprint):
        """Get an image."""
        response = client.api.images[fingerprint].get()

        image = cls(client, **response.json()['metadata'])
        return image

    @classmethod
    def get_by_alias(cls, client, alias):
        """Get an image by its alias."""
        response = client.api.images.aliases[alias].get()

        fingerprint = response.json()['metadata']['target']
        return cls.get(client, fingerprint)

    @classmethod
    def all(cls, client):
        """Get all images."""
        response = client.api.images.get()

        images = []
        for url in response.json()['metadata']:
            fingerprint = url.split('/')[-1]
            images.append(cls(client, fingerprint=fingerprint))
        return images

    @classmethod
    def create(
            cls, client, image_data, metadata=None, public=False, wait=True):
        """Create an image.

        If metadata is provided, a multipart form data request is formed to
        push metadata and image together in a single request. The metadata must
        be a tar achive.

        `wait` parameter is now ignored, as the image fingerprint cannot be
        reliably determined consistently until after the image is indexed.
        """

        if wait is False:  # pragma: no cover
            warnings.warn(
                'Image.create wait parameter ignored and will be removed in '
                '2.3', DeprecationWarning)

        headers = {}
        if public:
            headers['X-LXD-Public'] = '1'

        if metadata is not None:
            # Image uploaded as chunked/stream (metadata, rootfs)
            # multipart message.
            # Order of parts is important metadata should be passed first
            files = collections.OrderedDict(
                metadata=('metadata', metadata, 'application/octet-stream'),
                rootfs=('rootfs', image_data, 'application/octet-stream'))
            data = MultipartEncoder(files)
            headers.update({"Content-Type": data.content_type})
        else:
            data = image_data

        response = client.api.images.post(data=data, headers=headers)
        operation = client.operations.wait_for_operation(
            response.json()['operation'])
        return cls(client, fingerprint=operation.metadata['fingerprint'])

    @classmethod
    def create_from_simplestreams(cls, client, server, alias,
                                  public=False, auto_update=False):
        """Copy an image from simplestreams."""
        config = {
            'public': public,
            'auto_update': auto_update,

            'source': {
                'type': 'image',
                'mode': 'pull',
                'server': server,
                'protocol': 'simplestreams',
                'fingerprint': alias
            }
        }

        op = _image_create_from_config(client, config, wait=True)

        return client.images.get(op.metadata['fingerprint'])

    @classmethod
    def create_from_url(cls, client, url,
                        public=False, auto_update=False):
        """Copy an image from an url."""
        config = {
            'public': public,
            'auto_update': auto_update,

            'source': {
                'type': 'url',
                'mode': 'pull',
                'url': url
            }
        }

        op = _image_create_from_config(client, config, wait=True)

        return client.images.get(op.metadata['fingerprint'])

    def export(self):
        """Export the image.

        Because the image itself may be quite large, we stream the download
        in 1kb chunks, and write it to a temporary file on disk. Once that
        file is closed, it is deleted from the disk.
        """
        on_disk = tempfile.TemporaryFile()
        with contextlib.closing(self.api.export.get(stream=True)) as response:
            for chunk in response.iter_content(chunk_size=1024):
                on_disk.write(chunk)
        on_disk.seek(0)
        return on_disk

    def add_alias(self, name, description):
        """Add an alias to the image."""
        self.client.api.images.aliases.post(json={
            'description': description,
            'target': self.fingerprint,
            'name': name
        })

        # Update current aliases list
        self.aliases.append({
            'description': description,
            'target': self.fingerprint,
            'name': name
        })

    def delete_alias(self, name):
        """Delete an alias from the image."""
        self.client.api.images.aliases[name].delete()

        # Update current aliases list
        la = [a['name'] for a in self.aliases]
        try:
            del self.aliases[la.index(name)]
        except ValueError:
            pass

    def copy(self, new_client, public=None, auto_update=None, wait=False):
        """Copy an image to a another LXD.

        Destination host information is contained in the client
        connection passed in.
        """
        self.sync()  # Make sure the object isn't stale

        url = '/'.join(self.client.api._api_endpoint.split('/')[:-1])

        if public is None:
            public = self.public

        if auto_update is None:
            auto_update = self.auto_update

        config = {
            'filename': self.filename,
            'public': public,
            'auto_update': auto_update,
            'properties': self.properties,

            'source': {
                'type': 'image',
                'mode': 'pull',
                'server': url,
                'protocol': 'lxd',
                'fingerprint': self.fingerprint
            }
        }

        if self.public is not True:
            response = self.api.secret.post(json={})
            secret = response.json()['metadata']['metadata']['secret']
            config['source']['secret'] = secret
            cert = self.client.host_info['environment']['certificate']
            config['source']['certificate'] = cert

        _image_create_from_config(new_client, config, wait)

        if wait:
            return new_client.images.get(self.fingerprint)
