#
# Copyright (c) SAS Institute Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import getpass
import hashlib
import json
import logging
import os
import posixpath
import tempfile

from six.moves.urllib.parse import urlparse
import requests
import six

from . import utils
from .artifact import Artifact
from .errors import MissingArtifactError
from .errors import MissingPathError
from .pom import Pom
from .versioning import VersionRange

try:
    from xml.etree import cElementTree as ElementTree
except ImportError:
    from xml.etree import ElementTree

log = logging.getLogger(__name__)


class Struct(object):
    """ Simple object to mimic a requests.Response object
    """
    def __init__(self):
        self.status_code = None
        self.content = None
        self._json = None

    def __enter__(self):
        self._content = open(self.content, 'rb')
        return self._content

    def __exit__(self, exc_type, exc_value, traceback):
        self._content.close()

    @property
    @utils.memoize("_json")
    def json(self):
        with self as fh:
            return json.load(fh)

    def iter_content(self, size=None):
        with self as fh:
            for chunk in fh.read(size):
                yield chunk


class Cache(object):
    """ Local http cache
    """
    def __init__(self, cacheDir=None):
        if cacheDir is None:
            cacheDir = tempfile.mkdtemp(prefix=getpass.getuser())
        if not os.path.exists(cacheDir):
            os.makedirs(cacheDir, mode=0o700)
        self.cacheDir = cacheDir

    def _gen_key(self, method, uri, query_params):
        key = method + " " + uri
        if query_params:
            key += "?" + "&".join(
                ("%s=%s" % kv for kv in query_params.iteritems()))

        return key

    def _gen_hash(self, key):
        h = hashlib.sha1(key).hexdigest()
        return h

    def _gen_paths(self, hash):
        dhash = "%s.data" % hash
        hpath = os.path.join(self.cacheDir, hash)
        dhpath = os.path.join(self.cacheDir, dhash)
        return hpath, dhpath

    def cache(self, res, method, uri, query_params=None):
        """Access the cache for a request response

        :param str method: HTTP method
        :param str uri: location to requests
        :param dict query_params: query parameters
        :param requests.Response res: a request response to cache
        :return: the cached response
        """
        if query_params is None:
            query_params = {}

        key = self._gen_key(method, uri, query_params)
        h = self._gen_hash(key)
        hpath, dhpath = self._gen_paths(h)

        log.debug("Caching response %s with key %s", key, h)
        with open(hpath, "wb") as fh:
            for chunk in res.iter_content(1024):
                fh.write(chunk)
        with open(dhpath, "wb") as fh:
            json.dump({
                "status_code": res.status_code,
                "reason": res.reason,
                "method": method,
                "uri": uri,
                "param": query_params,
            }, fh)

        res = self._get(hpath, dhpath)
        return res

    def get(self, method, uri, query_params=None):
        if query_params is None:
            query_params = {}

        key = self._gen_key(method, uri, query_params)
        h = self._gen_hash(key)
        res = self._get(*self._gen_paths(h))
        if res is not None:
            log.debug("hit %s with key %s", key, h)
        else:
            log.debug("miss %s with key %s", key, h)
        return res

    def _get(self, hpath, dhpath):
        if os.path.exists(hpath) and os.path.exists(dhpath):
            data = json.load(open(dhpath))
            data["content"] = hpath
            res = Struct()
            for k, v in data.iteritems():
                setattr(res, k, v)
            return res


class MavenClient(object):
    """ Client for talking to a maven repository
    """
    def __init__(self, *urls):
        if isinstance(urls, six.string_types):
            urls = [urls]
        self._repos = []
        for url in urls:
            url = urlparse(url)
            if not url.scheme or url.scheme == "file":
                self._repos.append(LocalRepository(url.path))
            elif url.scheme.startswith("http"):
                self._repos.append(HttpRepository(url.geturl()))
            else:
                msg = "Unknown scheme: %s"
                log.error(msg, url)
                raise ValueError(msg % url.geturl())

    def find_artifacts(self, coordinate):
        """Find all artifacts matching the coordinate

        :param str coordinate: maven coordinate
        :retrun: list of Artifacts
        """
        artifacts = set([])
        for repo in self._repos:
            artifacts.update(set(repo.get_versions(coordinate)))
        return sorted(artifacts, reverse=True)

    def get_metadata(self, coordinate):
        """Return the metadata associated with the coordinates

        :param str coordinate: maven coordinate
        :raises: :py:exc:`pymaven.errors.MissingArtifactError`
        :return: the metadata requested
        :rtype: :py:class:`pymaven.pom.Pom`
        """
        query = Artifact(coordinate)
        assert query.version.version is not None, \
            "Cannot get metadata for version range"

        if query.type != "pom":
            query.type = "pom"

        for repo in self._repos:
            if repo.exists(query.path):
                return Pom(coordinate, self)
        else:
            raise MissingArtifactError(coordinate)

    def get_artifact(self, coordinate):
        """Return the actual artifact specified by the coordinate

        :param str coordinate: maven coordinate
        :raises: :py:exc:`pymaven.errors.MissingArtifactError`
        :return: the artifact requested
        :rtype: :py:class:`pymaven.Artifact`
        """
        query = Artifact(coordinate)
        assert query.version.version is not None, \
            "Cannot get artifact for version range"

        for repo in self._repos:
            if repo.exists(query.path):
                query.contents = repo.open(query.path)
                return query
        else:
            raise MissingArtifactError(coordinate)


class AbstractRepository(object):
    """
    Abstract class, this defines the interface that all repositories implement
    """
    def __init__(self, url):
        self._url = url

    def get_versions(self, coordinate):
        query = Artifact(coordinate)
        if self.exists(query.path):
            if query.version and query.version.version:
                return [Artifact(coordinate)]

            version_range = query.version
            if version_range is None:
                version_range = VersionRange("[,)")

            # base coordinate for all return values is everything up to the
            # version of the query
            base_coordinate = "%s:%s" % (query.group_id, query.artifact_id)
            if query.classifier:
                base_coordinate += "%s:%s" % (query.type, query.classifier)
            elif query.type != "jar":
                base_coordinate += ":%s" % query.type

            return sorted([
                Artifact(':'.join([base_coordinate, version]))
                for version in self.listdir(query.path)
                if version in version_range], reverse=True)

    def exists(self, path):
        """Return ``True`` if *path* exists in the repository, ``False``
        otherwise
        """
        return self._exists(path)

    def listdir(self, path):
        """List contents of *path*

        :raises: MissingPathError if *path* does not exist
        """
        try:
            return self._listdir(path)
        except (OSError, requests.exceptions.HTTPError):
            raise MissingPathError(path)

    def open(self, path):
        try:
            return self._open(path)
        except (IOError, requests.exceptions.HTTPError):
            raise MissingPathError("No such file: %s" % path)


class HttpRepository(AbstractRepository):
    """ Access a maven repository via http
    """
    def __init__(self, url, username=None, password=None):
        super(HttpRepository, self).__init__(url)
        self._cache = Cache()

    def _get(self, uri, **kwargs):
        res = self._request("GET", uri, **kwargs)
        return res

    def _head(self, uri, **kwargs):
        res = self._request("HEAD", uri, **kwargs)
        return res

    def _request(self, method, uri, json=False, **kwargs):
        url = utils.urljoin(self._url, uri)
        res = self._cache.get(method, uri, kwargs.get("params"))
        if not res:
            log.debug("requesting %s %s", method, url)
            res = requests.request(method, url, **kwargs)
            res = self._cache.cache(res, method, uri, kwargs.get("params"))

        if res.status_code != requests.codes.ok:
            raise requests.HTTPError(res.reason)

        if json:
            return res.json()
        return res

    def _exists(self, path):
        try:
            self._head(path)
        except requests.exceptions.HTTPError:
            return False
        return True

    def _listdir(self, path):
        uri = posixpath.join(path, "maven-metadata.xml")
        res = self._get(uri)
        with res as fh:
            metadata = ElementTree.parse(fh)

        return [elem.text.strip()
                for elem in metadata.findall("versioning/versions/version")
                if elem is not None]

    def _open(self, path):
        res = self._get(path, stream=True)
        return res


class LocalRepository(AbstractRepository):
    """A local disk-based repository
    """
    def _join(self, *args):
        return os.path.join(self._url, *args)

    def _exists(self, path):
        return os.path.exists(self._join(path))

    def _listdir(self, path):
        return os.listdir(self._join(path))

    def _open(self, path):
        """Open *path* as a file-like object

        The caller is responsible for calling close on the object
        """
        return open(self._join(path))
