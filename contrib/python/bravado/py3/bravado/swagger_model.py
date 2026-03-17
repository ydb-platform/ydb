# -*- coding: utf-8 -*-
import contextlib
import logging
import os.path
import typing

import yaml
try:
    from yaml import CSafeLoader as SafeLoader
except ImportError:  # pragma: no cover
    from yaml import SafeLoader  # type: ignore
from bravado_core.spec import is_yaml
from six import iteritems
from six import itervalues
from six.moves import urllib
from six.moves.urllib import parse as urlparse

import simplejson
from bravado.requests_client import RequestsClient

log = logging.getLogger(__name__)


def is_file_scheme_uri(url):
    return urlparse.urlparse(url).scheme == u'file'


class FileEventual(object):
    """Adaptor which supports the :class:`crochet.EventualResult`
    interface for retrieving api docs from a local file.
    """

    class FileResponse(object):

        def __init__(self, data):
            self.text = data
            self.headers = {}  # type: typing.Mapping[str, str]

        def json(self):
            return simplejson.loads(self.text.decode('utf-8'))

    def __init__(self, path):
        self.path = path
        self.is_yaml = is_yaml(path)

    def get_path(self):
        if not self.path.endswith('.json') and not self.is_yaml:
            return self.path + '.json'
        return self.path

    def wait(self, **kwargs):
        with contextlib.closing(urllib.request.urlopen(self.get_path())) as fp:
            content = fp.read()
            return self.FileResponse(content)

    def result(self, *args, **kwargs):
        return self.wait(*args, **kwargs)

    def cancel(self):
        pass


def request(http_client, url, headers):
    """Download and parse JSON from a URL.

    :param http_client: a :class:`bravado.http_client.HttpClient`
    :param url: url for api docs
    :return: an object with a :func`wait` method which returns the api docs
    """
    if is_file_scheme_uri(url):
        return FileEventual(url)

    request_params = {
        'method': 'GET',
        'url': url,
        'headers': headers,
    }

    return http_client.request(request_params)


class Loader(object):
    """Abstraction for loading Swagger API's.

    :param http_client: HTTP client interface.
    :type  http_client: http_client.HttpClient
    :param request_headers: dict of request headers
    """

    def __init__(self, http_client, request_headers=None):
        self.http_client = http_client
        self.request_headers = request_headers or {}

    def load_spec(self, spec_url, base_url=None):
        """Load a Swagger Spec from the given URL

        :param spec_url: URL to swagger.json
        :param base_url: TODO: need this?
        :returns: json spec in dict form
        """
        response = request(
            self.http_client,
            spec_url,
            self.request_headers,
        ).result()

        content_type = response.headers.get('content-type', '').lower()
        if is_yaml(spec_url, content_type):
            return self.load_yaml(response.text)
        else:
            return response.json()

    def load_yaml(self, text):
        """Load a YAML Swagger spec from the given string, transforming
        integer response status codes to strings. This is to keep
        compatibility with the existing YAML spec examples in
        https://github.com/OAI/OpenAPI-Specification/tree/master/examples/v2.0/yaml
        :param text: String from which to parse the YAML.
        :type  text: basestring
        :return: Python dictionary representing the spec.
        :raise: yaml.parser.ParserError: If the text is not valid YAML.
        """
        data = yaml.load(text, Loader=SafeLoader)
        for methods in itervalues(data.get('paths', {})):
            for operation in itervalues(methods):
                if 'responses' in operation:
                    operation['responses'] = {
                        str(code): response
                        for code, response in iteritems(operation['responses'])
                    }

        return data


# TODO: Adding the file scheme here just adds complexity to request()
# Is there a better way to handle this?
def load_file(spec_file, http_client=None):
    """Loads a spec file

    :param spec_file: Path to swagger.json.
    :param http_client: HTTP client interface.
    :return: validated json spec in dict form
    :raise: IOError: On error reading swagger.json.
    """
    file_path = os.path.abspath(spec_file)
    url = urlparse.urljoin(u'file:', urllib.request.pathname2url(file_path))
    # When loading from files, everything is relative to the spec file
    dir_path = os.path.dirname(file_path)
    base_url = urlparse.urljoin(u'file:', urllib.request.pathname2url(dir_path))
    return load_url(url, http_client=http_client, base_url=base_url)


def load_url(spec_url, http_client=None, base_url=None):
    """Loads a Swagger spec.

    :param spec_url: URL for swagger.json.
    :param http_client: HTTP client interface.
    :param base_url:    Optional URL to be the base URL for finding API
                        declarations. If not specified, 'basePath' from the
                        resource listing is used.
    :return: validated spec in dict form
    :raise: IOError, URLError: On error reading api-docs.
    """
    if http_client is None:
        http_client = RequestsClient()

    loader = Loader(http_client=http_client)
    return loader.load_spec(spec_url, base_url=base_url)
