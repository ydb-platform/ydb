# Copyright 2013 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Common client library functions and classes used by all products."""

import abc
import base64
import binascii
from functools import wraps
import inspect
import locale
import logging
import logging.config
import os
import ssl
import sys
import threading
import warnings
from urllib.request import HTTPSHandler, ProxyHandler, build_opener

import lxml.builder
import lxml.etree
import requests.exceptions
import yaml
import zeep
import zeep.cache
import zeep.exceptions
import zeep.helpers
import zeep.transports
import zeep.xsd
import googleads.errors
import googleads.oauth2
import googleads.util


_logger = logging.getLogger(__name__)

_PY_VERSION_MAJOR = sys.version_info.major
_PY_VERSION_MINOR = sys.version_info.minor
_PY_VERSION_MICRO = sys.version_info.micro
_DEPRECATED_VERSION_TEMPLATE = (
    'This library is being run by an unsupported Python version (%s.%s.%s). In '
    'order to benefit from important security improvements and ensure '
    'compatibility with this library, upgrade to Python 3.6 or higher.')

VERSION = '49.0.0'
_COMMON_LIB_SIG = 'googleads/%s' % VERSION
_LOGGING_KEY = 'logging'
_HTTP_PROXY_YAML_KEY = 'http'
_HTTPS_PROXY_YAML_KEY = 'https'
_PROXY_CONFIG_KEY = 'proxy_config'
_PYTHON_VERSION = 'Python/%d.%d.%d' % (
    _PY_VERSION_MAJOR, _PY_VERSION_MINOR, _PY_VERSION_MICRO)

# The required keys in the authentication dictionary that are used to construct
# installed application OAuth2 credentials.
_OAUTH2_INSTALLED_APP_KEYS = ('client_id', 'client_secret', 'refresh_token')

# The keys in the authentication dictionary that are used to construct service
# account OAuth2 credentials.
_OAUTH2_SERVICE_ACCT_KEYS = ('path_to_private_key_file',)
_OAUTH2_SERVICE_ACCT_KEYS_OPTIONAL = ('delegated_account',)

# A key used to configure the client to accept and automatically decompress
# gzip encoded SOAP responses.
ENABLE_COMPRESSION_KEY = 'enable_compression'

# A key used to configure the client to send arbitrary headers in SOAP requests.
CUSTOM_HEADERS_KEY = 'custom_http_headers'

# Global variables used to enable and store utility usage stats.
_utility_registry = googleads.util.UtilityRegistry()
_UTILITY_REGISTER_YAML_KEY = 'include_utilities_in_user_agent'
_UTILITY_LOCK = threading.Lock()


def GenerateLibSig(short_name):
  """Generates a library signature suitable for a user agent field.

  Args:
    short_name: The short, product-specific string name for the library.
  Returns:
    A library signature string to append to user-supplied user-agent value.
  """
  with _UTILITY_LOCK:
    utilities_used = ', '.join(list(sorted(_utility_registry)))
    _utility_registry.Clear()

  if utilities_used:
    return ' (%s, %s, %s, %s)' % (short_name, _COMMON_LIB_SIG, _PYTHON_VERSION,
                                  utilities_used)
  else:
    return ' (%s, %s, %s)' % (short_name, _COMMON_LIB_SIG, _PYTHON_VERSION)


class CommonClient(object):
  """Contains shared startup code between clients."""

  def __init__(self):
    # Warn users on deprecated Python versions on initialization.
    if _PY_VERSION_MAJOR == 3 and _PY_VERSION_MINOR < 6:
      _logger.warning(_DEPRECATED_VERSION_TEMPLATE, _PY_VERSION_MAJOR,
                      _PY_VERSION_MINOR, _PY_VERSION_MICRO)

    # Warn users about using non-utf8 encoding
    _, encoding = locale.getdefaultlocale()
    if encoding is None or encoding.lower() != 'utf-8':
      _logger.warn('Your default encoding, %s, is not UTF-8. Please run this'
                   ' script with UTF-8 encoding to avoid errors.', encoding)


def LoadFromString(yaml_doc, product_yaml_key, required_client_values,
                   optional_product_values):
  """Loads the data necessary for instantiating a client from file storage.

  In addition to the required_client_values argument, the yaml file must supply
  the keys used to create OAuth2 credentials. It may also optionally set proxy
  configurations.

  Args:
    yaml_doc: the yaml document whose keys should be used.
    product_yaml_key: The key to read in the yaml as a string.
    required_client_values: A tuple of strings representing values which must
      be in the yaml file for a supported API. If one of these keys is not in
      the yaml file, an error will  be raised.
    optional_product_values: A tuple of strings representing optional values
      which may be in the yaml file.

  Returns:
    A dictionary map of the keys in the yaml file to their values. This will not
    contain the keys used for OAuth2 client creation and instead will have a
    GoogleOAuth2Client object stored in the 'oauth2_client' field.

  Raises:
    A GoogleAdsValueError if the given yaml file does not contain the
    information necessary to instantiate a client object - either a
    required_client_values key was missing or an OAuth2 key was missing.
  """
  data = yaml.safe_load(yaml_doc) or {}

  if 'dfp' in data:
    raise googleads.errors.GoogleAdsValueError(
        'Please replace the "dfp" key in the configuration YAML string with'
        '"ad_manager" to fix this issue.')

  logging_config = data.get(_LOGGING_KEY)
  if logging_config:
    logging.config.dictConfig(logging_config)

  try:
    product_data = data[product_yaml_key]
  except KeyError:
    raise googleads.errors.GoogleAdsValueError(
        'The "%s" configuration is missing'
        % (product_yaml_key,))

  if not isinstance(product_data, dict):
    raise googleads.errors.GoogleAdsValueError(
        'The "%s" configuration is empty or invalid'
        % (product_yaml_key,))

  IncludeUtilitiesInUserAgent(data.get(_UTILITY_REGISTER_YAML_KEY, True))

  original_keys = list(product_data.keys())
  client_kwargs = {}
  try:
    for key in required_client_values:
      client_kwargs[key] = product_data[key]
      del product_data[key]
  except KeyError:
    raise googleads.errors.GoogleAdsValueError(
        'Some of the required values are missing. Required '
        'values are: %s, actual values are %s'
        % (required_client_values, original_keys))

  proxy_config_data = data.get(_PROXY_CONFIG_KEY, {})
  proxy_config = _ExtractProxyConfig(product_yaml_key, proxy_config_data)
  client_kwargs['proxy_config'] = proxy_config
  client_kwargs['oauth2_client'] = _ExtractOAuth2Client(
      product_yaml_key, product_data, proxy_config)

  client_kwargs[ENABLE_COMPRESSION_KEY] = data.get(
      ENABLE_COMPRESSION_KEY, False)

  client_kwargs[CUSTOM_HEADERS_KEY] = data.get(CUSTOM_HEADERS_KEY, None)

  for value in optional_product_values:
    if value in product_data:
      client_kwargs[value] = product_data[value]
      del product_data[value]

  if product_data:
    warnings.warn('Could not recognize the following keys: %s. '
                  'They were ignored.' % (product_data,), stacklevel=3)

  return client_kwargs


def LoadFromStorage(path, product_yaml_key, required_client_values,
                    optional_product_values):
  """Loads the data necessary for instantiating a client from file storage.

  In addition to the required_client_values argument, the yaml file must supply
  the keys used to create OAuth2 credentials. It may also optionally set proxy
  configurations.

  Args:
    path: A path string to the yaml document whose keys should be used.
    product_yaml_key: The key to read in the yaml as a string.
    required_client_values: A tuple of strings representing values which must
      be in the yaml file for a supported API. If one of these keys is not in
      the yaml file, an error will  be raised.
    optional_product_values: A tuple of strings representing optional values
      which may be in the yaml file.

  Returns:
    A dictionary map of the keys in the yaml file to their values. This will not
    contain the keys used for OAuth2 client creation and instead will have a
    GoogleOAuth2Client object stored in the 'oauth2_client' field.

  Raises:
    A GoogleAdsValueError if the given yaml file does not contain the
    information necessary to instantiate a client object - either a
    required_client_values key was missing or an OAuth2 key was missing.
  """

  if not os.path.isabs(path):
    path = os.path.expanduser(path)

  try:
    with open(path, 'rb') as handle:
      yaml_doc = handle.read()
  except IOError:
    raise googleads.errors.GoogleAdsValueError(
        'Given yaml file, %s, could not be opened.' % path)

  try:
    client_kwargs = LoadFromString(yaml_doc, product_yaml_key,
                                   required_client_values,
                                   optional_product_values)
  except googleads.errors.GoogleAdsValueError as e:
    raise googleads.errors.GoogleAdsValueError(
        'Given yaml file, %s, could not find some keys. %s' % (path, e))

  return client_kwargs


def _ExtractOAuth2Client(product_yaml_key, product_data, proxy_config):
  """Generates an GoogleOAuth2Client subclass using the given product_data.

  Args:
    product_yaml_key: a string key identifying the product being configured.
    product_data: a dict containing the configurations for a given product.
    proxy_config: a ProxyConfig instance.

  Returns:
    An instantiated GoogleOAuth2Client subclass.

  Raises:
    A GoogleAdsValueError if the OAuth2 configuration for the given product is
    misconfigured.
  """
  oauth2_kwargs = {
      'proxy_config': proxy_config
  }

  if all(config in product_data for config in _OAUTH2_INSTALLED_APP_KEYS):
    oauth2_args = [
        product_data['client_id'], product_data['client_secret'],
        product_data['refresh_token']
    ]
    oauth2_client = googleads.oauth2.GoogleRefreshTokenClient
    for key in _OAUTH2_INSTALLED_APP_KEYS:
      del product_data[key]
  elif all(config in product_data for config in _OAUTH2_SERVICE_ACCT_KEYS):
    oauth2_args = [
        product_data['path_to_private_key_file'],
        googleads.oauth2.GetAPIScope(product_yaml_key),
    ]
    oauth2_kwargs.update({
        'sub': product_data.get('delegated_account')
    })
    oauth2_client = googleads.oauth2.GoogleServiceAccountClient
    for key in _OAUTH2_SERVICE_ACCT_KEYS:
      del product_data[key]
    for optional_key in _OAUTH2_SERVICE_ACCT_KEYS_OPTIONAL:
      if optional_key in product_data:
        del product_data[optional_key]
  else:
    raise googleads.errors.GoogleAdsValueError(
        'Your yaml file is incorrectly configured for OAuth2. You need to '
        'specify credentials for either the installed application flow (%s) '
        'or service account flow (%s).' %
        (_OAUTH2_INSTALLED_APP_KEYS, _OAUTH2_SERVICE_ACCT_KEYS))

  return oauth2_client(*oauth2_args, **oauth2_kwargs)


def _ExtractProxyConfig(product_yaml_key, proxy_config_data):
  """Returns an initialized ProxyConfig using the given proxy_config_data.

  Args:
    product_yaml_key: a string indicating the client being loaded.
    proxy_config_data: a dict containing the contents of proxy_config from the
      YAML file.

  Returns:
    If there is a proxy to configure in proxy_config, this will return a
    ProxyConfig instance with those settings. Otherwise, it will return None.

  Raises:
    A GoogleAdsValueError if one of the required keys specified by _PROXY_KEYS
    is missing.
  """
  cafile = proxy_config_data.get('cafile', None)
  disable_certificate_validation = proxy_config_data.get(
      'disable_certificate_validation', False)

  http_proxy = proxy_config_data.get(_HTTP_PROXY_YAML_KEY)
  https_proxy = proxy_config_data.get(_HTTPS_PROXY_YAML_KEY)
  proxy_config = ProxyConfig(
      http_proxy=http_proxy,
      https_proxy=https_proxy,
      cafile=cafile,
      disable_certificate_validation=disable_certificate_validation)

  return proxy_config


def IncludeUtilitiesInUserAgent(value):
  """Configures the logging of utilities in the User-Agent.

  Args:
    value: a bool indicating that you want to include utility names in the
      User-Agent if set True, otherwise, these will not be added.
  """
  with _UTILITY_LOCK:
    _utility_registry.SetEnabled(value)


def AddToUtilityRegistry(utility_name):
  """Directly add a utility to the registry, not a decorator.

  Args:
    utility_name: The name of the utility to add.
  """
  with _UTILITY_LOCK:
    _utility_registry.Add(utility_name)


def RegisterUtility(utility_name, version_mapping=None):
  """Decorator that registers a class with the given utility name.

  This will only register the utilities being used if the UtilityRegistry is
  enabled. Note that only the utility class's public methods will cause the
  utility name to be added to the registry.

  Args:
    utility_name: A str specifying the utility name associated with the class.
    version_mapping: A dict containing optional version strings to append to the
    utility string for individual methods; where the key is the method name and
    the value is the text to be appended as the version.

  Returns:
    The decorated class.
  """
  def IsFunctionOrMethod(member):
    """Determines if given member is a function or method.

    These two are used in combination to ensure that inspect finds all of a
    given utility class's methods in both Python 2 and 3.

    Args:
      member: object that is a member of a class, to be determined whether it is
        a function or method.

    Returns:
      A boolean that is True if the provided member is a function or method, or
      False if it isn't.
    """
    return inspect.isfunction(member) or inspect.ismethod(member)

  def MethodDecorator(utility_method, version):
    """Decorates a method in the utility class."""
    registry_name = ('%s/%s' % (utility_name, version) if version
                     else utility_name)
    @wraps(utility_method)
    def Wrapper(*args, **kwargs):
      AddToUtilityRegistry(registry_name)
      return utility_method(*args, **kwargs)
    return Wrapper

  def ClassDecorator(cls):
    """Decorates a utility class."""
    for name, method in inspect.getmembers(cls, predicate=IsFunctionOrMethod):
      # Public methods of the class will have the decorator applied.
      if not name.startswith('_'):
        # The decorator will only be applied to unbound methods; this prevents
        # it from clobbering class methods. If the attribute doesn't exist, set
        # None for PY3 compatibility.
        if not getattr(method, '__self__', None):
          setattr(cls, name, MethodDecorator(
              method, version_mapping.get(name) if version_mapping else None))
    return cls

  return ClassDecorator


class ProxyConfig(object):
  """A utility for configuring the usage of a proxy."""

  def __init__(self, http_proxy=None, https_proxy=None, cafile=None,
               disable_certificate_validation=False):
    self._http_proxy = http_proxy
    self._https_proxy = https_proxy
    self.proxies = {}
    if self._https_proxy:
      self.proxies['https'] = str(self._https_proxy)
    if self._http_proxy:
      self.proxies['http'] = str(self._http_proxy)

    self.disable_certificate_validation = disable_certificate_validation
    self.cafile = None if disable_certificate_validation else cafile
    # Initialize the context used to generate the HTTPSHandler.
    self.ssl_context = self._InitSSLContext(
        self.cafile, self.disable_certificate_validation)

  def _InitSSLContext(self, cafile=None,
                      disable_ssl_certificate_validation=False):
    """Creates a ssl.SSLContext with the given settings.

    Args:
      cafile: A str identifying the resolved path to the cafile. If not set,
        this will use the system default cafile.
      disable_ssl_certificate_validation: A boolean indicating whether
        certificate verification is disabled. For security purposes, it is
        highly recommended that certificate verification remain enabled.

    Returns:
      An ssl.SSLContext instance, or None if the version of Python being used
      doesn't support it.
    """
    try:
      if disable_ssl_certificate_validation:
        ssl._create_default_https_context = ssl._create_unverified_context
        ssl_context = ssl.create_default_context()
      else:
        ssl_context = ssl.create_default_context(cafile=cafile)
    except AttributeError:
      # Earlier versions lack ssl.create_default_context()
      # Rather than raising the exception, no context will be provided for
      # legacy support. Of course, this means no certificate validation is
      # taking place!
      return None

    return ssl_context

  def BuildOpener(self):
    """Builds an OpenerDirector instance using the ProxyConfig settings.

    This will return a urllib2.request.OpenerDirector instance.

    Returns:
      An OpenerDirector instance instantiated with settings defined in the
      ProxyConfig instance.
    """
    return build_opener(*self.GetHandlers())

  def GetHandlers(self):
    """Retrieve the appropriate urllib handlers for the given configuration.

    Returns:
      A list of urllib.request.BaseHandler subclasses to be used when making
      calls with proxy.
    """
    handlers = []

    if self.ssl_context:
      handlers.append(HTTPSHandler(context=self.ssl_context))

    if self.proxies:
      handlers.append(ProxyHandler(self.proxies))

    return handlers


class _ZeepProxyTransport(zeep.transports.Transport):
  """A Zeep transport which configures caching, proxy support, and timeouts."""
  def __init__(self, timeout, proxy_config, cache):
    """Initializes _ZeepProxyTransport.

    Args:
      timeout: An integer timeout in MS for connections.
      proxy_config: A ProxyConfig instance representing proxy settings.
      cache: A zeep.cache.Base instance representing a cache strategy to employ.
    """
    if not cache:
      cache = zeep.cache.SqliteCache()
    elif cache == ZeepServiceProxy.NO_CACHE:
      cache = None

    super(_ZeepProxyTransport, self).__init__(
        timeout=timeout, operation_timeout=timeout, cache=cache)

    self.session.proxies = proxy_config.proxies


class SoapPacker(object):
  """A utility class to be passed to argument packing functions.

  A subclass should be used in cases where custom logic is needed to pack a
  given object in argument packing functions.
  """

  @classmethod
  def Pack(cls, obj):
    raise NotImplementedError('You must subclass SoapPacker.')


def GetSchemaHelperForLibrary():
  return ZeepSchemaHelper


class GoogleSchemaHelper(object):
  """Base class for type to xml conversion.

  Legacy class previously used for AdWords reporting specialness. A subclass
  should be created for each underlying SOAP implementation.
  """
  __metaclass__ = abc.ABCMeta

  @abc.abstractmethod
  def GetSoapXMLForComplexType(self, type_name, value):
    """Return an XML string representing a SOAP complex type.

    Args:
      type_name: The name of the type with namespace prefix if necessary.
      value: A python dictionary to hydrate the type instance with.

    Returns:
      A string containing the SOAP XML for the type.
    """
    return


class ZeepSchemaHelper(GoogleSchemaHelper):
  """Zeep schema helper implementation."""
  def __init__(self, endpoint, timeout,
               proxy_config, namespace_override, cache):
    """Initializes a ZeepSchemaHelper.

    Args:
       endpoint: A string representing the URL to connect to.
       timeout: An integer timeout in MS used to determine connection timeouts.
       proxy_config: A googleads.common.ProxyConfig instance which represents
           the proxy settings needed.
       namespace_override: A string to doctor the WSDL namespace with.
       cache: An instance of zeep.cache.Base to use for caching.

    Raises:
      GoogleAdsValueError: The wrong type was given for caching.
    """

    if cache and not (isinstance(cache, zeep.cache.Base) or
                      cache == ZeepServiceProxy.NO_CACHE):
      raise googleads.errors.GoogleAdsValueError(
          'Must use a proper zeep cache with zeep.')

    transport = _ZeepProxyTransport(timeout, proxy_config, cache)
    try:
      data = transport.load(endpoint)
    except requests.exceptions.HTTPError as e:
      raise googleads.errors.GoogleAdsSoapTransportError(str(e))

    self.schema = zeep.xsd.Schema(lxml.etree.fromstring(data))
    self._namespace_override = namespace_override
    self._element_maker = lxml.builder.ElementMaker(
        namespace=namespace_override, nsmap={'tns': namespace_override})

  def GetSoapXMLForComplexType(self, type_name, value):
    """Return an XML string representing a SOAP complex type.

    Args:
      type_name: The name of the type with namespace prefix if necessary.
      value: A python dictionary to hydrate the type instance with.

    Returns:
      A string containing the SOAP XML for the type.
    """
    element = self.schema.get_element(
        '{%s}%s' % (self._namespace_override, type_name))
    result_element = self._element_maker(element.qname.localname)
    element_value = element(**value)
    element.type.render(result_element, element_value)
    data = lxml.etree.tostring(result_element).strip()
    return data


def GetServiceClassForLibrary():
  return ZeepServiceProxy


class GoogleSoapService(object):
  """Base class for a SOAP service representation.

  A subclass should be created for each underlying SOAP implementation.
  """
  __metaclass__ = abc.ABCMeta

  def __init__(self, header_handler, packer, version):
    """Initializes a SOAP service.

    Args:
      header_handler: A googleads.common.HeaderHandler instance used to set
      SOAP and HTTP headers.
      packer: A googleads.common.SoapPacker instance used to transform
      entities.
      version: the version of the current API, e.g. 'v201811'
    """
    self._header_handler = header_handler
    self._packer = packer
    self._version = version
    self._method_proxies = {}

  @abc.abstractmethod
  def CreateSoapElementForType(self, type_name):
    """Create an instance of a SOAP type.

    Args:
      type_name: The name of the type.

    Returns:
      An instance of type type_name.
    """

  @abc.abstractmethod
  def GetRequestXML(self, method, *args):
    """Get the raw SOAP XML for a request.

    Args:
      method: The method name.
      *args: A list of arguments to be passed to the method.

    Returns:
      An element containing the raw XML that would be sent as the request.
    """

  @abc.abstractmethod
  def _WsdlHasMethod(self, method_name):
    """Determine if the wsdl contains a method.

    Args:
      method_name: The name of the method to search.

    Returns:
      True if the method is in the WSDL, otherwise False.
    """

  @abc.abstractmethod
  def _CreateMethod(self, method_name):
    """Create a method wrapping an invocation to the SOAP service.

    Args:
      method_name: A string identifying the name of the SOAP method to call.

    Returns:
      A callable that can be used to make the desired SOAP request.
    """

  def __getattr__(self, attr):
    """Support service.method() syntax."""
    if self._WsdlHasMethod(attr):
      if attr not in self._method_proxies:
        self._method_proxies[attr] = self._CreateMethod(attr)
      return self._method_proxies[attr]
    else:
      raise googleads.errors.GoogleAdsValueError('Service %s not found' % attr)


class _ZeepAuthHeaderPlugin(zeep.Plugin):
  """A zeep plugin responsible for setting our custom HTTP headers."""

  def __init__(self, header_handler):
    """Instantiate a new _ZeepAuthHeaderPlugin.

    Args:
      header_handler: A googleads.common.HeaderHandler instance.
    """
    self._header_handler = header_handler

  def egress(self, envelope, http_headers, operation, binding_options):
    """Overriding the egress function to set our headers.

    Args:
      envelope: An Element with the SOAP request data.
      http_headers: A dict of the current http headers.
      operation: The SoapOperation instance.
      binding_options: An options dict for the SOAP binding.

    Returns:
      A tuple of the envelope and headers.
    """
    custom_headers = self._header_handler.GetHTTPHeaders()
    http_headers.update(custom_headers)
    return envelope, http_headers


class ZeepServiceProxy(GoogleSoapService):
  """Wraps a zeep service object, allowing custom logic to be injected.

  This class is responsible for refreshing the HTTP and SOAP headers, so changes
  to the client object will be reflected in future SOAP calls, and for
  transforming SOAP call input parameters, allowing dictionary syntax to be used
  with all SOAP complex types.

  Attributes:
    zeep_client: The zeep.Client this service belongs to. If you are
    familiar with zeep, you can utilize this directly.
  """

  NO_CACHE = 'zeep_no_cache'

  def __init__(self, endpoint, header_handler, packer,
               proxy_config, timeout, version, cache=None):
    """Initializes a zeep service proxy.

    Args:
      endpoint: A URL for the service.
      header_handler: A HeaderHandler responsible for setting the SOAP and HTTP
          headers on the service client.
      packer: An optional subclass of googleads.common.SoapPacker that provides
        customized packing logic.
      proxy_config: A ProxyConfig that represents proxy settings.
      timeout: An integer to set the connection timeout.
      version: the version of the current API, e.g. 'v201811'
      cache: An instance of zeep.cache.Base to pass to the underlying SOAP
          library for caching. A file cache by default. To disable, pass
          googleads.common.ZeepServiceProxy.NO_CACHE.

    Raises:
      GoogleAdsValueError: The wrong type was given for caching.
    """
    super(ZeepServiceProxy, self).__init__(header_handler, packer, version)

    if cache and not (isinstance(cache, zeep.cache.Base) or
                      cache == self.NO_CACHE):
      raise googleads.errors.GoogleAdsValueError(
          'Must use a proper zeep cache with zeep.')

    transport = _ZeepProxyTransport(timeout, proxy_config, cache)
    plugins = [_ZeepAuthHeaderPlugin(header_handler),
               googleads.util.ZeepLogger()]
    try:
      self.zeep_client = zeep.Client(
          endpoint, transport=transport, plugins=plugins)
    except requests.exceptions.HTTPError as e:
      raise googleads.errors.GoogleAdsSoapTransportError(str(e))

    first_service = list(self.zeep_client.wsdl.services.values())[0]
    first_port = list(first_service.ports.values())[0]
    self._method_bindings = first_port.binding

  def CreateSoapElementForType(self, type_name):
    """Create an instance of a SOAP type.

    Args:
      type_name: The name of the type.

    Returns:
      An instance of type type_name.
    """
    return self.zeep_client.get_type(type_name)()

  def GetRequestXML(self, method, *args):
    """Get the raw SOAP XML for a request.

    Args:
      method: The method name.
      *args: A list of arguments to be passed to the method.

    Returns:
      An element containing the raw XML that would be sent as the request.
    """
    packed_args = self._PackArguments(method, args, set_type_attrs=True)
    headers = self._GetZeepFormattedSOAPHeaders()

    return self.zeep_client.create_message(
        self.zeep_client.service, method, *packed_args, _soapheaders=headers)

  def _WsdlHasMethod(self, method_name):
    """Determine if a method is in the wsdl.

    Args:
      method_name: The name of the method.

    Returns:
      True if the method is in the wsdl, otherwise False.
    """
    try:
      self._method_bindings.get(method_name)
      return True
    except ValueError:
      return False

  def _GetBindingNamespace(self):
    """Return a string with the namespace of the service binding in the WSDL."""
    return (list(self.zeep_client.wsdl.bindings.values())[0]
            .port_name.namespace)

  def _PackArguments(self, method_name, args, set_type_attrs=False):
    """Properly pack input dictionaries for zeep.

    Pack a list of python dictionaries into XML objects. Dictionaries which
    contain an 'xsi_type' entry are converted into that type instead of the
    argument default. This allows creation of complex objects which include
    inherited types.

    Args:
      method_name: The name of the method that will be called.
      args: A list of dictionaries containing arguments to the method.
      set_type_attrs: A boolean indicating whether or not attributes that end
        in .Type should be set. This is only necessary for batch job service.

    Returns:
      A list of XML objects that can be passed to zeep.
    """
    # Get the params for the method to find the initial types to instantiate.
    op_params = self.zeep_client.get_element(
        '{%s}%s' % (self._GetBindingNamespace(), method_name)).type.elements
    result = [self._PackArgumentsHelper(param, param_data, set_type_attrs)
              for ((_, param), param_data) in zip(op_params, args)]
    return result

  @classmethod
  def _IsBase64(cls, s):
    """An imperfect but decent method for determining if a string is base64.

    Args:
      s: A string with the data to test.

    Returns:
      True if s is base64, else False.
    """
    try:
      if base64.b64encode(base64.b64decode(s)).decode('utf-8') == s:
        return True
    except (TypeError, binascii.Error):
      pass
    return False

  def _PackArgumentsHelper(self, elem, data, set_type_attrs):
    """Recursive helper for PackArguments.

    Args:
      elem: The element type we are creating.
      data: The data to instantiate it with.
      set_type_attrs: A boolean indicating whether or not attributes that end
        in .Type should be set. This is only necessary for batch job service.

    Returns:
      An instance of type 'elem'.
    """
    if self._packer:
      data = self._packer.Pack(data, self._version)

    if isinstance(data, dict):  # Instantiate from simple Python dict
      # See if there is a manually specified derived type.
      type_override = data.get('xsi_type')
      if type_override:
        elem_type = self._DiscoverElementTypeFromLocalname(type_override)
      else:
        elem_type = elem.type

      data_formatted = data.items()
      packed_result = self._CreateComplexTypeFromData(
          elem_type, type_override is not None, data_formatted, set_type_attrs)
    elif isinstance(data, zeep.xsd.CompoundValue):
      # Here the data is already a SOAP element but we still need to look
      # through it in case it has been edited with Python dicts.
      elem_type = data._xsd_type
      data_formatted = zip(dir(data), [data[k] for k in dir(data)])
      packed_result = self._CreateComplexTypeFromData(
          elem_type, False, data_formatted, set_type_attrs)
    elif isinstance(data, (list, tuple)):
      packed_result = [self._PackArgumentsHelper(elem, item, set_type_attrs)
                       for item in data]
    else:
      packed_result = data

    return packed_result

  def _DiscoverElementTypeFromLocalname(self, type_localname):
    """Searches all namespaces for a type by name.

    Args:
      type_localname: The name of the type.

    Returns:
      A fully qualified SOAP type with the specified name.

    Raises:
      A zeep.exceptions.LookupError if the type cannot be found in any
        namespace.
    """
    elem_type = None
    last_exception = None
    for ns_prefix in self.zeep_client.wsdl.types.prefix_map.values():
      try:
        elem_type = self.zeep_client.get_type(
            '{%s}%s' % (ns_prefix, type_localname))
      except zeep.exceptions.LookupError as e:
        last_exception = e
        continue
      break
    if not elem_type:
      raise last_exception
    return elem_type

  def _CreateComplexTypeFromData(
      self, elem_type, type_is_override, data, set_type_attrs):
    """Initialize a SOAP element with specific data.

    Args:
      elem_type: The type of the element to create.
      type_is_override: A boolean specifying if the type is being overridden.
      data: The data to hydrate the type with.
      set_type_attrs: A boolean indicating whether or not attributes that end
        in .Type should be set. This is only necessary for batch job service.

    Returns:
      An fully initialized SOAP element.
    """
    elem_arguments = dict(elem_type.elements)

    # A post order traversal of the original data, need to instantiate from
    # the bottom up.
    instantiated_arguments = {
        k: self._PackArgumentsHelper(elem_arguments[k], v, set_type_attrs)
        for k, v in data if k != 'xsi_type'}
    if set_type_attrs:
      found_type_attr = next((e_name for e_name, _ in elem_type.elements
                              if e_name.endswith('.Type')), None)
      if found_type_attr and type_is_override:
        instantiated_arguments[found_type_attr] = elem_type.qname.localname
    # Now go back through the tree instantiating SOAP types as we go.
    return elem_type(**instantiated_arguments)


  def _GetZeepFormattedSOAPHeaders(self):
    """Returns a dict with SOAP headers in the right format for zeep."""
    headers = self._header_handler.GetSOAPHeaders(self.CreateSoapElementForType)
    soap_headers = {'RequestHeader': headers}
    return soap_headers

  def _CreateMethod(self, method_name):
    """Create a method wrapping an invocation to the SOAP service.

    Args:
      method_name: A string identifying the name of the SOAP method to call.

    Returns:
      A callable that can be used to make the desired SOAP request.
    """
    soap_service_method = self.zeep_client.service[method_name]

    def MakeSoapRequest(*args):
      AddToUtilityRegistry('zeep')
      soap_headers = self._GetZeepFormattedSOAPHeaders()
      packed_args = self._PackArguments(method_name, args)
      try:
        return soap_service_method(
            *packed_args, _soapheaders=soap_headers)['body']['rval']
      except zeep.exceptions.Fault as e:
        error_list = ()
        if e.detail is not None:
          underlying_exception = e.detail.find(
              '{%s}ApiExceptionFault' % self._GetBindingNamespace())
          fault_type = self.zeep_client.get_element(
              '{%s}ApiExceptionFault' % self._GetBindingNamespace())
          fault = fault_type.parse(
              underlying_exception, self.zeep_client.wsdl.types)
          error_list = fault.errors or error_list
        raise googleads.errors.GoogleAdsServerFault(
            e.detail, errors=error_list, message=e.message)
    return MakeSoapRequest


class HeaderHandler(object):
  """A generic header handler interface that must be subclassed by each API."""

  __metaclass__ = abc.ABCMeta

  @abc.abstractmethod
  def GetSOAPHeaders(self, create_method):
    """Returns the required SOAP Headers."""

  @abc.abstractmethod
  def GetHTTPHeaders(self):
    """Returns the required HTTP headers."""
