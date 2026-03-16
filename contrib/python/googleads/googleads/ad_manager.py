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

"""Client library for the Ad Manager API."""


import csv
import datetime
import logging
import numbers
import os
import sys
import time
from urllib.request import build_opener

import pytz
import googleads.common
import googleads.errors

# The default application name.
DEFAULT_APPLICATION_NAME = 'INSERT_APPLICATION_NAME_HERE'
# The endpoint server for Ad Manager.
DEFAULT_ENDPOINT = 'https://ads.google.com'
# The suggested page limit per page fetched from the API.
SUGGESTED_PAGE_LIMIT = 500
# The chunk size used for report downloads.
_CHUNK_SIZE = 16 * 1024


_data_downloader_logger = logging.getLogger(
    '%s.%s' % (__name__, 'data_downloader'))


# A giant dictionary of Ad Manager versions and the services they support.
_SERVICE_MAP = {
    'v202505': (
        'AdjustmentService',
        'AdRuleService',
        'AdsTxtService',
        'AudienceSegmentService',
        'CdnConfigurationService',
        'CmsMetadataService',
        'CompanyService',
        'ContactService',
        'ContentBundleService',
        'ContentService',
        'CreativeService',
        'CreativeSetService',
        'CreativeTemplateService',
        'CreativeWrapperService',
        'CustomFieldService',
        'CustomTargetingService',
        'DaiAuthenticationKeyService',
        'DaiEncodingProfileService',
        'ForecastService',
        'InventoryService',
        'LabelService',
        'LineItemCreativeAssociationService',
        'LineItemService',
        'LineItemTemplateService',
        'LiveStreamEventService',
        'MobileApplicationService',
        'NativeStyleService',
        'NetworkService',
        'OrderService',
        'PlacementService',
        'ProposalLineItemService',
        'ProposalService',
        'PublisherQueryLanguageService',
        'ReportService',
        'SegmentPopulationService',
        'SiteService',
        'StreamActivityMonitorService',
        'SuggestedAdUnitService',
        'TeamService',
        'TargetingPresetService',
        'UserService',
        'UserTeamAssociationService',
        'YieldGroupService',
    ),
    'v202508': (
        'AdjustmentService',
        'AdRuleService',
        'AdsTxtService',
        'AudienceSegmentService',
        'CdnConfigurationService',
        'CmsMetadataService',
        'CompanyService',
        'ContactService',
        'ContentBundleService',
        'ContentService',
        'CreativeService',
        'CreativeSetService',
        'CreativeTemplateService',
        'CreativeWrapperService',
        'CustomFieldService',
        'CustomTargetingService',
        'DaiAuthenticationKeyService',
        'DaiEncodingProfileService',
        'ForecastService',
        'InventoryService',
        'LabelService',
        'LineItemCreativeAssociationService',
        'LineItemService',
        'LineItemTemplateService',
        'LiveStreamEventService',
        'MobileApplicationService',
        'NativeStyleService',
        'NetworkService',
        'OrderService',
        'PlacementService',
        'ProposalLineItemService',
        'ProposalService',
        'PublisherQueryLanguageService',
        'ReportService',
        'SegmentPopulationService',
        'SiteService',
        'StreamActivityMonitorService',
        'SuggestedAdUnitService',
        'TeamService',
        'TargetingPresetService',
        'UserService',
        'UserTeamAssociationService',
        'YieldGroupService',
    ),
    'v202511': (
        'AdjustmentService',
        'AdRuleService',
        'AdsTxtService',
        'AudienceSegmentService',
        'CdnConfigurationService',
        'CmsMetadataService',
        'CompanyService',
        'ContactService',
        'ContentBundleService',
        'ContentService',
        'CreativeService',
        'CreativeSetService',
        'CreativeTemplateService',
        'CreativeWrapperService',
        'CustomFieldService',
        'CustomTargetingService',
        'DaiAuthenticationKeyService',
        'DaiEncodingProfileService',
        'ForecastService',
        'InventoryService',
        'LabelService',
        'LineItemCreativeAssociationService',
        'LineItemService',
        'LineItemTemplateService',
        'LiveStreamEventService',
        'MobileApplicationService',
        'NativeStyleService',
        'NetworkService',
        'OrderService',
        'PlacementService',
        'ProposalLineItemService',
        'ProposalService',
        'PublisherQueryLanguageService',
        'ReportService',
        'SegmentPopulationService',
        'SiteService',
        'StreamActivityMonitorService',
        'SuggestedAdUnitService',
        'TeamService',
        'TargetingPresetService',
        'UserService',
        'UserTeamAssociationService',
        'YieldGroupService',
    ),
    'v202602': (
        'AdjustmentService',
        'AdRuleService',
        'AdsTxtService',
        'AudienceSegmentService',
        'CdnConfigurationService',
        'CmsMetadataService',
        'CompanyService',
        'ContactService',
        'ContentBundleService',
        'ContentService',
        'CreativeService',
        'CreativeSetService',
        'CreativeTemplateService',
        'CreativeWrapperService',
        'CustomFieldService',
        'CustomTargetingService',
        'DaiAuthenticationKeyService',
        'DaiEncodingProfileService',
        'ForecastService',
        'InventoryService',
        'LabelService',
        'LineItemCreativeAssociationService',
        'LineItemService',
        'LineItemTemplateService',
        'LiveStreamEventService',
        'MobileApplicationService',
        'NativeStyleService',
        'NetworkService',
        'OrderService',
        'PlacementService',
        'ProposalLineItemService',
        'ProposalService',
        'PublisherQueryLanguageService',
        'ReportService',
        'SegmentPopulationService',
        'SiteService',
        'StreamActivityMonitorService',
        'SuggestedAdUnitService',
        'TeamService',
        'TargetingPresetService',
        'UserService',
        'UserTeamAssociationService',
        'YieldGroupService',
    ),
}


class AdManagerClient(googleads.common.CommonClient):
  """A central location to set headers and create web service clients.

  Attributes:
    oauth2_client: A googleads.oauth2.GoogleOAuth2Client used to authorize your
        requests.
    application_name: An arbitrary string which will be used to identify your
        application
    network_code: A string identifying the network code of the network you are
        accessing. All requests other than some NetworkService calls require
        this header to be set.
  """

  # The key in the storage yaml which contains Ad Manager data.
  _YAML_KEY = 'ad_manager'
  # A list of values which must be provided to use Ad Manager.
  _REQUIRED_INIT_VALUES = ('application_name',)
  # A list of values which may optionally be provided when using Ad Manager.
  _OPTIONAL_INIT_VALUES = (
      'network_code', googleads.common.ENABLE_COMPRESSION_KEY)
  # The format of SOAP service WSDLs. A server, version, and service name need
  # to be formatted in.
  _SOAP_SERVICE_FORMAT = '%s/apis/ads/publisher/%s/%s?wsdl'

  @classmethod
  def LoadFromString(cls, yaml_doc):
    """Creates a AdManagerClient with information stored in a yaml string.

    Args:
      yaml_doc: The yaml string containing the cached Ad Manager data.

    Returns:
      A AdManagerClient initialized with the values cached in the yaml string.

    Raises:
      A GoogleAdsValueError if the given yaml string does not contain the
      information necessary to instantiate a client object - either a
      required key was missing or an OAuth2 key was missing.
    """
    return cls(**googleads.common.LoadFromString(
        yaml_doc, cls._YAML_KEY, cls._REQUIRED_INIT_VALUES,
        cls._OPTIONAL_INIT_VALUES))

  @classmethod
  def LoadFromStorage(cls, path=None):
    """Creates a AdManagerClient with information stored in a yaml file.

    Args:
      [optional]
      path: str The path to the file containing cached Ad Manager data.

    Returns:
      A AdManagerClient initialized with the values cached in the file.

    Raises:
      A GoogleAdsValueError if the given yaml file does not contain the
      information necessary to instantiate a client object - either a
      required key was missing or an OAuth2 key was missing.
    """
    if path is None:
      path = os.path.join(os.path.expanduser('~'), 'googleads.yaml')

    return cls(**googleads.common.LoadFromStorage(
        path, cls._YAML_KEY, cls._REQUIRED_INIT_VALUES,
        cls._OPTIONAL_INIT_VALUES))

  def __init__(self, oauth2_client, application_name, network_code=None,
               cache=None, proxy_config=None, timeout=3600,
               custom_http_headers=None,
               enable_compression=False):
    """Initializes a AdManagerClient.

    For more information on these arguments, see our SOAP headers guide:
    https://developers.google.com/ad-manager/docs/soap_xml

    Args:
      oauth2_client: A googleads.oauth2.GoogleOAuth2Client used to authorize
          your requests.
      application_name: An arbitrary string which will be used to identify your
          application
      [optional]
      network_code: A string identifying the network code of the network you are
          accessing. All requests other than getAllNetworks require this header
          to be set.
      cache: A subclass of zeep.cache.Base. If not set,
          this will default to a basic file cache. To disable caching for Zeep,
          pass googleads.common.ZeepServiceProxy.NO_CACHE.
      proxy_config: A googleads.common.ProxyConfig instance or None if a proxy
        isn't being used.
      timeout: An integer timeout in MS for connections made to Ad Manager.
      custom_http_headers: A dictionary with HTTP headers to add to outgoing
          requests.
      enable_compression: A boolean indicating if you want to enable compression
        of the SOAP response. If True, the SOAP response will use gzip
        compression, and will be decompressed for you automatically.
    """
    super(AdManagerClient, self).__init__()

    if not application_name or (DEFAULT_APPLICATION_NAME in application_name):
      raise googleads.errors.GoogleAdsValueError(
          'Application name must be set and not contain the default [%s]' %
          DEFAULT_APPLICATION_NAME)

    self.oauth2_client = oauth2_client
    self.application_name = application_name
    self.network_code = network_code
    self.cache = cache
    self.custom_http_headers = custom_http_headers
    self._header_handler = _AdManagerHeaderHandler(
        self, enable_compression, custom_http_headers)
    self.proxy_config = (proxy_config if proxy_config
                         else googleads.common.ProxyConfig())

    if enable_compression:
      self.application_name = '%s (gzip)' % self.application_name

    self.timeout = timeout

  def GetService(self, service_name, version=sorted(_SERVICE_MAP.keys())[-1],
                 server=None):
    """Creates a service client for the given service.

    Args:
      service_name: A string identifying which Ad Manager service to create a
          service client for.
      [optional]
      version: A string identifying the Ad Manager version to connect to. This
          defaults to what is currently the latest version. This will be
          updated in future releases to point to what is then the
          latest version.
      server: A string identifying the webserver hosting the Ad Manager API.

    Returns:
      A googleads.common.GoogleSoapService instance which has the headers
      and proxy configured for use.

    Raises:
      A GoogleAdsValueError if the service or version provided do not exist.
    """
    if not server:
      server = DEFAULT_ENDPOINT

    server = server[:-1] if server[-1] == '/' else server

    try:
      service = googleads.common.GetServiceClassForLibrary()(
          self._SOAP_SERVICE_FORMAT % (server, version, service_name),
          self._header_handler,
          _AdManagerPacker,
          self.proxy_config,
          self.timeout,
          version,
          cache=self.cache)

      return service
    except googleads.errors.GoogleAdsSoapTransportError:
      if version in _SERVICE_MAP:
        if service_name in _SERVICE_MAP[version]:
          raise
        else:
          raise googleads.errors.GoogleAdsValueError(
              'Unrecognized service for the Ad Manager API. Service given: %s '
              'Supported services: %s'
              % (service_name, _SERVICE_MAP[version]))
      else:
        raise googleads.errors.GoogleAdsValueError(
            'Unrecognized version of the Ad Manager API. Version given: %s '
            'Supported versions: %s' % (version, _SERVICE_MAP.keys()))

  def GetDataDownloader(self, version=sorted(_SERVICE_MAP.keys())[-1],
                        server=None):
    """Creates a downloader for Ad Manager reports and PQL result sets.

    This is a convenience method. It is functionally identical to calling
    DataDownloader(ad_manager_client, version, server)

    Args:
      [optional]
      version: A string identifying the Ad Manager version to connect to.
          This defaults to what is currently the latest version. This will be
          updated in future releases to point to what is then the
          latest version.
      server: A string identifying the webserver hosting the Ad Manager API.

    Returns:
      A DataDownloader tied to this AdManagerClient, ready to download reports.
    """
    if not server:
      server = DEFAULT_ENDPOINT

    return DataDownloader(self, version, server)


class _AdManagerHeaderHandler(googleads.common.HeaderHandler):
  """Handler which sets the headers for an Ad Manager SOAP call."""

  # The library signature for Ad Manager, to be appended to all
  # application_names.
  _PRODUCT_SIG = 'DfpApi-Python'
  # The name of the WSDL-defined SOAP Header class used in all requests.
  _SOAP_HEADER_CLASS = 'ns0:SoapRequestHeader'

  def __init__(
      self, ad_manager_client, enable_compression, custom_http_headers=None):
    """Initializes an AdManagerHeaderHandler.

    Args:
      ad_manager_client: The AdManagerClient whose data will be used to fill
          in the headers. We retain a reference to this object so that the
          header handler picks up changes to the client.
      enable_compression: A boolean indicating if you want to enable compression
        of the SOAP response. If True, the SOAP response will use gzip
        compression, and will be decompressed for you automatically.
      custom_http_headers: A dictionary of custom HTTP headers to send with all
        requests.
    """
    self._ad_manager_client = ad_manager_client
    self.enable_compression = enable_compression
    self.custom_http_headers = custom_http_headers or {}

  def GetSOAPHeaders(self, create_method):
    """Returns the SOAP headers required for request authorization.

    Args:
      create_method: The SOAP library specific method used to instantiate SOAP
      objects.

    Returns:
      A SOAP object containing the headers.
    """
    header = create_method(self._SOAP_HEADER_CLASS)
    header.networkCode = self._ad_manager_client.network_code
    header.applicationName = ''.join([
        self._ad_manager_client.application_name,
        googleads.common.GenerateLibSig(self._PRODUCT_SIG)])
    return header

  def GetHTTPHeaders(self):
    """Returns the HTTP headers required for request authorization.

    Returns:
      A dictionary containing the required headers.
    """
    http_headers = self._ad_manager_client.oauth2_client.CreateHttpHeader()
    if self.enable_compression:
      http_headers['accept-encoding'] = 'gzip'

    http_headers.update(self.custom_http_headers)

    return http_headers


class _AdManagerPacker(googleads.common.SoapPacker):
  """A utility applying customized packing logic for Ad Manager."""

  @classmethod
  def Pack(cls, obj, version):
    """Pack the given object using Ad Manager-specific logic.

    Args:
      obj: an object to be packed for SOAP using Ad Manager-specific logic, if
          applicable.
      version: the version of the current API, e.g. 'v201811'

    Returns:
      The given object packed with Ad Manager-specific logic for SOAP,
      if applicable. Otherwise, returns the given object unmodified.
    """
    if isinstance(obj, (datetime.datetime, datetime.date)):
      return cls.AdManagerDateTimePacker(obj, version)
    return obj

  @classmethod
  def AdManagerDateTimePacker(cls, value, version):
    """Returns dicts formatted for Ad Manager SOAP based on date/datetime.

    Args:
      value: A date or datetime object to be converted.
      version: the version of the current API, e.g. 'v201811'

    Returns:
      The value object correctly represented for Ad Manager SOAP.
    """

    if isinstance(value, datetime.datetime):
      if value.tzinfo is None:
        raise googleads.errors.GoogleAdsValueError(
            'Datetime %s is not timezone aware.' % value
        )
      return {
          'date': cls.AdManagerDateTimePacker(value.date(), version),
          'hour': value.hour,
          'minute': value.minute,
          'second': value.second,
          'timeZoneId': value.tzinfo.zone,
      }
    elif isinstance(value, datetime.date):
      return {'year': value.year, 'month': value.month, 'day': value.day}


@googleads.common.RegisterUtility('StatementBuilder')
class StatementBuilder(object):
  """Provides the ability to programmatically construct PQL queries."""

  class _OrderByPair(object):
    """Stores and serializes a pair of column/ascending values."""

    def __init__(self, column, ascending):
      """Initializes a pair of column/ascending values.

      Args:
        column: a string specifying the column name.
        ascending: a boolean specifying sort order ascending or descending.
      """
      self.column = column
      self.ascending = ascending

    def __repr__(self):
      """The string representation of this class is valid PQL."""
      return '%s %s' % (self.column, 'ASC' if self.ascending else 'DESC')

  _SELECT_PART = 'SELECT %s FROM %s'
  _WHERE_PART = 'WHERE %s'
  _ORDER_BY_PART = 'ORDER BY %s'
  _LIMIT_PART = 'LIMIT %s'
  _OFFSET_PART = 'OFFSET %s'

  def __init__(self, select_columns=None, from_table=None, where=None,
               order_by=None, order_ascending=True,
               limit=SUGGESTED_PAGE_LIMIT, offset=0,
               version=sorted(_SERVICE_MAP.keys())[-1]):
    """Initializes StatementBuilder.

    Args:
      select_columns: a comma separated string of column names.
      from_table: a string specifying the table to select from.
      where: a string with the where clause.
      order_by: a string with the order by clause.
      order_ascending: a boolean specifying sort order ascending or descending.
      limit: an integer with the limit clause.
      offset: an integer with the offset clause.
      version: A string identifying the Ad Manager version this statement is
          compatible with. This defaults to what is currently the latest
          version. This will be updated in future releases to point to what is
          then the latest version.
    """
    self._select = select_columns
    self._from_ = from_table
    self._where = where
    self.limit = limit
    self.offset = offset
    self._version = version
    if order_by:
      self._order_by = self._OrderByPair(column=order_by,
                                         ascending=order_ascending)
    else:
      self._order_by = None
    self._values = {}  # Use a dict to prevent duplicates

  def ToStatement(self):
    """Builds a PQL string from the current state.

    Returns:
      A string representation of the PQL statement.
    """

    if self._select and not self._from_:
      raise googleads.errors.GoogleAdsError('FROM clause required with SELECT.')

    if self._from_ and not self._select:
      raise googleads.errors.GoogleAdsError('SELECT clause required with FROM.')

    query = []

    if self._select:
      query.append(self._SELECT_PART % (self._select, self._from_))

    if self._where:
      query.append(self._WHERE_PART % self._where)

    if self._order_by:
      query.append(self._ORDER_BY_PART % self._order_by)

    if self.limit:
      query.append(self._LIMIT_PART % self.limit)

    if self.offset is not None:
      query.append(self._OFFSET_PART % self.offset)

    return {'query': ' '.join(query),
            'values': (PQLHelper.GetQueryValuesFromDict(
                self._values, self._version) if self._values else None)}

  def Select(self, columns):
    """Adds a SELECT clause.

    Args:
      columns: A comma separated string specifying the columns.

    Returns:
      A reference to the StatementBuilder.
    """
    self._select = columns
    return self

  def From(self, table):
    """Adds a FROM clause.

    Args:
      table: A string specifying the table.

    Returns:
      A reference to the StatementBuilder
    """
    self._from_ = table
    return self

  def Where(self, clause):
    """Adds a WHERE clause.

    Args:
      clause: A string specifying the where clause.

    Returns:
      A reference to the StatementBuilder.
    """
    self._where = clause
    return self

  def Limit(self, limit=SUGGESTED_PAGE_LIMIT):
    """Adds a LIMIT clause.

    Args:
      limit: An integer specifying the limit value.

    Returns:
      A reference to the StatementBuilder.
    """
    self.limit = limit
    return self

  def Offset(self, value):
    """Adds an OFFSET clause.

    Args:
      value: An integer specifying the offset value.

    Returns:
      A reference to the StatementBuilder.
    """
    self.offset = value
    return self

  def OrderBy(self, column, ascending=True):
    """Adds an ORDER BY clause.

    Args:
      column: A string specifying the column to order by.
      ascending: A bool to indicate ascending vs descending.

    Returns:
      A reference to the StatementBuilder
    """
    self._order_by = self._OrderByPair(column=column,
                                       ascending=ascending)
    return self

  def WithBindVariable(self, key, value):
    """Binds a value to a variable in the statement.

    Args:
      key: A string identifying the variable.
      value: A object of an acceptable type specifying the value.

    Returns:
      A reference to the StatementBuilder.
    """

    # Make this call to throw the exception here if there is a problem
    PQLHelper.GetValueRepresentation(value, self._version)

    self._values[key] = value
    return self


class PQLHelper(object):
  """Utility class for PQL."""

  @classmethod
  def GetQueryValuesFromDict(cls, d, version=sorted(_SERVICE_MAP.keys())[-1]):
    """Converts a dict of python types into a list of PQL types.

    Args:
      d: A dictionary of variable names to python types.
      version: A string identifying the Ad Manager version the values object
          is compatible with. This defaults to what is currently the latest
          version. This will be updated in future releases to point to what is
          then the latest version.

    Returns:
      A list of variables formatted for PQL statements which are compatible with
      a particular API version.
    """
    return [{
        'key': key,
        'value': cls.GetValueRepresentation(value, version)
    } for key, value in d.items()]

  @classmethod
  def GetValueRepresentation(cls, value,
                             version=sorted(_SERVICE_MAP.keys())[-1]):
    """Converts a single python value to its PQL representation.

    Args:
      value: A python value.
      version: A string identifying the Ad Manager version the value object
          is compatible with. This defaults to what is currently the latest
          version. This will be updated in future releases to point to what is
          then the latest version.

    Returns:
      The value formatted for PQL statements which are compatible with a
      particular API version.
    """
    if isinstance(value, str):
      return {'value': value, 'xsi_type': 'TextValue'}
    elif isinstance(value, bool):
      return {'value': value, 'xsi_type': 'BooleanValue'}
    elif isinstance(value, numbers.Number):
      return {'value': value, 'xsi_type': 'NumberValue'}
    # It's important that datetime is checked for before date
    # because isinstance(datetime.datetime.now(), datetime.date) is True
    elif isinstance(value, datetime.datetime):
      if value.tzinfo is None:
        raise googleads.errors.GoogleAdsValueError(
            'Datetime %s is not timezone aware.' % value
        )

      return {
          'xsi_type': 'DateTimeValue',
          'value': {
              'date': {
                  'year': value.year,
                  'month': value.month,
                  'day': value.day,
              },
              'hour': value.hour,
              'minute': value.minute,
              'second': value.second,
              'timeZoneId': value.tzinfo.zone,
          }
      }
    elif isinstance(value, datetime.date):
      return {
          'xsi_type': 'DateValue',
          'value': {
              'year': value.year,
              'month': value.month,
              'day': value.day,
          }
      }
    elif isinstance(value, list):
      if value and not all(isinstance(x, type(value[0])) for x in value):
        raise googleads.errors.GoogleAdsValueError('Cannot pass more than one '
                                                   'type in a set.')

      return {
          'xsi_type': 'SetValue',
          'values': [cls.GetValueRepresentation(v, version) for v in value]
      }
    else:
      raise googleads.errors.GoogleAdsValueError(
          'Can\'t represent unknown type: %s.' % type(value))


@googleads.common.RegisterUtility('FilterStatement')
class FilterStatement(object):
  """A statement object for PQL and get*ByStatement queries.

  The FilterStatement object allows for user control of limit/offset. It
  automatically limits queries to the suggested page limit if not explicitly
  set.
  """

  def __init__(self, where_clause='', values=None, limit=SUGGESTED_PAGE_LIMIT,
               offset=0):
    self.where_clause = where_clause
    self.values = values
    self.limit = limit
    self.offset = offset

  def ToStatement(self):
    """Returns this statement object in the format Ad Manager requires."""
    return {'query': ('%s LIMIT %d OFFSET %d' %
                      (self.where_clause, self.limit, self.offset)),
            'values': self.values}


class DataDownloader(object):
  """A utility that can be used to download reports and PQL result sets."""

  def __init__(self, ad_manager_client, version=sorted(_SERVICE_MAP.keys())[-1],
               server=None):
    """Initializes a DataDownloader.

    Args:
      ad_manager_client: The AdManagerClient whose attributes will be used to
          authorize your report download and PQL query requests.
      [optional]
      version: A string identifying the Ad Manager version to connect to.
          This defaults to what is currently the latest version. This will be
          updated in future releases to point to what is then the
          latest version.
      server: A string identifying the webserver hosting the Ad Manager API.
    """
    if not server:
      server = DEFAULT_ENDPOINT

    if server[-1] == '/':
      server = server[:-1]

    self._ad_manager_client = ad_manager_client
    self._version = version
    self._server = server
    self._report_service = None
    self._pql_service = None
    self.proxy_config = self._ad_manager_client.proxy_config
    handlers = self.proxy_config.GetHandlers()
    self.url_opener = build_opener(*handlers)

    if self._ad_manager_client.custom_http_headers:
      self.url_opener.addheaders.extend(
          self._ad_manager_client.custom_http_headers.items())

  def _GetReportService(self):
    """Lazily initializes a report service client."""
    if not self._report_service:
      self._report_service = self._ad_manager_client.GetService(
          'ReportService', self._version, self._server)
    return self._report_service

  def _GetPqlService(self):
    """Lazily initializes a PQL service client."""
    if not self._pql_service:
      self._pql_service = self._ad_manager_client.GetService(
          'PublisherQueryLanguageService', self._version, self._server)
    return self._pql_service

  def WaitForReport(self, report_job, poll_time_seconds=30):
    """Runs a report, then waits (blocks) for the report to finish generating.

    Args:
      report_job: The report job to wait for. This may be a dictionary or an
          instance of the SOAP ReportJob class.
      poll_time_seconds: The number of seconds to wait between calls to
          getReportJobStatus. Defaults to 30 seconds.

    Returns:
      The completed report job's ID as a string.

    Raises:
      An AdManagerReportError if the report job fails to complete.
    """
    service = self._GetReportService()
    report_job_id = service.runReportJob(report_job)['id']

    status = service.getReportJobStatus(report_job_id)

    while status != 'COMPLETED' and status != 'FAILED':
      _data_downloader_logger.debug('Report job status: %s', status)
      time.sleep(poll_time_seconds)
      status = service.getReportJobStatus(report_job_id)

    if status == 'FAILED':
      raise googleads.errors.AdManagerReportError(report_job_id)
    else:
      _data_downloader_logger.debug('Report has completed successfully')
      return report_job_id

  def DownloadReportToFile(self, report_job_id, export_format, outfile,
                           include_report_properties=False,
                           include_totals_row=None, use_gzip_compression=True):
    """Downloads report data and writes it to a file.

    The report job must be completed before calling this function.

    Args:
      report_job_id: The ID of the report job to wait for, as a string.
      export_format: The export format for the report file, as a string.
      outfile: A writeable, file-like object to write to.
      include_report_properties: Whether or not to include the report
        properties (e.g. network, user, date generated...)
        in the generated report.
      include_totals_row: Whether or not to include the totals row.
      use_gzip_compression: Whether or not to use gzip compression.
    """
    service = self._GetReportService()

    if include_totals_row is None:  # True unless CSV export if not specified
      include_totals_row = True if export_format != 'CSV_DUMP' else False
    opts = {
        'exportFormat': export_format,
        'includeReportProperties': include_report_properties,
        'includeTotalsRow': include_totals_row,
        'useGzipCompression': use_gzip_compression
    }
    report_url = service.getReportDownloadUrlWithOptions(report_job_id, opts)
    _data_downloader_logger.info('Request Summary: Report job ID: %s, %s',
                                 report_job_id, opts)
    _data_downloader_logger.info('Report URL: %s', report_url)
    response = self.url_opener.open(report_url)

    _data_downloader_logger.debug(
        'Incoming response: %s %s REDACTED REPORT DATA', response.code,
        response.msg)

    while True:
      chunk = response.read(_CHUNK_SIZE)
      if not chunk: break
      outfile.write(chunk)

  def DownloadPqlResultToList(self, pql_query, values=None):
    """Downloads the results of a PQL query to a list.

    Args:
      pql_query: str a statement filter to apply (the query should not include
                 the limit or the offset)
      [optional]
      values: A dict of python objects or a list of raw SOAP values to bind
              to the pql_query.

    Returns:
      a list of lists with the first being the header row and each subsequent
      list being a row of results.
    """
    results = []
    self._PageThroughPqlSet(pql_query, results.append, values)
    return results

  def DownloadPqlResultToCsv(self, pql_query, file_handle, values=None):
    """Downloads the results of a PQL query to CSV.

    Args:
      pql_query: str a statement filter to apply (the query should not include
                 the limit or the offset)
      file_handle: file the file object to write to.
      [optional]
      values: A dict of python objects or a list of raw SOAP values to bind
              to the pql_query.
    """
    pql_writer = csv.writer(file_handle, delimiter=',',
                            quotechar='"', quoting=csv.QUOTE_ALL)
    self._PageThroughPqlSet(pql_query, pql_writer.writerow, values)

  def _ConvertValueForCsv(self, pql_value):
    """Sanitizes a field value from a Value object to a CSV suitable format.

    Args:
      pql_value: dict a dictionary containing the data for a single field of an
                 entity.

    Returns:
      str a CSV writer friendly value formatted by Value.Type.
    """
    if 'value' in pql_value:
      field = pql_value['value']
    elif 'values' in pql_value:
      field = pql_value['values']
    else:
      field = None

    if field:
      if isinstance(field, list):
        if all(AdManagerClassType(single_field) == AdManagerClassType(field[0])
               for single_field in field):
          return ','.join([
              '"%s"' % str(self._ConvertValueForCsv(single_field))
              for single_field in field])
        else:
          raise googleads.errors.GoogleAdsValueError(
              'The set value returned contains unsupported mix value types')

      class_type = AdManagerClassType(pql_value)

      if class_type == 'TextValue':
        s = field.replace('"', '""')

        # Encode UTF-8 characters for Python 2 only.
        if sys.version_info.major < 3:
          s = s.encode('UTF8')
        return s
      elif class_type == 'NumberValue':
        return float(field) if '.' in field else int(field)
      elif class_type == 'DateTimeValue':
        return self._ConvertDateTimeToOffset(field)
      elif class_type == 'DateValue':
        return datetime.date(int(field['year']),
                             int(field['month']),
                             int(field['day'])).isoformat()
      else:
        return field
    else:
      return '-'

  def _PageThroughPqlSet(self, pql_query, output_function, values):
    """Pages through a pql_query and performs an action (output_function).

    Args:
      pql_query: str a statement filter to apply (the query should not include
                 the limit or the offset)
      output_function: the function to call to output the results (csv or in
                       memory)
      values: A dict of python objects or a list of raw SOAP values to bind
              to the pql_query.
    """
    if isinstance(values, dict):
      values = PQLHelper.GetQueryValuesFromDict(values, self._version)

    pql_service = self._GetPqlService()
    current_offset = 0

    while True:
      query_w_limit_offset = '%s LIMIT %d OFFSET %d' % (pql_query,
                                                        SUGGESTED_PAGE_LIMIT,
                                                        current_offset)
      response = pql_service.select({'query': query_w_limit_offset,
                                     'values': values})

      if 'rows' in response:
        # Write the header row only on first pull
        if current_offset == 0:
          header = response['columnTypes']
          output_function([label['labelName'] for label in header])

        entities = response['rows']
        result_set_size = len(entities)

        for entity in entities:
          output_function([self._ConvertValueForCsv(value) for value
                           in entity['values']])

        current_offset += result_set_size
        if result_set_size != SUGGESTED_PAGE_LIMIT:
          break
      else:
        break

  def _ConvertDateTimeToOffset(self, date_time_value):
    """Converts the PQL formatted response for a dateTime object.

    Output conforms to ISO 8061 format, e.g. 'YYYY-MM-DDTHH:MM:SSz.'

    Args:
      date_time_value: dict The date time value from the PQL response.

    Returns:
      str: A string representation of the date time value uniform to
           ReportService.
    """
    date_time_obj = datetime.datetime(int(date_time_value['date']['year']),
                                      int(date_time_value['date']['month']),
                                      int(date_time_value['date']['day']),
                                      int(date_time_value['hour']),
                                      int(date_time_value['minute']),
                                      int(date_time_value['second']))
    date_time_str = pytz.timezone(
        date_time_value['timeZoneId']).localize(date_time_obj).isoformat()

    if date_time_str[-5:] == '00:00':
      return date_time_str[:-6] + 'Z'
    else:
      return date_time_str


def AdManagerClassType(value):
  """Returns the class type for an object.

  Args:
    value: generic object to return type for.

  Returns:
    str: A string representation of the value response type.
  """
  return value.__class__.__name__
