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
"""OAuth2 integration for the googleads library.

This module provides a basic interface which the googleads library uses to
authorize API requests and some simple implementations built on
oauth2client.

If our OAuth2 workflows doesn't meet your requirements, you can implement this
interface in your own way. For example, you could pull credentials from a shared
server and/or centralize refreshing credentials to prevent every Python process
from independently refreshing the credentials.
"""

import googleads.errors
import requests

import google.auth.transport.requests
import google.oauth2.credentials
import google.oauth2.service_account


# The scopes used for authorizing with the APIs supported by this library.
SCOPES = {'ad_manager': 'https://www.googleapis.com/auth/dfp'}


def GetAPIScope(api_name):
  """Retrieves the scope for the given API name.

  Args:
    api_name: A string identifying the name of the API we want to retrieve a
        scope for.

  Returns:
    A string that is the scope for the given API name.

  Raises:
    GoogleAdsValueError: If the given api_name is invalid; currently accepted
        value is "ad_manager".
  """
  try:
    return SCOPES[api_name]
  except KeyError:
    raise googleads.errors.GoogleAdsValueError(
        'Invalid API name "%s" provided. Acceptable values are: %s' %
        (api_name, SCOPES.keys()))


class GoogleOAuth2Client(object):
  """An OAuth2 client for use with Google APIs."""
  # The web address for generating OAuth2 credentials at Google.
  _GOOGLE_OAUTH2_ENDPOINT = 'https://accounts.google.com/o/oauth2/token'
  # The placeholder URL is used when adding the access token to our request. A
  # well-formed URL is required, but since we're using HTTP header placement for
  # the token, this URL is completely unused.
  _TOKEN_URL = 'https://www.google.com'
  _USER_AGENT = 'Google Ads Python Client Library'

  def CreateHttpHeader(self):
    """Creates an OAuth2 HTTP header.

    The OAuth2 credentials will be refreshed as necessary.

    Returns:
      A dictionary containing one entry: the OAuth2 Bearer header under the
      'Authorization' key.
    """
    raise NotImplementedError('You must subclass GoogleOAuth2Client.')


class GoogleRefreshableOAuth2Client(GoogleOAuth2Client):
  """A refreshable OAuth2 client for use with Google APIs.

  This interface assumes all responsibility for refreshing credentials when
  necessary.
  """

  def Refresh(self):
    """Refreshes the access token used by the client."""
    raise NotImplementedError(
        'You must subclass GoogleRefreshableOAuth2Client.')


class GoogleAccessTokenClient(GoogleOAuth2Client):
  """A simple client for using OAuth2 for Google APIs with an access token.

  This class is not capable of supporting any flows other than taking an
  existing, active access token. It does not matter which of Google's OAuth2
  flows you used to generate the access token (installed application, web flow,
  etc.).

  When the provided access token expires, a GoogleAdsError will be raised.
  """

  def __init__(self, access_token, token_expiry):
    """Initializes a GoogleAccessTokenClient.

    Args:
      access_token: A string containing your access token.
      token_expiry: A datetime instance indicating when the given access token
      expires.
    """
    self.creds = google.oauth2.credentials.Credentials(
        token=access_token)
    self.creds.expiry = token_expiry

  def CreateHttpHeader(self):
    """Creates an OAuth2 HTTP header.

    The OAuth2 credentials will be refreshed as necessary. In the event that
    the credentials fail to refresh, a message is logged but no exception is
    raised.

    Returns:
      A dictionary containing one entry: the OAuth2 Bearer header under the
      'Authorization' key.

    Raises:
      GoogleAdsError: If the access token has expired.
    """
    oauth2_header = {}

    if self.creds.expired:
      raise googleads.errors.GoogleAdsError('Access token has expired.')

    self.creds.apply(oauth2_header)
    return oauth2_header


class GoogleRefreshTokenClient(GoogleRefreshableOAuth2Client):
  """A simple client for using OAuth2 for Google APIs with a refresh token.

  This class is not capable of supporting any flows other than taking an
  existing, active refresh token and generating credentials from it. It does not
  matter which of Google's OAuth2 flows you used to generate the refresh
  token (installed application, web flow, etc.).

  Attributes:
    proxy_info: A ProxyInfo instance used for refresh requests.
  """

  def __init__(self, client_id, client_secret, refresh_token, **kwargs):
    """Initializes a GoogleRefreshTokenClient.

    Args:
      client_id: A string containing your client ID.
      client_secret: A string containing your client secret.
      refresh_token: A string containing your refresh token.
      **kwargs: Keyword arguments.

    Keyword Arguments:
      access_token: A string containing your access token.
      token_expiry: A datetime instance indicating when the given access token
      expires.
      proxy_config: A googleads.common.ProxyConfig instance or None if a proxy
        isn't being used.
    """
    self.creds = google.oauth2.credentials.Credentials(
        kwargs.get('access_token'), refresh_token=refresh_token,
        client_id=client_id, client_secret=client_secret,
        token_uri=self._GOOGLE_OAUTH2_ENDPOINT)
    self.creds.expiry = kwargs.get('token_expiry')
    self.proxy_config = kwargs.get('proxy_config',
                                   googleads.common.ProxyConfig())

  def CreateHttpHeader(self):
    """Creates an OAuth2 HTTP header.

    The OAuth2 credentials will be refreshed as necessary. In the event that
    the credentials fail to refresh, a message is logged but no exception is
    raised.

    Returns:
      A dictionary containing one entry: the OAuth2 Bearer header under the
      'Authorization' key.

    Raises:
      google.auth.exceptions.RefreshError: If the refresh fails.
    """
    oauth2_header = {}

    if self.creds.expiry is None or self.creds.expired:
      self.Refresh()

    self.creds.apply(oauth2_header)
    return oauth2_header

  def Refresh(self):
    """Uses the Refresh Token to retrieve and set a new Access Token.

    Raises:
      google.auth.exceptions.RefreshError: If the refresh fails.
    """
    with requests.Session() as session:
      session.proxies = self.proxy_config.proxies
      session.verify = not self.proxy_config.disable_certificate_validation
      session.cert = self.proxy_config.cafile

      self.creds.refresh(
          google.auth.transport.requests.Request(session=session))


class GoogleCredentialsClient(GoogleRefreshableOAuth2Client):
  """A simple client for using OAuth2 for Google APIs with a credentials object.

  This class is not capable of supporting any flows other than taking an
  existing credentials (google.auth.credentials) to generate the refresh
  and access tokens.
  """

  def __init__(self, credentials):
    """Initializes an OAuth2 client using a credentials object.

    Args:
      credentials: A credentials object implementing google.auth.credentials.
    """
    self.creds = credentials

  def CreateHttpHeader(self):
    """Creates an OAuth2 HTTP header.

    Returns:
      A dictionary containing one entry: the OAuth2 Bearer header under the
      'Authorization' key.
    """
    if self.creds.expiry is None or self.creds.expired:
      self.Refresh()

    oauth2_header = {}
    self.creds.apply(oauth2_header)
    return oauth2_header

  def Refresh(self):
    """Uses the credentials object to retrieve and set a new Access Token."""
    transport = google.auth.transport.requests.Request()
    self.creds.refresh(transport)


class GoogleServiceAccountClient(GoogleRefreshableOAuth2Client):
  """A simple client for using OAuth2 for Google APIs with a service account.

  This class is not capable of supporting any flows other than generating
  credentials from a service account email and key file. This is incompatible
  with App Engine.

  Attributes:
    proxy_info: A ProxyInfo instance used for refresh requests.
  """
  _USER_AGENT = 'Google Ads Python Client Library'
  _FILE_NOT_FOUND_TEMPLATE = 'The specified key file (%s) does not exist.'

  def __init__(self, key_file, scope, sub=None, proxy_config=None):
    """Initializes a GoogleServiceAccountClient.

    Args:
      key_file: A string containing the path to your Service Account
          JSON key file.
      scope: The scope of the API you're authorizing for.
      [optional]
      sub: A string containing the email address of a user account you want to
           impersonate.
      proxy_config: A googleads.common.ProxyConfig instance.

    Raises:
      GoogleAdsError: If an unsupported version of oauth2client is installed.
      GoogleAdsValueError: If the given key file does not exist.
    """
    try:
      self.creds = (
          google.oauth2.service_account.Credentials.from_service_account_file(
              key_file, scopes=[scope], subject=sub))
    except IOError:
      raise googleads.errors.GoogleAdsValueError(
          self._FILE_NOT_FOUND_TEMPLATE % key_file)

    self.proxy_config = (proxy_config if proxy_config else
                         googleads.common.ProxyConfig())
    self.Refresh()

  def CreateHttpHeader(self):
    """Creates an OAuth2 HTTP header.

    The OAuth2 credentials will be refreshed as necessary. In the event that
    the credentials fail to refresh, a message is logged but no exception is
    raised.

    Returns:
      A dictionary containing one entry: the OAuth2 Bearer header under the
      'Authorization' key.

    Raises:
      google.auth.exceptions.RefreshError: If the refresh fails.
    """
    oauth2_header = {}

    if self.creds.expiry is None or self.creds.expired:
      self.Refresh()

    self.creds.apply(oauth2_header)
    return oauth2_header

  def Refresh(self):
    """Retrieve and set a new Access Token.

    Raises:
      google.auth.exceptions.RefreshError: If the refresh fails.
    """
    with requests.Session() as session:
      session.proxies = self.proxy_config.proxies
      session.verify = not self.proxy_config.disable_certificate_validation
      session.cert = self.proxy_config.cafile

      self.creds.refresh(
          google.auth.transport.requests.Request(session=session))
