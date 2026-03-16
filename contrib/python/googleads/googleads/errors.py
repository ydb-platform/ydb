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

"""Errors used by the Google Ads Client Library."""


class GoogleAdsError(Exception):
  """Parent class of all errors raised by this library."""
  pass


class GoogleAdsValueError(GoogleAdsError):
  """Error indicating that the user input for a function was invalid."""
  pass


class GoogleAdsSoapTransportError(GoogleAdsError):
  pass


class GoogleAdsServerFault(GoogleAdsError):

  def __init__(self, document, errors=(), message=None):
    super(GoogleAdsServerFault, self).__init__(
        message if message else 'Server Error')

    self.errors = errors
    self.document = document


class AdManagerReportError(GoogleAdsError):
  """Error indicating that an Ad Manager report download request failed.

  Attributes:
    report_job_id: The ID of the report job which failed.
  """

  def __init__(self, report_job_id):
    """Initializes a AdManagerReportError.

    Args:
      report_job_id: The ID of the report job which failed.
    """
    super(AdManagerReportError, self).__init__(
        'Ad Manager report job failed. The ID of the failed report is: %s'
        % report_job_id)
    self.report_job_id = report_job_id
