"""
Provides AWS4Auth class for handling Amazon Web Services version 4
authentication with the Requests module.

"""

# Licensed under the MIT License:
# http://opensource.org/licenses/MIT


class RequestsAws4AuthException(Exception): pass
class DateMismatchError(RequestsAws4AuthException): pass
class NoSecretKeyError(RequestsAws4AuthException): pass
class DateFormatError(RequestsAws4AuthException): pass
