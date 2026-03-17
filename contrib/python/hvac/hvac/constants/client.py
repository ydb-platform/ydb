#!/usr/bin/env python
"""Constants related to the hvac.Client class."""

from os import getenv

DEPRECATED_PROPERTIES = {}
# ^ this follows the format defined in utils.getattr_with_deprecated_properties
# example:
#   {
#       "old_property_one": {
#           "to_be_removed_in_version": "99.0.0",
#           "client_property": "auth",
#       },
#       "old_property_two": {
#           "to_be_removed_in_version": "99.0.0",
#           "client_property": "secrets",
#           "new_property": "new_property_two",
#       },
#   }
#
# Result is that `client.old_property_one` will return the value of `client.auth.old_property_one`,
# and `client.old_property_two` will return `client.secrets.new_property_two`.

DEFAULT_URL = "http://localhost:8200"
VAULT_CACERT = getenv("VAULT_CACERT")
VAULT_CAPATH = getenv("VAULT_CAPATH")
VAULT_CLIENT_CERT = getenv("VAULT_CLIENT_CERT")
VAULT_CLIENT_KEY = getenv("VAULT_CLIENT_KEY")
