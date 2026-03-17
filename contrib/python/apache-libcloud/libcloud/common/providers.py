# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Common methods for obtaining a reference to the provider driver class.
"""

__all__ = ["get_driver", "set_driver"]


def get_driver(drivers, provider, deprecated_providers=None, deprecated_constants=None):
    """
    Get a driver.

    :param drivers: Dictionary containing valid providers.
    :type drivers: ``dict``

    :param provider: Id (constant) of provider to get the driver for.
    :type provider: :class:`libcloud.types.Provider`

    :param: deprecated_providers: Dictionary with information about the
            deprecated drivers.
    :type deprecated_providers: ``dict``

    :param: deprecated_constants: Dictionary with information about the
            deprecated provider constants.
    :type deprecated_constants: ``dict``
    """
    # Those providers have been shut down or similar.
    deprecated_providers = deprecated_providers or {}
    if provider in deprecated_providers:
        url = deprecated_providers[provider]["url"]
        reason = deprecated_providers[provider]["reason"]
        msg = "Provider no longer supported: {}, please visit: {}".format(url, reason)
        raise Exception(msg)

    # Those drivers have moved to "region" constructor argument model
    deprecated_constants = deprecated_constants or {}
    if provider in deprecated_constants:
        old_name = provider.upper()
        new_name = deprecated_constants[provider].upper()

        url = "https://s.apache.org/lc0140un"
        msg = (
            'Provider constant "%s" has been removed. New constant '
            'is now called "%s".\n'
            "For more information on this change and how to modify your "
            "code to work with it, please visit: %s" % (old_name, new_name, url)
        )
        raise Exception(msg)

    if provider in drivers:
        mod_name, driver_name = drivers[provider]
        _mod = __import__(mod_name, globals(), locals(), [driver_name])
        return getattr(_mod, driver_name)

    # NOTE: This is for backward compatibility reasons where user could use
    # a string value instead of a Provider.FOO enum constant and this function
    # would still work
    for provider_name, (mod_name, driver_name) in drivers.items():
        # NOTE: This works because Provider enum class overloads __eq__
        if provider.lower() == provider_name.lower():
            _mod = __import__(mod_name, globals(), locals(), [driver_name])
            return getattr(_mod, driver_name)

    raise AttributeError("Provider %s does not exist" % (provider))


def set_driver(drivers, provider, module, klass):
    """
    Sets a driver.

    :param drivers: Dictionary to store providers.
    :param provider: Id of provider to set driver for

    :type provider: :class:`libcloud.types.Provider`
    :param module: The module which contains the driver

    :type module: L
    :param klass: The driver class name

    :type klass:
    """

    if provider in drivers:
        raise AttributeError("Provider %s already registered" % (provider))

    drivers[provider] = (module, klass)

    # Check if this driver is valid
    try:
        driver = get_driver(drivers, provider)
    except (ImportError, AttributeError) as exp:
        drivers.pop(provider)
        raise exp

    return driver
