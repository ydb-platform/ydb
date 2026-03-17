'''
Created on Feb 23, 2014

@author: pupssman
'''

import warnings

warnings.warn('"allure.adaptor" is deprecated, use "allure.pytest_plugin" instead.')

# impersonate the old ``adaptor``
from allure.pytest_plugin import *  # @UnusedWildImport  # noqa
