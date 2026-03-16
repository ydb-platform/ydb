from . import base
from .http import base as http_driver

base.dialect = http_driver.dialect
