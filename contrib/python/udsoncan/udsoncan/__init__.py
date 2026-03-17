import logging
import logging.config
from os import path

from udsoncan.exceptions import *
from udsoncan.Response import Response
from udsoncan.Request import Request

from udsoncan.common.AddressAndLengthFormatIdentifier import *
from udsoncan.common.Baudrate import *
from udsoncan.common.CommunicationType import *
from udsoncan.common.DataFormatIdentifier import *
from udsoncan.common.dids import *
from udsoncan.common.DidCodec import *
from udsoncan.common.dtc import *
from udsoncan.common.DynamicDidDefinition import *
from udsoncan.common.Filesize import *
from udsoncan.common.IOControls import *
from udsoncan.common.MemoryLocation import *
from udsoncan.common.Routine import *
from udsoncan.common.Units import *
from udsoncan.typing import *

__version__ = '1.25.2'
__license__ = 'MIT'
__author__ = 'Pier-Yves Lessard'

latest_standard = 2020
valid_standards = [2006,2013,2020]

__default_log_config_file = path.join(path.dirname(path.abspath(__file__)), 'logging.conf')


def setup_logging(config_file:str=__default_log_config_file):
    """
    This function setup the logger accordingly to the module provided cfg file
    """
    try:
        logging.config.fileConfig(config_file)
    except Exception as e:
        logging.warning('Cannot load logging configuration from %s. %s:%s' % (config_file, e.__class__.__name__, str(e)))
