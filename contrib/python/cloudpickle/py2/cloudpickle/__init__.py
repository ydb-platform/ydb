from __future__ import absolute_import

import sys
import pickle


from cloudpickle.cloudpickle import *
if sys.version_info[:2] >= (3, 8):
    from cloudpickle.cloudpickle_fast import CloudPickler, dumps, dump

__version__ = '1.3.0'
