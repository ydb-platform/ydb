#
# This file is part of pysmi software.
#
# Copyright (c) 2015-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysmi/license.html
#
from pysmi.parser.dialect import smi_v2
from pysmi.parser.smi import parserFactory

# compatibility stub
SmiV2Parser = parserFactory(**smi_v2)
