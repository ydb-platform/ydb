#
# This file is part of pysmi software.
#
# Copyright (c) 2015-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysmi/license.html
#
from pysmi.parser.dialect import smi_v1_relaxed
from pysmi.parser.smi import parserFactory

# compatibility stub
SmiV1CompatParser = parserFactory(**smi_v1_relaxed)
SmiStarParser = SmiV1CompatParser
