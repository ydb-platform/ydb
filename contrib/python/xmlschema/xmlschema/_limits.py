#
# Copyright (c), 2016-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
"""
Protected package limits, values are managed by xmlschema.limits.LimitsModule.
A specular protected module is used for performance penalties of the managed module, e.g.:

>>> import timeit
>>> timeit.timeit("limits.MAX_XML_DEPTH", "from xmlschema import limits")
0.019063591957092285
>>> timeit.timeit("_limits.MAX_XML_DEPTH", "from xmlschema import _limits")
0.01225003704894334

"""
MAX_MODEL_DEPTH = 15
MAX_SCHEMA_SOURCES = 1000
MAX_XML_DEPTH = 1000
MAX_XML_ELEMENTS = 10 ** 6
