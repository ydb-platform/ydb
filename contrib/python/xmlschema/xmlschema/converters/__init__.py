#
# Copyright (c), 2016-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
from typing import Any, Union, cast


from xmlschema.exceptions import XMLSchemaTypeError
from xmlschema.translation import gettext as _
from xmlschema.arguments import Option
from xmlschema.utils.misc import is_subclass

from .base import ElementData, XMLSchemaConverter
from .unordered import UnorderedConverter
from .parker import ParkerConverter
from .badgerfish import BadgerFishConverter
from .gdata import GDataConverter
from .abdera import AbderaConverter
from .jsonml import JsonMLConverter
from .columnar import ColumnarConverter

__all__ = ['XMLSchemaConverter', 'UnorderedConverter', 'ParkerConverter',
           'BadgerFishConverter', 'AbderaConverter', 'JsonMLConverter',
           'ColumnarConverter', 'ElementData', 'GDataConverter',
           'ConverterType', 'ConverterOption']


ConverterType = Union[type[XMLSchemaConverter], XMLSchemaConverter]


class ConverterOption(Option[ConverterType | None]):
    def validated_value(self, value: Any) -> ConverterType | None:
        if value is None or isinstance(value, XMLSchemaConverter) \
                or is_subclass(value, XMLSchemaConverter):
            return cast(ConverterType, value)
        msg = _("invalid type {!r} for {}, must be a {!r} instance/subclass or None")
        raise XMLSchemaTypeError(msg.format(type(value), self, XMLSchemaConverter))
