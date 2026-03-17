#
# Copyright (c), 2024-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
import io
from xml.sax import SAXParseException
from xml.sax import expatreader  # type: ignore[attr-defined, unused-ignore]
from xml.dom import pulldom
from pyexpat import XMLParserType

from xmlschema.aliases import IOType
from xmlschema.exceptions import XMLSchemaTypeError, XMLSchemaValueError, \
    XMLResourceError, XMLResourceForbidden, XMLResourceOSError
from xmlschema.utils.streams import DefusableReader


class SafeExpatParser(expatreader.ExpatParser):  # type: ignore[misc, unused-ignore]
    _parser: XMLParserType

    def forbid_entity_declaration(self, name, is_parameter_entity,  # type: ignore
                                  value, base, sysid, pubid, notation_name):
        raise XMLResourceForbidden(f"Entities are forbidden (entity_name={name!r})")

    def forbid_unparsed_entity_declaration(self, name, base,  # type: ignore
                                           sysid, pubid, notation_name):
        raise XMLResourceForbidden(f"Unparsed entities are forbidden (entity_name={name!r})")

    def forbid_external_entity_reference(self, context, base, sysid, pubid):  # type: ignore
        raise XMLResourceForbidden(
            f"External references are forbidden (system_id={sysid!r}, public_id={pubid!r})"
        )  # pragma: no cover

    def reset(self) -> None:
        super().reset()
        self._parser.EntityDeclHandler = self.forbid_entity_declaration
        self._parser.UnparsedEntityDeclHandler = self.forbid_unparsed_entity_declaration
        self._parser.ExternalEntityRefHandler = self.forbid_external_entity_reference


def defuse_xml(fp: IOType, rewind: bool = True) -> IOType:
    """
    Defuses an XML source using a file-like object. For default the file-like object
    must be seekable because the file-like object is rewound to start position after
    the check. If it's not seekable, the file-like object is wrapped in a buffered
    reader if it's a `io.RawIOBase` or a `io.BufferedIOBase` object.

    :param fp: the file-like object to defuse.
    :param rewind: if `True` the file-like object is rewound after defusing.
    :return: the file-like object or its wrapper buffered reader.
    """
    if rewind and not fp.seekable():
        if isinstance(fp, io.RawIOBase):
            # Wrap a not seekable raw IO object in a BufferedReader
            fp = io.BufferedReader(fp)
        elif isinstance(fp, io.BufferedIOBase):
            # Other not seekable BufferedIOBase resources are wrapped in
            # a custom reader with an initial buffer of 64KiB bytes.
            try:
                fp = DefusableReader(fp)
            except (OSError, TypeError, ValueError) as err:
                if isinstance(err, OSError):
                    raise XMLResourceOSError(err)
                elif isinstance(err, TypeError):
                    raise XMLSchemaTypeError(err)
                else:
                    raise XMLSchemaValueError(err)
        else:
            msg = f"can't defuse {fp!r}: it can't be rewound after the check"
            raise XMLResourceError(msg)

    parser = SafeExpatParser()
    try:
        for event, node in pulldom.parse(fp, parser):
            if event == pulldom.START_ELEMENT:
                break
    except SAXParseException:
        pass  # the purpose is to defuse not to check xml source syntax
    except OSError as err:
        raise XMLResourceOSError(err)

    if rewind:
        try:
            fp.seek(0)
        except OSError as err:
            raise XMLResourceOSError(err)

    return fp
