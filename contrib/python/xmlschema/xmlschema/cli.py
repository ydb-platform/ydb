#
# Copyright (c), 2016-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
# mypy: ignore-errors
"""Command Line Interface"""
import sys
import os
import argparse
import logging
import pathlib
from urllib.error import URLError

import xmlschema
from xmlschema import XMLSchema, XMLSchema11, iter_errors, to_json, from_json, etree_tostring
from xmlschema.exceptions import XMLSchemaValueError


PROGRAM_NAME = os.path.basename(sys.argv[0])

CONVERTERS_MAP = {
    'unordered': xmlschema.UnorderedConverter,
    'parker': xmlschema.ParkerConverter,
    'badgerfish': xmlschema.BadgerFishConverter,
    'gdata': xmlschema.GDataConverter,
    'abdera': xmlschema.AbderaConverter,
    'jsonml': xmlschema.JsonMLConverter,
    'columnar': xmlschema.ColumnarConverter,
}


def xsd_version_number(value):
    if value not in ('1.0', '1.1'):
        raise argparse.ArgumentTypeError("%r is not a valid XSD version" % value)
    return value


def defuse_data(value):
    if value not in ('always', 'remote', 'never'):
        raise argparse.ArgumentTypeError("%r is not a valid value" % value)
    return value


def get_loglevel(verbosity):
    if verbosity <= 0:
        return logging.ERROR
    elif verbosity == 1:
        return logging.WARNING
    elif verbosity == 2:
        return logging.INFO
    else:
        return logging.DEBUG


def get_converter(name):
    if not isinstance(name, str):
        return None

    try:
        return CONVERTERS_MAP[name.lower()]
    except KeyError:
        raise ValueError(f"--converter must be in {tuple(CONVERTERS_MAP)!r}")


def xml2json():
    parser = argparse.ArgumentParser(prog=PROGRAM_NAME, add_help=True,
                                     description="decode a set of XML files to JSON.")
    parser.usage = "%(prog)s [OPTION]... [FILE]...\n" \
                   "Try '%(prog)s --help' for more information."

    parser.add_argument('-v', dest='verbosity', action='count', default=0,
                        help="increase output verbosity.")
    parser.add_argument('--schema', type=str, metavar='PATH',
                        help="path or URL to an XSD schema.")
    parser.add_argument('--version', type=xsd_version_number, default='1.0',
                        help="XSD schema validator to use (default is 1.0).")
    parser.add_argument('-L', dest='locations', nargs=2, type=str, action='append',
                        metavar="URI/URL", help="schema location hint overrides.")
    parser.add_argument('--converter', type=str, metavar='NAME',
                        help="use a different XML to JSON convention instead of "
                             "the default converter. Option value can be one of "
                             "{!r}.".format(tuple(CONVERTERS_MAP)))
    parser.add_argument('--indent', type=int, default=None,
                        help="indentation for a pretty-printed JSON output "
                             "(default is the most compact representation)")
    parser.add_argument('--lazy', action='store_true', default=False,
                        help="use lazy decoding mode (slower but use less memory).")
    parser.add_argument('--defuse', metavar='(always, remote, never)',
                        type=defuse_data, default='remote',
                        help="when to defuse XML data, on remote resources for default.")
    parser.add_argument('-o', '--output', type=str, default='.',
                        help="where to write the encoded XML files, current dir by default.")
    parser.add_argument('-f', '--force', action="store_true", default=False,
                        help="do not prompt before overwriting.")
    parser.add_argument('files', metavar='[XML_FILE ...]', nargs='+',
                        help="XML files to be decoded to JSON.")

    args = parser.parse_args()

    loglevel = get_loglevel(args.verbosity)
    schema_class = XMLSchema if args.version == '1.0' else XMLSchema11
    converter = get_converter(args.converter)
    if args.schema is not None:
        schema = schema_class(args.schema, locations=args.locations, loglevel=loglevel)
    else:
        schema = None

    json_options = {}
    if args.indent is not None and args.indent >= 0:
        json_options['indent'] = args.indent

    base_path = pathlib.Path(args.output)
    if not base_path.exists():
        base_path.mkdir()
    elif not base_path.is_dir():
        raise XMLSchemaValueError(f"{str(base_path)!r} is not a directory")

    tot_errors = 0
    for xml_path in map(pathlib.Path, args.files):
        json_path = base_path.joinpath(xml_path.name).with_suffix('.json')
        if json_path.exists() and not args.force:
            print(f"skip {str(json_path)}: the destination file exists!")
            continue

        with open(str(json_path), 'w') as fp:
            try:
                errors = to_json(
                    xml_document=str(xml_path),
                    fp=fp,
                    schema=schema,
                    cls=schema_class,
                    converter=converter,
                    lazy=args.lazy,
                    defuse=args.defuse,
                    validation='lax',
                    json_options=json_options,
                )
            except (xmlschema.XMLSchemaException, URLError) as err:
                tot_errors += 1
                print(f"error with {str(xml_path)}: {str(err)}")
                continue
            else:
                if not errors:
                    print(f"{str(xml_path)} converted to {str(json_path)}")
                else:
                    tot_errors += len(errors)
                    print("{} converted to {} with {} errors".format(
                        str(xml_path), str(json_path), len(errors)
                    ))

    sys.exit(tot_errors)


def json2xml():
    parser = argparse.ArgumentParser(prog=PROGRAM_NAME, add_help=True,
                                     description="encode a set of JSON files to XML.")
    parser.usage = "%(prog)s [OPTION]... [FILE]...\n" \
                   "Try '%(prog)s --help' for more information."

    parser.add_argument('-v', dest='verbosity', action='count', default=0,
                        help="increase output verbosity.")
    parser.add_argument('--schema', type=str, metavar='PATH',
                        help="path or URL to an XSD schema.")
    parser.add_argument('--version', type=xsd_version_number, default='1.0',
                        help="XSD schema validator to use (default is 1.0).")
    parser.add_argument('-L', dest='locations', nargs=2, type=str, action='append',
                        metavar="URI/URL", help="schema location hint overrides.")
    parser.add_argument('--converter', type=str, metavar='NAME',
                        help="use a different XML to JSON convention instead of "
                             "the default converter. Option value can be one of "
                             "{!r}.".format(tuple(CONVERTERS_MAP)))
    parser.add_argument('--indent', type=int, default=4,
                        help="indentation for XML output (default is 4 spaces)")
    parser.add_argument('-o', '--output', type=str, default='.',
                        help="where to write the encoded XML files, current dir by default.")
    parser.add_argument('-f', '--force', action="store_true", default=False,
                        help="do not prompt before overwriting")
    parser.add_argument('files', metavar='[JSON_FILE ...]', nargs='+',
                        help="JSON files to be encoded to XML.")

    args = parser.parse_args()

    loglevel = get_loglevel(args.verbosity)
    schema_class = XMLSchema if args.version == '1.0' else XMLSchema11
    converter = get_converter(args.converter)
    schema = schema_class(args.schema, locations=args.locations, loglevel=loglevel)

    base_path = pathlib.Path(args.output)
    if not base_path.exists():
        base_path.mkdir()
    elif not base_path.is_dir():
        raise XMLSchemaValueError(f"{str(base_path)!r} is not a directory")

    tot_errors = 0
    for json_path in map(pathlib.Path, args.files):
        xml_path = base_path.joinpath(json_path.name).with_suffix('.xml')
        if xml_path.exists() and not args.force:
            print(f"skip {str(xml_path)}: the destination file exists!")
            continue

        with open(str(json_path)) as fp:
            try:
                root, errors = from_json(
                    source=fp,
                    schema=schema,
                    converter=converter,
                    validation='lax',
                    indent=args.indent,
                )
            except (xmlschema.XMLSchemaException, URLError) as err:
                tot_errors += 1
                print(f"error with {str(xml_path)}: {str(err)}")
                continue
            else:
                if not errors:
                    print(f"{str(json_path)} converted to {str(xml_path)}")
                else:
                    tot_errors += len(errors)
                    print("{} converted to {} with {} errors".format(
                        str(json_path), str(xml_path), len(errors)
                    ))

        with open(str(xml_path), 'w') as fp:
            fp.write(etree_tostring(root))

    sys.exit(tot_errors)


def validate():
    parser = argparse.ArgumentParser(prog=PROGRAM_NAME, add_help=True,
                                     description="validate a set of XML files.")
    parser.usage = "%(prog)s [OPTION]... [FILE]...\n" \
                   "Try '%(prog)s --help' for more information."
    parser.add_argument('-v', dest='verbosity', action='count', default=0,
                        help="increase output verbosity.")
    parser.add_argument('--schema', type=str, metavar='PATH',
                        help="path or URL to an XSD schema.")
    parser.add_argument('--version', type=xsd_version_number, default='1.0',
                        help="XSD schema validator to use (default is 1.0).")
    parser.add_argument('-L', dest='locations', nargs=2, type=str, action='append',
                        metavar="URI/URL", help="schema location hint overrides.")
    parser.add_argument('--lazy', action='store_true', default=False,
                        help="use lazy validation mode (slower but use less memory).")
    parser.add_argument('--defuse', metavar='(always, remote, never)',
                        type=defuse_data, default='remote',
                        help="when to defuse XML data, on remote resources for default.")
    parser.add_argument('files', metavar='[XML_FILE ...]', nargs='+',
                        help="XML files to be validated.")

    args = parser.parse_args()

    schema_class = XMLSchema if args.version == '1.0' else XMLSchema11

    tot_errors = 0
    for filepath in args.files:
        try:
            errors = list(iter_errors(filepath, schema=args.schema, cls=schema_class,
                                      locations=args.locations, lazy=args.lazy, defuse=args.defuse))
        except (xmlschema.XMLSchemaException, URLError) as err:
            tot_errors += 1
            sys.stderr.write(f"{err}\n")
            continue
        else:
            if not errors:
                sys.stdout.write(f"{filepath} is valid\n")
            else:
                tot_errors += len(errors)
                sys.stderr.write(f"{filepath} is not valid\n")
                if args.verbosity > 0:
                    for error in errors:
                        sys.stderr.write(f"{error}\n")

    sys.exit(tot_errors)
