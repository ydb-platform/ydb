""" Python methods for importing and exporting '.proto' files from the BBP type
definition format.
"""

# Copyright (c) 2018-2024 NCC Group Plc
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import io
import re
import logging
import re
import six

from blackboxprotobuf.lib.exceptions import TypedefException, ProtofileException
import blackboxprotobuf.lib.api

if six.PY3:
    import typing

    if typing.TYPE_CHECKING:
        from blackboxprotobuf.lib.config import Config
        from typing import Any, TextIO, Tuple, Dict, Optional, List
        from blackboxprotobuf.lib.pytypes import TypeDefDict, FieldDefDict

logger = logging.getLogger(__name__)

PROTO_FILE_TYPE_MAP = {
    "uint": "uint64",
    "int": "int64",
    "sint": "sint64",
    "fixed32": "fixed32",
    "sfixed32": "sfixed32",
    "float": "float",
    "fixed64": "fixed64",
    "sfixed64": "sfixed64",
    "double": "double",
    "bytes": "bytes",
    "bytes_hex": "bytes",
    "string": "string",
}

PACKABLE_TYPES = [
    "uint",
    "int",
    "sint",
    "fixed32",
    "sfixed32",
    "float",
    "fixed64",
    "sfixed64",
    "double",
]

# Inverse of the above, but we have to include more types
PROTO_FILE_TYPE_TO_BBP = {
    "double": "double",
    "float": "float",
    "int32": "int",
    "int64": "int",
    "uint32": "uint",
    "uint64": "uint",
    "sint32": "sint",
    "sint64": "sint",
    "fixed32": "fixed32",
    "fixed64": "fixed64",
    "sfixed32": "sfixed32",
    "sfixed64": "sfixed64",
    "bool": "uint",
    "string": "string",
    # should be default_binary_type, but can't handle that well here
    "bytes": "bytes",
}

NAME_REGEX = re.compile(r"\A[a-zA-Z_][a-zA-Z0-9_]*\Z")

# add packed types to the list
for packable_type in PACKABLE_TYPES:
    packed_type = "packed_" + packable_type
    PROTO_FILE_TYPE_MAP[packed_type] = PROTO_FILE_TYPE_MAP[packable_type]


def _print_message(message_name, typedef, output_file, depth=0):
    # type: (str, TypeDefDict, TextIO, int) -> None
    indent = six.u("  ") * depth
    if not NAME_REGEX.match(message_name):
        raise TypedefException("Message name: %s is not valid" % message_name)

    # sort typedef for better looking output
    typedef = blackboxprotobuf.lib.api.sort_typedef(typedef)

    message_name = message_name.strip()
    output_file.write(six.u("\n"))
    output_file.write(indent)
    output_file.write(six.u("message %s {\n") % message_name)
    for field_number, field_typedef in typedef.items():
        proto_type = None
        field_name = None
        field_options = ""

        # a repeated field with one element is indistinduishable from a
        # repeated field so we just put repeated if we have proof that it is
        # repeatable, but this might be wrong sometimes
        # maybe some sort of protobuf discovery tool can detect this
        is_repeated = field_typedef.get("seen_repeated", False)

        if "name" in field_typedef and field_typedef["name"] != "":
            field_name = field_typedef["name"]
            field_name = field_name.strip()
            if not NAME_REGEX.match(field_name):
                field_name = None
        if field_name is None:
            field_name = six.ensure_text("field%s" % field_number)

        if field_typedef["type"] == "message":
            # If we have multiple typedefs, this means is something like the Any
            # message, and has to be manually reparsed to each type
            if "alt_typedefs" in field_typedef:
                proto_type = "bytes"
            else:
                proto_type = field_name + "_type"
                _print_message(
                    proto_type, field_typedef["message_typedef"], output_file, depth + 1
                )
        else:
            if field_typedef["type"] not in PROTO_FILE_TYPE_MAP:
                raise TypedefException(
                    "Type %s does not have a mapping to protobuf types."
                    % field_typedef["type"]
                )
            proto_type = PROTO_FILE_TYPE_MAP[field_typedef["type"]]

        # we're using proto3 syntax. Repeated numeric fields are packed by default
        # if it's repeated and not packed, then make sure we specify it's not packed
        if is_repeated and field_typedef["type"] in PACKABLE_TYPES:
            field_options = " [packed=false]"
        # if it's a packed type, we'll explicitoly set that too, can't hurt
        elif field_typedef["type"].startswith("packed_"):
            field_options = " [packed=true]"
            is_repeated = True

        output_file.write(indent)
        output_file.write(
            six.u("  %s%s %s = %s%s;\n")
            % (
                "repeated " if is_repeated else "",
                proto_type,
                field_name,
                field_number,
                field_options,
            )
        )

    output_file.write(indent)
    output_file.write(six.u("}\n\n"))


def export_proto(typedef_map, output_filename=None, output_file=None, package=None):
    # type: (Dict[str, TypeDefDict], Optional[str], Optional[TextIO], Optional[str]) -> str | None
    """Export the given type definitons as a '.proto' file. Typedefs are
    expected as a dictionary of {'message_name': typedef }

    Write to output_file or output_filename if provided, otherwise return a string
    output_filename will be overwritten if it exists
    """
    if output_filename is not None:
        output_file = io.open(output_filename, "w+")

    if output_file is None:
        output_file = io.StringIO()

    # preamble
    output_file.write(six.u('syntax = "proto3";\n\n'))
    if package:
        output_file.write(six.u("package %s;\n\n") % package)

    for typedef_name, typedef in typedef_map.items():
        _print_message(typedef_name, typedef, output_file)

    if isinstance(output_file, io.StringIO):
        return output_file.getvalue()
    # close the file if we opened it
    elif output_filename is not None:
        output_file.close()
    return None


MESSAGE_START_REGEX = re.compile(r"^message +([a-zA-Z_0-9]+) *{.*")
FIELD_REGEX = re.compile(
    r"^ *(repeated|optional|required)? *([a-zA-Z0-9_]+) +([a-zA-Z0-9_]+) += +([0-9]+) *(\[[a-z]+=[a-z]*\])?.*;.*$"
)
SYNTAX_REGEX = re.compile(r'^ *syntax += +"(proto\d)" *;.*')
ENUM_REGEX = re.compile(r"^ *enum +([a-zA-Z0-9_]+) *{.*")
PACKAGE_REGEX = re.compile(r"^ *package +([a-zA-Z0-9_.]+) *;.*")


def import_proto(config, input_string=None, input_filename=None, input_file=None):
    # type: (Config, Optional[str], Optional[str], Optional[TextIO]) -> Dict[str, TypeDefDict]
    typedef_map = {}  # type: Dict[str, TypeDefDict]
    if input_string is not None:
        input_file = io.StringIO(input_string)
    if input_file is None and input_filename is not None:
        input_file = io.open(input_filename, "r")
    if input_file is None:
        raise ProtofileException(
            "No file provided to import_proto", filename=input_filename
        )

    syntax_version = "proto2"
    package_prefix = ""
    enum_names = []
    message_trees = []
    message_names = []

    line = input_file.readline()
    while line:
        line = line.strip()

        if line.startswith("syntax"):
            syntax_match = SYNTAX_REGEX.match(line)
            if syntax_match:
                syntax_version = syntax_match.group(1)

        elif line.startswith("package"):
            package_match = PACKAGE_REGEX.match(line)
            if package_match:
                package_prefix = package_match.group(1) + "."

        elif line.startswith("import"):
            logger.warning(
                "Proto file has import which is not supported "
                "by the parser. Ensure the imported files are "
                "processed first: %s",
                line,
            )
        elif line.startswith("enum"):
            enum_match = ENUM_REGEX.match(line)
            if enum_match:
                enum_name = _parse_enum(enum_match, line, input_file)
                enum_names.append(enum_name)

        elif line.startswith("message"):
            message_start_match = MESSAGE_START_REGEX.match(line)
            if message_start_match:
                message_tree = _preparse_message(message_start_match, line, input_file)
                message_trees.append(message_tree)

        line = input_file.readline()

    for tree in message_trees:
        new_message_names, new_enum_names = _collect_names(package_prefix, tree)
        enum_names += new_enum_names
        message_names += new_message_names

    logger.debug("Got the following enum_names: %s", enum_names)
    logger.debug("Got the following message_names: %s", message_names)

    for tree in message_trees:
        _parse_message(
            tree,
            typedef_map,
            message_names,
            enum_names,
            package_prefix,
            syntax_version == "proto3",
            config,
        )

    return typedef_map


def _parse_enum(enum_match, line, input_file):
    # type: (re.Match[str], str, TextIO) -> str
    """Parse an enum out of the file. Goes from enum declaration to next }
    Returns the enum's name
    """
    enum_name = enum_match.group(1)
    # parse until the next '}'
    while "}" not in line:
        line = input_file.readline()
        if not line:
            raise ProtofileException("Did not find close of enum")
    return enum_name


def _preparse_message(message_start_match, line, input_file):
    # type: (re.Match[str], str, TextIO) -> Dict[str, Any]
    # TODO Should put together better types than Any, but we'll stick with this
    # for now
    """Parse out a message name and the lines that make it up"""
    message_name = message_start_match.group(1)
    message_lines = []
    inner_enums = []
    inner_messages = []

    while "}" not in line:
        line = input_file.readline()
        if not line:
            raise ProtofileException("Did not find close of message")

        line = line.strip()
        if line.startswith("enum"):
            enum_match = ENUM_REGEX.match(line)
            if enum_match:
                enum_name = _parse_enum(enum_match, line, input_file)
                inner_enums.append(enum_name)

        elif line.startswith("message"):
            inner_message_start_match = MESSAGE_START_REGEX.match(line)
            if inner_message_start_match:
                message_tree = _preparse_message(
                    inner_message_start_match, line, input_file
                )
                inner_messages.append(message_tree)
        # not an inner enum or message
        else:
            message_lines.append(line)

    return {
        "name": message_name,
        "data": message_lines,
        "enums": inner_enums,
        "inner_messages": inner_messages,
    }


def _collect_names(prefix, message_tree):
    # type: (str, Dict[str, Any]) -> Tuple[List[str], List[str]]
    message_names = []
    enum_names = []

    name = prefix + message_tree["name"]
    message_names.append(name)
    for enum_name in message_tree["enums"]:
        enum_names.append(prefix + enum_name)
    for inner_message in message_tree["inner_messages"]:
        new_message_names, new_enum_names = _collect_names(name + ".", inner_message)
        message_names += new_message_names
        enum_names += new_enum_names
    return message_names, enum_names


def _check_message_name(current_path, name, known_message_names, config):
    # type: (str, str, List[str], Config) -> str | None
    # Verify message name against preparsed message names and global
    # known_messages
    # For example, if we have:
    #   Message.InnerMesage
    # referenced from:
    #    PackageA.Message2
    # we would look up:
    # PackageA.Message2.Message.InnerMessage
    # PackageA.Message.InnerMessage
    # should also work for enums
    if name in config.known_types:
        return name
    # search for anything under a common prefix in known_message_names
    logger.debug("Testing message name: %s", name)

    prefix_options = [""]
    for part in current_path.split("."):
        if part:
            prefix_options = [prefix_options[0] + part + "."] + prefix_options

    logger.debug("prefix_options: %s", prefix_options)
    for prefix in prefix_options:
        logger.debug("Testing message name: %s", prefix + name)
        if prefix + name in known_message_names:
            return prefix + name
        # remove the last bit of the prefix
        if "." not in prefix:
            break
        prefix = ".".join(prefix.split(".")[:-1])
    logger.debug(
        "Message %s not found from %s Known names are: %s",
        name,
        current_path,
        known_message_names,
    )
    return None


def _parse_message(
    message_tree, typdef_map, known_message_names, enum_names, prefix, is_proto3, config
):
    # type: (Dict[str, Any], Dict[str, TypeDefDict], List[str], List[str], str, bool, Config) -> None
    message_typedef = {}
    message_name = prefix + message_tree["name"]
    prefix = message_name + "."
    # parse the actual message fields
    for line in message_tree["data"]:
        # lines should already be stripped and should not have messages or enums
        assert all([not line.strip().startswith(x) for x in ["message ", "enum "]])
        # Check if the line matches the field regex
        match = FIELD_REGEX.match(line)
        if match:
            field_number, field_typedef = _parse_field(
                match, known_message_names, enum_names, prefix, is_proto3, config
            )
            message_typedef[field_number] = field_typedef

    # add the messsage to tyep returned typedefs
    logger.debug("Adding message %s to typedef maps", message_name)
    typdef_map[message_name] = message_typedef

    for inner_message in message_tree["inner_messages"]:
        _parse_message(
            inner_message,
            typdef_map,
            known_message_names,
            enum_names,
            prefix,
            is_proto3,
            config,
        )


# parse a field into a dictionary for the typedef
def _parse_field(match, known_message_names, enum_names, prefix, is_proto3, config):
    # type: (re.Match[str], List[str], List[str], str, bool, Config) -> Tuple[str, FieldDefDict]
    typedef = {}  # type: FieldDefDict

    field_name = match.group(3)
    if not field_name:
        raise ProtofileException("Could not parse field name from line: %s" % match)
    typedef["name"] = six.ensure_text(field_name)
    field_number = match.group(4)
    if not field_number:
        raise ProtofileException("Could not parse field number from line: %s" % match)

    # figure out repeated
    field_rule = match.group(1)
    is_repeated = False
    if field_rule and "repeated" in field_rule:
        is_repeated = True
        typedef["seen_repeated"] = True

    field_type = match.group(2)
    if not field_type:
        raise ProtofileException("Could not parse field type from line: %s" % match)
    # check normal types
    bbp_type = PROTO_FILE_TYPE_TO_BBP.get(field_type, None)
    if not bbp_type:
        logger.debug("Got non-basic type: %s, checking enums", field_type)
        # check enum names
        if _check_message_name(prefix, field_type, enum_names, config):
            # enum = uint
            bbp_type = "uint"
    if not bbp_type:
        # Not enum or normal type, check messages
        message_name = _check_message_name(
            prefix, field_type, known_message_names, config
        )
        if message_name:
            bbp_type = "message"
            typedef["message_type_name"] = message_name

    if not bbp_type:
        # If we don't have a type now, then fail
        raise ProtofileException(
            "Could not get a type for field %s: %s" % (field_name, field_type)
        )

    # figure out packed
    # default based on repeated + proto3, fallback to options
    field_options = match.group(5)
    is_packed = is_repeated and is_proto3 and (bbp_type in PACKABLE_TYPES)
    if is_packed and field_options and "packed=false" in field_options:
        is_packed = False
    elif is_repeated and field_options and "packed=true" in field_options:
        is_packed = True

    # make sure the type lines up with packable
    if is_packed and bbp_type not in PACKABLE_TYPES:
        raise ProtofileException(
            "Field %s set as packable, but not a packable type: %s"
            % (field_name, bbp_type)
        )
    if is_packed:
        bbp_type = "packed_" + bbp_type

    typedef["type"] = bbp_type

    logger.debug("Parsed field number %s: %s", field_number, typedef)
    return field_number, typedef
