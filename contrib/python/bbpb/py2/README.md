# BlackBox Protobuf Library

## Description
Blackbox protobuf library is a Python module for decoding and re-encoding protobuf
messages without access to the source protobuf descriptor file. This library
provides a simple Python interface to encode/decode messages that can be
integrated into other tools.

This library is targeted towards use in penetration testing where being able to
modify messages is critical and a protocol buffer definition may not be readily
available.

## Background
Protocol Buffers (protobufs)  are a standard published by Google with
accompanying libraries for binary serialization of data. Protocol buffers are
defined by a `.proto` file known to both the sender and the receiver. The actual
binary message does not contain information such as field names or most type
information.

For each field, the serialized protocol buffer includes two pieces of metadata,
a field number and the wire type. The wire type tells a parser how to parse the
length of the field, so that it can be skipped if it is not known (one protocol
buffer design goal is being able to handle messages with unknown fields). A
single wire-type generally encompasses multiple protocol buffer types, for
example the length delimited wire-type can be used for string, bytestring,
inner message or packed repeated fields. See
<https://developers.google.com/protocol-buffers/docs/encoding#structure> for
the breakdown of wire types.

The protocol buffer compiler (`protoc`) does support a similar method of
decoding protocol buffers without the definition with the `--decode_raw`
option. However, it does not provide any functionality to re-encode the decoded
message.

## How it works
The library makes a best effort guess of the type based on the provided wire type (and
occasionally field content) and builds a type definition that can be used to
re-encode the data. In general, most fields of interest are likely to be parsed
into a usable form. Users can optionally pass in custom type definitions that
override the guessed type. Custom type definitions also allow naming of fields to
improve user friendliness.

# Usage
## Installation    
The package can be installed from source with:

```
poetry install
```

BlackBox Protobuf is also available on PyPi at <https://pypi.org/project/bbpb>.
It can be installed with:

```
pip install bbpb
```

## CLI
The package defines a `bbpb` command for command line encoding/decoding.

For command line usage see [CLI.md](./CLI.md).

## Interface
The main `blackboxprotobuf` module defines an API with the core encode/decode
message functions, along with several convenience functions to make it easier
to use blackboxprotobuf with a user interface, such as encoding/decoding
directly to JSON and validating modified type definitions.

### Decode 
Decoding functions takes a protobuf bytestring, and optionally
either a type definition or a known message name mapped to a type definition
(in `blackboxprotobuf.known_messages`). If a type definition isn't provided, an
empty message type is assumed and all types are derived from the protobuf
binary.

The decoder returns a tuple containing a dictionary with the decoded data and a
dictionary containing the generated type definition. If the input type
definition does not include types for all fields in the message, the output
type definitions will include type guesses for those fields.

Example use:
```python
import blackboxprotobuf
import base64

data = base64.b64decode('KglNb2RpZnkgTWU=')
message,typedef = blackboxprotobuf.protobuf_to_json(data)
print(message)
```

### Encode
The encoding functions takes a Python dictionary containing the data and a type
definition. Unlike decoding, the type definition is required and will fail if
any fields are not defined. Generally, the type definition should be the output
from the decoding function or a modified version thereof.

Example use:
```python
import blackboxprotobuf
import base64

data = base64.b64decode('KglNb2RpZnkgTWU=')
message,typedef = blackboxprotobuf.decode_message(data)

message[5] = 'Modified Me'

data = blackboxprotobuf.encode_message(message,typedef)
print(data)
```

### Type definition structure
The type definition object is a Python dictionary representing the type
structure of a message, it includes a type for each field and optionally a
name. Each entry in the dictionary represents a field in the message. The key
should be the field number and the value is a dictionary containing attributes.

At the minimum the dictionary should contain the 'type' entry which contains a
string identifier for the type. Valid type identifiers can be found in
`blackboxprotobuf/lib/types/type_maps.py`.

Message fields will also contain one of two entries, 'message_typedef' or
'message_type_name'. 'message_typedef' should contain a second type definition
structure for the inner message. 'message_type_name' should contain the string
identifier for a message type previously stored in
`blackboxprotobuf.known_messages`. If both are specified, the 'message_type_name'
will be ignored.

### JSON Encode/Decode

The `protobuf_to_json` and `protobuf_from_json` functions are convenience
functions for  encoding/decoding messages to JSON instead of a python
dictionary. These functions are designed for user-facing input/output and will
also automatically sort the output, try to encode bytestrings for better
printing and annotate example values onto the type definition structure.

### Export/import protofile

The `export_protofile` and `import_protofile` will attempt to convert a
protobuffer `.proto` file into the blackboxprotobuf type definition and vice
versa. These functions provide a higher level interface to
`blackboxprotobuf.lib.protofile` which only takes a filename. The protofile
functions do not implement a full proper parser and may break on some types.
One common case to be aware of is the "import" statements in ".proto" files,
which are not supported. Any imported files must be manually imported with
`import_protofile` and saved in `blackboxprotobuf.known_messages` first.


### Validate Typedef

The `validate_typedef` function is designed to sanity check modified type
definitions and make sure they are internally consistent and consistent with
the previous type definition (if provided). This should help catch issues such
as changing a field to an incompatible type or duplicate field names.

### Output Helper Functions

The `json_safe_transform` is a helper function to help create more readable
JSON output of bytes. It will encode/decode bytes types as `latin1` based on
the type in the type definition.

The `sort_output` is a helper function which sorts the output message based on
the field numbers from the typedef. This helps makes the JSON output more
consistent and predictable.

The `sort_typedef` function sorts the fields of the typedef in order to make
the output more readable. The message fields are sorted by their number and
type fields (eg. name, type, inner message typedef) are sorted to prioritize
important short fields at the top and especially to keep the name and type
fields from getting buried underneath a long inner typedef.

### Config

Many of the functions accept a `config` keyword argument of the
`blackboxprotobuf.lib.config.Config` class. The config object allows modifying
some of the encoding/decoding functionality and storing some state. This
replaces some variables that were global before.

At the moment this includes:

* `known_types` - Mapping of message type names to typedef (previously
  `blackboxprotobuf.known_messages`)

* `default_binary_type` - Change the default type choice for binary fields when
  decoding previously unknown fields. Defaults to `bytes` but can be set to
  `bytes_hex` to return a hex encoded string instead. `bytes_base64` might be
  another option in the future. The type can always be changed for an
  individual field by changing the `type` in the typedef.

* `default_types` - Change the default type choice for any wiretype when
  decoding a previously unknown field. For example, to default to unsigned
  integers for all varints, set `default_types[WIRETYPE_VARINT] =
  'uint'`.

The `api` functions like `blackboxprotobuf.decode_message` will default to
using the global `blackboxprotobuf.lib.config.default` object if one is not
specified.

## Type Breakdown
The following is a quick breakdown of wire types and default values. See
<https://developers.google.com/protocol-buffers/docs/encoding> for more detailed
information from Google.

### Variable Length Integers (varint)
The `varint` wire type represents integers with multiple bytes where one bit of
each is dedicated to indicating if it is the last byte. This can be used to
represent integers (signed/unsigned), boolean values or enums. Integers can be
encoded using three variations:

- `uint`: Varint encoding with no representation of negative numbers.
- `int`: Standard encoding but inefficient for negative numbers (always 10 bytes).
- `sint`: Uses ZigZag encoding to efficiently represent negative numbers by
  mapping negative numbers into the integer space. For example -1 is converted
  to 1, 1 to 2, -2 to 3, and so on. This can result in drastically different
  numbers if a type is misinterpreted and either the original or incorrect type
  is `sint`.

The default is currently `int` with no ZigZag encoding.

### Fixed32/64
The fixed length wire types have an implicit size based on the wire type. These
support either fixed size integers (signed/unsigned) or fixed size floating
point numbers (float/double). The default type for these is the floating point
type as most integers are more likely to be represented by a varint.

### Length Delimited
Length delimited wire types are prefixed with a `varint` indicating the length.
This is used for strings, bytestrings, inner messages and packed repeated
fields. Messages can generally be identified by validating if it is a valid
protobuf binary. If it is not a message, the default type is a string/byte
which are relatively interchangeable in Python. A different default type (such
as `bytes_hex`) can be specified by changing
`blackboxprotobuf.lib.types.default_binary_type`.

Packed repeated fields are arrays of either `varints` or a fixed length wire
type. Non-packed repeated fields use a separate tag (wire type + field number)
for each element, allowing them to be easily identified and parsed. However,
packed repeated fields only have the initial length delimited wire type tag.
The parser is assumed to know the full type already for parsing out the
individual elements. This makes this field type difficult to differentiate from
an arbitrary byte string and will require user intervention to identify. In
protobuf version 2, repeated fields had to be explicitly declared packed in the
definition. In protobuf version 3, repeated fields are packed by default and
are likely to become more common.
