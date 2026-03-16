# cython: language_level=3

"""Python code for reading AVRO files"""

# This code is a modified version of the code at
# http://svn.apache.org/viewvc/avro/trunk/lang/py/src/avro/ which is under
# Apache 2.0 license (http://www.apache.org/licenses/LICENSE-2.0)

import bz2
import lzma
import zlib
from datetime import datetime, timezone
from decimal import Context
from io import BytesIO
from warnings import warn

import json

from .logical_readers import LOGICAL_READERS
from ._schema import (
    extract_record_type,
    is_single_record_union,
    is_single_name_union,
    extract_logical_type,
    parse_schema,
)
from ._read_common import (
    SchemaResolutionError,
    MAGIC,
    SYNC_SIZE,
    HEADER_SCHEMA,
    missing_codec_lib,
)
from .const import NAMED_TYPES, AVRO_TYPES

CYTHON_MODULE = 1  # Tests check this to confirm whether using the Cython code.

decimal_context = Context()
epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
epoch_naive = datetime(1970, 1, 1)

ctypedef int int32
ctypedef unsigned int uint32
ctypedef unsigned long long ulong64
ctypedef long long long64


class ReadError(Exception):
    pass


cpdef _default_named_schemas():
    return {"writer": {}, "reader": {}}


cpdef match_types(writer_type, reader_type, named_schemas):
    if isinstance(writer_type, list) or isinstance(reader_type, list):
        return True
    if isinstance(writer_type, dict) or isinstance(reader_type, dict):
        matching_schema = match_schemas(
            writer_type, reader_type, named_schemas, raise_on_error=False
        )
        return matching_schema is not None
    if writer_type == reader_type:
        return True
    # promotion cases
    elif writer_type == "int" and reader_type in ["long", "float", "double"]:
        return True
    elif writer_type == "long" and reader_type in ["float", "double"]:
        return True
    elif writer_type == "float" and reader_type == "double":
        return True
    elif writer_type == "string" and reader_type == "bytes":
        return True
    elif writer_type == "bytes" and reader_type == "string":
        return True
    writer_schema = named_schemas["writer"].get(writer_type)
    reader_schema = named_schemas["reader"].get(reader_type)
    if writer_schema is not None and reader_schema is not None:
        return match_types(writer_schema, reader_schema, named_schemas)
    return False


cpdef match_schemas(w_schema, r_schema, named_schemas, raise_on_error=True):
    if isinstance(w_schema, list):
        # If the writer is a union, checks will happen in read_union after the
        # correct schema is known
        return r_schema
    elif isinstance(r_schema, list):
        # If the reader is a union, ensure one of the new schemas is the same
        # as the writer
        for schema in r_schema:
            if match_types(w_schema, schema, named_schemas):
                return schema
        else:
            if raise_on_error:
                raise SchemaResolutionError(
                    f"Schema mismatch: {w_schema} is not {r_schema}"
                )
            else:
                return None
    else:
        # Check for dicts as primitive types are just strings
        if isinstance(w_schema, dict):
            w_type = w_schema["type"]
        else:
            w_type = w_schema
        if isinstance(r_schema, dict):
            r_type = r_schema["type"]
        else:
            r_type = r_schema

        if w_type == r_type == "map":
            if match_types(w_schema["values"], r_schema["values"], named_schemas):
                return r_schema
        elif w_type == r_type == "array":
            if match_types(w_schema["items"], r_schema["items"], named_schemas):
                return r_schema
        elif w_type in NAMED_TYPES and r_type in NAMED_TYPES:
            if w_type == r_type == "fixed" and w_schema["size"] != r_schema["size"]:
                if raise_on_error:
                    raise SchemaResolutionError(
                        f"Schema mismatch: {w_schema} size is different than {r_schema} size"
                    )
                else:
                    return None

            w_unqual_name = w_schema["name"].split(".")[-1]
            r_unqual_name = r_schema["name"].split(".")[-1]
            r_aliases = r_schema.get("aliases", [])
            if w_unqual_name == r_unqual_name or w_schema["name"] in r_aliases or w_unqual_name in r_aliases:
                return r_schema
        elif w_type not in AVRO_TYPES and r_type in NAMED_TYPES:
            if match_types(w_type, r_schema["name"], named_schemas):
                return r_schema["name"]
        elif match_types(w_type, r_type, named_schemas):
            return r_schema
        if raise_on_error:
            raise SchemaResolutionError(
                f"Schema mismatch: {w_schema} is not {r_schema}"
            )
        else:
            return None


cpdef inline read_null(fo):
    """null is written as zero bytes."""
    return None


cpdef inline skip_null(fo):
    """null is written as zero bytes."""
    pass


cpdef inline read_boolean(fo):
    """A boolean is written as a single byte whose value is either 0 (false) or
    1 (true).
    """
    cdef unsigned char ch_temp
    cdef bytes bytes_temp = fo.read(1)
    if len(bytes_temp) == 1:
        # technically 0x01 == true and 0x00 == false, but many languages will
        # cast anything other than 0 to True and only 0 to False
        ch_temp = bytes_temp[0]
        return ch_temp != 0
    else:
        raise ReadError


cpdef inline skip_boolean(fo):
    """A boolean is written as a single byte whose value is either 0 (false) or
    1 (true).
    """
    fo.read(1)


cpdef long64 read_long(fo) except? -1:
    """int and long values are written using variable-length, zig-zag
    coding."""
    cdef ulong64 b
    cdef ulong64 n
    cdef int32 shift
    cdef bytes c = fo.read(1)

    # We do EOF checking only here, since most reader start here
    if not c:
        raise EOFError

    b = <unsigned char>(c[0])
    n = b & 0x7F
    shift = 7

    while (b & 0x80) != 0:
        c = fo.read(1)
        b = <unsigned char>(c[0])
        n |= (b & 0x7F) << shift
        shift += 7

    return (n >> 1) ^ -(n & 1)


cpdef skip_long(fo):
    """int and long values are written using variable-length, zig-zag
    coding."""
    cdef ulong64 b
    cdef bytes c = fo.read(1)

    b = <unsigned char>(c[0])

    while (b & 0x80) != 0:
        c = fo.read(1)
        b = <unsigned char>(c[0])


cpdef skip_int(fo):
    skip_long(fo)


cdef union float_uint32:
    float f
    uint32 n


cpdef read_float(fo):
    """A float is written as 4 bytes.

    The float is converted into a 32-bit integer using a method equivalent to
    Java's floatToIntBits and then encoded in little-endian format.
    """
    cdef bytes data
    cdef unsigned char ch_data[4]
    cdef float_uint32 fi
    data = fo.read(4)
    if len(data) == 4:
        ch_data[:4] = data
        fi.n = (ch_data[0]
                | (ch_data[1] << 8)
                | (ch_data[2] << 16)
                | (ch_data[3] << 24))
        return fi.f
    else:
        raise ReadError


cpdef skip_float(fo):
    """A float is written as 4 bytes.

    The float is converted into a 32-bit integer using a method equivalent to
    Java's floatToIntBits and then encoded in little-endian format.
    """
    fo.read(4)


cdef union double_ulong64:
    double d
    ulong64 n


cpdef read_double(fo):
    """A double is written as 8 bytes.

    The double is converted into a 64-bit integer using a method equivalent to
    Java's doubleToLongBits and then encoded in little-endian format.
    """
    cdef bytes data
    cdef unsigned char ch_data[8]
    cdef double_ulong64 dl
    data = fo.read(8)
    if len(data) == 8:
        ch_data[:8] = data
        dl.n = (ch_data[0]
                | (<ulong64>(ch_data[1]) << 8)
                | (<ulong64>(ch_data[2]) << 16)
                | (<ulong64>(ch_data[3]) << 24)
                | (<ulong64>(ch_data[4]) << 32)
                | (<ulong64>(ch_data[5]) << 40)
                | (<ulong64>(ch_data[6]) << 48)
                | (<ulong64>(ch_data[7]) << 56))
        return dl.d
    else:
        raise ReadError


cpdef skip_double(fo):
    """A double is written as 8 bytes.

    The double is converted into a 64-bit integer using a method equivalent to
    Java's doubleToLongBits and then encoded in little-endian format.
    """
    fo.read(8)


cpdef read_bytes(fo):
    """Bytes are encoded as a long followed by that many bytes of data."""
    cdef long64 size = read_long(fo)
    out =  fo.read(<long>size)
    if len(out) != size:
        raise EOFError(f"Expected {size} bytes, read {len(out)}")
    return out


cpdef skip_bytes(fo):
    """Bytes are encoded as a long followed by that many bytes of data."""
    cdef long64 size = read_long(fo)
    fo.read(<long>size)


cpdef unicode read_utf8(fo, handle_unicode_errors="strict"):
    """A string is encoded as a long followed by that many bytes of UTF-8
    encoded character data.
    """
    return read_bytes(fo).decode(errors=handle_unicode_errors)


cpdef skip_utf8(fo):
    """A string is encoded as a long followed by that many bytes of UTF-8
    encoded character data.
    """
    skip_bytes(fo)


cpdef read_fixed(fo, writer_schema):
    """Fixed instances are encoded using the number of bytes declared in the
    schema."""
    out = fo.read(writer_schema["size"])
    if len(out) != writer_schema["size"]:
        raise EOFError(f"Expected {writer_schema['size']} bytes, read {len(out)}")
    return out


cpdef skip_fixed(fo, writer_schema):
    """Fixed instances are encoded using the number of bytes declared in the
    schema."""
    fo.read(writer_schema["size"])


cpdef read_enum(fo, writer_schema, reader_schema):
    """An enum is encoded by a int, representing the zero-based position of the
    symbol in the schema.
    """
    index = read_long(fo)
    symbol = writer_schema["symbols"][index]
    if reader_schema and symbol not in reader_schema["symbols"]:
        default = reader_schema.get("default")
        if default:
            return default
        else:
            symlist = reader_schema["symbols"]
            msg = f"{symbol} not found in reader symbol list {reader_schema['name']}, known symbols: {symlist}"
            raise SchemaResolutionError(msg)
    return symbol


cpdef skip_enum(fo):
    """An enum is encoded by a int, representing the zero-based position of the
    symbol in the schema.
    """
    read_long(fo)


cpdef read_array(
    fo,
    writer_schema,
    named_schemas,
    reader_schema=None,
    options={},
):
    """Arrays are encoded as a series of blocks.

    Each block consists of a long count value, followed by that many array
    items.  A block with count zero indicates the end of the array.  Each item
    is encoded per the array's item schema.

    If a block's count is negative, then the count is followed immediately by a
    long block size, indicating the number of bytes in the block.  The actual
    count in this case is the absolute value of the count written.
    """
    cdef list read_items
    cdef long64 block_count
    cdef long64 i

    read_items = []

    block_count = read_long(fo)

    while block_count != 0:
        if block_count < 0:
            block_count = -block_count
            # Read block size, unused
            read_long(fo)

        if reader_schema:
            for i in range(block_count):
                read_items.append(_read_data(
                    fo,
                    writer_schema["items"],
                    named_schemas,
                    reader_schema["items"],
                    options,
                ))
        else:
            for i in range(block_count):
                read_items.append(_read_data(
                    fo,
                    writer_schema["items"],
                    named_schemas,
                    None,
                    options,
                ))
        block_count = read_long(fo)

    return read_items


cpdef skip_array(fo, writer_schema, named_schemas):
    """Arrays are encoded as a series of blocks.

    Each block consists of a long count value, followed by that many array
    items.  A block with count zero indicates the end of the array.  Each item
    is encoded per the array's item schema.

    If a block's count is negative, then the count is followed immediately by a
    long block size, indicating the number of bytes in the block.  The actual
    count in this case is the absolute value of the count written.
    """
    cdef long64 block_count
    cdef long64 i

    block_count = read_long(fo)

    while block_count != 0:
        if block_count < 0:
            block_size = read_long(fo)
            fo.read(block_size)
        else:
            for i in range(block_count):
                _skip_data(fo, writer_schema["items"], named_schemas)

        block_count = read_long(fo)


cpdef read_map(
    fo,
    writer_schema,
    named_schemas,
    reader_schema=None,
    options={},
):
    """Maps are encoded as a series of blocks.

    Each block consists of a long count value, followed by that many key/value
    pairs.  A block with count zero indicates the end of the map.  Each item is
    encoded per the map's value schema.

    If a block's count is negative, then the count is followed immediately by a
    long block size, indicating the number of bytes in the block.  The actual
    count in this case is the absolute value of the count written.
    """
    cdef dict read_items
    cdef long64 block_count
    cdef long64 i
    cdef unicode key

    read_items = {}
    block_count = read_long(fo)
    while block_count != 0:
        if block_count < 0:
            block_count = -block_count
            # Read block size, unused
            read_long(fo)

        if reader_schema:
            for i in range(block_count):
                key = read_utf8(fo, options.get("handle_unicode_errors", "strict"))
                read_items[key] = _read_data(
                    fo,
                    writer_schema["values"],
                    named_schemas,
                    reader_schema["values"],
                    options,
                )
        else:
            for i in range(block_count):
                key = read_utf8(fo, options.get("handle_unicode_errors", "strict"))
                read_items[key] = _read_data(
                    fo,
                    writer_schema["values"],
                    named_schemas,
                    None,
                    options,
                )
        block_count = read_long(fo)

    return read_items


cpdef skip_map(fo, writer_schema, named_schemas):
    """Maps are encoded as a series of blocks.

    Each block consists of a long count value, followed by that many key/value
    pairs.  A block with count zero indicates the end of the map.  Each item is
    encoded per the map's value schema.

    If a block's count is negative, then the count is followed immediately by a
    long block size, indicating the number of bytes in the block.  The actual
    count in this case is the absolute value of the count written.
    """
    cdef long64 block_count
    cdef long64 i

    block_count = read_long(fo)
    while block_count != 0:
        if block_count < 0:
            block_size = read_long(fo)
            fo.read(block_size)
        else:
            for i in range(block_count):
                skip_utf8(fo)
                _skip_data(fo, writer_schema["values"], named_schemas)

        block_count = read_long(fo)


cpdef read_union(
    fo,
    writer_schema,
    named_schemas,
    reader_schema=None,
    options={}
):
    """A union is encoded by first writing a long value indicating the
    zero-based position within the union of the schema of its value.

    The value is then encoded per the indicated schema within the union.
    """
    # schema resolution
    index = read_long(fo)
    idx_schema = writer_schema[index]
    idx_reader_schema = None

    if reader_schema:
        # Handle case where the reader schema is just a single type (not union)
        if not isinstance(reader_schema, list):
            if match_types(idx_schema, reader_schema, named_schemas):
                result = _read_data(
                    fo,
                    idx_schema,
                    named_schemas,
                    reader_schema,
                    options,
                )
            else:
                raise SchemaResolutionError(
                    f"schema mismatch: {writer_schema} not found in {reader_schema}"
                )
        else:
            for schema in reader_schema:
                if match_types(idx_schema, schema, named_schemas):
                    idx_reader_schema = schema
                    result = _read_data(
                        fo,
                        idx_schema,
                        named_schemas,
                        schema,
                        options,
                    )
                    break
            else:
                raise SchemaResolutionError(
                    f"schema mismatch: {writer_schema} not found in {reader_schema}"
                )
    else:
        result = _read_data(fo, idx_schema, named_schemas, None, options)

    return_record_name_override = options.get("return_record_name_override")
    return_record_name = options.get("return_record_name")
    return_named_type_override = options.get("return_named_type_override")
    return_named_type = options.get("return_named_type")
    if return_named_type_override and is_single_name_union(writer_schema):
        return result
    elif return_named_type and extract_record_type(idx_schema) in NAMED_TYPES:
        schema_name = (
            idx_reader_schema["name"] if idx_reader_schema else idx_schema["name"]
        )
        return (schema_name, result)
    elif return_named_type and extract_record_type(idx_schema) not in AVRO_TYPES:
        # idx_schema is a named type
        schema_name = (
            named_schemas["reader"][idx_reader_schema]["name"]
            if idx_reader_schema
            else named_schemas["writer"][idx_schema]["name"]
        )
        return (schema_name, result)
    elif return_record_name_override and is_single_record_union(writer_schema):
        return result
    elif return_record_name and extract_record_type(idx_schema) == "record":
        schema_name = (
            idx_reader_schema["name"] if idx_reader_schema else idx_schema["name"]
        )
        return (schema_name, result)
    elif return_record_name and extract_record_type(idx_schema) not in AVRO_TYPES:
        # idx_schema is a named type
        schema_name = (
            named_schemas["reader"][idx_reader_schema]["name"]
            if idx_reader_schema
            else named_schemas["writer"][idx_schema]["name"]
        )
        return (schema_name, result)
    else:
        return result


cpdef skip_union(fo, writer_schema, named_schemas):
    """A union is encoded by first writing a long value indicating the
    zero-based position within the union of the schema of its value.

    The value is then encoded per the indicated schema within the union.
    """
    # schema resolution
    index = read_long(fo)
    _skip_data(fo, writer_schema[index], named_schemas)


cpdef read_record(
    fo,
    writer_schema,
    named_schemas,
    reader_schema=None,
    options={},
):
    """A record is encoded by encoding the values of its fields in the order
    that they are declared. In other words, a record is encoded as just the
    concatenation of the encodings of its fields.  Field values are encoded per
    their schema.

    Schema Resolution:
     * the ordering of fields may be different: fields are matched by name.
     * schemas for fields with the same name in both records are resolved
         recursively.
     * if the writer's record contains a field with a name not present in the
         reader's record, the writer's value for that field is ignored.
     * if the reader's record schema has a field that contains a default value,
         and writer's schema does not have a field with the same name, then the
         reader should use the default value from its field.
     * if the reader's record schema has a field with no default value, and
         writer's schema does not have a field with the same name, then the
         field's value is unset.
    """
    record = {}
    if reader_schema is None:
        for field in writer_schema["fields"]:
            record[field["name"]] = _read_data(
                fo,
                field["type"],
                named_schemas,
                None,
                options,
            )
    else:
        readers_field_dict = {}
        aliases_field_dict = {}
        for f in reader_schema["fields"]:
            readers_field_dict[f["name"]] = f
            for alias in f.get("aliases", []):
                aliases_field_dict[alias] = f

        for field in writer_schema["fields"]:
            readers_field = readers_field_dict.get(
                field["name"],
                aliases_field_dict.get(field["name"]),
            )
            if readers_field:
                readers_field_name = readers_field["name"]
                record[readers_field_name] = _read_data(
                    fo,
                    field["type"],
                    named_schemas,
                    readers_field["type"],
                    options,
                )
                del readers_field_dict[readers_field_name]
            else:
                _skip_data(fo, field["type"], named_schemas)

        # fill in default values
        for f_name, field in readers_field_dict.items():
            if "default" in field:
                record[field["name"]] = field["default"]
            else:
                msg = f"No default value for field {field['name']} in {reader_schema['name']}"
                raise SchemaResolutionError(msg)

    return record


cpdef skip_record(fo, writer_schema, named_schemas):
    for field in writer_schema["fields"]:
        _skip_data(fo, field["type"], named_schemas)


cpdef maybe_promote(data, writer_type, reader_type):
    if writer_type == "int":
        # No need to promote to long since they are the same type in Python
        if reader_type == "float" or reader_type == "double":
            return float(data)
    if writer_type == "long":
        if reader_type == "float" or reader_type == "double":
            return float(data)
    if writer_type == "string" and reader_type == "bytes":
        return data.encode()
    if writer_type == "bytes" and reader_type == "string":
        return data.decode()
    return data


cpdef _read_data(
    fo,
    writer_schema,
    named_schemas,
    reader_schema=None,
    options={},
):
    """Read data from file object according to schema."""

    record_type = extract_record_type(writer_schema)

    if reader_schema:
        reader_schema = match_schemas(
            writer_schema,
            reader_schema,
            named_schemas,
        )

    try:
        if record_type == "null":
            data = read_null(fo)
        elif record_type == "string":
            data = read_utf8(fo, options.get("handle_unicode_errors", "strict"))
        elif record_type == "int" or record_type == "long":
            data = read_long(fo)
        elif record_type == "float":
            data = read_float(fo)
        elif record_type == "double":
            data = read_double(fo)
        elif record_type == "boolean":
            data = read_boolean(fo)
        elif record_type == "bytes":
            data = read_bytes(fo)
        elif record_type == "fixed":
            data = read_fixed(fo, writer_schema)
        elif record_type == "enum":
            data = read_enum(fo, writer_schema, reader_schema)
        elif record_type == "array":
            data = read_array(
                fo,
                writer_schema,
                named_schemas,
                reader_schema,
                options,
            )
        elif record_type == "map":
            data = read_map(
                fo,
                writer_schema,
                named_schemas,
                reader_schema,
                options,
            )
        elif record_type == "union" or record_type == "error_union":
            data = read_union(
                fo,
                writer_schema,
                named_schemas,
                reader_schema,
                options,
            )
        elif record_type == "record" or record_type == "error":
            data = read_record(
                fo,
                writer_schema,
                named_schemas,
                reader_schema,
                options,
            )
        else:
            return _read_data(
                fo,
                named_schemas["writer"][record_type],
                named_schemas,
                named_schemas["reader"].get(reader_schema),
                options,
            )
    except ReadError:
        raise EOFError(f"cannot read {record_type} from {fo}")

    if "logicalType" in writer_schema:
        logical_type = extract_logical_type(writer_schema)
        fn = LOGICAL_READERS.get(logical_type)
        if fn:
            return fn(data, writer_schema, reader_schema)

    if reader_schema is not None:
        return maybe_promote(
            data,
            record_type,
            extract_record_type(reader_schema)
        )
    else:
        return data


cpdef _skip_data(
    fo,
    writer_schema,
    named_schemas,
):
    record_type = extract_record_type(writer_schema)

    if record_type == "null":
        skip_null(fo)
    elif record_type == "string":
        skip_utf8(fo)
    elif record_type == "int" or record_type == "long":
        skip_long(fo)
    elif record_type == "float":
        skip_float(fo)
    elif record_type == "double":
        skip_double(fo)
    elif record_type == "boolean":
        skip_boolean(fo)
    elif record_type == "bytes":
        skip_bytes(fo)
    elif record_type == "fixed":
        skip_fixed(fo, writer_schema)
    elif record_type == "enum":
        skip_enum(fo)
    elif record_type == "array":
        skip_array(fo, writer_schema, named_schemas)
    elif record_type == "map":
        skip_map(fo, writer_schema, named_schemas)
    elif record_type == "union" or record_type == "error_union":
        skip_union(fo, writer_schema, named_schemas)
    elif record_type == "record" or record_type == "error":
        skip_record(fo, writer_schema, named_schemas)
    else:
        _skip_data(fo, named_schemas["writer"][record_type], named_schemas)


cpdef skip_sync(fo, sync_marker):
    """Skip an expected sync marker, complaining if it doesn't match"""
    if fo.read(SYNC_SIZE) != sync_marker:
        raise ValueError("expected sync marker not found")


cpdef null_read_block(fo):
    """Read block in "null" codec."""
    return BytesIO(read_bytes(fo))


cpdef deflate_read_block(fo):
    """Read block in "deflate" codec."""
    data = read_bytes(fo)
    # -15 is the log of the window size; negative indicates "raw" (no
    # zlib headers) decompression.  See zlib.h.
    return BytesIO(zlib.decompressobj(-15).decompress(data))


cpdef bzip2_read_block(fo):
    """Read block in "bzip2" codec."""
    data = read_bytes(fo)
    return BytesIO(bz2.decompress(data))


cpdef xz_read_block(fo):
    length = read_long(fo)
    data = fo.read(length)
    return BytesIO(lzma.decompress(data))


BLOCK_READERS = {
    "null": null_read_block,
    "deflate": deflate_read_block,
    "bzip2": bzip2_read_block,
    "xz": xz_read_block,
}


cpdef snappy_read_block(fo):
    length = read_long(fo)
    data = fo.read(length - 4)
    fo.read(4)  # CRC
    return BytesIO(snappy_decompress(data))


try:
    from cramjam import snappy
    snappy_decompress = snappy.decompress_raw
except ImportError:
    try:
        import snappy
        snappy_decompress = snappy.decompress
        warn(
            "Snappy compression will use `cramjam` in the future. Please make sure you have `cramjam` installed",
            DeprecationWarning,
        )
    except ImportError:
        BLOCK_READERS["snappy"] = missing_codec_lib("snappy", "cramjam")
    else:
        BLOCK_READERS["snappy"] = snappy_read_block
else:
    BLOCK_READERS["snappy"] = snappy_read_block


cpdef zstandard_read_block(fo):
    length = read_long(fo)
    data = fo.read(length)
    return BytesIO(zstd.ZstdDecompressor().decompressobj().decompress(data))


try:
    import zstandard as zstd
except ImportError:
    BLOCK_READERS["zstandard"] = missing_codec_lib("zstandard", "zstandard")
else:
    BLOCK_READERS["zstandard"] = zstandard_read_block


cpdef lz4_read_block(fo):
    length = read_long(fo)
    data = fo.read(length)
    return BytesIO(lz4.block.decompress(data))


try:
    import lz4.block
except ImportError:
    BLOCK_READERS["lz4"] = missing_codec_lib("lz4", "lz4")
else:
    BLOCK_READERS["lz4"] = lz4_read_block


def _iter_avro_records(
    fo,
    header,
    codec,
    writer_schema,
    named_schemas,
    reader_schema,
    options,
):
    cdef int32 i

    sync_marker = header["sync"]

    read_block = BLOCK_READERS.get(codec)
    if not read_block:
        raise ValueError(f"Unrecognized codec: {codec}")

    block_count = 0
    while True:
        try:
            block_count = read_long(fo)
        except EOFError:
            return

        block_fo = read_block(fo)

        for i in range(block_count):
            yield _read_data(
                block_fo,
                writer_schema,
                named_schemas,
                reader_schema,
                options,
            )

        skip_sync(fo, sync_marker)


def _iter_avro_blocks(
    fo,
    header,
    codec,
    writer_schema,
    named_schemas,
    reader_schema,
    options,
):
    sync_marker = header["sync"]

    read_block = BLOCK_READERS.get(codec)
    if not read_block:
        raise ValueError(f"Unrecognized codec: {codec}")

    while True:
        offset = fo.tell()
        try:
            num_block_records = read_long(fo)
        except EOFError:
            return

        block_bytes = read_block(fo)

        skip_sync(fo, sync_marker)

        size = fo.tell() - offset

        yield Block(
            block_bytes, num_block_records, codec, reader_schema,
            writer_schema, named_schemas, offset, size, options,
        )


class Block:
    def __init__(
        self,
        bytes_,
        num_records,
        codec,
        reader_schema,
        writer_schema,
        named_schemas,
        offset,
        size,
        options,
    ):
        self.bytes_ = bytes_
        self.num_records = num_records
        self.codec = codec
        self.reader_schema = reader_schema
        self.writer_schema = writer_schema
        self._named_schemas = named_schemas
        self.offset = offset
        self.size = size
        self.options = options

    def __iter__(self):
        for i in range(self.num_records):
            yield _read_data(
                self.bytes_,
                self.writer_schema,
                self._named_schemas,
                self.reader_schema,
                self.options,
            )

    def __str__(self):
        return (
            f"Avro block: {len(self.bytes_)} bytes, {self.num_records} records, "
            + f"codec: {self.codec}, position {self.offset}+{self.size}"
        )


class file_reader:
    def __init__(self, fo, reader_schema=None, options={}):
        self.fo = fo
        self.options = options
        try:
            self._header = _read_data(self.fo, HEADER_SCHEMA, {}, None, self.options)
        except EOFError:
            raise ValueError("cannot read header - is it an avro file?")

        # `meta` values are bytes. So, the actual decoding has to be external.
        self.metadata = {
            k: v.decode() for k, v in self._header["meta"].items()
        }

        self._schema = json.loads(self.metadata["avro.schema"])
        self.codec = self.metadata.get("avro.codec", "null")

        self._named_schemas = _default_named_schemas()
        if reader_schema:
            self.reader_schema = parse_schema(
                reader_schema, self._named_schemas["reader"], _write_hint=False
            )
            # Older avro files created before we were more strict about
            # defaults might have been writen with a bad default. Since we re-parse
            # the writer schema here, it will now fail. Therefore, if a user
            # provides a reader schema that passes parsing, we will ignore those
            # default errors
            ignore_default_error = True
        else:
            self.reader_schema = None
            ignore_default_error = False

        self.writer_schema = parse_schema(
            self._schema,
            self._named_schemas["writer"],
            _write_hint=False,
            _force=True,
            _ignore_default_error=ignore_default_error,
        )

        self._elems = None

    @property
    def schema(self):
        import warnings
        warnings.warn(
            "The 'schema' attribute is deprecated. Please use 'writer_schema'",
            DeprecationWarning,
        )
        return self._schema

    def __iter__(self):
        if not self._elems:
            raise NotImplementedError
        return self._elems

    def __next__(self):
        return next(self._elems)


class reader(file_reader):
    def __init__(
        self,
        fo,
        reader_schema=None,
        return_record_name=False,
        return_record_name_override=False,
        handle_unicode_errors="strict",
        return_named_type=False,
        return_named_type_override=False,
    ):
        options = {
            "return_record_name": return_record_name,
            "return_record_name_override": return_record_name_override,
            "handle_unicode_errors": handle_unicode_errors,
            "return_named_type": return_named_type,
            "return_named_type_override": return_named_type_override,
        }
        super().__init__(fo, reader_schema, options)

        self._elems = _iter_avro_records(self.fo,
                                         self._header,
                                         self.codec,
                                         self.writer_schema,
                                         self._named_schemas,
                                         self.reader_schema,
                                         self.options)


class block_reader(file_reader):
    def __init__(
        self,
        fo,
        reader_schema=None,
        return_record_name=False,
        return_record_name_override=False,
        handle_unicode_errors="strict",
        return_named_type=False,
        return_named_type_override=False,
    ):
        options = {
            "return_record_name": return_record_name,
            "return_record_name_override": return_record_name_override,
            "handle_unicode_errors": handle_unicode_errors,
            "return_named_type": return_named_type,
            "return_named_type_override": return_named_type_override,
        }
        super().__init__(fo, reader_schema, options)

        self._elems = _iter_avro_blocks(self.fo,
                                        self._header,
                                        self.codec,
                                        self.writer_schema,
                                        self._named_schemas,
                                        self.reader_schema,
                                        self.options)


cpdef schemaless_reader(
    fo,
    writer_schema,
    reader_schema=None,
    return_record_name=False,
    return_record_name_override=False,
    handle_unicode_errors="strict",
    return_named_type=False,
    return_named_type_override=False,
):
    if writer_schema == reader_schema:
        # No need for the reader schema if they are the same
        reader_schema = None

    named_schemas = _default_named_schemas()
    writer_schema = parse_schema(writer_schema, named_schemas["writer"])

    if reader_schema:
        reader_schema = parse_schema(reader_schema, named_schemas["reader"])

    options = {
        "return_record_name": return_record_name,
        "return_record_name_override": return_record_name_override,
        "handle_unicode_errors": handle_unicode_errors,
        "return_named_type": return_named_type,
        "return_named_type_override": return_named_type_override,
    }
    return _read_data(
        fo,
        writer_schema,
        named_schemas,
        reader_schema,
        options,
    )


cpdef is_avro(path_or_buffer):
    if isinstance(path_or_buffer, str):
        fp = open(path_or_buffer, "rb")
        close = True
    else:
        fp = path_or_buffer
        close = False

    try:
        header = fp.read(len(MAGIC))
        return header == MAGIC
    finally:
        if close:
            fp.close()
