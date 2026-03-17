# cython: language_level=3

"""Python code for writing AVRO files"""

# This code is a modified version of the code at
# http://svn.apache.org/viewvc/avro/trunk/lang/py/src/avro/ which is under
# Apache 2.0 license (http://www.apache.org/licenses/LICENSE-2.0)

from cpython cimport array
import array
import json
from binascii import crc32
from os import urandom
import bz2
import lzma
import zlib
import sys
from warnings import warn

from fastavro import const
from ._logical_writers import LOGICAL_WRITERS
from ._validation import _validate
from ._read import HEADER_SCHEMA, SYNC_SIZE, MAGIC, reader
from ._schema import extract_record_type, extract_logical_type, parse_schema
from ._write_common import _is_appendable

CYTHON_MODULE = 1  # Tests check this to confirm whether using the Cython code.

ctypedef int int32
ctypedef unsigned int uint32
ctypedef unsigned long long ulong64
ctypedef long long long64

cdef long64 MCS_PER_SECOND = const.MCS_PER_SECOND
cdef long64 MCS_PER_MINUTE = const.MCS_PER_MINUTE
cdef long64 MCS_PER_HOUR = const.MCS_PER_HOUR

cdef long64 MLS_PER_SECOND = const.MLS_PER_SECOND
cdef long64 MLS_PER_MINUTE = const.MLS_PER_MINUTE
cdef long64 MLS_PER_HOUR = const.MLS_PER_HOUR


cdef inline write_null(object fo, datum):
    """null is written as zero bytes"""
    pass


cdef inline write_boolean(bytearray fo, bint datum):
    """A boolean is written as a single byte whose value is either 0 (false) or
    1 (true)."""
    cdef unsigned char ch_temp[1]
    ch_temp[0] = 1 if datum else 0
    fo += ch_temp[:1]


cdef inline write_int(bytearray fo, long64 datum):
    """int and long values are written using variable-length, zig-zag coding.
    """
    cdef ulong64 n
    cdef unsigned char ch_temp[1]
    n = (datum << 1) ^ (datum >> 63)
    while (n & ~0x7F) != 0:
        ch_temp[0] = (n & 0x7f) | 0x80
        fo += ch_temp[:1]
        n >>= 7
    ch_temp[0] = n
    fo += ch_temp[:1]


cdef inline write_long(bytearray fo, datum):
    write_int(fo, datum)


cdef union float_uint32:
    float f
    uint32 n


cdef inline write_float(bytearray fo, float datum):
    """A float is written as 4 bytes.  The float is converted into a 32-bit
    integer using a method equivalent to Java's floatToIntBits and then encoded
    in little-endian format."""
    cdef float_uint32 fi
    cdef unsigned char ch_temp[4]

    fi.f = datum
    ch_temp[0] = fi.n & 0xff
    ch_temp[1] = (fi.n >> 8) & 0xff
    ch_temp[2] = (fi.n >> 16) & 0xff
    ch_temp[3] = (fi.n >> 24) & 0xff

    fo += ch_temp[:4]


cdef union double_ulong64:
    double d
    ulong64 n


cdef inline write_double(bytearray fo, double datum):
    """A double is written as 8 bytes.  The double is converted into a 64-bit
    integer using a method equivalent to Java's doubleToLongBits and then
    encoded in little-endian format.  """
    cdef double_ulong64 fi
    cdef unsigned char ch_temp[8]

    fi.d = datum
    ch_temp[0] = fi.n & 0xff
    ch_temp[1] = (fi.n >> 8) & 0xff
    ch_temp[2] = (fi.n >> 16) & 0xff
    ch_temp[3] = (fi.n >> 24) & 0xff
    ch_temp[4] = (fi.n >> 32) & 0xff
    ch_temp[5] = (fi.n >> 40) & 0xff
    ch_temp[6] = (fi.n >> 48) & 0xff
    ch_temp[7] = (fi.n >> 56) & 0xff

    fo += ch_temp[:8]


cdef inline write_bytes(bytearray fo, const unsigned char[:] datum):
    """Bytes are encoded as a long followed by that many bytes of data."""
    write_long(fo, len(datum))
    fo += datum


cdef inline write_utf8(bytearray fo, datum):
    """A string is encoded as a long followed by that many bytes of UTF-8
    encoded character data."""
    try:
        b_datum = datum.encode()
    except AttributeError:
        raise TypeError("must be string")
    write_long(fo, len(b_datum))
    fo += b_datum


cdef inline write_crc32(bytearray fo, bytes bytes):
    """A 4-byte, big-endian CRC32 checksum"""
    cdef unsigned char ch_temp[4]
    cdef uint32 data = crc32(bytes) & 0xFFFFFFFF

    ch_temp[0] = (data >> 24) & 0xff
    ch_temp[1] = (data >> 16) & 0xff
    ch_temp[2] = (data >> 8) & 0xff
    ch_temp[3] = data & 0xff
    fo += ch_temp[:4]


cdef inline write_fixed(bytearray fo, object datum, dict schema, dict named_schemas):
    """Fixed instances are encoded using the number of bytes declared in the
    schema."""
    if len(datum) != schema["size"]:
        raise ValueError(
            f"data of length {len(datum)} does not match schema size: {schema}"
        )
    fo += datum


cdef inline write_enum(bytearray fo, datum, schema, dict named_schemas):
    """An enum is encoded by a int, representing the zero-based position of
    the symbol in the schema."""
    index = schema["symbols"].index(datum)
    write_int(fo, index)


cdef write_array(bytearray fo, list datum, schema, dict named_schemas, fname, dict options):
    """Arrays are encoded as a series of blocks.

    Each block consists of a long count value, followed by that many array
    items.  A block with count zero indicates the end of the array.  Each item
    is encoded per the array's item schema.

    If a block's count is negative, then the count is followed immediately by a
    long block size, indicating the number of bytes in the block.  The actual
    count in this case is the absolute value of the count written.  """

    if len(datum) > 0:
        write_long(fo, len(datum))
        dtype = schema["items"]
        for item in datum:
            write_data(fo, item, dtype, named_schemas, fname, options)
    write_long(fo, 0)


cdef write_python_array(bytearray fo,
                        array.array datum,
                        schema,
                        named_schemas,
                        fname,
                        dict options):
    """Array specialization for python arrays."""
    if len(datum) > 0:
        write_long(fo, len(datum))
        record_type = extract_record_type(schema["items"])
        if record_type in ("int", "long"):
            for item in datum:
                write_int(fo, item)
        elif record_type == "float":
            for item in datum:
                write_float(fo, item)
        elif record_type == "double":
            for item in datum:
                write_double(fo, item)
        else:
            dtype = schema["items"]
            for item in datum:
                write_data(fo, item, dtype, named_schemas, fname, options)
    write_long(fo, 0)


cdef write_map(bytearray fo, object datum, dict schema, dict named_schemas, fname, dict options):
    """Maps are encoded as a series of blocks.

    Each block consists of a long count value, followed by that many key/value
    pairs.  A block with count zero indicates the end of the map.  Each item is
    encoded per the map's value schema.

    If a block's count is negative, then the count is followed immediately by a
    long block size, indicating the number of bytes in the block. The actual
    count in this case is the absolute value of the count written."""
    cdef dict d_datum
    try:
        d_datum = <dict?>(datum)
    except TypeError:
        # Slower, general-purpose code where datum is something besides a dict,
        # e.g. a collections.OrderedDict or collections.defaultdict.
        if len(datum) > 0:
            write_long(fo, len(datum))
            vtype = schema["values"]
            for key, val in datum.items():
                write_utf8(fo, key)
                write_data(fo, val, vtype, named_schemas, fname, options)
        write_long(fo, 0)
    else:
        # Faster, special-purpose code where datum is a Python dict.
        if len(d_datum) > 0:
            write_long(fo, len(d_datum))
            vtype = schema["values"]
            for key, val in d_datum.items():
                write_utf8(fo, key)
                write_data(fo, val, vtype, named_schemas, fname, options)
        write_long(fo, 0)


cdef write_union(bytearray fo, datum, schema, dict named_schemas, fname, dict options):
    """A union is encoded by first writing a long value indicating the
    zero-based position within the union of the schema of its value. The value
    is then encoded per the indicated schema within the union."""

    cdef int32 best_match_index
    cdef int32 most_fields
    cdef int32 index
    cdef int32 fields
    cdef str extracted_type
    cdef str schema_name
    best_match_index = -1
    if isinstance(datum, tuple) and not options.get("disable_tuple_notation"):
        (name, datum) = datum
        for index, candidate in enumerate(schema):
            extracted_type = extract_record_type(candidate)
            if extracted_type in const.NAMED_TYPES:
                schema_name = candidate["name"]
            else:
                schema_name = extracted_type
            if name == schema_name:
                best_match_index = index
                break

        if best_match_index == -1:
            field = f"on field {fname}" if fname else ""
            msg = (
                f"provided union type name {name} not found in schema "
                + f"{schema} {field}"
            )
            raise ValueError(msg)
        index = best_match_index
    else:
        pytype = type(datum)
        most_fields = -1

        # All of Python's floating point values are doubles, so to
        # avoid loss of precision, we should always prefer 'double'
        # if we are forced to choose between float and double.
        #
        # If 'double' comes before 'float' in the union, then we'll immediately
        # choose it, and don't need to worry. But if 'float' comes before
        # 'double', we don't want to pick it.
        #
        # So, if we ever see 'float', we skim through the rest of the options,
        # just to see if 'double' is a possibility, because we'd prefer it.
        could_be_float = False
        for index, candidate in enumerate(schema):

            if could_be_float:
                if extract_record_type(candidate) == "double":
                    best_match_index = index
                    break
                else:
                    # Nothing except "double" is even worth considering.
                    continue

            if _validate(
                datum,
                candidate,
                named_schemas,
                field="",
                raise_errors=False,
                options=options,
            ):
                record_type = extract_record_type(candidate)

                if record_type in named_schemas:
                    # Convert named record types into their full schema so that we can check most_fields
                    candidate = named_schemas[record_type]
                    record_type = extract_record_type(candidate)

                if record_type == "record":
                    logical_type = extract_logical_type(candidate)
                    if logical_type:
                        prepare = LOGICAL_WRITERS.get(logical_type)
                        if prepare:
                            datum = prepare(datum, candidate)

                    candidate_fields = set(
                        f["name"] for f in candidate["fields"]
                    )
                    datum_fields = set(datum)
                    fields = len(candidate_fields.intersection(datum_fields))
                    if fields > most_fields:
                        best_match_index = index
                        most_fields = fields
                elif record_type == "float":
                    best_match_index = index
                    # Continue in the loop, because it's possible that there's
                    # another candidate which has record type 'double'
                    could_be_float = True
                else:
                    best_match_index = index
                    break

        if best_match_index == -1:
            field = f"on field {fname}" if fname else ""
            raise ValueError(
                f"{repr(datum)} (type {pytype}) do not match {schema} {field}"
            )
        index = best_match_index

    # write data
    write_long(fo, index)
    write_data(fo, datum, schema[index], named_schemas, fname, options)


cdef write_record(bytearray fo, object datum, dict schema, dict named_schemas, dict options):
    """A record is encoded by encoding the values of its fields in the order
    that they are declared. In other words, a record is encoded as just the
    concatenation of the encodings of its fields.  Field values are encoded per
    their schema."""
    cdef list fields
    cdef dict field
    cdef dict d_datum
    fields = schema["fields"]

    extras = set(datum) - set(field["name"] for field in fields)
    if (options.get("strict") or options.get("strict_allow_default")) and extras:
        raise ValueError(
            f'record contains more fields than the schema specifies: {", ".join(extras)}'
        )

    try:
        d_datum = <dict?>(datum)
    except TypeError:
        # Slower, general-purpose code where datum is something besides a dict,
        # e.g. a collections.OrderedDict or collections.defaultdict.
        for field in fields:
            name = field["name"]
            field_type = field["type"]
            if name not in datum:
                if options.get("strict") or (
                    options.get("strict_allow_default") and "default" not in field
                ):
                    raise ValueError(
                        f"Field {name} is specified in the schema but missing from the record"
                    )
                elif "default" not in field and "null" not in field_type:
                    raise ValueError(f"no value and no default for {name}")
            datum_value = datum.get(name, field.get("default"))
            if field_type == "float" or field_type == "double":
                # Handle float values like "NaN"
                datum_value = float(datum_value)
            write_data(fo, datum_value, field_type, named_schemas, name, options)
    else:
        # Faster, special-purpose code where datum is a Python dict.
        for field in fields:
            name = field["name"]
            field_type = field["type"]
            if name not in d_datum:
                if options.get("strict") or (
                    options.get("strict_allow_default") and "default" not in field
                ):
                    raise ValueError(
                        f"Field {name} is specified in the schema but missing from the record"
                    )
                elif "default" not in field and "null" not in field_type:
                    raise ValueError(f"no value and no default for {name}")
            d_datum_value = d_datum.get(name, field.get("default"))
            if field_type == "float" or field_type == "double":
                # Handle float values like "NaN"
                d_datum_value = float(d_datum_value)
            write_data(fo, d_datum_value, field_type, named_schemas, name, options)


cpdef write_data(bytearray fo, datum, schema, dict named_schemas, fname, dict options):
    """Write a datum of data to output stream.

    Parameters
    ----------
    fo: file like
        Output file
    datum: object
        Data to write
    schema: dict
        Schema to use
    """
    cdef str logical_type = None
    if isinstance(schema, dict):
        logical_type = extract_logical_type(schema)
        if logical_type:
            prepare = LOGICAL_WRITERS.get(logical_type)
            if prepare:
                datum = prepare(datum, schema)

    record_type = extract_record_type(schema)
    try:
        if record_type == "null":
            return write_null(fo, datum)
        elif record_type == "string":
            return write_utf8(fo, datum)
        elif record_type == "int" or record_type == "long":
            return write_long(fo, datum)
        elif record_type == "float":
            return write_float(fo, datum)
        elif record_type == "double":
            return write_double(fo, datum)
        elif record_type == "boolean":
            return write_boolean(fo, datum)
        elif record_type == "bytes":
            return write_bytes(fo, datum)
        elif record_type == "fixed":
            return write_fixed(fo, datum, schema, named_schemas)
        elif record_type == "enum":
            return write_enum(fo, datum, schema, named_schemas)
        elif record_type == "array":
            if isinstance(datum, array.array):
                return write_python_array(
                    fo, datum, schema, named_schemas, fname, options
                )
            elif not isinstance(datum, list):
                datum = list(datum)
            return write_array(fo, datum, schema, named_schemas, fname, options)
        elif record_type == "map":
            return write_map(fo, datum, schema, named_schemas, fname, options)
        elif record_type == "union" or record_type == "error_union":
            return write_union(fo, datum, schema, named_schemas, fname, options)
        elif record_type == "record" or record_type == "error":
            return write_record(fo, datum, schema, named_schemas, options)
        else:
            return write_data(
                fo, datum, named_schemas[record_type], named_schemas, fname, options
            )
    except TypeError as ex:
        if fname:
            msg = f"{ex} on field {fname}"
            # TODO: Can we use `raise TypeError(msg) from ex` here?
            raise TypeError(msg).with_traceback(sys.exc_info()[2])
        raise


cpdef write_header(bytearray fo, dict metadata, bytes sync_marker):
    header = {
        "magic": MAGIC,
        "meta": {key: value.encode() for key, value in metadata.items()},
        "sync": sync_marker
    }
    write_data(fo, header, HEADER_SCHEMA, {}, "", {})


cpdef null_write_block(object fo, bytes block_bytes, compression_level):
    """Write block in "null" codec."""
    cdef bytearray tmp = bytearray()
    write_long(tmp, len(block_bytes))
    fo.write(tmp)
    fo.write(block_bytes)


cpdef deflate_write_block(object fo, bytes block_bytes, compression_level):
    """Write block in "deflate" codec."""
    cdef bytearray tmp = bytearray()
    # The first two characters and last character are zlib
    # wrappers around deflate data.
    if compression_level is not None:
        data = zlib.compress(block_bytes, compression_level)[2:-1]
    else:
        data = zlib.compress(block_bytes)[2:-1]

    write_long(tmp, len(data))
    fo.write(tmp)
    fo.write(data)


cpdef bzip2_write_block(object fo, bytes block_bytes, compression_level):
    """Write block in "bzip2" codec."""
    cdef bytearray tmp = bytearray()
    data = bz2.compress(block_bytes)
    write_long(tmp, len(data))
    fo.write(tmp)
    fo.write(data)


cpdef xz_write_block(object fo, bytes block_bytes, compression_level):
    """Write block in "xz" codec."""
    cdef bytearray tmp = bytearray()
    data = lzma.compress(block_bytes)
    write_long(tmp, len(data))
    fo.write(tmp)
    fo.write(data)


BLOCK_WRITERS = {
    "null": null_write_block,
    "deflate": deflate_write_block,
    "bzip2": bzip2_write_block,
    "xz": xz_write_block,
}


def _missing_dependency(codec, *libraries):
    def missing(fo, block_bytes, compression_level):
        raise ValueError(
            f"{codec} codec is supported but you need to install one of the "
            + f"following libraries: {libraries}"
        )
    return missing


try:
    from cramjam import snappy
    snappy_compress = snappy.compress_raw
except ImportError:
    try:
        import snappy
        snappy_compress = snappy.compress
        warn(
            "Snappy compression will use `cramjam` in the future. Please make sure you have `cramjam` installed",
            DeprecationWarning,
        )
    except ImportError:
        BLOCK_WRITERS["snappy"] = _missing_dependency("snappy", "cramjam")


cpdef snappy_write_block(object fo, bytes block_bytes, compression_level):
    """Write block in "snappy" codec."""
    cdef bytearray tmp = bytearray()
    data = snappy_compress(block_bytes)

    write_long(tmp, len(data) + 4)  # for CRC
    fo.write(tmp)
    fo.write(data)
    tmp[:] = b""
    write_crc32(tmp, block_bytes)
    fo.write(tmp)


if BLOCK_WRITERS.get("snappy") is None:
    BLOCK_WRITERS["snappy"] = snappy_write_block


try:
    import zstandard as zstd
except ImportError:
    BLOCK_WRITERS["zstandard"] = _missing_dependency("zstandard", "zstandard")


cpdef zstandard_write_block(object fo, bytes block_bytes, compression_level):
    """Write block in "zstandard" codec."""
    cdef bytearray tmp = bytearray()
    if compression_level is not None:
        data = zstd.ZstdCompressor(level=compression_level).compress(block_bytes)
    else:
        data = zstd.ZstdCompressor().compress(block_bytes)
    write_long(tmp, len(data))
    fo.write(tmp)
    fo.write(data)


if BLOCK_WRITERS.get("zstandard") is None:
    BLOCK_WRITERS["zstandard"] = zstandard_write_block


try:
    import lz4.block
except ImportError:
    BLOCK_WRITERS["lz4"] = _missing_dependency("lz4", "lz4")


cpdef lz4_write_block(object fo, bytes block_bytes, compression_level):
    """Write block in "lz4" codec."""
    cdef bytearray tmp = bytearray()
    data = lz4.block.compress(block_bytes)
    write_long(tmp, len(data))
    fo.write(tmp)
    fo.write(data)


if BLOCK_WRITERS.get("lz4") is None:
    BLOCK_WRITERS["lz4"] = lz4_write_block


cdef class MemoryIO:
    cdef bytearray value

    def __init__(self):
        self.value = bytearray()

    cpdef int32 tell(self):
        return len(self.value)

    cpdef bytes getvalue(self):
        return bytes(self.value)

    cpdef clear(self):
        self.value[:] = b""


cdef class Writer:
    cdef public object fo
    cdef public object schema
    cdef public object validate_fn
    cdef public object sync_marker
    cdef public MemoryIO io
    cdef public int32 block_count
    cdef public object metadata
    cdef public long64 sync_interval
    cdef public object block_writer
    cdef public object compression_level
    cdef public dict _named_schemas
    cdef public dict options

    def __init__(
        self,
        fo,
        schema,
        codec="null",
        sync_interval=1000 * SYNC_SIZE,
        metadata=None,
        validator=None,
        sync_marker=None,
        compression_level=None,
        options={},
    ):
        cdef bytearray tmp = bytearray()

        self.fo = fo
        self._named_schemas = {}
        self.validate_fn = _validate if validator else None
        self.io = MemoryIO()
        self.block_count = 0
        self.sync_interval = sync_interval
        self.compression_level = compression_level
        self.options = options

        if _is_appendable(self.fo):
            # Seed to the beginning to read the header
            self.fo.seek(0)
            avro_reader = reader(self.fo)
            header = avro_reader._header

            self.schema = parse_schema(avro_reader.writer_schema, self._named_schemas)
            codec = avro_reader.metadata.get("avro.codec", "null")

            self.sync_marker = header["sync"]

            # Seek to the end of the file
            self.fo.seek(0, 2)

            self.block_writer = BLOCK_WRITERS[codec]
        else:
            self.schema = parse_schema(schema, self._named_schemas)
            self.sync_marker = sync_marker or urandom(SYNC_SIZE)

            self.metadata = metadata or {}
            self.metadata["avro.codec"] = codec

            if isinstance(schema, dict):
                schema = {
                    key: value
                    for key, value in schema.items()
                    if key not in ("__fastavro_parsed", "__named_schemas")
                }
            elif isinstance(schema, list):
                schemas = []
                for s in schema:
                    if isinstance(s, dict):
                        schemas.append(
                            {
                                key: value
                                for key, value in s.items()
                                if key not in (
                                    "__fastavro_parsed", "__named_schemas"
                                )
                            }
                        )
                    else:
                        schemas.append(s)
                schema = schemas

            self.metadata["avro.schema"] = json.dumps(schema)

            try:
                self.block_writer = BLOCK_WRITERS[codec]
            except KeyError:
                raise ValueError(f"unrecognized codec: {codec}")

            write_header(tmp, self.metadata, self.sync_marker)
            self.fo.write(tmp)

    def dump(self):
        cdef bytearray tmp = bytearray()
        write_long(tmp, self.block_count)
        self.fo.write(tmp)
        self.block_writer(self.fo, self.io.getvalue(), self.compression_level)
        self.fo.write(self.sync_marker)
        self.io.clear()
        self.block_count = 0

    def write(self, record):
        if self.validate_fn:
            self.validate_fn(record, self.schema, self._named_schemas, "", True, self.options)
        write_data(self.io.value, record, self.schema, self._named_schemas, "", self.options)
        self.block_count += 1
        if self.io.tell() >= self.sync_interval:
            self.dump()

    def write_block(self, block):
        # Clear existing block if there are any records pending
        if self.io.tell() or self.block_count > 0:
            self.dump()
        cdef bytearray tmp = bytearray()
        write_long(tmp, block.num_records)
        self.fo.write(tmp)
        self.block_writer(self.fo, block.bytes_.getvalue(), self.compression_level)
        self.fo.write(self.sync_marker)

    def flush(self):
        if self.io.tell() or self.block_count > 0:
            self.dump()
        self.fo.flush()


def writer(
    fo,
    schema,
    records,
    codec="null",
    sync_interval=1000 * SYNC_SIZE,
    metadata=None,
    validator=None,
    sync_marker=None,
    codec_compression_level=None,
    *,
    strict=False,
    strict_allow_default=False,
    disable_tuple_notation=False,
):
    # Sanity check that records is not a single dictionary (as that is a common
    # mistake and the exception that gets raised is not helpful)
    if isinstance(records, dict):
        raise ValueError('"records" argument should be an iterable, not dict')

    output = Writer(
        fo,
        schema,
        codec,
        sync_interval,
        metadata,
        validator,
        sync_marker,
        codec_compression_level,
        options={
            "strict": strict,
            "strict_allow_default": strict_allow_default,
            "disable_tuple_notation": disable_tuple_notation,
        }
    )

    for record in records:
        output.write(record)
    output.flush()


def schemaless_writer(
    fo,
    schema,
    record,
    strict=False,
    strict_allow_default=False,
    disable_tuple_notation=False,
):
    cdef bytearray tmp = bytearray()
    named_schemas = {}
    schema = parse_schema(schema, named_schemas)
    write_data(
        tmp,
        record,
        schema,
        named_schemas,
        "",
        {
            "strict": strict,
            "strict_allow_default": strict_allow_default,
            "disable_tuple_notation": disable_tuple_notation,
        },
    )
    fo.write(tmp)
