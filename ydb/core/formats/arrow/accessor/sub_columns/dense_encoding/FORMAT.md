# Dense encoding format

Dense encoding stores binary sub-columns and binary dictionary values. The
constructor selected by `TEncodingParams` defines the format; the blob does not
contain a format version, type, or codec - they come from the subcolumn header.

Only little-endian hosts are supported. Multi-byte integers are little-endian.

## Frames and sections

A frame is:

```
[raw_size: ui32][raw bytes or codec-compressed raw bytes]
```

The codec is supplied by the accessor serializer. With no codec, the payload is
stored as-is and must have `raw_size` bytes.

A section is a frame prefixed with its encoded size:

```
[frame_size: ui32][frame]
```

`frame_size` lets a reader find the next section without decoding this one.

## Binary array

The binary-array blob is:

```
[has_nulls: ui8]
[validity section]                 // only when has_nulls is 1
[lengths section]
[values section]
```

The validity payload is Arrow's LSB-first validity bitmap, cropped to the
array's range: bit `i` is one for a present value. `has_nulls` is zero when the
array has no nulls, so no bitmap is stored.

The values payload is the concatenation of the non-null binary values. The
lengths section has one entry per non-null value; the validity bitmap maps them
back to records. Its raw payload is:

```
[length_width: ui8][byte-stream-split lengths]
```

`length_width` is 1, 2, or 4, selected from the largest length. The
byte-stream-split element type has that width. The decoder widens lengths to
`ui32` and reconstructs Arrow offsets by prefix sum.

## Dictionary array

A dictionary blob is:

```
[dictionary_length: ui32][binary-array blob][positions frame]
```

`TDictionaryAccessorData` stores the `ui32` size of the dictionary prefix; the
positions blob occupies the remaining bytes. The dictionary is encoded with the
binary-array layout above.

The positions frame contains this raw payload before frame compression:

```
[has_nulls: ui8][validity bitmap when has_nulls][index values]
```

Index values use the narrowest unsigned Arrow integer type that represents the
dictionary cardinality. They are not delta- or byte-stream-split: repeated,
small-alphabet indexes are left for the outer codec. Null index slots are
omitted and restored from the validity bitmap.

## Differences from Arrow IPC

| Dense encoding | Arrow IPC |
| --- | --- |
| Accessor-specific, with type, codec, and flags supplied externally. | Record-batch metadata describes buffers and compression; schema and dictionary batches follow the IPC protocol. |
| Explicit field sections with `ui32` frame sizes; no alignment padding. | Buffer layout is described by FlatBuffer metadata and IPC body buffers are aligned. |
| Binary lengths omit null positions and use byte-stream-split with a 1/2/4-byte width. | Binary arrays use a fixed-width offset buffer with one logical slot per array element. |
| Dictionary values and positions share one accessor blob; accessor metadata gives the dictionary boundary. | Dictionary values are emitted as dictionary batches and indexes are an array buffer in a record batch. |
| A compressed frame has a `ui32` uncompressed-size prefix and uses the externally selected codec. | Each compressed body buffer has the IPC compression framing, including an `int64` uncompressed-size prefix. |

Arrow IPC's array-to-buffer traversal and per-buffer compression are implemented
in `contrib/libs/apache/arrow/cpp/src/arrow/ipc/writer.cc`. Dense encoding is
implemented in `encoding.cpp` and assembled by `constructors.cpp` in this
directory.
