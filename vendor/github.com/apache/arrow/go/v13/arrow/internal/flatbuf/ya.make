GO_LIBRARY()

LICENSE(
    Apache-2.0 AND
    BSD-2-Clause AND
    BSD-3-Clause AND
    BSL-1.0 AND
    CC-BY-3.0 AND
    HPND AND
    MIT AND
    NCSA AND
    OpenSSL AND
    Zlib
)

SRCS(
    Binary.go
    Block.go
    BodyCompression.go
    BodyCompressionMethod.go
    Bool.go
    Buffer.go
    CompressionType.go
    Date.go
    DateUnit.go
    Decimal.go
    DictionaryBatch.go
    DictionaryEncoding.go
    DictionaryKind.go
    Duration.go
    Endianness.go
    Feature.go
    Field.go
    FieldNode.go
    FixedSizeBinary.go
    FixedSizeList.go
    FloatingPoint.go
    Footer.go
    Int.go
    Interval.go
    IntervalUnit.go
    KeyValue.go
    LargeBinary.go
    LargeList.go
    LargeUtf8.go
    List.go
    Map.go
    Message.go
    MessageHeader.go
    MetadataVersion.go
    Null.go
    Precision.go
    RecordBatch.go
    RunEndEncoded.go
    RunLengthEncoded.go
    Schema.go
    SparseMatrixCompressedAxis.go
    SparseMatrixIndexCSR.go
    SparseMatrixIndexCSX.go
    SparseTensor.go
    SparseTensorIndex.go
    SparseTensorIndexCOO.go
    SparseTensorIndexCSF.go
    Struct_.go
    Tensor.go
    TensorDim.go
    Time.go
    TimeUnit.go
    Timestamp.go
    Type.go
    Union.go
    UnionMode.go
    Utf8.go
)

END()
