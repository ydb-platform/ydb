LIBRARY()

SRCS(
    yt_codec_cg.cpp
    yt_codec_cg.h
)

PEERDIR(
    library/cpp/resource
    ydb/library/binary_json
    ydb/library/yql/minikql/computation/llvm14
    ydb/library/yql/parser/pg_wrapper/interface
    ydb/library/yql/utils
)

IF (NOT MKQL_DISABLE_CODEGEN)
    PEERDIR(
        ydb/library/yql/minikql/codegen/llvm14
    )
    USE_LLVM_BC14()
    LLVM_BC(
        yt_codec_bc.cpp
        NAME
        YtCodecFuncs
        SYMBOLS
        WriteJust
        WriteNothing
        WriteBool
        Write8
        Write16
        Write32
        Write64
        Write120
        WriteDecimal32
        WriteDecimal64
        WriteDecimal128
        WriteFloat
        WriteDouble
        WriteString
        ReadBool
        ReadInt8
        ReadUint8
        ReadInt16
        ReadUint16
        ReadInt32
        ReadUint32
        ReadInt64
        ReadUint64
        ReadInt120
        ReadDecimal32
        ReadDecimal64
        ReadDecimal128
        ReadFloat
        ReadDouble
        ReadOptional
        ReadVariantData
        SkipFixedData
        SkipVarData
        ReadTzDate
        ReadTzDatetime
        ReadTzTimestamp
        ReadTzDate32
        ReadTzDatetime64
        ReadTzTimestamp64
        WriteTzDate
        WriteTzDatetime
        WriteTzTimestamp
        WriteTzDate32
        WriteTzDatetime64
        WriteTzTimestamp64
        GetWrittenBytes
        FillZero
    )
ELSE()
    CFLAGS(
        -DMKQL_DISABLE_CODEGEN
    )
ENDIF()

YQL_LAST_ABI_VERSION()

PROVIDES(YT_CODEC_CODEGEN)

END()

RECURSE(
    no_llvm
)

RECURSE_FOR_TESTS(
    ut
)
