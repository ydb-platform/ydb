SET(ORIG_SRC_DIR yt/yql/providers/yt/codec/codegen)

SET(ORIG_SOURCES
    yt_codec_cg.cpp
    yt_codec_bc.cpp
)

ADDINCL(
    ${ORIG_SRC_DIR}
)

PEERDIR(
    library/cpp/resource
    yql/essentials/types/binary_json
    yql/essentials/parser/pg_wrapper/interface
    yql/essentials/utils
    yt/yql/providers/yt/codec/codegen
    yt/yql/providers/yt/codec
)

IF (NOT MKQL_DISABLE_CODEGEN)
    LLVM_BC(
        yt_codec_bc.cpp
        NAME
        YtCodecFuncs
        SUFFIX .${LLVM_VER}
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

INCLUDE(${ARCADIA_ROOT}/yql/essentials/minikql/computation/header.ya.make.inc)

COPY(
    WITH_CONTEXT
    AUTO
    FROM ${ORIG_SRC_DIR}
    ${ORIG_SOURCES}
    OUTPUT_INCLUDES
        yql/essentials/minikql/codegen/codegen.h
        ${BINDIR}/yql/essentials/minikql/computation/mkql_computation_node_codegen.h
        llvm/IR/Module.h
        llvm/IR/Instructions.h
)
