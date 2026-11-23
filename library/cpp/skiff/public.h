#pragma once

#include <util/system/types.h>

#include <vector>
#include <memory>
#include <limits>

namespace NSkiff {

////////////////////////////////////////////////////////////////////////////////

enum class EWireType
{
    Nothing           /* "nothing" */,
    Boolean           /* "boolean" */,
    Int8              /* "int8" */,
    Int16             /* "int16" */,
    Int32             /* "int32" */,
    Int64             /* "int64" */,
    Int128            /* "int128" */,
    Int256            /* "int256" */,
    VarInt32          /* "var_int32" */,
    VarInt64          /* "var_int64" */,
    Uint8             /* "uint8" */,
    Uint16            /* "uint16" */,
    Uint32            /* "uint32" */,
    Uint64            /* "uint64" */,
    Uint128           /* "uint128" */,
    Uint256           /* "uint256" */,
    Float             /* "float" */,
    Double            /* "double" */,
    String32          /* "string32" */,
    StringVar         /* "string_var" */,
    Yson32            /* "yson32" */,

    StringFixed       /* "string_fixed" */,

    Tuple             /* "tuple" */,
    Variant8          /* "variant8" */,
    Variant16         /* "variant16" */,
    VariantVar        /* "variant_var" */,
    RepeatedVariant8  /* "repeated_variant8" */,
    RepeatedVariant16 /* "repeated_variant16" */,
    RepeatedBlockVar  /* "repeated_block_var" */,
};

////////////////////////////////////////////////////////////////////////////////

// 4GB - 1.
constexpr i64 MaxStringLength = std::numeric_limits<ui32>::max();

////////////////////////////////////////////////////////////////////////////////

struct TBlockVarHeader;

class TSkiffSchema;
using TSkiffSchemaPtr = std::shared_ptr<TSkiffSchema>;

using TSkiffSchemaList = std::vector<TSkiffSchemaPtr>;

class TSimpleTypeSchema;
using TSimpleTypeSchemaPtr = std::shared_ptr<TSimpleTypeSchema>;

class TSkiffValidator;

class TUncheckedSkiffParser;
class TCheckedSkiffParser;

class TUncheckedSkiffWriter;
class TCheckedSkiffWriter;

#ifdef DEBUG
using TCheckedInDebugSkiffParser = TCheckedSkiffParser;
using TCheckedInDebugSkiffWriter = TCheckedSkiffWriter;
#else
using TCheckedInDebugSkiffParser = TUncheckedSkiffParser;
using TCheckedInDebugSkiffWriter = TUncheckedSkiffWriter;
#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkiff
