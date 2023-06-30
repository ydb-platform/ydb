#pragma once

#include <vector>
#include <memory>

namespace NSkiff {

////////////////////////////////////////////////////////////////////////////////

enum class EWireType
{
    Nothing  /* "nothing" */,
    Int8     /* "int8" */,
    Int16    /* "int16" */,
    Int32    /* "int32" */,
    Int64    /* "int64" */,
    Int128   /* "int128" */,
    Uint8    /* "uint8" */,
    Uint16   /* "uint16" */,
    Uint32   /* "uint32" */,
    Uint64   /* "uint64" */,
    Uint128  /* "uint128" */,
    Double   /* "double" */,
    Boolean  /* "boolean" */,
    String32 /* "string32" */,
    Yson32   /* "yson32" */,

    Tuple             /* "tuple" */,
    Variant8          /* "variant8" */,
    Variant16         /* "variant16" */,
    RepeatedVariant8  /* "repeated_variant8" */,
    RepeatedVariant16 /* "repeated_variant16" */,
};

////////////////////////////////////////////////////////////////////////////////

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
