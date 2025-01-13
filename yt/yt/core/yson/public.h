#pragma once

#include <yt/yt/core/misc/public.h>
#include <yt/yt/core/misc/configurable_singleton_decl.h>

#include <library/cpp/yt/yson/public.h>
#include <library/cpp/yt/yson_string/public.h>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

enum class ETokenType;

class TYsonProducer;
template <class... TAdditionalArgs>
class TExtendedYsonProducer;

class TYsonInput;
class TYsonOutput;

class TUncheckedYsonTokenWriter;
class TCheckedYsonTokenWriter;

#ifdef NDEBUG
using TCheckedInDebugYsonTokenWriter = TUncheckedYsonTokenWriter;
#else
using TCheckedInDebugYsonTokenWriter = TCheckedYsonTokenWriter;
#endif

class TStatelessLexer;

class TTokenizer;

class TProtobufMessageType;

struct IFlushableYsonConsumer;
struct IAsyncYsonConsumer;

enum class EYsonItemType : ui8;
class TYsonItem;
class TYsonPullParser;
class TYsonPullParserCursor;

class TForwardingYsonConsumer;

DEFINE_ENUM(EUnknownYsonFieldsMode,
    (Skip)
    (Fail)
    (Keep)
    (Forward)
);

////////////////////////////////////////////////////////////////////////////////

//
// We need two limits for YSON parsing:
//  - the smaller (CypressWriteNestingLevelLimit) is used for commands like set or create.
//  - the larger (NewNestingLevelLimit) is used elsewhere.
// Thus we try to avoid the problem when we cannot read
// a value that was written successfully, i.e. get("//a/@") after set("//a/@x", <deep yson>).
// See YT-15698.
constexpr int OriginalNestingLevelLimit = 64;
constexpr int CypressWriteNestingLevelLimit = 128;
constexpr int NewNestingLevelLimit = 256;

constexpr int DefaultYsonParserNestingLevelLimit = NewNestingLevelLimit;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EUtf8Check,
    (Disable)
    (LogOnFail)
    (ThrowOnFail)
);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EEnumYsonStorageType,
    (String)
    (Int)
);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TProtobufInteropConfig);
DECLARE_REFCOUNTED_CLASS(TProtobufInteropDynamicConfig);

////////////////////////////////////////////////////////////////////////////////

//! An opaque reflected counterpart of ::google::protobuf::Descriptor.
/*!
 *  Reflecting a descriptor takes the following options into account:
 *  NYT.NProto.NYson.field_name:      overrides the default name of field
 *  NYT.NProto.NYson.enum_value_name: overrides the default name of enum value
 */
class TProtobufMessageType;

//! An opaque reflected counterpart of ::google::protobuf::EnumDescriptor.
class TProtobufEnumType;

YT_DECLARE_RECONFIGURABLE_SINGLETON(TProtobufInteropConfig, TProtobufInteropDynamicConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
