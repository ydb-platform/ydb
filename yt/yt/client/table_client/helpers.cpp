#include "helpers.h"
#include "schema.h"
#include "name_table.h"
#include "key_bound.h"
#include "composite_compare.h"

#include <yt/yt_proto/yt/client/table_chunk_format/proto/chunk_meta.pb.h>

#include <yt/yt/core/ytree/attributes.h>
#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/yson/parser.h>
#include <yt/yt/core/yson/protobuf_interop.h>
#include <yt/yt/core/yson/token_writer.h>

#include <library/cpp/yt/coding/varint.h>

#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

namespace NYT::NTableClient {

using namespace NYTree;
using namespace NYson;
using namespace NNet;
using namespace NChunkClient;

using namespace google::protobuf;
using namespace google::protobuf::io;

////////////////////////////////////////////////////////////////////////////////

bool IsValidTableChunkFormat(EChunkFormat chunkFormat)
{
    return
        chunkFormat == EChunkFormat::TableUnversionedSchemaful ||
        chunkFormat == EChunkFormat::TableUnversionedSchemalessHorizontal ||
        chunkFormat == EChunkFormat::TableUnversionedColumnar ||
        chunkFormat == EChunkFormat::TableVersionedSimple ||
        chunkFormat == EChunkFormat::TableVersionedIndexed ||
        chunkFormat == EChunkFormat::TableVersionedColumnar ||
        chunkFormat == EChunkFormat::TableVersionedSlim;
}

bool IsTableChunkFormatVersioned(EChunkFormat chunkFormat)
{
    return
        chunkFormat == EChunkFormat::TableVersionedSimple ||
        chunkFormat == EChunkFormat::TableVersionedIndexed ||
        chunkFormat == EChunkFormat::TableVersionedColumnar ||
        chunkFormat == EChunkFormat::TableVersionedSlim;
}

void ValidateTableChunkFormat(EChunkFormat chunkFormat)
{
    if (!IsValidTableChunkFormat(chunkFormat)) {
        THROW_ERROR_EXCEPTION(
            NTableClient::EErrorCode::InvalidTableChunkFormat,
            "%Qlv is not a valid table chunk format",
            chunkFormat);
    }
}

void ValidateTableChunkFormatAndOptimizeFor(
    EChunkFormat chunkFormat,
    EOptimizeFor optimizeFor)
{
    ValidateTableChunkFormat(chunkFormat);
    if (OptimizeForFromFormat(chunkFormat) != optimizeFor) {
        THROW_ERROR_EXCEPTION(
            NTableClient::EErrorCode::InvalidTableChunkFormat,
            "%Qlv is not a valid %Qlv chunk format",
            chunkFormat,
            optimizeFor);
    }
}

void ValidateTableChunkFormatVersioned(
    EChunkFormat chunkFormat,
    bool versioned)
{
    if (IsTableChunkFormatVersioned(chunkFormat) != versioned) {
        THROW_ERROR_EXCEPTION(
            NTableClient::EErrorCode::InvalidTableChunkFormat,
            "%Qlv is not a valid %v chunk format",
            chunkFormat,
            versioned ? "versioned" : "unversioned");
    }
}

EOptimizeFor OptimizeForFromFormat(EChunkFormat chunkFormat)
{
    ValidateTableChunkFormat(chunkFormat);
    switch (chunkFormat) {
        case EChunkFormat::TableUnversionedSchemaful:
        case EChunkFormat::TableUnversionedSchemalessHorizontal:
        case EChunkFormat::TableVersionedSimple:
        case EChunkFormat::TableVersionedIndexed:
        case EChunkFormat::TableVersionedSlim:
            return EOptimizeFor::Lookup;

        case EChunkFormat::TableUnversionedColumnar:
        case EChunkFormat::TableVersionedColumnar:
            return EOptimizeFor::Scan;

        default:
            YT_ABORT();
    }
}

EChunkFormat DefaultFormatFromOptimizeFor(
    EOptimizeFor optimizeFor,
    bool versioned)
{
    if (versioned) {
        switch (optimizeFor) {
            case EOptimizeFor::Lookup:
                return EChunkFormat::TableVersionedSimple;
            case EOptimizeFor::Scan:
                return EChunkFormat::TableVersionedColumnar;
            default:
                YT_ABORT();
        }
    } else {
        switch (optimizeFor) {
            case EOptimizeFor::Lookup:
                return EChunkFormat::TableUnversionedSchemalessHorizontal;
            case EOptimizeFor::Scan:
                return EChunkFormat::TableUnversionedColumnar;
            default:
                YT_ABORT();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

namespace {

void YTreeNodeToUnversionedValue(
    TUnversionedOwningRowBuilder* builder,
    const INodePtr& value,
    int id,
    EValueFlags flags)
{
    switch (value->GetType()) {
        #define XX(type, cppType) \
        case ENodeType::type: \
            builder->AddValue(MakeUnversioned ## type ## Value(value->As ## type()->GetValue(), id, flags)); \
            break;
        ITERATE_SCALAR_YTREE_NODE_TYPES(XX)
        #undef XX
        case ENodeType::Entity:
            builder->AddValue(MakeUnversionedSentinelValue(EValueType::Null, id, flags));
            break;
        default:
            builder->AddValue(MakeUnversionedAnyValue(ConvertToYsonString(value).AsStringBuf(), id, flags));
            break;
    }
}

} // namespace

TUnversionedOwningRow YsonToSchemafulRow(
    const TString& yson,
    const TTableSchema& tableSchema,
    bool treatMissingAsNull,
    NYson::EYsonType ysonType,
    bool validateValues)
{
    auto nameTable = TNameTable::FromSchema(tableSchema);

    auto rowParts = ConvertTo<THashMap<TString, INodePtr>>(
        TYsonString(yson, ysonType));

    TUnversionedOwningRowBuilder rowBuilder;
    auto validateAndAddValue = [&rowBuilder, &validateValues] (const TUnversionedValue& value, const TColumnSchema& column) {
        if (validateValues) {
            ValidateValueType(
                value,
                column,
                /*typeAnyAcceptsAllValues*/ true,
                /*ignoreRequired*/ false,
                /*validateAnyIsValidYson*/ false);
        }

        rowBuilder.AddValue(value);
    };

    auto addValue = [&] (int id, INodePtr value) {
        try {
            auto column = tableSchema.Columns()[id];
            if (value->GetType() == ENodeType::Entity) {
                validateAndAddValue(MakeUnversionedSentinelValue(
                    value->Attributes().Get<EValueType>("type", EValueType::Null), id), column);
                return;
            }

            auto type = column.GetWireType();
            switch (type) {
                #define XX(type, cppType) \
                case EValueType::type: \
                    validateAndAddValue(MakeUnversioned ## type ## Value(value->As ## type()->GetValue(), id), column); \
                    break;
                ITERATE_SCALAR_YTREE_NODE_TYPES(XX)
                #undef XX
                case EValueType::Any:
                    validateAndAddValue(MakeUnversionedAnyValue(ConvertToYsonString(value).AsStringBuf(), id), column);
                    break;
                case EValueType::Composite:
                    validateAndAddValue(MakeUnversionedCompositeValue(ConvertToYsonString(value).AsStringBuf(), id), column);
                    break;
                default:
                    THROW_ERROR_EXCEPTION("Unsupported value type %Qlv",
                        type);
            }
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing value of column %Qv",
                tableSchema.Columns()[id].Name())
                << ex;
        }
    };

    const auto& keyColumns = tableSchema.GetKeyColumns();

    // Key
    for (int id = 0; id < std::ssize(keyColumns); ++id) {
        auto it = rowParts.find(nameTable->GetName(id));
        if (it == rowParts.end()) {
            validateAndAddValue(MakeUnversionedSentinelValue(EValueType::Null, id), tableSchema.Columns()[id]);
        } else {
            addValue(id, it->second);
        }
    }

    // Fixed values
    for (int id = std::ssize(keyColumns); id < std::ssize(tableSchema.Columns()); ++id) {
        auto it = rowParts.find(nameTable->GetName(id));
        if (it != rowParts.end()) {
            addValue(id, it->second);
        } else if (treatMissingAsNull) {
            validateAndAddValue(MakeUnversionedSentinelValue(EValueType::Null, id), tableSchema.Columns()[id]);
        } else if (validateValues && tableSchema.Columns()[id].Required()) {
            THROW_ERROR_EXCEPTION(
                EErrorCode::SchemaViolation,
                "Required column %v cannot have %Qlv value",
                tableSchema.Columns()[id].GetDiagnosticNameString(),
                EValueType::Null);
        }
    }

    // Variable values
    for (const auto& [name, value] : rowParts) {
        int id = nameTable->GetIdOrRegisterName(name);
        if (id >= std::ssize(tableSchema.Columns())) {
            if (validateValues && tableSchema.GetStrict()) {
                THROW_ERROR_EXCEPTION(
                    EErrorCode::SchemaViolation,
                    "Unknown column %Qv in strict schema",
                    name);
            }
            YTreeNodeToUnversionedValue(&rowBuilder, value, id, EValueFlags::None);
        }
    }

    return rowBuilder.FinishRow();
}

TUnversionedOwningRow YsonToSchemalessRow(const TString& valueYson)
{
    TUnversionedOwningRowBuilder builder;

    auto values = ConvertTo<std::vector<INodePtr>>(TYsonString(valueYson, EYsonType::ListFragment));
    for (const auto& value : values) {
        int id = value->Attributes().Get<int>("id");
        auto flags = EValueFlags::None;
        if (value->Attributes().Get<bool>("aggregate", false)) {
            flags |= EValueFlags::Aggregate;
        }
        YTreeNodeToUnversionedValue(&builder, value, id, flags);
    }

    return builder.FinishRow();
}

TVersionedRow YsonToVersionedRow(
    const TRowBufferPtr& rowBuffer,
    const TString& keyYson,
    const TString& valueYson,
    const std::vector<TTimestamp>& deleteTimestamps,
    const std::vector<TTimestamp>& extraWriteTimestamps)
{
    TVersionedRowBuilder builder(rowBuffer);

    auto keys = ConvertTo<std::vector<INodePtr>>(TYsonString(keyYson, EYsonType::ListFragment));

    for (auto key : keys) {
        int id = key->Attributes().Get<int>("id");
        switch (key->GetType()) {
            #define XX(type, cppType) \
            case ENodeType::type: \
                builder.AddKey(MakeUnversioned ## type ## Value(key->As ## type()->GetValue(), id)); \
                break;
            ITERATE_SCALAR_YTREE_NODE_TYPES(XX)
            #undef XX
            case ENodeType::Entity:
                builder.AddKey(MakeUnversionedSentinelValue(EValueType::Null, id));
                break;
            default:
                YT_ABORT();
                break;
        }
    }

    auto values = ConvertTo<std::vector<INodePtr>>(TYsonString(valueYson, EYsonType::ListFragment));
    for (auto value : values) {
        int id = value->Attributes().Get<int>("id");
        auto timestamp = value->Attributes().Get<TTimestamp>("ts");
        auto flags = EValueFlags::None;
        if (value->Attributes().Get<bool>("aggregate", false)) {
            flags |= EValueFlags::Aggregate;
        }
        switch (value->GetType()) {
            #define XX(type, cppType) \
            case ENodeType::type: \
                builder.AddValue(MakeVersioned ## type ## Value(value->As ## type()->GetValue(), timestamp, id, flags)); \
                break;
            ITERATE_SCALAR_YTREE_NODE_TYPES(XX)
            #undef XX
            case ENodeType::Entity:
                builder.AddValue(MakeVersionedSentinelValue(EValueType::Null, timestamp, id, flags));
                break;
            default:
                builder.AddValue(MakeVersionedAnyValue(ConvertToYsonString(value).AsStringBuf(), timestamp, id, flags));
                break;
        }
    }

    for (auto timestamp : deleteTimestamps) {
        builder.AddDeleteTimestamp(timestamp);
    }

    for (auto timestamp : extraWriteTimestamps) {
        builder.AddWriteTimestamp(timestamp);
    }

    return builder.FinishRow();
}

TVersionedOwningRow YsonToVersionedRow(
    const TString& keyYson,
    const TString& valueYson,
    const std::vector<TTimestamp>& deleteTimestamps,
    const std::vector<TTimestamp>& extraWriteTimestamps)
{
    // NB: this implementation is extra slow, it is intended only for using in tests.
    auto rowBuffer = New<TRowBuffer>();
    auto row = YsonToVersionedRow(rowBuffer, keyYson, valueYson, deleteTimestamps, extraWriteTimestamps);
    return TVersionedOwningRow(row);
}

TUnversionedOwningRow YsonToKey(const TString& yson)
{
    TUnversionedOwningRowBuilder keyBuilder;
    auto keyParts = ConvertTo<std::vector<INodePtr>>(
        TYsonString(yson, EYsonType::ListFragment));

    for (int id = 0; id < std::ssize(keyParts); ++id) {
        const auto& keyPart = keyParts[id];
        switch (keyPart->GetType()) {
            #define XX(type, cppType) \
            case ENodeType::type: \
                keyBuilder.AddValue( \
                    MakeUnversioned ## type ## Value(keyPart->As ## type()->GetValue(), \
                    id)); \
                break;
            ITERATE_SCALAR_YTREE_NODE_TYPES(XX)
            #undef XX
            case ENodeType::Entity:
                keyBuilder.AddValue(MakeUnversionedSentinelValue(
                    keyPart->Attributes().Get<EValueType>("type", EValueType::Null),
                    id));
                break;
            default:
                keyBuilder.AddValue(MakeUnversionedAnyValue(
                    ConvertToYsonString(keyPart).AsStringBuf(),
                    id));
                break;
        }
    }

    return keyBuilder.FinishRow();
}

TString KeyToYson(TUnversionedRow row)
{
    return ConvertToYsonString(row, EYsonFormat::Text).ToString();
}

////////////////////////////////////////////////////////////////////////////////

void ToUnversionedValue(
    TUnversionedValue* unversionedValue,
    std::nullopt_t,
    const TRowBufferPtr& /*rowBuffer*/,
    int id,
    EValueFlags flags)
{
    *unversionedValue = MakeUnversionedSentinelValue(EValueType::Null, id, flags);
}

////////////////////////////////////////////////////////////////////////////////

void ToUnversionedValue(
    TUnversionedValue* unversionedValue,
    TGuid value,
    const TRowBufferPtr& rowBuffer,
    int id,
    EValueFlags flags)
{
    std::array<char, MaxGuidStringSize> buffer;
    auto* bufferEnd = WriteGuidToBuffer(buffer.data(), value);
    TStringBuf bufferStr(buffer.begin(), bufferEnd);
    *unversionedValue = value
        ? rowBuffer->CaptureValue(MakeUnversionedStringValue(bufferStr, id, flags))
        : MakeUnversionedSentinelValue(EValueType::Null, id, flags);
}

void FromUnversionedValue(TGuid* value, TUnversionedValue unversionedValue)
{
    if (unversionedValue.Type == EValueType::Null) {
        *value = TGuid();
        return;
    }
    if (unversionedValue.Type != EValueType::String) {
        THROW_ERROR_EXCEPTION("Cannot parse object id value from %Qlv",
            unversionedValue.Type);
    }
    *value = TGuid::FromString(unversionedValue.AsStringBuf());
}

////////////////////////////////////////////////////////////////////////////////

void ToUnversionedValue(
    TUnversionedValue* unversionedValue,
    const TString& value,
    const TRowBufferPtr& rowBuffer,
    int id,
    EValueFlags flags)
{
    ToUnversionedValue(unversionedValue, static_cast<TStringBuf>(value), rowBuffer, id, flags);
}

void FromUnversionedValue(TString* value, TUnversionedValue unversionedValue)
{
    TStringBuf uncapturedValue;
    FromUnversionedValue(&uncapturedValue, unversionedValue);
    *value = TString(uncapturedValue);
}

////////////////////////////////////////////////////////////////////////////////

void ToUnversionedValue(
    TUnversionedValue* unversionedValue,
    TStringBuf value,
    const TRowBufferPtr& rowBuffer,
    int id,
    EValueFlags flags)
{
    *unversionedValue = rowBuffer->CaptureValue(MakeUnversionedStringValue(value, id, flags));
}

void FromUnversionedValue(TStringBuf* value, TUnversionedValue unversionedValue)
{
    if (unversionedValue.Type == EValueType::Null) {
        *value = TStringBuf{};
        return;
    }
    if (unversionedValue.Type != EValueType::String) {
        THROW_ERROR_EXCEPTION("Cannot parse string value from %Qlv",
            unversionedValue.Type);
    }
    *value = unversionedValue.AsStringBuf();
}

////////////////////////////////////////////////////////////////////////////////

void ToUnversionedValue(
    TUnversionedValue* unversionedValue,
    const char* value,
    const TRowBufferPtr& rowBuffer,
    int id,
    EValueFlags flags)
{
    ToUnversionedValue(unversionedValue, TStringBuf(value), rowBuffer, id, flags);
}

void FromUnversionedValue(const char** value, TUnversionedValue unversionedValue)
{
    *value = FromUnversionedValue<TStringBuf>(unversionedValue).data();
}

////////////////////////////////////////////////////////////////////////////////

void ToUnversionedValue(
    TUnversionedValue* unversionedValue,
    bool value,
    const TRowBufferPtr& rowBuffer,
    int id,
    EValueFlags flags)
{
    *unversionedValue = rowBuffer->CaptureValue(MakeUnversionedBooleanValue(value, id, flags));
}

void FromUnversionedValue(bool* value, TUnversionedValue unversionedValue)
{
    if (unversionedValue.Type == EValueType::Null) {
        *value = false;
        return;
    }
    if (unversionedValue.Type != EValueType::Boolean) {
        THROW_ERROR_EXCEPTION("Cannot parse \"boolean\" value from %Qlv",
            unversionedValue.Type);
    }
    *value = unversionedValue.Data.Boolean;
}

////////////////////////////////////////////////////////////////////////////////

void ToUnversionedValue(
    TUnversionedValue* unversionedValue,
    const TYsonString& value,
    const TRowBufferPtr& rowBuffer,
    int id,
    EValueFlags flags)
{
    YT_ASSERT(value.GetType() == EYsonType::Node);
    *unversionedValue = rowBuffer->CaptureValue(MakeUnversionedAnyValue(value.AsStringBuf(), id, flags));
}

void FromUnversionedValue(TYsonString* value, TUnversionedValue unversionedValue)
{
    if (!IsAnyOrComposite(unversionedValue.Type)) {
        THROW_ERROR_EXCEPTION("Cannot parse YSON string from %Qlv",
            unversionedValue.Type);
    }
    *value = TYsonString(unversionedValue.AsString());
}

////////////////////////////////////////////////////////////////////////////////

void ToUnversionedValue(
    TUnversionedValue* unversionedValue,
    const NYson::TYsonStringBuf& value,
    const TRowBufferPtr& rowBuffer,
    int id,
    EValueFlags flags)
{
    YT_ASSERT(value.GetType() == EYsonType::Node);
    *unversionedValue = rowBuffer->CaptureValue(MakeUnversionedAnyValue(value.AsStringBuf(), id, flags));
}

void FromUnversionedValue(NYson::TYsonStringBuf* value, TUnversionedValue unversionedValue)
{
    if (!IsAnyOrComposite(unversionedValue.Type)) {
        THROW_ERROR_EXCEPTION("Cannot parse YSON string from %Qlv",
            unversionedValue.Type);
    }
    *value = TYsonStringBuf(unversionedValue.AsStringBuf());
}

////////////////////////////////////////////////////////////////////////////////

#define XX(cppType, codeType, humanReadableType) \
    void ToUnversionedValue( \
        TUnversionedValue* unversionedValue, \
        cppType value, \
        const TRowBufferPtr& /*rowBuffer*/, \
        int id, \
        EValueFlags flags) \
    { \
        *unversionedValue = MakeUnversioned ## codeType ## Value(value, id, flags); \
    } \
    \
    void FromUnversionedValue(cppType* value, TUnversionedValue unversionedValue) \
    { \
        switch (unversionedValue.Type) { \
            case EValueType::Int64: \
                *value = CheckedIntegralCast<cppType>(unversionedValue.Data.Int64); \
                break; \
            case EValueType::Uint64: \
                *value = CheckedIntegralCast<cppType>(unversionedValue.Data.Uint64); \
                break; \
            default: \
                THROW_ERROR_EXCEPTION("Cannot parse \"" #humanReadableType "\" value from %Qlv", \
                    unversionedValue.Type); \
        } \
    }

XX(i64,  Int64,  int64)
XX(ui64, Uint64, uint64)
XX(i32,  Int64,  int32)
XX(ui32, Uint64, uint32)
XX(i16,  Int64,  int32)
XX(ui16, Uint64, uint16)
XX(i8,   Int64,  int8)
XX(ui8,  Uint64, uint8)

#undef XX

////////////////////////////////////////////////////////////////////////////////

void ToUnversionedValue(
    TUnversionedValue* unversionedValue,
    double value,
    const TRowBufferPtr& /*rowBuffer*/,
    int id,
    EValueFlags flags)
{
    *unversionedValue = MakeUnversionedDoubleValue(value, id, flags);
}

void FromUnversionedValue(double* value, TUnversionedValue unversionedValue)
{
    if (unversionedValue.Type != EValueType::Double) {
        THROW_ERROR_EXCEPTION("Cannot parse \"double\" value from %Qlv",
            unversionedValue.Type);
    }
    *value = unversionedValue.Data.Double;
}

////////////////////////////////////////////////////////////////////////////////

void ToUnversionedValue(
    TUnversionedValue* unversionedValue,
    TInstant value,
    const TRowBufferPtr& /*rowBuffer*/,
    int id,
    EValueFlags flags)
{
    *unversionedValue = MakeUnversionedUint64Value(value.MicroSeconds(), id, flags);
}

void FromUnversionedValue(TInstant* value, TUnversionedValue unversionedValue)
{
    switch (unversionedValue.Type) {
        case EValueType::Int64:
            *value = TInstant::MicroSeconds(CheckedIntegralCast<ui64>(unversionedValue.Data.Int64));
            break;
        case EValueType::Uint64:
            *value = TInstant::MicroSeconds(unversionedValue.Data.Uint64);
            break;
        case EValueType::String:
            *value = TInstant::ParseIso8601(unversionedValue.AsStringBuf());
            break;
        default:
            THROW_ERROR_EXCEPTION("Cannot parse instant from %Qlv",
                unversionedValue.Type);
    }
}

////////////////////////////////////////////////////////////////////////////////

void ToUnversionedValue(
    TUnversionedValue* unversionedValue,
    TDuration value,
    const TRowBufferPtr& /*rowBuffer*/,
    int id,
    EValueFlags flags)
{
    *unversionedValue = MakeUnversionedUint64Value(value.MicroSeconds(), id, flags);
}

void FromUnversionedValue(TDuration* value, TUnversionedValue unversionedValue)
{
    switch (unversionedValue.Type) {
        case EValueType::Int64:
            *value = TDuration::MicroSeconds(CheckedIntegralCast<ui64>(unversionedValue.Data.Int64));
            break;
        case EValueType::Uint64:
            *value = TDuration::MicroSeconds(unversionedValue.Data.Uint64);
            break;
        default:
            THROW_ERROR_EXCEPTION("Cannot parse duration from %Qlv",
                unversionedValue.Type);
    }
}

////////////////////////////////////////////////////////////////////////////////

void ToUnversionedValue(
    TUnversionedValue* unversionedValue,
    const IMapNodePtr& value,
    const TRowBufferPtr& rowBuffer,
    int id,
    EValueFlags flags)
{
    *unversionedValue = rowBuffer->CaptureValue(MakeUnversionedAnyValue(ConvertToYsonString(value).AsStringBuf(), id, flags));
}

void FromUnversionedValue(IMapNodePtr* value, TUnversionedValue unversionedValue)
{
    if (unversionedValue.Type == EValueType::Null) {
        *value = nullptr;
    }
    if (unversionedValue.Type != EValueType::Any) {
        THROW_ERROR_EXCEPTION("Cannot parse YSON map from %Qlv",
            unversionedValue.Type);
    }
    *value = ConvertTo<IMapNodePtr>(FromUnversionedValue<TYsonStringBuf>(unversionedValue));
}

////////////////////////////////////////////////////////////////////////////////

void ToUnversionedValue(
    TUnversionedValue* unversionedValue,
    const TIP6Address& value,
    const TRowBufferPtr& rowBuffer,
    int id,
    EValueFlags flags)
{
    ToUnversionedValue(unversionedValue, ToString(value), rowBuffer, id, flags);
}

void FromUnversionedValue(TIP6Address* value, TUnversionedValue unversionedValue)
{
    if (unversionedValue.Type == EValueType::Null) {
        *value = TIP6Address();
    }
    auto strValue = FromUnversionedValue<TString>(unversionedValue);
    *value = TIP6Address::FromString(strValue);
}

////////////////////////////////////////////////////////////////////////////////

void ToUnversionedValue(
    TUnversionedValue* unversionedValue,
    const TError& value,
    const TRowBufferPtr& rowBuffer,
    int id,
    EValueFlags flags)
{
    auto errorYson = ConvertToYsonString(value);
    *unversionedValue = rowBuffer->CaptureValue(MakeUnversionedAnyValue(errorYson.AsStringBuf(), id, flags));
}

void FromUnversionedValue(TError* value, TUnversionedValue unversionedValue)
{
    if (unversionedValue.Type == EValueType::Null) {
        *value = {};
    }
    if (unversionedValue.Type != EValueType::Any) {
        THROW_ERROR_EXCEPTION(
            "Cannot parse error from value of type %Qlv",
            unversionedValue.Type);
    }
    *value = ConvertTo<TError>(FromUnversionedValue<TYsonStringBuf>(unversionedValue));
}

////////////////////////////////////////////////////////////////////////////////

void ProtobufToUnversionedValueImpl(
    TUnversionedValue* unversionedValue,
    const Message& value,
    const TProtobufMessageType* type,
    const TRowBufferPtr& rowBuffer,
    int id,
    EValueFlags flags)
{
    auto byteSize = value.ByteSizeLong();
    auto* pool = rowBuffer->GetPool();
    auto* wireBuffer = pool->AllocateUnaligned(byteSize);
    YT_VERIFY(value.SerializePartialToArray(wireBuffer, byteSize));
    ArrayInputStream inputStream(wireBuffer, byteSize);
    TString ysonBytes;
    TStringOutput outputStream(ysonBytes);
    TYsonWriter ysonWriter(&outputStream);
    ParseProtobuf(&ysonWriter, &inputStream, type);
    *unversionedValue = rowBuffer->CaptureValue(MakeUnversionedAnyValue(ysonBytes, id, flags));
}

////////////////////////////////////////////////////////////////////////////////

void UnversionedValueToProtobufImpl(
    Message* value,
    const TProtobufMessageType* type,
    TUnversionedValue unversionedValue)
{
    if (unversionedValue.Type == EValueType::Null) {
        value->Clear();
        return;
    }
    if (unversionedValue.Type != EValueType::Any) {
        THROW_ERROR_EXCEPTION("Cannot parse a protobuf message from %Qlv",
            unversionedValue.Type);
    }
    TString wireBytes;
    StringOutputStream outputStream(&wireBytes);
    TProtobufWriterOptions options;
    options.UnknownYsonFieldModeResolver = TProtobufWriterOptions::CreateConstantUnknownYsonFieldModeResolver(EUnknownYsonFieldsMode::Keep);
    auto protobufWriter = CreateProtobufWriter(&outputStream, type, options);
    ParseYsonStringBuffer(
        unversionedValue.AsStringBuf(),
        EYsonType::Node,
        protobufWriter.get());
    if (!value->ParseFromArray(wireBytes.data(), wireBytes.size())) {
        THROW_ERROR_EXCEPTION("Error parsing %v from wire bytes",
            value->GetTypeName());
    }
}

////////////////////////////////////////////////////////////////////////////////

void ListToUnversionedValueImpl(
    TUnversionedValue* unversionedValue,
    const std::function<bool(TUnversionedValue*)> producer,
    const TRowBufferPtr& rowBuffer,
    int id,
    EValueFlags flags)
{
    TString ysonBytes;
    TStringOutput outputStream(ysonBytes);
    NYT::NYson::TYsonWriter writer(&outputStream);
    writer.OnBeginList();

    TUnversionedValue itemValue;
    while (true) {
        writer.OnListItem();
        if (!producer(&itemValue)) {
            break;
        }
        UnversionedValueToYson(itemValue, &writer);
    }
    writer.OnEndList();

    *unversionedValue = rowBuffer->CaptureValue(MakeUnversionedAnyValue(ysonBytes, id, flags));
}

void UnversionedValueToListImpl(
    std::function<google::protobuf::Message*()> appender,
    const TProtobufMessageType* type,
    TUnversionedValue unversionedValue)
{
    if (unversionedValue.Type == EValueType::Null) {
        return;
    }

    if (unversionedValue.Type != EValueType::Any) {
        THROW_ERROR_EXCEPTION("Cannot parse vector from %Qlv",
            unversionedValue.Type);
    }

    class TConsumer
        : public IYsonConsumer
    {
    public:
        TConsumer(
            std::function<google::protobuf::Message*()> appender,
            const TProtobufMessageType* type)
            : Appender_(std::move(appender))
            , Type_(type)
            , OutputStream_(&WireBytes_)
        { }

        void OnStringScalar(TStringBuf value) override
        {
            GetUnderlying()->OnStringScalar(value);
        }

        void OnInt64Scalar(i64 value) override
        {
            GetUnderlying()->OnInt64Scalar(value);
        }

        void OnUint64Scalar(ui64 value) override
        {
            GetUnderlying()->OnUint64Scalar(value);
        }

        void OnDoubleScalar(double value) override
        {
            GetUnderlying()->OnDoubleScalar(value);
        }

        void OnBooleanScalar(bool value) override
        {
            GetUnderlying()->OnBooleanScalar(value);
        }

        void OnEntity() override
        {
            GetUnderlying()->OnEntity();
        }

        void OnBeginList() override
        {
            if (Depth_ > 0) {
                GetUnderlying()->OnBeginList();
            }
            ++Depth_;
        }

        void OnListItem() override
        {
            if (Depth_ == 1) {
                NextElement();
            } else {
                GetUnderlying()->OnListItem();
            }
        }

        void OnEndList() override
        {
            --Depth_;
            if (Depth_ == 0) {
                FlushElement();
            } else {
                GetUnderlying()->OnEndList();
            }
        }

        void OnBeginMap() override
        {
            ++Depth_;
            GetUnderlying()->OnBeginMap();
        }

        void OnKeyedItem(TStringBuf key) override
        {
            GetUnderlying()->OnKeyedItem(key);
        }

        void OnEndMap() override
        {
            --Depth_;
            GetUnderlying()->OnEndMap();
        }

        void OnBeginAttributes() override
        {
            GetUnderlying()->OnBeginAttributes();
        }

        void OnEndAttributes() override
        {
            GetUnderlying()->OnEndAttributes();
        }

        void OnRaw(TStringBuf yson, EYsonType type) override
        {
            GetUnderlying()->OnRaw(yson, type);
        }

    private:
        const std::function<google::protobuf::Message*()> Appender_;
        const TProtobufMessageType* const Type_;

        std::unique_ptr<IYsonConsumer> Underlying_;
        int Depth_ = 0;

        TString WireBytes_;
        StringOutputStream OutputStream_;


        IYsonConsumer* GetUnderlying()
        {
            if (!Underlying_) {
                THROW_ERROR_EXCEPTION("YSON value must be a list without attributes");
            }
            return Underlying_.get();
        }

        void NextElement()
        {
            FlushElement();
            WireBytes_.clear();
            Underlying_ = CreateProtobufWriter(&OutputStream_, Type_);
        }

        void FlushElement()
        {
            if (!Underlying_) {
                return;
            }
            auto* value = Appender_();
            if (!value->ParseFromArray(WireBytes_.data(), WireBytes_.size())) {
                THROW_ERROR_EXCEPTION("Error parsing %v from wire bytes",
                    value->GetTypeName());
            }
            Underlying_.reset();
        }
    } consumer(std::move(appender), type);

    ParseYsonStringBuffer(
        unversionedValue.AsStringBuf(),
        EYsonType::Node,
        &consumer);
}

void UnversionedValueToListImpl(
    std::function<void(TUnversionedValue)> appender,
    TUnversionedValue unversionedValue)
{
    if (unversionedValue.Type == EValueType::Null) {
        return;
    }

    if (unversionedValue.Type != EValueType::Any) {
        THROW_ERROR_EXCEPTION("Cannot parse a vector from %Qlv",
            unversionedValue.Type);
    }

    class TConsumer
        : public TYsonConsumerBase
    {
    public:
        explicit TConsumer(std::function<void(TUnversionedValue)> appender)
            : Appender_(std::move(appender))
        { }

        void OnStringScalar(TStringBuf value) override
        {
            EnsureInList();
            Appender_(MakeUnversionedStringValue(value));
        }

        void OnInt64Scalar(i64 value) override
        {
            EnsureInList();
            Appender_(MakeUnversionedInt64Value(value));
        }

        void OnUint64Scalar(ui64 value) override
        {
            EnsureInList();
            Appender_(MakeUnversionedUint64Value(value));
        }

        void OnDoubleScalar(double value) override
        {
            EnsureInList();
            Appender_(MakeUnversionedDoubleValue(value));
        }

        void OnBooleanScalar(bool value) override
        {
            EnsureInList();
            Appender_(MakeUnversionedBooleanValue(value));
        }

        void OnEntity() override
        {
            THROW_ERROR_EXCEPTION("YSON entities are not supported");
        }

        void OnBeginList() override
        {
            EnsureNotInList();
            InList_ = true;
        }

        void OnListItem() override
        { }

        void OnEndList() override
        { }

        void OnBeginMap() override
        {
            THROW_ERROR_EXCEPTION("YSON maps are not supported");
        }

        void OnKeyedItem(TStringBuf /*key*/) override
        {
            YT_ABORT();
        }

        void OnEndMap() override
        {
            YT_ABORT();
        }

        void OnBeginAttributes() override
        {
            THROW_ERROR_EXCEPTION("YSON attributes are not supported");
        }

        void OnEndAttributes() override
        {
            YT_ABORT();
        }

    private:
        const std::function<void(TUnversionedValue)> Appender_;

        bool InList_ = false;

        void EnsureInList()
        {
            if (!InList_) {
                THROW_ERROR_EXCEPTION("YSON list expected");
            }
        }

        void EnsureNotInList()
        {
            if (InList_) {
                THROW_ERROR_EXCEPTION("YSON list is unexpected");
            }
        }
    } consumer(std::move(appender));

    ParseYsonStringBuffer(
        unversionedValue.AsStringBuf(),
        EYsonType::Node,
        &consumer);
}

////////////////////////////////////////////////////////////////////////////////

void MapToUnversionedValueImpl(
    TUnversionedValue* unversionedValue,
    const std::function<bool(TString*, TUnversionedValue*)> producer,
    const TRowBufferPtr& rowBuffer,
    int id,
    EValueFlags flags)
{
    TString ysonBytes;
    TStringOutput outputStream(ysonBytes);
    NYT::NYson::TYsonWriter writer(&outputStream);
    writer.OnBeginMap();

    TString itemKey;
    TUnversionedValue itemValue;
    while (true) {
        if (!producer(&itemKey, &itemValue)) {
            break;
        }
        writer.OnKeyedItem(itemKey);
        UnversionedValueToYson(itemValue, &writer);
    }
    writer.OnEndMap();

    *unversionedValue = rowBuffer->CaptureValue(MakeUnversionedAnyValue(ysonBytes, id, flags));
}

void UnversionedValueToMapImpl(
    std::function<google::protobuf::Message*(TString)> appender,
    const TProtobufMessageType* type,
    TUnversionedValue unversionedValue)
{
    if (unversionedValue.Type == EValueType::Null) {
        return;
    }

    if (unversionedValue.Type != EValueType::Any) {
        THROW_ERROR_EXCEPTION("Cannot parse map from %Qlv",
            unversionedValue.Type);
    }

    class TConsumer
        : public IYsonConsumer
    {
    public:
        TConsumer(
            std::function<google::protobuf::Message*(TString)> appender,
            const TProtobufMessageType* type)
            : Appender_(std::move(appender))
            , Type_(type)
            , OutputStream_(&WireBytes_)
        { }

        void OnStringScalar(TStringBuf value) override
        {
            GetUnderlying()->OnStringScalar(value);
        }

        void OnInt64Scalar(i64 value) override
        {
            GetUnderlying()->OnInt64Scalar(value);
        }

        void OnUint64Scalar(ui64 value) override
        {
            GetUnderlying()->OnUint64Scalar(value);
        }

        void OnDoubleScalar(double value) override
        {
            GetUnderlying()->OnDoubleScalar(value);
        }

        void OnBooleanScalar(bool value) override
        {
            GetUnderlying()->OnBooleanScalar(value);
        }

        void OnEntity() override
        {
            GetUnderlying()->OnEntity();
        }

        void OnBeginList() override
        {
            ++Depth_;
            GetUnderlying()->OnBeginList();
        }

        void OnListItem() override
        {
            GetUnderlying()->OnListItem();
        }

        void OnEndList() override
        {
            --Depth_;
            GetUnderlying()->OnEndList();
        }

        void OnBeginMap() override
        {
            if (Depth_ > 0) {
                GetUnderlying()->OnBeginMap();
            }
            ++Depth_;
        }

        void OnKeyedItem(TStringBuf key) override
        {
            if (Depth_ == 1) {
                NextElement(key);
            } else {
                GetUnderlying()->OnKeyedItem(key);
            }
        }

        void OnEndMap() override
        {
            --Depth_;
            if (Depth_ == 0) {
                FlushElement();
            } else {
                GetUnderlying()->OnEndMap();
            }
        }

        void OnBeginAttributes() override
        {
            GetUnderlying()->OnBeginAttributes();
        }

        void OnEndAttributes() override
        {
            GetUnderlying()->OnEndAttributes();
        }

        void OnRaw(TStringBuf yson, EYsonType type) override
        {
            GetUnderlying()->OnRaw(yson, type);
        }

    private:
        const std::function<google::protobuf::Message*(TString)> Appender_;
        const TProtobufMessageType* const Type_;

        std::optional<TString> Key_;
        std::unique_ptr<IYsonConsumer> Underlying_;
        int Depth_ = 0;

        TString WireBytes_;
        StringOutputStream OutputStream_;


        IYsonConsumer* GetUnderlying()
        {
            if (!Underlying_) {
                THROW_ERROR_EXCEPTION("YSON value must be a list without attributes");
            }
            return Underlying_.get();
        }

        void NextElement(TStringBuf key)
        {
            FlushElement();
            WireBytes_.clear();
            Key_ = TString(key);
            Underlying_ = CreateProtobufWriter(&OutputStream_, Type_);
        }

        void FlushElement()
        {
            if (!Underlying_) {
                return;
            }
            auto* value = Appender_(*Key_);
            if (!value->ParseFromArray(WireBytes_.data(), WireBytes_.size())) {
                THROW_ERROR_EXCEPTION("Error parsing %v from wire bytes",
                    value->GetTypeName());
            }
            Underlying_.reset();
            Key_.reset();
        }
    } consumer(std::move(appender), type);

    ParseYsonStringBuffer(
        unversionedValue.AsStringBuf(),
        EYsonType::Node,
        &consumer);
}

////////////////////////////////////////////////////////////////////////////////

void UnversionedValueToYson(TUnversionedValue unversionedValue, TCheckedInDebugYsonTokenWriter* tokenWriter)
{
    switch (unversionedValue.Type) {
        case EValueType::Int64:
            tokenWriter->WriteBinaryInt64(unversionedValue.Data.Int64);
            return;
        case EValueType::Uint64:
            tokenWriter->WriteBinaryUint64(unversionedValue.Data.Uint64);
            return;
        case EValueType::Double:
            tokenWriter->WriteBinaryDouble(unversionedValue.Data.Double);
            return;
        case EValueType::String:
            tokenWriter->WriteBinaryString(unversionedValue.AsStringBuf());
            return;
        case EValueType::Any:
        case EValueType::Composite:
            tokenWriter->WriteRawNodeUnchecked(unversionedValue.AsStringBuf());
            return;
        case EValueType::Boolean:
            tokenWriter->WriteBinaryBoolean(unversionedValue.Data.Boolean);
            return;
        case EValueType::Null:
            tokenWriter->WriteEntity();
            return;
        case EValueType::TheBottom:
        case EValueType::Min:
        case EValueType::Max:
            YT_ABORT();
    }
    ThrowUnexpectedValueType(unversionedValue.Type);
}

void UnversionedValueToYson(TUnversionedValue unversionedValue, IYsonConsumer* consumer)
{
    switch (unversionedValue.Type) {
        case EValueType::Int64:
            consumer->OnInt64Scalar(unversionedValue.Data.Int64);
            return;
        case EValueType::Uint64:
            consumer->OnUint64Scalar(unversionedValue.Data.Uint64);
            return;
        case EValueType::Double:
            consumer->OnDoubleScalar(unversionedValue.Data.Double);
            return;
        case EValueType::String:
            consumer->OnStringScalar(unversionedValue.AsStringBuf());
            return;
        case EValueType::Any:
        case EValueType::Composite:
            consumer->OnRaw(unversionedValue.AsStringBuf(), EYsonType::Node);
            return;
        case EValueType::Boolean:
            consumer->OnBooleanScalar(unversionedValue.Data.Boolean);
            return;
        case EValueType::Null:
            consumer->OnEntity();
            return;
        case EValueType::Min:
        case EValueType::Max:
        case EValueType::TheBottom:
            YT_ABORT();
    }
    ThrowUnexpectedValueType(unversionedValue.Type);
}

TYsonString UnversionedValueToYson(TUnversionedValue unversionedValue, bool enableRaw)
{
    TString data;
    data.reserve(GetYsonSize(unversionedValue));
    TStringOutput output(data);
    TYsonWriter writer(&output, EYsonFormat::Binary, EYsonType::Node, /* enableRaw */ enableRaw);
    UnversionedValueToYson(unversionedValue, &writer);
    return TYsonString(std::move(data));
}

////////////////////////////////////////////////////////////////////////////////

TUnversionedValue EncodeUnversionedAnyValue(
    TUnversionedValue value,
    TChunkedMemoryPool* memoryPool)
{
    YT_ASSERT(None(value.Flags));
    switch (value.Type) {
        case EValueType::Any:
        case EValueType::Composite:
            return value;

        case EValueType::Null: {
            auto size = 1;
            char* begin = memoryPool->AllocateUnaligned(size);
            char* current = begin;
            *current++ = '#';
            return MakeUnversionedAnyValue(TStringBuf(begin, current), value.Id, value.Flags);
        }

        case EValueType::String: {
            auto size = 1 + MaxVarInt32Size + value.Length;
            char* begin = memoryPool->AllocateUnaligned(size);
            char* current = begin;
            *current++ = NYson::NDetail::StringMarker;
            current += WriteVarInt32(current, value.Length);
            WriteRef(current, TRef(value.Data.String, value.Length));
            return MakeUnversionedAnyValue(TStringBuf(begin, current), value.Id, value.Flags);
        }

        case EValueType::Int64: {
            auto size = 1 + MaxVarInt64Size;
            char* begin = memoryPool->AllocateUnaligned(size);
            char* current = begin;
            *current++ = NYson::NDetail::Int64Marker;
            current += WriteVarInt64(current, value.Data.Int64);
            return MakeUnversionedAnyValue(TStringBuf(begin, current), value.Id, value.Flags);
        }

        case EValueType::Uint64: {
            auto size = 1 + MaxVarUint64Size;
            char* begin = memoryPool->AllocateUnaligned(size);
            char* current = begin;
            *current++ = NYson::NDetail::Uint64Marker;
            current += WriteVarUint64(current, value.Data.Uint64);
            return MakeUnversionedAnyValue(TStringBuf(begin, current), value.Id, value.Flags);
        }

        case EValueType::Double: {
            auto size = 1 + sizeof(double);
            char* begin = memoryPool->AllocateUnaligned(size);
            char* current = begin;
            *current++ = NYson::NDetail::DoubleMarker;
            WritePod(current, value.Data.Double);
            return MakeUnversionedAnyValue(TStringBuf(begin, current), value.Id, value.Flags);
        }

        case EValueType::Boolean: {
            auto size = 1;
            char* begin = memoryPool->AllocateUnaligned(size);
            char* current = begin;
            *current++ = value.Data.Boolean ? NYson::NDetail::TrueMarker : NYson::NDetail::FalseMarker;
            return MakeUnversionedAnyValue(TStringBuf(begin, current), value.Id, value.Flags);
        }

        default:
            YT_ABORT();
    }
}

TUnversionedValue TryDecodeUnversionedAnyValue(
    TUnversionedValue value,
    const TRowBufferPtr& rowBuffer)
{
    YT_VERIFY(value.Type == EValueType::Any);
    YT_VERIFY(None(value.Flags & EValueFlags::Hunk));

    TStatelessLexer lexer; // this will not allocate on happy path
    TToken token;
    lexer.ParseToken(value.AsStringBuf(), &token);
    YT_VERIFY(!token.IsEmpty());

    switch (token.GetType()) {
        case ETokenType::Int64:
            return MakeUnversionedInt64Value(token.GetInt64Value(), value.Id, value.Flags);

        case ETokenType::Uint64:
            return MakeUnversionedUint64Value(token.GetUint64Value(), value.Id, value.Flags);

        case ETokenType::String: {
            auto decodedValue = MakeUnversionedStringValue(token.GetStringValue(), value.Id, value.Flags);
            if (!token.IsBinaryString()) {
                if (!rowBuffer) {
                    THROW_ERROR_EXCEPTION("Cannot decode non-binary YSON string in \"any\"-typed column %v",
                        value.Id);
                }
                decodedValue = rowBuffer->CaptureValue(decodedValue);
            }
            return decodedValue;
        }

        case ETokenType::Double:
            return MakeUnversionedDoubleValue(token.GetDoubleValue(), value.Id, value.Flags);

        case ETokenType::Boolean:
            return MakeUnversionedBooleanValue(token.GetBooleanValue(), value.Id, value.Flags);

        case ETokenType::Hash:
            return MakeUnversionedSentinelValue(EValueType::Null, value.Id, value.Flags);

        default:
            return value;
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TDefaultUnversionedRowsBuilderTag
{ };

TUnversionedRowsBuilder::TUnversionedRowsBuilder()
    : TUnversionedRowsBuilder(New<TRowBuffer>(TDefaultUnversionedRowsBuilderTag()))
{ }

TUnversionedRowsBuilder::TUnversionedRowsBuilder(TRowBufferPtr rowBuffer)
    : RowBuffer_(std::move(rowBuffer))
{ }

void TUnversionedRowsBuilder::ReserveRows(int rowCount)
{
    Rows_.reserve(rowCount);
}

void TUnversionedRowsBuilder::AddRow(TUnversionedRow row)
{
    Rows_.push_back(RowBuffer_->CaptureRow(row));
}

void TUnversionedRowsBuilder::AddRow(TMutableUnversionedRow row)
{
    AddRow(TUnversionedRow(row));
}

void TUnversionedRowsBuilder::AddProtoRow(const TString& protoRow)
{
    auto& row = Rows_.emplace_back();
    FromProto(&row, protoRow, RowBuffer_);
}

TSharedRange<TUnversionedRow> TUnversionedRowsBuilder::Build()
{
    return MakeSharedRange(std::move(Rows_), std::move(RowBuffer_));
}

////////////////////////////////////////////////////////////////////////////////

REGISTER_INTERMEDIATE_PROTO_INTEROP_BYTES_FIELD_REPRESENTATION(
    NProto::TDataBlockMeta,
    /*last_key*/ 9,
    TUnversionedOwningRow)

REGISTER_INTERMEDIATE_PROTO_INTEROP_BYTES_FIELD_REPRESENTATION(
    NProto::TBoundaryKeysExt,
    /*min*/ 1,
    TUnversionedOwningRow)
REGISTER_INTERMEDIATE_PROTO_INTEROP_BYTES_FIELD_REPRESENTATION(
    NProto::TBoundaryKeysExt,
    /*max*/ 2,
    TUnversionedOwningRow)

REGISTER_INTERMEDIATE_PROTO_INTEROP_BYTES_FIELD_REPRESENTATION(
    NProto::TSamplesExt,
    /*entries*/ 1,
    TUnversionedOwningRow)

REGISTER_INTERMEDIATE_PROTO_INTEROP_BYTES_FIELD_REPRESENTATION(
    NProto::THeavyColumnStatisticsExt,
    /*column_data_weights*/ 5,
    TUnversionedOwningRow)

////////////////////////////////////////////////////////////////////////////////

TUnversionedValueRangeTruncationResult TruncateUnversionedValues(
    TUnversionedValueRange values,
    const TRowBufferPtr& rowBuffer,
    const TUnversionedValueRangeTruncationOptions& options)
{
    std::vector<TUnversionedValue> truncatedValues;
    truncatedValues.reserve(values.size());

    int truncatableValueCount = 0;
    i64 remainingSize = options.MaxTotalSize;
    for (const auto& value : values) {
        if (IsStringLikeType(value.Type)) {
            ++truncatableValueCount;
        } else {
            remainingSize -= EstimateRowValueSize(value);
        }
    }

    auto maxSizePerValue = std::max<i64>(0, remainingSize) / std::max(truncatableValueCount, 1);

    i64 resultSize = 0;
    bool clipped = false;

    for (const auto& value : values) {
        if (Any(value.Flags & EValueFlags::Hunk)) {
            // NB: This may be the case for ordered tables with attached hunk storage.
            continue;
        }

        truncatedValues.push_back(value);
        auto& truncatedValue = truncatedValues.back();

        if (clipped) {
            truncatedValue = MakeUnversionedNullValue(value.Id, value.Flags);
        } else if (value.Type == EValueType::Any || value.Type == EValueType::Composite) {
            if (auto truncatedYsonValue = TruncateYsonValue(TYsonStringBuf(value.AsStringBuf()), maxSizePerValue)) {
                truncatedValue = rowBuffer->CaptureValue(MakeUnversionedStringLikeValue(value.Type, truncatedYsonValue->AsStringBuf(), value.Id, value.Flags));
            } else {
                truncatedValue = MakeUnversionedNullValue(value.Id, value.Flags);
            }
        } else if (value.Type == EValueType::String) {
            truncatedValue.Length = std::min<ui32>(truncatedValue.Length, maxSizePerValue);
        }

        if (options.ClipAfterOverflow && IsStringLikeType(value.Type) && (truncatedValue.Type == EValueType::Null || truncatedValue.Length < value.Length)) {
            clipped = true;
        }

        // This funciton also accounts for the representation of the id and type of the unversioned value.
        // The limit can be slightly exceeded this way.
        resultSize += EstimateRowValueSize(truncatedValue);

        if (options.ClipAfterOverflow && resultSize >= options.MaxTotalSize) {
            clipped = true;
        }
    }

    return {MakeSharedRange(std::move(truncatedValues), rowBuffer), resultSize, clipped};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
