#include "unversioned_row.h"

#include "composite_compare.h"
#include "helpers.h"
#include "serialize.h"
#include "unversioned_value.h"
#include "validate_logical_type.h"

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/library/decimal/decimal.h>

#include <yt/yt/core/misc/range.h>

#include <yt/yt/core/yson/consumer.h>

#include <yt/yt/core/ytree/attributes.h>
#include <yt/yt/core/ytree/node.h>
#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/yt/misc/hash.h>

#include <library/cpp/yt/farmhash/farm_hash.h>

#include <library/cpp/yt/coding/varint.h>

#include <util/generic/ymath.h>

#include <util/charset/utf8.h>
#include <util/stream/str.h>

#include <cmath>

namespace NYT::NTableClient {

using namespace NChunkClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

const TString SerializedNullRow;

////////////////////////////////////////////////////////////////////////////////

size_t EstimateRowValueSize(const TUnversionedValue& value, bool isInlineHunkValue)
{
    size_t result = MaxVarUint32Size * 2; // id and type

    switch (value.Type) {
        case EValueType::Null:
        case EValueType::Min:
        case EValueType::Max:
        case EValueType::TheBottom:
            break;

        case EValueType::Int64:
        case EValueType::Uint64:
            result += MaxVarInt64Size;
            break;

        case EValueType::Double:
            result += sizeof(double);
            break;

        case EValueType::Boolean:
            result += 1;
            break;

        case EValueType::String:
        case EValueType::Any:
        case EValueType::Composite:
            if (isInlineHunkValue) {
                result += 1; // hunk value tag
            }
            result += MaxVarUint32Size + value.Length;
            break;
    }

    return result;
}

size_t WriteRowValue(char* output, const TUnversionedValue& value, bool isInlineHunkValue)
{
    char* current = output;

    current += WriteVarUint32(current, value.Id);
    current += WriteVarUint32(current, static_cast<ui16>(value.Type));

    switch (value.Type) {
        case EValueType::Null:
        case EValueType::Min:
        case EValueType::Max:
        case EValueType::TheBottom:
            break;

        case EValueType::Int64:
            current += WriteVarInt64(current, value.Data.Int64);
            break;

        case EValueType::Uint64:
            current += WriteVarUint64(current, value.Data.Uint64);
            break;

        case EValueType::Double:
            ::memcpy(current, &value.Data.Double, sizeof (double));
            current += sizeof (double);
            break;

        case EValueType::Boolean:
            *current++ = value.Data.Boolean ? '\x01' : '\x00';
            break;

        case EValueType::String:
        case EValueType::Any:
        case EValueType::Composite:
            current += WriteVarUint32(current, value.Length + (isInlineHunkValue ? 1 : 0));
            if (isInlineHunkValue) {
                *current++ = static_cast<char>(EHunkValueTag::Inline);
            }
            ::memcpy(current, value.Data.String, value.Length);
            current += value.Length;
            break;

        default:
            YT_ABORT();
    }

    return current - output;
}

size_t ReadRowValue(const char* input, TUnversionedValue* value)
{
    const char* current = input;

    ui32 id;
    current += ReadVarUint32(current, &id);

    ui32 typeValue;
    current += ReadVarUint32(current, &typeValue);
    auto type = static_cast<EValueType>(typeValue);

    switch (type) {
        case EValueType::Null:
        case EValueType::Min:
        case EValueType::Max:
        case EValueType::TheBottom:
            *value = MakeUnversionedSentinelValue(type, id);
            break;

        case EValueType::Int64: {
            i64 data;
            current += ReadVarInt64(current, &data);
            *value = MakeUnversionedInt64Value(data, id);
            break;
        }

        case EValueType::Uint64: {
            ui64 data;
            current += ReadVarUint64(current, &data);
            *value = MakeUnversionedUint64Value(data, id);
            break;
        }

        case EValueType::Double: {
            double data;
            ::memcpy(&data, current, sizeof (double));
            current += sizeof (double);
            *value = MakeUnversionedDoubleValue(data, id);
            break;
        }

        case EValueType::Boolean: {
            bool data = (*current) == 1;
            current += 1;
            *value = MakeUnversionedBooleanValue(data, id);
            break;
        }

        case EValueType::Any:
        case EValueType::Composite:
        case EValueType::String: {
            ui32 length;
            current += ReadVarUint32(current, &length);
            TStringBuf data(current, current + length);
            current += length;
            *value = MakeUnversionedStringLikeValue(type, data, id);
            break;
        }

        default:
            ThrowUnexpectedValueType(type);
    }

    return current - input;
}

void Save(TStreamSaveContext& context, const TUnversionedValue& value)
{
    auto* output = context.GetOutput();
    if (IsStringLikeType(value.Type)) {
        output->Write(&value, sizeof (ui16) + sizeof (ui16) + sizeof (ui32)); // Id, Type, Length
        if (value.Length != 0) {
            output->Write(value.Data.String, value.Length);
        }
    } else {
        output->Write(&value, sizeof (TUnversionedValue));
    }
}

void Load(TStreamLoadContext& context, TUnversionedValue& value, TChunkedMemoryPool* pool)
{
    auto* input = context.GetInput();
    const size_t fixedSize = sizeof (ui16) + sizeof (ui16) + sizeof (ui32); // Id, Type, Length
    YT_VERIFY(input->Load(&value, fixedSize) == fixedSize);
    if (IsStringLikeType(value.Type)) {
        if (value.Length != 0) {
            value.Data.String = pool->AllocateUnaligned(value.Length);
            YT_VERIFY(input->Load(const_cast<char*>(value.Data.String), value.Length) == value.Length);
        } else {
            value.Data.String = nullptr;
        }
    } else {
        YT_VERIFY(input->Load(&value.Data, sizeof (value.Data)) == sizeof (value.Data));
    }
}

size_t GetYsonSize(const TUnversionedValue& value)
{
    switch (value.Type) {
        case EValueType::Any:
        case EValueType::Composite:
            return value.Length;

        case EValueType::Null:
            // Marker type.
            return 1;

        case EValueType::Int64:
        case EValueType::Uint64:
            // Type marker + size;
            return 1 + MaxVarInt64Size;

        case EValueType::Double:
            // Type marker + sizeof double.
            return 1 + 8;

        case EValueType::String:
            // Type marker + length + string bytes.
            return 1 + MaxVarInt32Size + value.Length;

        case EValueType::Boolean:
            // Type marker + value.
            return 1 + 1;

        default:
            YT_ABORT();
    }
}

size_t WriteYson(char* buffer, const TUnversionedValue& unversionedValue)
{
    // TODO(psushin): get rid of output stream.
    TMemoryOutput output(buffer, GetYsonSize(unversionedValue));
    TYsonWriter writer(&output, EYsonFormat::Binary);
    switch (unversionedValue.Type) {
        case EValueType::Int64:
            writer.OnInt64Scalar(unversionedValue.Data.Int64);
            break;
        case EValueType::Uint64:
            writer.OnUint64Scalar(unversionedValue.Data.Uint64);
            break;

        case EValueType::Double:
            writer.OnDoubleScalar(unversionedValue.Data.Double);
            break;

        case EValueType::String:
            writer.OnStringScalar(unversionedValue.AsStringBuf());
            break;

        case EValueType::Boolean:
            writer.OnBooleanScalar(unversionedValue.Data.Boolean);
            break;

        case EValueType::Null:
            writer.OnEntity();
            break;

        default:
            YT_ABORT();
    }
    return output.Buf() - buffer;
}

namespace {

[[noreturn]] void ThrowIncomparableTypes(const TUnversionedValue& lhs, const TUnversionedValue& rhs)
{
    THROW_ERROR_EXCEPTION(
        NTableClient::EErrorCode::IncomparableTypes,
        "Cannot compare values of types %Qlv and %Qlv; only scalar types are allowed for key columns",
        lhs.Type,
        rhs.Type)
        << TErrorAttribute("lhs_value", lhs)
        << TErrorAttribute("rhs_value", rhs);
}

} // namespace

Y_FORCE_INLINE bool IsSentinel(EValueType valueType)
{
    return valueType == EValueType::Min || valueType == EValueType::Max;
}

int CompareRowValues(const TUnversionedValue& lhs, const TUnversionedValue& rhs)
{
    // TODO(babenko): check flags; forbid comparing hunks and aggregates.

    if (lhs.Type == EValueType::Any || rhs.Type == EValueType::Any) {
        if (!IsSentinel(lhs.Type) && !IsSentinel(rhs.Type)) {
            // Never compare composite values with non-sentinels.
            ThrowIncomparableTypes(lhs, rhs);
        }
    }

    if (lhs.Type == EValueType::Composite || rhs.Type == EValueType::Composite) {
        if (lhs.Type != rhs.Type) {
            if (!IsSentinel(lhs.Type) && lhs.Type != EValueType::Null &&
                !IsSentinel(rhs.Type) && rhs.Type != EValueType::Null)
            {
                ThrowIncomparableTypes(lhs, rhs);
            }
            return static_cast<int>(lhs.Type) - static_cast<int>(rhs.Type);
        }
        try {
            auto lhsData = TYsonStringBuf(lhs.AsStringBuf());
            auto rhsData = TYsonStringBuf(rhs.AsStringBuf());
            return CompareCompositeValues(lhsData, rhsData);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION(
                NTableClient::EErrorCode::IncomparableComplexValues,
                "Cannot compare complex values")
                << TErrorAttribute("lhs_value", lhs)
                << TErrorAttribute("rhs_value", rhs)
                << ex;
        }
    }

    if (Y_UNLIKELY(lhs.Type != rhs.Type)) {
        return static_cast<int>(lhs.Type) - static_cast<int>(rhs.Type);
    }

    switch (lhs.Type) {
        case EValueType::Int64: {
            auto lhsValue = lhs.Data.Int64;
            auto rhsValue = rhs.Data.Int64;
            if (lhsValue < rhsValue) {
                return -1;
            } else if (lhsValue > rhsValue) {
                return +1;
            } else {
                return 0;
            }
        }

        case EValueType::Uint64: {
            auto lhsValue = lhs.Data.Uint64;
            auto rhsValue = rhs.Data.Uint64;
            if (lhsValue < rhsValue) {
                return -1;
            } else if (lhsValue > rhsValue) {
                return +1;
            } else {
                return 0;
            }
        }

        case EValueType::Double: {
            double lhsValue = lhs.Data.Double;
            double rhsValue = rhs.Data.Double;
            if (lhsValue < rhsValue) {
                return -1;
            } else if (lhsValue > rhsValue) {
                return +1;
            } else if (std::isnan(lhsValue)) {
                if (std::isnan(rhsValue)) {
                    return 0;
                } else {
                    return 1;
                }
            } else if (std::isnan(rhsValue)) {
                return -1;
            } else {
                return 0;
            }
        }

        case EValueType::Boolean: {
            bool lhsValue = lhs.Data.Boolean;
            bool rhsValue = rhs.Data.Boolean;
            if (lhsValue < rhsValue) {
                return -1;
            } else if (lhsValue > rhsValue) {
                return +1;
            } else {
                return 0;
            }
        }

        case EValueType::String: {
            size_t lhsLength = lhs.Length;
            size_t rhsLength = rhs.Length;
            size_t minLength = std::min(lhsLength, rhsLength);
            int result = ::memcmp(lhs.Data.String, rhs.Data.String, minLength);
            if (result == 0) {
                if (lhsLength < rhsLength) {
                    return -1;
                } else if (lhsLength > rhsLength) {
                    return +1;
                } else {
                    return 0;
                }
            } else {
                return result;
            }
        }

        // All sentinel types are equal.
        default:
            return 0;
    }
}

bool operator == (const TUnversionedValue& lhs, const TUnversionedValue& rhs)
{
    return CompareRowValues(lhs, rhs) == 0;
}

bool operator <= (const TUnversionedValue& lhs, const TUnversionedValue& rhs)
{
    return CompareRowValues(lhs, rhs) <= 0;
}

bool operator < (const TUnversionedValue& lhs, const TUnversionedValue& rhs)
{
    return CompareRowValues(lhs, rhs) < 0;
}

bool operator >= (const TUnversionedValue& lhs, const TUnversionedValue& rhs)
{
    return CompareRowValues(lhs, rhs) >= 0;
}

bool operator > (const TUnversionedValue& lhs, const TUnversionedValue& rhs)
{
    return CompareRowValues(lhs, rhs) > 0;
}

////////////////////////////////////////////////////////////////////////////////

int CompareValueRanges(TUnversionedValueRange lhs, TUnversionedValueRange rhs)
{
    auto* lhsCurrent = lhs.begin();
    auto* rhsCurrent = rhs.begin();
    while (lhsCurrent != lhs.end() && rhsCurrent != rhs.end()) {
        int result = CompareRowValues(*lhsCurrent++, *rhsCurrent++);
        if (result != 0) {
            return result;
        }
    }
    return std::ssize(lhs) - std::ssize(rhs);
}

int CompareRows(TUnversionedRow lhs, TUnversionedRow rhs, ui32 prefixLength)
{
    if (!lhs && !rhs) {
        return 0;
    }

    if (lhs && !rhs) {
        return +1;
    }

    if (!lhs && rhs) {
        return -1;
    }

    return CompareValueRanges(
        lhs.FirstNElements(std::min(lhs.GetCount(), prefixLength)),
        rhs.FirstNElements(std::min(rhs.GetCount(), prefixLength)));
}

bool operator == (TUnversionedRow lhs, TUnversionedRow rhs)
{
    return CompareRows(lhs, rhs) == 0;
}

bool operator <= (TUnversionedRow lhs, TUnversionedRow rhs)
{
    return CompareRows(lhs, rhs) <= 0;
}

bool operator < (TUnversionedRow lhs, TUnversionedRow rhs)
{
    return CompareRows(lhs, rhs) < 0;
}

bool operator >= (TUnversionedRow lhs, TUnversionedRow rhs)
{
    return CompareRows(lhs, rhs) >= 0;
}

bool operator > (TUnversionedRow lhs, TUnversionedRow rhs)
{
    return CompareRows(lhs, rhs) > 0;
}

////////////////////////////////////////////////////////////////////////////////

bool operator == (TUnversionedRow lhs, const TUnversionedOwningRow& rhs)
{
    return CompareRows(lhs, rhs) == 0;
}

bool operator <= (TUnversionedRow lhs, const TUnversionedOwningRow& rhs)
{
    return CompareRows(lhs, rhs) <= 0;
}

bool operator < (TUnversionedRow lhs, const TUnversionedOwningRow& rhs)
{
    return CompareRows(lhs, rhs) < 0;
}

bool operator >= (TUnversionedRow lhs, const TUnversionedOwningRow& rhs)
{
    return CompareRows(lhs, rhs) >= 0;
}

bool operator > (TUnversionedRow lhs, const TUnversionedOwningRow& rhs)
{
    return CompareRows(lhs, rhs) > 0;
}

////////////////////////////////////////////////////////////////////////////////

// Needed by NYT::FarmFingerprint below.
// TODO(babenko): consider refactoring.
TFingerprint FarmFingerprint(const TUnversionedValue& value)
{
    return GetFarmFingerprint(value);
}

TFingerprint GetFarmFingerprint(TUnversionedValueRange range)
{
    return NYT::FarmFingerprint<TUnversionedValue>(range.begin(), range.end());
}

TFingerprint GetFarmFingerprint(TUnversionedRow row)
{
    return GetFarmFingerprint(row.Elements());
}

size_t GetUnversionedRowByteSize(ui32 valueCount)
{
    return sizeof(TUnversionedRowHeader) + sizeof(TUnversionedValue) * valueCount;
}

size_t GetDataWeight(TUnversionedRow row)
{
    if (!row) {
        return 0;
    }

    size_t result = 1;
    for (const auto& value : row) {
        result += GetDataWeight(value);
    }
    return result;
}

size_t GetDataWeight(TRange<TUnversionedRow> rows)
{
    size_t result = 0;
    for (auto row : rows) {
        result += GetDataWeight(row);
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TMutableUnversionedRow TMutableUnversionedRow::Allocate(TChunkedMemoryPool* pool, ui32 valueCount)
{
    size_t byteSize = GetUnversionedRowByteSize(valueCount);
    return Create(pool->AllocateAligned(byteSize), valueCount);
}

TMutableUnversionedRow TMutableUnversionedRow::Create(void* buffer, ui32 valueCount)
{
    auto* header = reinterpret_cast<TUnversionedRowHeader*>(buffer);
    header->Count = valueCount;
    header->Capacity = valueCount;
    return TMutableUnversionedRow(header);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

class TYsonAnyValidator
    : public IYsonConsumer
{
public:
    void OnStringScalar(TStringBuf /*value*/) override
    { }

    void OnInt64Scalar(i64 /*value*/) override
    { }

    void OnUint64Scalar(ui64 /*value*/) override
    { }

    void OnDoubleScalar(double /*value*/) override
    { }

    void OnBooleanScalar(bool /*value*/) override
    { }

    void OnEntity() override
    { }

    void OnBeginList() override
    {
        ++Depth_;
    }

    void OnListItem() override
    { }

    void OnEndList() override
    {
        --Depth_;
    }

    void OnBeginMap() override
    {
        ++Depth_;
    }

    void OnKeyedItem(TStringBuf /*key*/) override
    { }

    void OnEndMap() override
    {
        --Depth_;
    }

    void OnBeginAttributes() override
    {
        if (Depth_ == 0) {
            THROW_ERROR_EXCEPTION("Table values cannot have top-level attributes");
        }
    }

    void OnEndAttributes() override
    { }

    void OnRaw(TStringBuf /*yson*/, EYsonType /*type*/) override
    { }

private:
    int Depth_ = 0;
};

void ValidateAnyValue(TStringBuf yson)
{
    TYsonAnyValidator validator;
    ParseYsonStringBuffer(yson, EYsonType::Node, &validator);
}

void ValidateDynamicValue(const TUnversionedValue& value, bool isKey)
{
    switch (value.Type) {
        case EValueType::String:
            if (value.Length > MaxStringValueLength) {
                THROW_ERROR_EXCEPTION(
                    NTableClient::EErrorCode::StringLikeValueLengthLimitExceeded,
                    "Value of type %Qlv is too long for dynamic data: length %v, limit %v",
                    value.Type,
                    value.Length,
                    MaxStringValueLength);
            }
            break;

        case EValueType::Any:
            if (value.Length > MaxAnyValueLength) {
                THROW_ERROR_EXCEPTION(
                    NTableClient::EErrorCode::StringLikeValueLengthLimitExceeded,
                    "Value of type %Qlv is too long for dynamic data: length %v, limit %v",
                    value.Type,
                    value.Length,
                    MaxAnyValueLength);
            }
            ValidateAnyValue(value.AsStringBuf());
            break;

        case EValueType::Double:
            if (isKey && std::isnan(value.Data.Double)) {
                THROW_ERROR_EXCEPTION(
                    NTableClient::EErrorCode::KeyCannotBeNan,
                    "Key of type \"double\" cannot be NaN");
            }
            break;

        default:
            break;
    }
}

void ValidateClientRow(
    TUnversionedRow row,
    const TTableSchema& schema,
    const TNameTableToSchemaIdMapping& idMapping,
    const TNameTablePtr& nameTable,
    bool isKey,
    bool allowMissingKeyColumns = false,
    std::optional<int> tabletIndexColumnId = std::nullopt)
{
    if (!row) {
        THROW_ERROR_EXCEPTION("Row cannot be null");
    }

    ValidateRowValueCount(row.GetCount());
    ValidateKeyColumnCount(schema.GetKeyColumnCount());

    bool keyColumnSeen[MaxKeyColumnCount]{};
    bool haveDataColumns = false;

    for (const auto& value : row) {
        int mappedId = ApplyIdMapping(value, &idMapping);
        if (mappedId < 0 || mappedId >= std::ssize(schema.Columns())) {
            int size = nameTable->GetSize();
            if (value.Id < 0 || value.Id >= size) {
                THROW_ERROR_EXCEPTION("Expected value id in range [0:%v] but got %v",
                    size - 1,
                    value.Id);
            }

            THROW_ERROR_EXCEPTION("Unexpected column %Qv", nameTable->GetName(value.Id));
        }

        const auto& column = schema.Columns()[mappedId];
        ValidateValueType(value, schema, mappedId, /*typeAnyAcceptsAllValues*/false);

        if (Any(value.Flags & EValueFlags::Aggregate) && !column.Aggregate()) {
            THROW_ERROR_EXCEPTION(
                "\"aggregate\" flag is set for value in column %v which is not aggregating",
                column.GetDiagnosticNameString());
        }

        if (mappedId < schema.GetKeyColumnCount()) {
            if (keyColumnSeen[mappedId]) {
                THROW_ERROR_EXCEPTION("Duplicate key column %v",
                    column.GetDiagnosticNameString());
            }
            keyColumnSeen[mappedId] = true;
            ValidateKeyValue(value);
        } else if (isKey) {
            THROW_ERROR_EXCEPTION("Non-key column %v in a key",
                column.GetDiagnosticNameString());
        } else {
            haveDataColumns = true;
            ValidateDataValue(value);
        }
    }

    if (!isKey && !haveDataColumns) {
        THROW_ERROR_EXCEPTION("At least one non-key column must be specified");
    }

    if (tabletIndexColumnId) {
        YT_VERIFY(std::ssize(idMapping) > *tabletIndexColumnId);
        auto mappedId = idMapping[*tabletIndexColumnId];
        YT_VERIFY(mappedId >= 0);
        keyColumnSeen[mappedId] = true;
    }

    if (!allowMissingKeyColumns) {
        for (int index = 0; index < schema.GetKeyColumnCount(); ++index) {
            if (!keyColumnSeen[index] && !schema.Columns()[index].Expression()) {
                THROW_ERROR_EXCEPTION("Missing key column %v",
                    schema.Columns()[index].GetDiagnosticNameString());
            }
        }
    }

    auto dataWeight = GetDataWeight(row);
    if (dataWeight >= MaxClientVersionedRowDataWeight) {
        THROW_ERROR_EXCEPTION(
            NTableClient::EErrorCode::RowWeightLimitExceeded,
            "Row is too large: data weight %v, limit %v",
            dataWeight,
            MaxClientVersionedRowDataWeight);
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TString SerializeToString(TUnversionedRow row)
{
    return row
        ? SerializeToString(row.Elements())
        : SerializedNullRow;
}

TString SerializeToString(TUnversionedValueRange range)
{
    int size = 2 * MaxVarUint32Size; // header size
    for (const auto& value : range) {
        size += EstimateRowValueSize(value);
    }

    TString buffer;
    buffer.resize(size);

    char* current = const_cast<char*>(buffer.data());
    current += WriteVarUint32(current, 0); // format version
    current += WriteVarUint32(current, range.size());

    for (const auto& value : range) {
        current += WriteRowValue(current, value);
    }

    buffer.resize(current - buffer.data());

    return buffer;
}

TUnversionedOwningRow DeserializeFromString(TString&& data, std::optional<int> nullPaddingWidth = std::nullopt)
{
    if (data == SerializedNullRow) {
        return TUnversionedOwningRow();
    }
    auto dataRef = TSharedRef::FromString(std::move(data));

    const char* current = dataRef.begin();

    ui32 version;
    current += ReadVarUint32(current, &version);
    YT_VERIFY(version == 0);

    ui32 valueCount;
    current += ReadVarUint32(current, &valueCount);

    // TODO(max42): YT-14049.
    int nullCount = nullPaddingWidth ? std::max<int>(0, *nullPaddingWidth - static_cast<int>(valueCount)) : 0;

    size_t fixedSize = GetUnversionedRowByteSize(valueCount + nullCount);
    auto rowData = TSharedMutableRef::Allocate<TOwningRowTag>(fixedSize, {.InitializeStorage = false});
    auto* header = reinterpret_cast<TUnversionedRowHeader*>(rowData.Begin());

    header->Count = static_cast<i32>(valueCount + nullCount);
    header->Capacity = static_cast<i32>(valueCount + nullCount);

    auto* values = reinterpret_cast<TUnversionedValue*>(header + 1);
    for (int index = 0; index < static_cast<int>(valueCount); ++index) {
        auto* value = values + index;
        current += ReadRowValue(current, value);
    }
    for (int index = valueCount; index < static_cast<int>(valueCount + nullCount); ++index) {
        values[index] = MakeUnversionedNullValue(index);
    }

    return TUnversionedOwningRow(std::move(rowData), std::move(dataRef));
}

TUnversionedRow DeserializeFromString(const TString& data, const TRowBufferPtr& rowBuffer)
{
    if (data == SerializedNullRow) {
        return TUnversionedRow();
    }

    const char* current = data.data();

    ui32 version;
    current += ReadVarUint32(current, &version);
    YT_VERIFY(version == 0);

    ui32 valueCount;
    current += ReadVarUint32(current, &valueCount);

    auto row = rowBuffer->AllocateUnversioned(valueCount);

    auto* values = row.begin();
    for (int index = 0; index < static_cast<int>(valueCount); ++index) {
        auto* value = values + index;
        current += ReadRowValue(current, value);
        rowBuffer->CaptureValue(value);
    }

    return row;
}

////////////////////////////////////////////////////////////////////////////////

void TUnversionedRow::Save(TSaveContext& context) const
{
    NYT::Save(context, SerializeToString(*this));
}

void TUnversionedRow::Load(TLoadContext& context)
{
    *this = DeserializeFromString(NYT::Load<TString>(context), context.GetRowBuffer());
}

void ValidateValueType(
    const TUnversionedValue& value,
    const TTableSchema& schema,
    int schemaId,
    bool typeAnyAcceptsAllValues,
    bool ignoreRequired,
    bool validateAnyIsValidYson)
{
    ValidateValueType(
        value,
        schema.Columns()[schemaId],
        typeAnyAcceptsAllValues,
        ignoreRequired,
        validateAnyIsValidYson);
}

[[noreturn]] static void ThrowInvalidColumnType(EValueType expected, EValueType actual)
{
    THROW_ERROR_EXCEPTION(
        NTableClient::EErrorCode::SchemaViolation,
        "Invalid type: expected %Qlv, actual %Qlv",
        expected,
        actual);
}

static inline void ValidateColumnType(EValueType expected, const TUnversionedValue& value)
{
    if (value.Type != expected) {
        ThrowInvalidColumnType(expected, value.Type);
    }
}

template <ESimpleLogicalValueType logicalType>
Y_FORCE_INLINE auto GetValue(const TUnversionedValue& value)
{
    constexpr auto physicalType = GetPhysicalType(logicalType);
    ValidateColumnType(physicalType, value);
    if constexpr (physicalType == EValueType::Int64) {
        return value.Data.Int64;
    } else if constexpr (physicalType == EValueType::Uint64) {
        return value.Data.Uint64;
    } else if constexpr (physicalType == EValueType::Double) {
        return value.Data.Double;
    } else if constexpr (physicalType == EValueType::Boolean) {
        return value.Data.Boolean;
    } else {
        static_assert(physicalType == EValueType::String || physicalType == EValueType::Any);
        return value.AsStringBuf();
    }
}

static const TLogicalTypePtr& UnwrapTaggedAndOptional(const TLogicalTypePtr& type)
{
    const TLogicalTypePtr* current = &type;
    while ((*current)->GetMetatype() == ELogicalMetatype::Tagged) {
        current = &(*current)->UncheckedAsTaggedTypeRef().GetElement();
    }

    if ((*current)->GetMetatype() != ELogicalMetatype::Optional) {
        return *current;
    }

    const auto& optionalType = (*current)->UncheckedAsOptionalTypeRef();
    if (optionalType.IsElementNullable()) {
        return *current;
    }

    current = &optionalType.GetElement();

    while ((*current)->GetMetatype() == ELogicalMetatype::Tagged) {
        current = &(*current)->UncheckedAsTaggedTypeRef().GetElement();
    }

    return *current;
}

void ValidateValueType(
    const TUnversionedValue& value,
    const TColumnSchema& columnSchema,
    bool typeAnyAcceptsAllValues,
    bool ignoreRequired,
    bool validateAnyIsValidYson)
{
    if (value.Type == EValueType::Null) {
        if (columnSchema.Required()) {
            if (ignoreRequired) {
                return;
            }

            THROW_ERROR_EXCEPTION(
                NTableClient::EErrorCode::SchemaViolation,
                "Required column %v cannot have %Qlv value",
                columnSchema.GetDiagnosticNameString(),
                value.Type);
        } else {
            return;
        }
    }

    try {
        auto v1Type = columnSchema.CastToV1Type();
        switch (v1Type) {
            case ESimpleLogicalValueType::Null:
            case ESimpleLogicalValueType::Void:
                // this case should be handled before
                ValidateColumnType(EValueType::Null, value);
                return;
            case ESimpleLogicalValueType::Any:
                if (columnSchema.IsOfV1Type()) {
                    if (!typeAnyAcceptsAllValues) {
                        ValidateColumnType(EValueType::Any, value);
                    } else if (!IsAnyColumnCompatibleType(value.Type)) {
                        THROW_ERROR_EXCEPTION(
                            NTableClient::EErrorCode::SchemaViolation,
                            "Cannot write value of type %Qlv into type any column",
                            value.Type);
                    }
                    if (IsAnyOrComposite(value.Type) && validateAnyIsValidYson) {
                        ValidateAnyValue(value.AsStringBuf());
                    }
                } else {
                    ValidateColumnType(EValueType::Composite, value);
                    ValidateComplexLogicalType(value.AsStringBuf(), columnSchema.LogicalType());
                }
                return;
            case ESimpleLogicalValueType::String:
                if (columnSchema.IsOfV1Type()) {
                    ValidateSimpleLogicalType<ESimpleLogicalValueType::String>(GetValue<ESimpleLogicalValueType::String>(value));
                } else {
                    ValidateColumnType(EValueType::String, value);
                    auto type = UnwrapTaggedAndOptional(columnSchema.LogicalType());
                    YT_VERIFY(type->GetMetatype() == ELogicalMetatype::Decimal);
                    NDecimal::TDecimal::ValidateBinaryValue(
                        value.AsStringBuf(),
                        type->UncheckedAsDecimalTypeRef().GetPrecision(),
                        type->UncheckedAsDecimalTypeRef().GetScale());
                }
                return;
#define CASE(x) \
        case x: \
            ValidateSimpleLogicalType<x>(GetValue<x>(value)); \
            return;

            CASE(ESimpleLogicalValueType::Int64)
            CASE(ESimpleLogicalValueType::Uint64)
            CASE(ESimpleLogicalValueType::Double)
            CASE(ESimpleLogicalValueType::Boolean)

            CASE(ESimpleLogicalValueType::Float)

            CASE(ESimpleLogicalValueType::Int8)
            CASE(ESimpleLogicalValueType::Int16)
            CASE(ESimpleLogicalValueType::Int32)

            CASE(ESimpleLogicalValueType::Uint8)
            CASE(ESimpleLogicalValueType::Uint16)
            CASE(ESimpleLogicalValueType::Uint32)

            CASE(ESimpleLogicalValueType::Utf8)
            CASE(ESimpleLogicalValueType::Date)
            CASE(ESimpleLogicalValueType::Datetime)
            CASE(ESimpleLogicalValueType::Timestamp)
            CASE(ESimpleLogicalValueType::Interval)
            CASE(ESimpleLogicalValueType::Json)
            CASE(ESimpleLogicalValueType::Uuid)

            CASE(ESimpleLogicalValueType::Date32)
            CASE(ESimpleLogicalValueType::Datetime64)
            CASE(ESimpleLogicalValueType::Timestamp64)
            CASE(ESimpleLogicalValueType::Interval64)
#undef CASE
        }
        YT_ABORT();
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION(
            NTableClient::EErrorCode::SchemaViolation,
            "Error validating column %v",
            columnSchema.GetDiagnosticNameString())
            << ex;
    }
}

void ValidateStaticValue(const TUnversionedValue& value)
{
    ValidateDataValueType(value.Type);
    if (IsStringLikeType(value.Type)) {
        if (value.Length > MaxRowWeightLimit) {
            THROW_ERROR_EXCEPTION(
                NTableClient::EErrorCode::StringLikeValueLengthLimitExceeded,
                "Value of type %Qlv is too long for static data: length %v, limit %v",
                value.Type,
                value.Length,
                MaxRowWeightLimit);
        }
    }
}

void ValidateDataValue(const TUnversionedValue& value)
{
    if (auto remainingFlags = value.Flags & ~EValueFlags::Aggregate; Any(remainingFlags)) {
        THROW_ERROR_EXCEPTION(
            "Value has unsupported flag(s) %Qlv",
            remainingFlags);
    }
    ValidateDataValueType(value.Type);
    ValidateDynamicValue(value, /*isKey*/ false);
}

void ValidateKeyValue(const TUnversionedValue& value)
{
    if (value.Flags != EValueFlags::None) {
        THROW_ERROR_EXCEPTION(
            "Key value has unsupported flag(s) %Qlv",
            value.Flags);
    }
    ValidateKeyValueType(value.Type);
    ValidateDynamicValue(value, /*isKey*/ true);
}

void ValidateRowValueCount(int count)
{
    if (count < 0) {
        THROW_ERROR_EXCEPTION("Negative number of values in row");
    }
    if (count > MaxValuesPerRow) {
        THROW_ERROR_EXCEPTION(
            NTableClient::EErrorCode::TooManyValuesInRow,
            "Too many values in row: actual %v, limit %v",
            count,
            MaxValuesPerRow);
    }
}

void ValidateKeyColumnCount(int count)
{
    if (count < 0) {
        THROW_ERROR_EXCEPTION("Negative number of key columns");
    }
    if (count > MaxKeyColumnCount) {
        THROW_ERROR_EXCEPTION(
            NTableClient::EErrorCode::TooManyColumnsInKey,
            "Too many columns in key: actual %v, limit %v",
            count,
            MaxKeyColumnCount);
    }
}

void ValidateRowCount(int count)
{
    if (count < 0) {
        THROW_ERROR_EXCEPTION("Negative number of rows in rowset");
    }
    if (count > MaxRowsPerRowset) {
        THROW_ERROR_EXCEPTION(
            NTableClient::EErrorCode::TooManyRowsInRowset,
            "Too many rows in rowset: actual %v, limit %v",
            count,
            MaxRowsPerRowset);
    }
}

void ValidateClientDataRow(
    TUnversionedRow row,
    const TTableSchema& schema,
    const TNameTableToSchemaIdMapping& idMapping,
    const TNameTablePtr& nameTable,
    std::optional<int> tabletIndexColumnId,
    bool allowMissingKeyColumns)
{
    ValidateClientRow(row, schema, idMapping, nameTable, false, allowMissingKeyColumns, tabletIndexColumnId);
}

void ValidateDuplicateAndRequiredValueColumns(
    TUnversionedRow row,
    const TTableSchema& schema,
    const TNameTableToSchemaIdMapping& idMapping,
    std::vector<bool>* columnPresenceBuffer)
{
    auto& columnSeen = *columnPresenceBuffer;
    YT_VERIFY(std::ssize(columnSeen) >= schema.GetColumnCount());
    std::fill(columnSeen.begin(), columnSeen.end(), 0);

    for (const auto& value : row) {
        int mappedId = ApplyIdMapping(value, &idMapping);
        if (mappedId < 0) {
            continue;
        }
        const auto& column = schema.Columns()[mappedId];

        if (columnSeen[mappedId]) {
            THROW_ERROR_EXCEPTION(
                NTableClient::EErrorCode::DuplicateColumnInSchema,
                "Duplicate column %v in table schema",
                column.GetDiagnosticNameString());
        }
        columnSeen[mappedId] = true;
    }

    for (int index = schema.GetKeyColumnCount(); index < schema.GetColumnCount(); ++index) {
        if (!columnSeen[index] && schema.Columns()[index].Required()) {
            THROW_ERROR_EXCEPTION(
                NTableClient::EErrorCode::MissingRequiredColumnInSchema,
                "Missing required column %v in table schema",
                schema.Columns()[index].GetDiagnosticNameString());
        }
    }
}

bool ValidateNonKeyColumnsAgainstLock(
    TUnversionedRow row,
    const TLockMask& locks,
    const TTableSchema& schema,
    const TNameTableToSchemaIdMapping& idMapping,
    const TNameTablePtr& nameTable,
    const std::vector<int>& columnIndexToLockIndex,
    bool allowSharedWriteLocks)
{
    bool hasNonKeyColumns = false;
    for (const auto& value : row) {
        int mappedId = ApplyIdMapping(value, &idMapping);
        if (mappedId < 0 || mappedId >= std::ssize(schema.Columns())) {
            int size = nameTable->GetSize();
            if (value.Id < 0 || value.Id >= size) {
                THROW_ERROR_EXCEPTION("Expected value id in range [0:%v] but got %v",
                    size - 1,
                    value.Id);
            }

            THROW_ERROR_EXCEPTION("Unexpected column %Qv",
                nameTable->GetName(value.Id));
        }

        auto lockIndex = columnIndexToLockIndex[mappedId];
        if (lockIndex == -1) {
            continue;
        }

        auto lockType = locks.Get(lockIndex);

        if (lockType == ELockType::SharedWrite && !allowSharedWriteLocks) {
            THROW_ERROR_EXCEPTION("Shared write locks are not allowed for the table");
        }

        if (mappedId >= schema.GetKeyColumnCount()) {
            hasNonKeyColumns = true;

            if (lockType != ELockType::Exclusive && lockType != ELockType::SharedWrite) {
                THROW_ERROR_EXCEPTION("No write lock taken for column %Qv",
                    nameTable->GetName(value.Id));
            }
        }
    }

    return hasNonKeyColumns;
}

void ValidateClientKey(TLegacyKey key)
{
    for (const auto& value : key) {
        ValidateKeyValue(value);
    }
}

void ValidateClientKey(
    TLegacyKey key,
    const TTableSchema& schema,
    const TNameTableToSchemaIdMapping& idMapping,
    const TNameTablePtr& nameTable)
{
    ValidateClientRow(key, schema, idMapping, nameTable, true);
}

void ValidateReadTimestamp(TTimestamp timestamp)
{
    if (timestamp != SyncLastCommittedTimestamp &&
        timestamp != AsyncLastCommittedTimestamp &&
        (timestamp < MinTimestamp || timestamp > MaxTimestamp))
    {
        THROW_ERROR_EXCEPTION("Invalid read timestamp %x", timestamp);
    }
}

void ValidateGetInSyncReplicasTimestamp(TTimestamp timestamp)
{
    if (timestamp != SyncLastCommittedTimestamp &&
        (timestamp < MinTimestamp || timestamp > MaxTimestamp))
    {
        THROW_ERROR_EXCEPTION("Invalid GetInSyncReplicas timestamp %x", timestamp);
    }
}

void ValidateWriteTimestamp(TTimestamp timestamp)
{
    if (timestamp < MinTimestamp || timestamp > MaxTimestamp) {
        THROW_ERROR_EXCEPTION("Invalid write timestamp %x", timestamp);
    }
}

int ApplyIdMapping(
    const TUnversionedValue& value,
    const TNameTableToSchemaIdMapping* idMapping)
{
    auto valueId = value.Id;
    if (idMapping) {
        const auto& idMapping_ = *idMapping;
        if (valueId >= idMapping_.size()) {
            THROW_ERROR_EXCEPTION("Invalid column id during remapping: expected in range [0, %v), got %v",
                idMapping_.size(),
                valueId);
        }
        return idMapping_[valueId];
    } else {
        return valueId;
    }
}

////////////////////////////////////////////////////////////////////////////////

TLegacyOwningKey GetKeySuccessorImpl(TLegacyKey key, ui32 prefixLength, EValueType sentinelType)
{
    auto length = std::min(prefixLength, key.GetCount());
    TUnversionedOwningRowBuilder builder(length + 1);
    for (int index = 0; index < static_cast<int>(length); ++index) {
        builder.AddValue(key[index]);
    }
    builder.AddValue(MakeUnversionedSentinelValue(sentinelType));
    return builder.FinishRow();
}

TLegacyKey GetKeySuccessorImpl(TLegacyKey key, ui32 prefixLength, EValueType sentinelType, const TRowBufferPtr& rowBuffer)
{
    auto length = std::min(prefixLength, key.GetCount());
    auto result = rowBuffer->AllocateUnversioned(length + 1);
    for (int index = 0; index < static_cast<int>(length); ++index) {
        result[index] = rowBuffer->CaptureValue(key[index]);
    }
    result[length] = MakeUnversionedSentinelValue(sentinelType);
    return result;
}

TLegacyOwningKey GetKeySuccessor(TLegacyKey key)
{
    return GetKeySuccessorImpl(
        key,
        key.GetCount(),
        EValueType::Min);
}

TLegacyKey GetKeySuccessor(TLegacyKey key, const TRowBufferPtr& rowBuffer)
{
    return GetKeySuccessorImpl(
        key,
        key.GetCount(),
        EValueType::Min,
        rowBuffer);
}

TLegacyOwningKey GetKeyPrefixSuccessor(TLegacyKey key, ui32 prefixLength)
{
    return GetKeySuccessorImpl(
        key,
        prefixLength,
        EValueType::Max);
}

TLegacyKey GetKeyPrefixSuccessor(TLegacyKey key, ui32 prefixLength, const TRowBufferPtr& rowBuffer)
{
    return GetKeySuccessorImpl(
        key,
        prefixLength,
        EValueType::Max,
        rowBuffer);
}

TLegacyOwningKey GetKeyPrefix(TLegacyKey key, ui32 prefixLength)
{
    return TLegacyOwningKey(key.FirstNElements(std::min(key.GetCount(), prefixLength)));
}

TLegacyKey GetKeyPrefix(TLegacyKey key, ui32 prefixLength, const TRowBufferPtr& rowBuffer)
{
    return rowBuffer->CaptureRow(key.FirstNElements(std::min(key.GetCount(), prefixLength)));
}

TLegacyKey GetStrictKey(TLegacyKey key, ui32 keyColumnCount, const TRowBufferPtr& rowBuffer, EValueType sentinelType)
{
    if (key.GetCount() > keyColumnCount) {
        return GetKeyPrefix(key, keyColumnCount, rowBuffer);
    } else {
        return WidenKey(key, keyColumnCount, rowBuffer, sentinelType);
    }
}

TLegacyKey GetStrictKeySuccessor(TLegacyKey key, ui32 keyColumnCount, const TRowBufferPtr& rowBuffer, EValueType sentinelType)
{
    if (key.GetCount() >= keyColumnCount) {
        return GetKeyPrefixSuccessor(key, keyColumnCount, rowBuffer);
    } else {
        return WidenKeySuccessor(key, keyColumnCount, rowBuffer, sentinelType);
    }
}

////////////////////////////////////////////////////////////////////////////////

static TLegacyOwningKey MakeSentinelKey(EValueType type)
{
    TUnversionedOwningRowBuilder builder;
    builder.AddValue(MakeUnversionedSentinelValue(type));
    return builder.FinishRow();
}

static const TLegacyOwningKey CachedMinKey = MakeSentinelKey(EValueType::Min);
static const TLegacyOwningKey CachedMaxKey = MakeSentinelKey(EValueType::Max);

const TLegacyOwningKey MinKey()
{
    return CachedMinKey;
}

const TLegacyOwningKey MaxKey()
{
    return CachedMaxKey;
}

static TLegacyOwningKey MakeEmptyKey()
{
    TUnversionedOwningRowBuilder builder;
    return builder.FinishRow();
}

static const TLegacyOwningKey CachedEmptyKey = MakeEmptyKey();

const TLegacyOwningKey EmptyKey()
{
    return CachedEmptyKey;
}

const TLegacyOwningKey& ChooseMinKey(const TLegacyOwningKey& a, const TLegacyOwningKey& b)
{
    int result = CompareRows(a, b);
    return result <= 0 ? a : b;
}

const TLegacyOwningKey& ChooseMaxKey(const TLegacyOwningKey& a, const TLegacyOwningKey& b)
{
    int result = CompareRows(a, b);
    return result >= 0 ? a : b;
}

void ToProto(TProtoStringType* protoRow, TUnversionedRow row)
{
    *protoRow = SerializeToString(row);
}

void ToProto(TProtoStringType* protoRow, const TUnversionedOwningRow& row)
{
    ToProto(protoRow, row.Get());
}

void ToProto(TProtoStringType* protoRow, TUnversionedValueRange range)
{
    *protoRow = SerializeToString(range);
}

void ToProto(TProtoStringType* protoRow, const TRange<TUnversionedOwningValue>& values)
{
    std::vector<TUnversionedValue> notOwningValues(values.size());
    std::copy(values.begin(), values.end(), notOwningValues.begin());
    ToProto(protoRow, notOwningValues);
}

void FromProto(TUnversionedOwningRow* row, const TProtoStringType& protoRow, std::optional<int> nullPaddingWidth)
{
    *row = DeserializeFromString(TString{protoRow}, nullPaddingWidth);
}

void FromProto(TUnversionedRow* row, const TProtoStringType& protoRow, const TRowBufferPtr& rowBuffer)
{
    if (protoRow == SerializedNullRow) {
        *row = TUnversionedRow();
        return;
    }

    const char* current = protoRow.data();

    ui32 version;
    current += ReadVarUint32(current, &version);
    YT_VERIFY(version == 0);

    ui32 valueCount;
    current += ReadVarUint32(current, &valueCount);

    auto mutableRow = rowBuffer->AllocateUnversioned(valueCount);
    *row = mutableRow;

    auto* values = mutableRow.Begin();
    for (auto* value = values; value < values + valueCount; ++value) {
        current += ReadRowValue(current, value);
        rowBuffer->CaptureValue(value);
    }
}

void FromProto(std::vector<TUnversionedOwningValue>* values, const TProtoStringType& protoRow)
{
    TUnversionedOwningRow row;
    FromProto(&row, protoRow);
    values->resize(row.GetCount());
    std::copy(row.Begin(), row.End(), values->begin());
}

void ToBytes(TString* bytes, const TUnversionedOwningRow& row)
{
    *bytes = SerializeToString(row);
}

void FromBytes(TUnversionedOwningRow* row, TStringBuf bytes)
{
    *row = DeserializeFromString(TString(bytes));
}

void PrintTo(const TUnversionedOwningRow& key, ::std::ostream* os)
{
    *os << KeyToYson(key);
}

void PrintTo(const TUnversionedRow& value, ::std::ostream* os)
{
    *os << ToString(value);
}

TLegacyOwningKey RowToKey(
    const TTableSchema& schema,
    TUnversionedRow row)
{
    TUnversionedOwningRowBuilder builder;
    for (int index = 0; index < schema.GetKeyColumnCount(); ++index) {
        builder.AddValue(row[index]);
    }
    return builder.FinishRow();
}

namespace {

template <class TRow>
std::pair<TSharedRange<TUnversionedRow>, i64> CaptureRowsImpl(
    TRange<TRow> rows,
    TRefCountedTypeCookie tagCookie)
{
    size_t bufferSize = 0;
    bufferSize += sizeof (TUnversionedRow) * rows.Size();
    for (auto row : rows) {
        bufferSize += GetUnversionedRowByteSize(row.GetCount());
        for (const auto& value : row) {
            if (IsStringLikeType(value.Type)) {
                bufferSize += value.Length;
            }
        }
    }
    auto buffer = TSharedMutableRef::Allocate(bufferSize, {.InitializeStorage = false}, tagCookie);

    char* alignedPtr = buffer.Begin();
    auto allocateAligned = [&] (size_t size) {
        auto* result = alignedPtr;
        alignedPtr += size;
        return result;
    };

    char* unalignedPtr = buffer.End();
    auto allocateUnaligned = [&] (size_t size) {
        unalignedPtr -= size;
        return unalignedPtr;
    };

    auto* capturedRows = reinterpret_cast<TUnversionedRow*>(allocateAligned(sizeof (TUnversionedRow) * rows.Size()));
    for (size_t index = 0; index < rows.Size(); ++index) {
        auto row = rows[index];
        int valueCount = row.GetCount();
        auto* capturedHeader = reinterpret_cast<TUnversionedRowHeader*>(allocateAligned(GetUnversionedRowByteSize(valueCount)));
        capturedHeader->Capacity = valueCount;
        capturedHeader->Count = valueCount;
        auto capturedRow = TMutableUnversionedRow(capturedHeader);
        capturedRows[index] = capturedRow;
        ::memcpy(capturedRow.Begin(), row.Begin(), sizeof (TUnversionedValue) * row.GetCount());
        for (auto& capturedValue : capturedRow) {
            if (IsStringLikeType(capturedValue.Type)) {
                auto* capturedString = allocateUnaligned(capturedValue.Length);
                ::memcpy(capturedString, capturedValue.Data.String, capturedValue.Length);
                capturedValue.Data.String = capturedString;
            }
        }
    }

    YT_VERIFY(alignedPtr == unalignedPtr);

    return {
        MakeSharedRange(
            MakeRange(capturedRows, rows.Size()),
            std::move(buffer.ReleaseHolder())),
        bufferSize
    };
}

} // namespace

std::pair<TSharedRange<TUnversionedRow>, i64> CaptureRows(
    TRange<TUnversionedRow> rows,
    TRefCountedTypeCookie tagCookie)
{
    return CaptureRowsImpl(rows, tagCookie);
}

std::pair<TSharedRange<TUnversionedRow>, i64> CaptureRows(
    TRange<TUnversionedOwningRow> rows,
    TRefCountedTypeCookie tagCookie)
{
    return CaptureRowsImpl(rows, tagCookie);
}

void Serialize(const TUnversionedValue& value, IYsonConsumer* consumer, bool anyAsRaw)
{
    if (Any(value.Flags)) {
        consumer->OnBeginAttributes();
        if (Any(value.Flags & EValueFlags::Aggregate)) {
            consumer->OnKeyedItem("aggregate");
            consumer->OnBooleanScalar(true);
        }
        if (Any(value.Flags & EValueFlags::Hunk)) {
            consumer->OnKeyedItem("hunk");
            consumer->OnBooleanScalar(true);
        }
        consumer->OnEndAttributes();
    }
    auto type = value.Type;
    switch (type) {
        case EValueType::Int64:
            consumer->OnInt64Scalar(value.Data.Int64);
            break;

        case EValueType::Uint64:
            consumer->OnUint64Scalar(value.Data.Uint64);
            break;

        case EValueType::Double:
            consumer->OnDoubleScalar(value.Data.Double);
            break;

        case EValueType::Boolean:
            consumer->OnBooleanScalar(value.Data.Boolean);
            break;

        case EValueType::String:
            consumer->OnStringScalar(value.AsStringBuf());
            break;

        case EValueType::Any:
            if (anyAsRaw) {
                consumer->OnRaw(value.AsStringBuf(), EYsonType::Node);
            } else {
                ParseYsonStringBuffer(value.AsStringBuf(), EYsonType::Node, consumer);
            }
            break;

        case EValueType::Null:
            consumer->OnEntity();
            break;

        case EValueType::Composite:
            consumer->OnBeginAttributes();
            consumer->OnKeyedItem("type");
            consumer->OnStringScalar(FormatEnum(type));
            consumer->OnEndAttributes();
            if (anyAsRaw) {
                consumer->OnRaw(value.AsStringBuf(), EYsonType::Node);
            } else {
                ParseYsonStringBuffer(value.AsStringBuf(), EYsonType::Node, consumer);
            }
            break;

        default:
            consumer->OnBeginAttributes();
            consumer->OnKeyedItem("type");
            consumer->OnStringScalar(FormatEnum(type));
            consumer->OnEndAttributes();
            consumer->OnEntity();
            break;
    }
}

void Serialize(TLegacyKey key, IYsonConsumer* consumer)
{
    consumer->OnBeginList();
    for (const auto& value : key) {
        consumer->OnListItem();
        Serialize(value, consumer);
    }
    consumer->OnEndList();
}

void Serialize(const TLegacyOwningKey& key, IYsonConsumer* consumer)
{
    return Serialize(key.Get(), consumer);
}

void Deserialize(TLegacyOwningKey& key, INodePtr node)
{
    if (node->GetType() != ENodeType::List) {
        THROW_ERROR_EXCEPTION("Key cannot be parsed from %Qlv",
            node->GetType());
    }

    TUnversionedOwningRowBuilder builder;
    int id = 0;
    for (const auto& item : node->AsList()->GetChildren()) {
        try {
            switch (item->GetType()) {
                #define XX(type,  cppType) \
                case ENodeType::type: \
                    builder.AddValue(MakeUnversioned ## type ## Value(item->As ## type()->GetValue(), id)); \
                    break;
                ITERATE_SCALAR_YTREE_NODE_TYPES(XX)
                #undef XX

                case ENodeType::Entity: {
                    auto valueType = item->Attributes().Get<EValueType>("type", EValueType::Null);
                    if (valueType != EValueType::Null && !IsSentinelType(valueType)) {
                        THROW_ERROR_EXCEPTION("Entities can only represent %Qlv and sentinel values but "
                            "not values of type %Qlv",
                            EValueType::Null,
                            valueType);
                    }
                    builder.AddValue(MakeUnversionedSentinelValue(valueType, id));
                    break;
                }

                default:
                    THROW_ERROR_EXCEPTION("Key cannot contain %Qlv values",
                        item->GetType());
            }
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error deserializing key component #%v", id)
                << ex;
        }
        ++id;
    }
    key = builder.FinishRow();
}

void Deserialize(TLegacyOwningKey& key, TYsonPullParserCursor* cursor)
{
    // TODO(levysotsky): Speed up?
    Deserialize(key, ExtractTo<INodePtr>(cursor));
}

void TUnversionedOwningRow::Save(TStreamSaveContext& context) const
{
    NYT::Save(context, SerializeToString(Get()));
}

void TUnversionedOwningRow::Load(TStreamLoadContext& context)
{
    *this = DeserializeFromString(NYT::Load<TString>(context));
}

////////////////////////////////////////////////////////////////////////////////

TUnversionedRowBuilder::TUnversionedRowBuilder(int initialValueCapacity /*= 16*/)
{
    RowData_.resize(GetUnversionedRowByteSize(initialValueCapacity));
    Reset();
    GetHeader()->Capacity = initialValueCapacity;
}

int TUnversionedRowBuilder::AddValue(const TUnversionedValue& value)
{
    auto* header = GetHeader();
    if (header->Count == header->Capacity) {
        auto valueCapacity = 2 * std::max(1U, header->Capacity);
        RowData_.resize(GetUnversionedRowByteSize(valueCapacity));
        header = GetHeader();
        header->Capacity = valueCapacity;
    }

    *GetValue(header->Count) = value;
    return header->Count++;
}

TMutableUnversionedRow TUnversionedRowBuilder::GetRow()
{
    return TMutableUnversionedRow(GetHeader());
}

void TUnversionedRowBuilder::Reset()
{
    auto* header = GetHeader();
    header->Count = 0;
}

TUnversionedRowHeader* TUnversionedRowBuilder::GetHeader()
{
    return reinterpret_cast<TUnversionedRowHeader*>(RowData_.data());
}

TUnversionedValue* TUnversionedRowBuilder::GetValue(ui32 index)
{
    return reinterpret_cast<TUnversionedValue*>(GetHeader() + 1) + index;
}

////////////////////////////////////////////////////////////////////////////////

TUnversionedOwningRowBuilder::TUnversionedOwningRowBuilder(int initialValueCapacity /*= 16*/)
    : InitialValueCapacity_(initialValueCapacity)
{
    Reset();
}

int TUnversionedOwningRowBuilder::AddValue(const TUnversionedValue& value)
{
    auto* header = GetHeader();
    if (header->Count == header->Capacity) {
        auto valueCapacity = 2 * std::max(1U, header->Capacity);
        RowData_.Resize(GetUnversionedRowByteSize(valueCapacity));
        header = GetHeader();
        header->Capacity = valueCapacity;
    }

    auto* newValue = GetValue(header->Count);
    *newValue = value;

    if (IsStringLikeType(value.Type)) {
        const char* oldStringDataPtr = StringData_.Begin();
        auto oldStringDataLength = StringData_.Size();
        StringData_.Append(value.Data.String, value.Length);
        const char* newStringDataPtr = StringData_.Begin();
        newValue->Data.String = newStringDataPtr + oldStringDataLength;
        if (newStringDataPtr != oldStringDataPtr) {
            for (int index = 0; index < static_cast<int>(header->Count); ++index) {
                auto* existingValue = GetValue(index);
                if (IsStringLikeType(existingValue->Type)) {
                    existingValue->Data.String = newStringDataPtr + (existingValue->Data.String - oldStringDataPtr);
                }
            }
        }
    }

    return header->Count++;
}

TUnversionedValue* TUnversionedOwningRowBuilder::BeginValues()
{
    return reinterpret_cast<TUnversionedValue*>(GetHeader() + 1);
}

TUnversionedValue* TUnversionedOwningRowBuilder::EndValues()
{
    return BeginValues() + GetHeader()->Count;
}

TUnversionedOwningRow TUnversionedOwningRowBuilder::FinishRow()
{
    auto row = TUnversionedOwningRow(
        TSharedMutableRef::FromBlob(std::move(RowData_)),
        TSharedRef::FromBlob(std::move(StringData_)));
    Reset();
    return row;
}

void TUnversionedOwningRowBuilder::Reset()
{
    RowData_.Resize(GetUnversionedRowByteSize(InitialValueCapacity_));

    auto* header = GetHeader();
    header->Count = 0;
    header->Capacity = InitialValueCapacity_;
}

TUnversionedRowHeader* TUnversionedOwningRowBuilder::GetHeader()
{
    return reinterpret_cast<TUnversionedRowHeader*>(RowData_.Begin());
}

TUnversionedValue* TUnversionedOwningRowBuilder::GetValue(ui32 index)
{
    return reinterpret_cast<TUnversionedValue*>(GetHeader() + 1) + index;
}

////////////////////////////////////////////////////////////////////////////////

void TUnversionedOwningRow::Init(TUnversionedValueRange range)
{
    int count = std::ssize(range);

    size_t fixedSize = GetUnversionedRowByteSize(count);
    RowData_ = TSharedMutableRef::Allocate<TOwningRowTag>(fixedSize, {.InitializeStorage = false});
    auto* header = GetHeader();

    header->Count = count;
    header->Capacity = count;
    ::memcpy(header + 1, range.begin(), sizeof(TUnversionedValue) * range.size());

    size_t variableSize = 0;
    for (const auto& otherValue : range) {
        if (IsStringLikeType(otherValue.Type)) {
            variableSize += otherValue.Length;
        }
    }

    if (variableSize > 0) {
        TBlob stringData;
        stringData.Resize(variableSize);
        char* current = stringData.Begin();
        for (int index = 0; index < count; ++index) {
            const auto& otherValue = range[index];
            auto& value = reinterpret_cast<TUnversionedValue*>(header + 1)[index];
            if (IsStringLikeType(otherValue.Type)) {
                ::memcpy(current, otherValue.Data.String, otherValue.Length);
                value.Data.String = current;
                current += otherValue.Length;
            }
        }
        StringData_ = TSharedRef::FromBlob(std::move(stringData));
    }
}

////////////////////////////////////////////////////////////////////////////////

TLegacyOwningKey WidenKey(const TLegacyOwningKey& key, ui32 keyColumnCount, EValueType sentinelType)
{
    return WidenKeyPrefix(key, key.GetCount(), keyColumnCount, sentinelType);
}

TLegacyKey WidenKey(const TLegacyKey& key, ui32 keyColumnCount, const TRowBufferPtr& rowBuffer, EValueType sentinelType)
{
    return WidenKeyPrefix(key, key.GetCount(), keyColumnCount, rowBuffer, sentinelType);
}

TLegacyOwningKey WidenKeySuccessor(const TLegacyOwningKey& key, ui32 keyColumnCount, EValueType sentinelType)
{
    YT_VERIFY(static_cast<int>(keyColumnCount) >= key.GetCount());

    TUnversionedOwningRowBuilder builder;
    for (int index = 0; index < key.GetCount(); ++index) {
        builder.AddValue(key[index]);
    }

    for (ui32 index = key.GetCount(); index < keyColumnCount; ++index) {
        builder.AddValue(MakeUnversionedSentinelValue(sentinelType));
    }

    builder.AddValue(MakeUnversionedSentinelValue(EValueType::Max));

    return builder.FinishRow();
}

TLegacyKey WidenKeySuccessor(const TLegacyKey& key, ui32 keyColumnCount, const TRowBufferPtr& rowBuffer, EValueType sentinelType)
{
    YT_VERIFY(keyColumnCount >= key.GetCount());

    auto wideKey = rowBuffer->AllocateUnversioned(keyColumnCount + 1);

    for (ui32 index = 0; index < key.GetCount(); ++index) {
        wideKey[index] = rowBuffer->CaptureValue(key[index]);
    }

    for (ui32 index = key.GetCount(); index < keyColumnCount; ++index) {
        wideKey[index] = MakeUnversionedSentinelValue(sentinelType);
    }

    wideKey[keyColumnCount] = MakeUnversionedSentinelValue(EValueType::Max);

    return wideKey;
}

TLegacyOwningKey WidenKeyPrefix(const TLegacyOwningKey& key, ui32 prefixLength, ui32 keyColumnCount, EValueType sentinelType)
{
    YT_VERIFY(static_cast<int>(prefixLength) <= key.GetCount() && prefixLength <= keyColumnCount);

    if (key.GetCount() == static_cast<int>(prefixLength) && prefixLength == keyColumnCount) {
        return key;
    }

    TUnversionedOwningRowBuilder builder;
    for (ui32 index = 0; index < prefixLength; ++index) {
        builder.AddValue(key[index]);
    }

    for (ui32 index = prefixLength; index < keyColumnCount; ++index) {
        builder.AddValue(MakeUnversionedSentinelValue(sentinelType));
    }

    return builder.FinishRow();
}

TLegacyKey WidenKeyPrefix(TLegacyKey key, ui32 prefixLength, ui32 keyColumnCount, const TRowBufferPtr& rowBuffer, EValueType sentinelType)
{
    YT_VERIFY(prefixLength <= key.GetCount() && prefixLength <= keyColumnCount);

    if (key.GetCount() == prefixLength && prefixLength == keyColumnCount) {
        return rowBuffer->CaptureRow(key);
    }

    auto wideKey = rowBuffer->AllocateUnversioned(keyColumnCount);

    for (ui32 index = 0; index < prefixLength; ++index) {
        wideKey[index] = rowBuffer->CaptureValue(key[index]);
    }

    for (ui32 index = prefixLength; index < keyColumnCount; ++index) {
        wideKey[index] = MakeUnversionedSentinelValue(sentinelType);
    }

    return wideKey;
}

////////////////////////////////////////////////////////////////////////////////

TSharedRange<TRowRange> MakeSingletonRowRange(
    TLegacyKey lowerBound,
    TLegacyKey upperBound,
    TRowBufferPtr rowBuffer)
{
    if (!rowBuffer) {
        rowBuffer = New<TRowBuffer>();
    }

    TCompactVector<TRowRange, 1> ranges(1, TRowRange(
        rowBuffer->CaptureRow(lowerBound),
        rowBuffer->CaptureRow(upperBound)));
    return MakeSharedRange(
        std::move(ranges),
        std::move(rowBuffer));
}

////////////////////////////////////////////////////////////////////////////////

TKeyRef ToKeyRef(TUnversionedRow row)
{
    return row.Elements();
}

TUnversionedValueRange ToKeyRef(TUnversionedRow row, int prefixLength)
{
    return row.FirstNElements(prefixLength);
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, TUnversionedRow row, TStringBuf format)
{
    if (row) {
        builder->AppendChar('[');
        JoinToString(
            builder,
            row.Begin(),
            row.End(),
            [&] (TStringBuilderBase* builder, const TUnversionedValue& value) {
                FormatValue(builder, value, format);
            });
        builder->AppendChar(']');
    } else {
        builder->AppendString("<null>");
    }
}

void FormatValue(TStringBuilderBase* builder, TMutableUnversionedRow row, TStringBuf format)
{
    FormatValue(builder, TUnversionedRow(row), format);
}

void FormatValue(TStringBuilderBase* builder, const TUnversionedOwningRow& row, TStringBuf format)
{
    FormatValue(builder, TUnversionedRow(row), format);
}

TString ToString(TUnversionedRow row, bool valuesOnly)
{
    return ToStringViaBuilder(row, valuesOnly ? "k" : "");
}

TString ToString(TMutableUnversionedRow row, bool valuesOnly)
{
    return ToString(TUnversionedRow(row), valuesOnly);
}

TString ToString(const TUnversionedOwningRow& row, bool valuesOnly)
{
    return ToString(row.Get(), valuesOnly);
}

////////////////////////////////////////////////////////////////////////////////

size_t TDefaultUnversionedValueRangeHash::operator()(TUnversionedValueRange range) const
{
    return GetFarmFingerprint(range);
}

bool TDefaultUnversionedValueRangeEqual::operator()(TUnversionedValueRange lhs, TUnversionedValueRange rhs) const
{
    if (lhs.size() != rhs.size()) {
        return false;
    }
    for (size_t index = 0; index < lhs.size(); ++index) {
        if (!TDefaultUnversionedValueEqual()(lhs[index], rhs[index])) {
            return false;
        }
    }
    return true;
}

////////////////////////////////////////////////////////////////////////////////

size_t TDefaultUnversionedRowHash::operator()(TUnversionedRow row) const
{
    return TDefaultUnversionedValueRangeHash()(row.Elements());
}

bool TDefaultUnversionedRowEqual::operator()(TUnversionedRow lhs, TUnversionedRow rhs) const
{
    return lhs == rhs;
}

////////////////////////////////////////////////////////////////////////////////

size_t TBitwiseUnversionedValueRangeHash::operator()(TUnversionedValueRange range) const
{
    size_t result = 0;
    for (const auto& value : range) {
        HashCombine(result, TBitwiseUnversionedValueHash()(value));
    }
    return result;
}

bool TBitwiseUnversionedValueRangeEqual::operator()(TUnversionedValueRange lhs, TUnversionedValueRange rhs) const
{
    if (lhs.size() != rhs.size()) {
        return false;
    }
    for (size_t index = 0; index < lhs.size(); ++index) {
        if (!TBitwiseUnversionedValueEqual()(lhs[index], rhs[index])) {
            return false;
        }
    }
    return true;
}

////////////////////////////////////////////////////////////////////////////////

size_t TBitwiseUnversionedRowHash::operator()(TUnversionedRow row) const
{
    return TBitwiseUnversionedValueRangeHash()(row.Elements());
}

bool TBitwiseUnversionedRowEqual::operator()(TUnversionedRow lhs, TUnversionedRow rhs) const
{
    return TBitwiseUnversionedValueRangeEqual()(lhs.Elements(), rhs.Elements());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
