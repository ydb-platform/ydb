#include "versioned_row.h"

#include "name_table.h"
#include "row_buffer.h"
#include "schema.h"

#include <library/cpp/yt/coding/varint.h>

#include <library/cpp/yt/memory/tls_scratch.h>

#include <numeric>

namespace NYT::NTableClient {

using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

size_t GetByteSize(const TVersionedValue& value)
{
    return EstimateRowValueSize(static_cast<TUnversionedValue>(value)) + MaxVarInt64Size;
}

size_t GetDataWeight(const TVersionedValue& value)
{
    return GetDataWeight(static_cast<TUnversionedValue>(value)) + sizeof(TTimestamp);
}

size_t ReadValue(const char* input, TVersionedValue* value)
{
    int result = ReadRowValue(input, static_cast<TUnversionedValue*>(value));
    result += ReadVarUint64(input + result, &value->Timestamp);
    return result;
}

size_t WriteValue(char* output, const TVersionedValue& value)
{
    int result = WriteRowValue(output, static_cast<TUnversionedValue>(value));
    result += WriteVarUint64(output + result, value.Timestamp);
    return result;
}

void Save(TStreamSaveContext& context, const TVersionedValue& value)
{
    NYT::Save(context, value.Timestamp);
    NTableClient::Save(context, static_cast<const TUnversionedValue&>(value));
}

void Load(TStreamLoadContext& context, TVersionedValue& value, TChunkedMemoryPool* pool)
{
    NYT::Load(context, value.Timestamp);
    NTableClient::Load(context, static_cast<TUnversionedValue&>(value), pool);
}

////////////////////////////////////////////////////////////////////////////////

size_t GetVersionedRowByteSize(
    int keyCount,
    int valueCount,
    int writeTimestampCount,
    int deleteTimestampCount)
{
    return
        sizeof(TVersionedRowHeader) +
        sizeof(TUnversionedValue) * keyCount +
        sizeof(TVersionedValue) * valueCount +
        sizeof(TTimestamp) * writeTimestampCount +
        sizeof(TTimestamp) * deleteTimestampCount;
}

size_t GetDataWeight(TVersionedRow row)
{
    if (!row) {
        return 0;
    }

    size_t result = 0;
    result += std::accumulate(
        row.BeginValues(),
        row.EndValues(),
        0ll,
        [] (size_t x, const TVersionedValue& value) {
            return GetDataWeight(value) + x;
        });

    result += std::accumulate(
        row.BeginKeys(),
        row.EndKeys(),
        0ll,
        [] (size_t x, const TUnversionedValue& value) {
            return GetDataWeight(value) + x;
        });

    result += row.GetWriteTimestampCount() * sizeof(TTimestamp);
    result += row.GetDeleteTimestampCount() * sizeof(TTimestamp);

    return result;
}

void ValidateClientDataRow(
    TVersionedRow row,
    const TTableSchema& schema,
    const TNameTableToSchemaIdMapping& idMapping,
    const TNameTablePtr& nameTable,
    bool allowMissingKeyColumns)
{
    int keyCount = row.GetKeyCount();
    ValidateKeyColumnCount(keyCount);
    ValidateRowValueCount(row.GetValueCount());

    if (!allowMissingKeyColumns) {
        if (keyCount != schema.GetKeyColumnCount()) {
            THROW_ERROR_EXCEPTION("Invalid key count: expected %v, got %v",
                schema.GetKeyColumnCount(),
                keyCount);
        }

        if (nameTable->GetSize() < keyCount) {
            THROW_ERROR_EXCEPTION("Name table size is too small to contain all keys: expected >=%v, got %v",
                row.GetKeyCount(),
                nameTable->GetSize());
        }

        for (int index = 0; index < keyCount; ++index) {
            const auto& expectedName = schema.Columns()[index].Name();
            auto actualName = nameTable->GetName(index);
            if (expectedName != actualName) {
                THROW_ERROR_EXCEPTION("Invalid key column %v in name table: expected %Qv, got %Qv",
                    index,
                    expectedName,
                    actualName);
            }
            ValidateValueType(row.Keys()[index], schema, index, /*typeAnyAcceptsAllValues*/ false);
            ValidateKeyValue(row.Keys()[index]);
        }
    } else {
        for (int index = 0; index < keyCount; ++index) {
            auto name = nameTable->GetName(index);
            int columnIndex = schema.GetColumnIndexOrThrow(name);
            if (columnIndex >= schema.GetColumnCount()) {
                THROW_ERROR_EXCEPTION("Invalid key column %Qv: appears as a key column in row but actually is a data column in table",
                    name);
            }
            ValidateValueType(row.Keys()[index], schema, columnIndex, /*typeAnyAcceptsAllValues*/ false);
            ValidateKeyValue(row.Keys()[index]);
        }
    }

    auto validateTimestamps = [&] (const TTimestamp* begin, const TTimestamp* end) {
        for (const auto* current = begin; current != end; ++current) {
            ValidateWriteTimestamp(*current);
            if (current != begin && *current >= *(current - 1)) {
                THROW_ERROR_EXCEPTION("Timestamps are not monotonically decreasing: %v >= %v",
                    *current,
                    *(current - 1));
            }
        }
    };
    validateTimestamps(row.BeginWriteTimestamps(), row.EndWriteTimestamps());
    validateTimestamps(row.BeginDeleteTimestamps(), row.EndDeleteTimestamps());

    for (const auto* current = row.BeginValues(); current != row.EndValues(); ++current) {
        if (current != row.BeginValues()) {
            auto* prev = current - 1;
            if (current->Id < prev->Id) {
                THROW_ERROR_EXCEPTION("Value ids must be non-decreasing: %v < %v",
                    current->Id,
                    prev->Id);
            }
            if (current->Id == prev->Id && current->Timestamp >= prev->Timestamp) {
                THROW_ERROR_EXCEPTION("Value timestamps must be decreasing: %v >= %v",
                    current->Timestamp,
                    prev->Timestamp);
            }
        }

        const auto& value = *current;
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

        if (mappedId < keyCount) {
            THROW_ERROR_EXCEPTION("Key component %v appears in value part",
                schema.Columns()[mappedId].GetDiagnosticNameString());
        }

        const auto& column = schema.Columns()[mappedId];
        ValidateValueType(value, schema, mappedId, /*typeAnyAcceptsAllValues*/ false);

        if (Any(value.Flags & EValueFlags::Aggregate) && !column.Aggregate()) {
            THROW_ERROR_EXCEPTION(
                "\"aggregate\" flag is set for value in non-aggregating column %v",
                column.GetDiagnosticNameString());
        }

        if (mappedId < schema.GetKeyColumnCount()) {
            THROW_ERROR_EXCEPTION("Key column %v in values",
                column.GetDiagnosticNameString());
        }

        ValidateDataValue(value);
    }

    auto dataWeight = GetDataWeight(row);
    if (dataWeight >= MaxClientVersionedRowDataWeight) {
        THROW_ERROR_EXCEPTION("Row is too large: data weight %v, limit %v",
            dataWeight,
            MaxClientVersionedRowDataWeight);
    }
}

void ValidateDuplicateAndRequiredValueColumns(
    TVersionedRow row,
    const TTableSchema& schema,
    const TNameTableToSchemaIdMapping& idMapping,
    const TTimestamp* writeTimestamps,
    int writeTimestampCount)
{
    if (writeTimestampCount == 0) {
        return;
    }

    auto columnSeenFlags = GetTlsScratchBuffer<bool>(schema.GetColumnCount());

    for (const auto* valueGroupBeginIt = row.BeginValues(), *valueGroupEndIt = valueGroupBeginIt;
        valueGroupBeginIt != row.EndValues();
        valueGroupBeginIt = valueGroupEndIt)
    {
        while (valueGroupEndIt != row.EndValues() && valueGroupBeginIt->Id == valueGroupEndIt->Id) {
            ++valueGroupEndIt;
        }

        int mappedId = ApplyIdMapping(*valueGroupBeginIt, &idMapping);
        if (mappedId < 0) {
            continue;
        }
        const auto& column = schema.Columns()[mappedId];

        if (columnSeenFlags[mappedId]) {
            THROW_ERROR_EXCEPTION("Duplicate value group %v in versioned row",
                column.GetDiagnosticNameString());
        }
        columnSeenFlags[mappedId] = true;

        if (column.Required()) {
            auto mismatch = std::mismatch(
                writeTimestamps,
                writeTimestamps + writeTimestampCount,
                valueGroupBeginIt,
                valueGroupEndIt,
                [] (TTimestamp expected, const TVersionedValue& actual) {
                    return expected == actual.Timestamp;
                });
            if (mismatch.first == writeTimestamps + writeTimestampCount) {
                if (mismatch.second != valueGroupEndIt) {
                    THROW_ERROR_EXCEPTION(
                        "Row-wise write timestamps do not contain write timestamp %v for column %v",
                        *mismatch.second,
                        column.GetDiagnosticNameString());
                }
            } else {
                THROW_ERROR_EXCEPTION(
                    "Required column %v does not contain value for timestamp %Qv",
                    column.GetDiagnosticNameString(),
                    *mismatch.first);
            }
        }
    }

    for (int index = schema.GetKeyColumnCount(); index < schema.GetColumnCount(); ++index) {
        if (!columnSeenFlags[index] && schema.Columns()[index].Required()) {
            THROW_ERROR_EXCEPTION("Missing values for required column %v",
                schema.Columns()[index].GetDiagnosticNameString());
        }
    }
}

TLegacyOwningKey ToOwningKey(TVersionedRow row)
{
    TUnversionedOwningRowBuilder builder;
    for (const auto& value : row.Keys()) {
        builder.AddValue(value);
    }
    return builder.FinishRow();
}

TKeyRef ToKeyRef(TVersionedRow row)
{
    return row.Keys();
}

////////////////////////////////////////////////////////////////////////////////

TVersionedRowBuilder::TVersionedRowBuilder(TRowBufferPtr buffer, bool compaction)
    : Buffer_(std::move(buffer))
    , Compaction_(compaction)
{ }

void TVersionedRowBuilder::AddKey(const TUnversionedValue& value)
{
    Keys_.push_back(Buffer_->CaptureValue(value));
}

void TVersionedRowBuilder::AddValue(const TVersionedValue& value)
{
    WriteTimestamps_.push_back(value.Timestamp);
    Values_.push_back(Buffer_->CaptureValue(value));
}

void TVersionedRowBuilder::AddDeleteTimestamp(TTimestamp timestamp)
{
    DeleteTimestamps_.push_back(timestamp);
}

void TVersionedRowBuilder::AddWriteTimestamp(TTimestamp timestamp)
{
    WriteTimestamps_.push_back(timestamp);
}

TMutableVersionedRow TVersionedRowBuilder::FinishRow()
{
    std::sort(
        Values_.begin(),
        Values_.end(),
        [] (const TVersionedValue& lhs, const TVersionedValue& rhs) -> bool {
            if (lhs.Id < rhs.Id) {
                return true;
            }
            if (lhs.Id > rhs.Id) {
                return false;
            }
            if (lhs.Timestamp < rhs.Timestamp) {
                return false;
            }
            if (lhs.Timestamp > rhs.Timestamp) {
                return true;
            }
            return false;
        });

    std::sort(WriteTimestamps_.begin(), WriteTimestamps_.end(), std::greater<TTimestamp>());

    if (Compaction_) {
        WriteTimestamps_.erase(
            std::unique(WriteTimestamps_.begin(), WriteTimestamps_.end()),
            WriteTimestamps_.end());
    } else if (!WriteTimestamps_.empty()) {
        WriteTimestamps_.erase(WriteTimestamps_.begin() + 1, WriteTimestamps_.end());
    }

    std::sort(DeleteTimestamps_.begin(), DeleteTimestamps_.end(), std::greater<TTimestamp>());
    DeleteTimestamps_.erase(
        std::unique(DeleteTimestamps_.begin(), DeleteTimestamps_.end()),
        DeleteTimestamps_.end());

    auto row = Buffer_->AllocateVersioned(
        Keys_.size(),
        Values_.size(),
        WriteTimestamps_.size(),
        DeleteTimestamps_.size());

    memcpy(row.BeginKeys(), Keys_.data(), sizeof (TUnversionedValue) * Keys_.size());
    memcpy(row.BeginValues(), Values_.data(), sizeof (TVersionedValue)* Values_.size());
    memcpy(row.BeginWriteTimestamps(), WriteTimestamps_.data(), sizeof (TTimestamp) * WriteTimestamps_.size());
    memcpy(row.BeginDeleteTimestamps(), DeleteTimestamps_.data(), sizeof (TTimestamp) * DeleteTimestamps_.size());

    Keys_.clear();
    Values_.clear();
    WriteTimestamps_.clear();
    DeleteTimestamps_.clear();

    return row;
}

////////////////////////////////////////////////////////////////////////////////

TVersionedOwningRow::TVersionedOwningRow(TVersionedRow other)
{
    if (!other) {
        return;
    }

    size_t fixedSize = GetVersionedRowByteSize(
        other.GetKeyCount(),
        other.GetValueCount(),
        other.GetWriteTimestampCount(),
        other.GetDeleteTimestampCount());

    size_t variableSize = 0;
    auto adjustVariableSize = [&] (const TUnversionedValue& value) {
        if (IsStringLikeType(value.Type)) {
            variableSize += value.Length;
        }
    };

    for (const auto& value : other.Keys()) {
        adjustVariableSize(value);
    }
    for (const auto& value : other.Values()) {
        adjustVariableSize(value);
    }

    Data_ = TSharedMutableRef::Allocate(fixedSize + variableSize, {.InitializeStorage = false});

    ::memcpy(GetMutableHeader(), other.GetHeader(), fixedSize);

    if (variableSize > 0) {
        char* current = Data_.Begin() + fixedSize;
        auto captureValue = [&] (TUnversionedValue* value) {
            if (IsStringLikeType(value->Type)) {
                ::memcpy(current, value->Data.String, value->Length);
                value->Data.String = current;
                current += value->Length;
            }
        };

        for (int index = 0; index < other.GetKeyCount(); ++index) {
            captureValue(BeginMutableKeys() + index);
        }
        for (int index = 0; index < other.GetValueCount(); ++index) {
            captureValue(BeginMutableValues() + index);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TVersionedValue& value, TStringBuf /*format*/)
{
    Format(
        builder,
        "%v@%x",
        static_cast<TUnversionedValue>(value),
        value.Timestamp);
}

void FormatValue(TStringBuilderBase* builder, const TVersionedRow& row, TStringBuf /*format*/)
{
    if (!row) {
        builder->AppendString("<null>");
        return;
    }

    builder->AppendChar('[');
    JoinToString(
        builder,
        row.BeginKeys(),
        row.EndKeys(),
        [&] (TStringBuilderBase* builder, const TUnversionedValue& value) {
            FormatValue(builder, value, "k");
        });
    builder->AppendString(" | ");
    JoinToString(builder, row.BeginValues(), row.EndValues(), TDefaultFormatter{});
    builder->AppendString(" | ");
    JoinToString(
        builder,
        row.BeginWriteTimestamps(),
        row.EndWriteTimestamps(),
        [] (TStringBuilderBase* builder, TTimestamp timestamp) {
            builder->AppendFormat("%x", timestamp);
        });
    builder->AppendString(" | ");
    JoinToString(
        builder,
        row.BeginDeleteTimestamps(),
        row.EndDeleteTimestamps(),
        [] (TStringBuilderBase* builder, TTimestamp timestamp) {
            builder->AppendFormat("%x", timestamp);
        });
    builder->AppendChar(']');
}

void FormatValue(TStringBuilderBase* builder, const TMutableVersionedRow& row, TStringBuf /*spec*/)
{
    FormatValue(builder, TVersionedRow(row), TStringBuf{"v"});
}

void FormatValue(TStringBuilderBase* builder, const TVersionedOwningRow& row, TStringBuf /*spec*/)
{
    FormatValue(builder, TVersionedRow(row), TStringBuf{"v"});
}

////////////////////////////////////////////////////////////////////////////////

size_t TBitwiseVersionedValueHash::operator()(const TVersionedValue& value) const
{
    size_t result = 0;
    HashCombine(result, value.Timestamp);
    HashCombine(result, TBitwiseUnversionedValueHash()(value));
    return result;
}

bool TBitwiseVersionedValueEqual::operator()(const TVersionedValue& lhs, const TVersionedValue& rhs) const
{
    return lhs.Timestamp == rhs.Timestamp && TBitwiseUnversionedValueEqual()(lhs, rhs);
}

////////////////////////////////////////////////////////////////////////////////

size_t TBitwiseVersionedRowHash::operator()(TVersionedRow row) const
{
    size_t result = 0;
    for (const auto& value : row.Keys()) {
        HashCombine(result, TBitwiseUnversionedValueHash()(value));
    }
    for (const auto& value : row.Values()) {
        HashCombine(result, TBitwiseVersionedValueHash()(value));
    }
    // Write timestamps are omitted since these are inferred from values.
    for (auto timestamp : row.DeleteTimestamps()) {
        HashCombine(result, timestamp);
    }
    return result;
}

bool TBitwiseVersionedRowEqual::operator()(TVersionedRow lhs, TVersionedRow rhs) const
{
    if (!lhs && !rhs) {
        return true;
    }

    if (!lhs || !rhs) {
        return false;
    }

    if (lhs.GetKeyCount() != rhs.GetKeyCount()) {
        return false;
    }

    for (int index = 0; index < lhs.GetKeyCount(); ++index) {
        if (!TBitwiseUnversionedValueEqual()(lhs.Keys()[index], rhs.Keys()[index])) {
            return false;
        }
    }

    if (lhs.GetValueCount() != rhs.GetValueCount()) {
        return false;
    }

    for (int i = 0; i < lhs.GetValueCount(); ++i) {
        if (!TBitwiseVersionedValueEqual()(lhs.Values()[i], rhs.Values()[i])) {
            return false;
        }
    }

    if (lhs.GetWriteTimestampCount() != rhs.GetWriteTimestampCount()) {
        return false;
    }

    for (int i = 0; i < lhs.GetWriteTimestampCount(); ++i) {
        if (lhs.WriteTimestamps()[i] != rhs.WriteTimestamps()[i]) {
            return false;
        }
    }

    if (lhs.GetDeleteTimestampCount() != rhs.GetDeleteTimestampCount()) {
        return false;
    }

    for (int i = 0; i < lhs.GetDeleteTimestampCount(); ++i) {
        if (lhs.DeleteTimestamps()[i] != rhs.DeleteTimestamps()[i]) {
            return false;
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
