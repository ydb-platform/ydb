#ifndef RECORD_HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include record_helpers.h"
// For the sake of sane code completion.
#include "record_helpers.h"
#endif
#undef RECORD_HELPERS_INL_H_

#include "row_buffer.h"

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

void ValidateRowNotNull(TUnversionedRow row);

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <class TRecord>
TRecord ToRecord(
    TUnversionedRow row,
    const typename TRecord::TRecordDescriptor::TIdMapping& idMapping)
{
    NTableClient::NDetail::ValidateRowNotNull(row);
    return TRecord::FromUnversionedRow(row, idMapping);
}

template <class TRecord>
std::vector<TRecord> ToRecords(
    TRange<TUnversionedRow> rows,
    const typename TRecord::TRecordDescriptor::TIdMapping& idMapping)
{
    std::vector<TRecord> records;
    records.reserve(rows.size());
    for (auto row : rows) {
        records.push_back(ToRecord<TRecord>(row, idMapping));
    }
    return records;
}

template <class TRecord>
std::vector<TRecord> ToRecords(const NApi::IUnversionedRowsetPtr& rowset)
{
    typename TRecord::TRecordDescriptor::TIdMapping idMapping(rowset->GetNameTable());
    return ToRecords<TRecord>(rowset->GetRows(), idMapping);
}

template <class TRecord>
std::optional<TRecord> ToOptionalRecord(
    TUnversionedRow row,
    const typename TRecord::TRecordDescriptor::TIdMapping& idMapping)
{
    if (!row) {
        return std::nullopt;
    }
    return ToRecord<TRecord>(row, idMapping);
}

template <class TRecord>
std::vector<std::optional<TRecord>> ToOptionalRecords(
    TRange<TUnversionedRow> rows,
    const typename TRecord::TRecordDescriptor::TIdMapping& idMapping)
{
    std::vector<std::optional<TRecord>> records;
    records.reserve(rows.size());
    for (auto row : rows) {
        records.push_back(ToOptionalRecord<TRecord>(row, idMapping));
    }
    return records;
}

template <class TRecord>
std::vector<std::optional<TRecord>> ToOptionalRecords(const NApi::IUnversionedRowsetPtr& rowset)
{
    typename TRecord::TRecordDescriptor::TIdMapping idMapping(rowset->GetNameTable());
    return ToOptionalRecords<TRecord>(rowset->GetRows(), idMapping);
}

////////////////////////////////////////////////////////////////////////////////

template <class TRecord>
TUnversionedRow FromRecord(
    const TRecord& record,
    const TRowBufferPtr& rowBuffer,
    const typename TRecord::TRecordDescriptor::TIdMapping& idMapping,
    NTableClient::EValueFlags flags)
{
    return record.ToUnversionedRow(rowBuffer, idMapping, flags);
}

template <class TRecord>
TUnversionedOwningRow FromRecord(
    const TRecord& record,
    const typename TRecord::TRecordDescriptor::TIdMapping& idMapping,
    NTableClient::EValueFlags flags)
{
    // TODO(babenko): optimize
    auto rowBuffer = New<TRowBuffer>(TDefaultRowBufferPoolTag(), 256);
    return TUnversionedOwningRow(FromRecord(record, rowBuffer, idMapping, flags));
}

template <class TRecord>
TSharedRange<TUnversionedRow> FromRecords(
    TRange<TRecord> records,
    const TRowBufferPtr& rowBuffer,
    const typename TRecord::TRecordDescriptor::TIdMapping& idMapping,
    NTableClient::EValueFlags flags)
{
    std::vector<TUnversionedRow> rows;
    rows.reserve(records.size());
    for (const auto& record : records) {
        rows.push_back(FromRecord(record, rowBuffer, idMapping, flags));
    }
    return MakeSharedRange(std::move(rows), rowBuffer);
}

template <class TRecord>
TSharedRange<TUnversionedRow> FromRecords(
    TRange<TRecord> records,
    const typename TRecord::TRecordDescriptor::TIdMapping& idMapping,
    NTableClient::EValueFlags flags)
{
    return FromRecords(records, New<TRowBuffer>(), idMapping, flags);
}

////////////////////////////////////////////////////////////////////////////////

template <class TRecordKey>
TLegacyKey FromRecordKey(
    const TRecordKey& recordKey,
    const TRowBufferPtr& rowBuffer)
{
    return recordKey.ToKey(rowBuffer);
}

template <class TRecordKey>
TLegacyOwningKey FromRecordKey(
    const TRecordKey& recordKey)
{
    // TODO(babenko): optimize
    auto rowBuffer = New<TRowBuffer>(TDefaultRowBufferPoolTag(), 256);
    return TUnversionedOwningRow(FromRecordKey(recordKey, rowBuffer));
}

template <class TRecordKey>
TSharedRange<TLegacyKey> FromRecordKeys(
    TRange<TRecordKey> recordKeys,
    const TRowBufferPtr& rowBuffer)
{
    std::vector<TLegacyKey> keys;
    keys.reserve(recordKeys.size());
    for (const auto& recordKey : recordKeys) {
        keys.push_back(FromRecordKey(recordKey, rowBuffer));
    }
    return MakeSharedRange(std::move(keys), rowBuffer);
}

template <class TRecordKey>
TSharedRange<TLegacyKey> FromRecordKeys(
    TRange<TRecordKey> recordKeys)
{
    return FromRecordKeys(recordKeys, New<TRowBuffer>());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
