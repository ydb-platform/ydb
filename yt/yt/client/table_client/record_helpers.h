#pragma once

#include "unversioned_row.h"

#include <yt/yt/client/api/rowset.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

template <class TRecord>
TRecord ToRecord(
    TUnversionedRow row,
    const typename TRecord::TRecordDescriptor::TIdMapping& idMapping);

template <class TRecord>
std::vector<TRecord> ToRecords(
    TRange<TUnversionedRow> rows,
    const typename TRecord::TRecordDescriptor::TIdMapping& idMapping);

template <class TRecord>
std::vector<TRecord> ToRecords(const NApi::IUnversionedRowsetPtr& rowset);

template <class TRecord>
std::optional<TRecord> ToOptionalRecord(
    TUnversionedRow row,
    const typename TRecord::TRecordDescriptor::TIdMapping& idMapping);

template <class TRecord>
std::vector<std::optional<TRecord>> ToOptionalRecords(
    TRange<TUnversionedRow> rows,
    const typename TRecord::TRecordDescriptor::TIdMapping& idMapping);

template <class TRecord>
std::vector<std::optional<TRecord>> ToOptionalRecords(const NApi::IUnversionedRowsetPtr& rowset);

////////////////////////////////////////////////////////////////////////////////

template <class TRecord>
TUnversionedRow FromRecord(
    const TRecord& record,
    const TRowBufferPtr& rowBuffer,
    const typename TRecord::TRecordDescriptor::TIdMapping& idMapping =
        TRecord::TRecordDescriptor::Get()->GetIdMapping(),
    NTableClient::EValueFlags flags = NTableClient::EValueFlags::None);

template <class TRecord>
TUnversionedOwningRow FromRecord(
    const TRecord& record,
    const typename TRecord::TRecordDescriptor::TIdMapping& idMapping =
        TRecord::TRecordDescriptor::Get()->GetIdMapping(),
    NTableClient::EValueFlags flags = NTableClient::EValueFlags::None);

template <class TRecord>
TSharedRange<TUnversionedRow> FromRecords(
    TRange<TRecord> records,
    const TRowBufferPtr& rowBuffer,
    const typename TRecord::TRecordDescriptor::TIdMapping& idMapping =
        TRecord::TRecordDescriptor::Get()->GetIdMapping(),
    NTableClient::EValueFlags flags = NTableClient::EValueFlags::None);

template <class TRecord>
TSharedRange<TUnversionedRow> FromRecords(
    TRange<TRecord> records,
    const typename TRecord::TRecordDescriptor::TIdMapping& idMapping =
        TRecord::TRecordDescriptor::Get()->GetIdMapping(),
    NTableClient::EValueFlags flags = NTableClient::EValueFlags::None);

////////////////////////////////////////////////////////////////////////////////

template <class TRecordKey>
TLegacyKey FromRecordKey(
    const TRecordKey& recordKey,
    const TRowBufferPtr& rowBuffer);

template <class TRecordKey>
TLegacyOwningKey FromRecordKey(
    const TRecordKey& recordKey);

template <class TRecordKey>
TSharedRange<TLegacyKey> FromRecordKeys(
    TRange<TRecordKey> recordKeys,
    const TRowBufferPtr& rowBuffer);

template <class TRecordKey>
TSharedRange<TLegacyKey> FromRecordKeys(
    TRange<TRecordKey> recordKeys);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

#define RECORD_HELPERS_INL_H_
#include "record_helpers-inl.h"
#undef RECORD_HELPERS_INL_H_
