#include "accessor.h"

#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/arrow_filter.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/formats/arrow/arrow_helpers.h>
#include <ydb/library/formats/arrow/permutations.h>
#include <ydb/library/formats/arrow/size_calcer.h>
#include <ydb/library/formats/arrow/switch/compare.h>
#include <ydb/library/formats/arrow/switch/switch_type.h>

namespace NKikimr::NArrow::NAccessor {

void IChunkedArray::TReader::AppendPositionTo(arrow::ArrayBuilder& builder, const ui64 position, ui64* recordSize) const {
    auto address = GetReadChunk(position);
    AFL_VERIFY(NArrow::Append(builder, *address.GetArray(), address.GetPosition(), recordSize));
}

std::shared_ptr<arrow::Array> IChunkedArray::TReader::CopyRecord(const ui64 recordIndex) const {
    auto address = GetReadChunk(recordIndex);
    return NArrow::CopyRecords(address.GetArray(), { address.GetPosition() });
}

std::shared_ptr<arrow::ChunkedArray> IChunkedArray::Slice(const ui32 offset, const ui32 count) const {
    return GetChunkedArray(TColumnConstructionContext().SetStartIndex(offset).SetRecordsCount(count));
}

IChunkedArray::TFullDataAddress IChunkedArray::GetChunk(const std::optional<TAddressChain>& chunkCurrent, const ui64 position) const {
    AFL_VERIFY(position < GetRecordsCount())("pos", position)("records", GetRecordsCount())(
        "current", chunkCurrent ? chunkCurrent->DebugString() : Default<TString>());
    std::optional<TCommonChunkAddress> address;

    if (IsDataOwner()) {
        if (chunkCurrent) {
            AFL_VERIFY(chunkCurrent->GetSize() == 1)("size", chunkCurrent->GetSize());
        }
        auto localAddress = GetLocalData(address, position);
        TAddressChain addressChain;
        addressChain.Add(localAddress.GetAddress());
        AFL_VERIFY(addressChain.Contains(position));
        return TFullDataAddress(localAddress.GetArray(), std::move(addressChain));
    } else {
        auto chunkedArrayAddress = GetArray(chunkCurrent, position, nullptr);
        //        if (chunkCurrent) {
        //            AFL_VERIFY(chunkCurrent->GetSize() == chunkedArrayAddress.GetAddress().GetSize())("current", chunkCurrent->GetSize())(
        //                                                      "chunked", chunkedArrayAddress.GetAddress().GetSize());
        //        }
        auto localAddress = chunkedArrayAddress.GetArray()->GetLocalData(address, chunkedArrayAddress.GetAddress().GetLocalIndex(position));
        auto fullAddress = std::move(chunkedArrayAddress.MutableAddress());
        fullAddress.Add(localAddress.GetAddress());
        AFL_VERIFY(fullAddress.Contains(position));
        return TFullDataAddress(localAddress.GetArray(), std::move(fullAddress));
    }
}

IChunkedArray::TFullChunkedArrayAddress IChunkedArray::GetArray(
    const std::optional<TAddressChain>& chunkCurrent, const ui64 position, const std::shared_ptr<IChunkedArray>& selfPtr) const {
    AFL_VERIFY(position < GetRecordsCount())("pos", position)("records_count", GetRecordsCount());
    if (IsDataOwner()) {
        AFL_VERIFY(selfPtr);
        TAddressChain chain;
        chain.Add(TCommonChunkAddress(0, GetRecordsCount(), 0));
        return IChunkedArray::TFullChunkedArrayAddress(selfPtr, std::move(chain));
    }
    TAddressChain addressChain;

    auto* currentLevel = this;
    ui32 currentPosition = position;
    ui32 idx = 0;
    std::vector<std::shared_ptr<IChunkedArray>> chainForTemporarySave;
    while (!currentLevel->IsDataOwner()) {
        std::optional<TCommonChunkAddress> currentAddress;
        if (chunkCurrent) {
            currentAddress = chunkCurrent->GetAddress(idx);
        }
        auto nextChunkedArray = currentLevel->GetLocalChunkedArray(currentAddress, currentPosition);
        chainForTemporarySave.emplace_back(nextChunkedArray.GetArray());
        currentLevel = chainForTemporarySave.back().get();
        addressChain.Add(nextChunkedArray.GetAddress());
        AFL_VERIFY(nextChunkedArray.GetAddress().GetStartPosition() <= currentPosition);
        currentPosition -= nextChunkedArray.GetAddress().GetStartPosition();
        ++idx;
    }
    AFL_VERIFY(!chunkCurrent || chunkCurrent->GetSize() - idx <= 1)("idx", idx)("size", chunkCurrent->GetSize());
    return TFullChunkedArrayAddress(chainForTemporarySave.back(), std::move(addressChain));
}

std::shared_ptr<IChunkedArray> IChunkedArray::DoApplyFilter(const TColumnFilter& filter) const {
    auto arr = GetChunkedArray();
    const arrow::FieldVector fields = { std::make_shared<arrow::Field>("applied", GetDataType()) };
    auto schema = std::make_shared<arrow::Schema>(fields);
    auto table = arrow::Table::Make(schema, { arr }, GetRecordsCount());
    AFL_VERIFY(table->num_columns() == 1);
    filter.Apply(table);
    if (table->column(0)->num_chunks() == 1) {
        return std::make_shared<TTrivialArray>(table->column(0)->chunk(0));
    } else {
        return std::make_shared<TTrivialChunkedArray>(table->column(0));
    }
}

std::shared_ptr<IChunkedArray> IChunkedArray::ApplyFilter(const TColumnFilter& filter, const std::shared_ptr<IChunkedArray>& selfPtr) const {
    if (filter.IsTotalAllowFilter()) {
        AFL_VERIFY(selfPtr);
        return selfPtr;
    }
    if (filter.IsTotalDenyFilter()) {
        return TTrivialArray::BuildEmpty(GetDataType());
    }
    auto result = DoApplyFilter(filter);
    AFL_VERIFY(result);
    AFL_VERIFY(result->GetRecordsCount() == filter.GetFilteredCountVerified());
    return result;
}

std::shared_ptr<arrow::ChunkedArray> IChunkedArray::GetChunkedArrayTrivial() const {
    std::vector<std::shared_ptr<arrow::Array>> chunks;
    std::optional<TFullDataAddress> address;
    for (ui32 position = 0; position < GetRecordsCount();) {
        address = GetChunk(address, position);
        chunks.emplace_back(address->GetArray());
        position += address->GetArray()->length();
    }
    return std::make_shared<arrow::ChunkedArray>(chunks, GetDataType());
}

std::shared_ptr<arrow::ChunkedArray> IChunkedArray::GetChunkedArray(const TColumnConstructionContext& context) const {
    if (context.GetStartIndex() || context.GetRecordsCount()) {
        const ui32 start = context.GetStartIndex().value_or(0);
        const ui32 count = context.GetRecordsCount().value_or(GetRecordsCount() - start);
        AFL_VERIFY(start + count <= GetRecordsCount())("start", start)("count", count)("records_count", GetRecordsCount());
    }

    return DoGetChunkedArray(context);
}

std::shared_ptr<arrow::ChunkedArray> IChunkedArray::DoGetChunkedArray(const TColumnConstructionContext& context) const {
    if (context.GetStartIndex() || context.GetRecordsCount()) {
        const ui32 start = context.GetStartIndex().value_or(0);
        const ui32 count = context.GetRecordsCount().value_or(GetRecordsCount() - start);
        auto slice = ISlice(start, count);
        if (context.GetFilter() && !context.GetFilter()->IsTotalAllowFilter()) {
            return slice->ApplyFilter(context.GetFilter()->Slice(start, count), slice)->GetChunkedArrayTrivial();
        } else {
            return slice->GetChunkedArrayTrivial();
        }
    } else if (context.GetFilter() && !context.GetFilter()->IsTotalAllowFilter()) {
        return ApplyFilter(*context.GetFilter(), nullptr)->GetChunkedArrayTrivial();
    } else {
        return GetChunkedArrayTrivial();
    }
}

TString IChunkedArray::TReader::DebugString(const ui32 position) const {
    auto address = GetReadChunk(position);
    return NArrow::DebugString(address.GetArray(), address.GetPosition());
}

std::partial_ordering IChunkedArray::TReader::CompareColumns(
    const std::vector<TReader>& l, const ui64 lPosition, const std::vector<TReader>& r, const ui64 rPosition) {
    AFL_VERIFY(l.size() == r.size());
    for (ui32 i = 0; i < l.size(); ++i) {
        const TAddress lAddress = l[i].GetReadChunk(lPosition);
        const TAddress rAddress = r[i].GetReadChunk(rPosition);
        auto cmp = lAddress.Compare(rAddress);
        if (std::is_neq(cmp)) {
            return cmp;
        }
    }
    return std::partial_ordering::equivalent;
}

IChunkedArray::TAddress IChunkedArray::TReader::GetReadChunk(const ui64 position) const {
    AFL_VERIFY(position < ChunkedArray->GetRecordsCount());
    if (CurrentChunkAddress && CurrentChunkAddress->GetAddress().Contains(position)) {
    } else {
        CurrentChunkAddress = ChunkedArray->GetChunk(CurrentChunkAddress, position);
    }
    return IChunkedArray::TAddress(CurrentChunkAddress->GetArray(), CurrentChunkAddress->GetAddress().GetLocalIndex(position));
}

const std::partial_ordering IChunkedArray::TAddress::Compare(const TAddress& item) const {
    return TComparator::TypedCompare<true>(*Array, Position, *item.Array, item.Position);
}

TChunkedArraySerialized::TChunkedArraySerialized(const std::shared_ptr<IChunkedArray>& array, const TString& serializedData)
    : Array(array)
    , SerializedData(serializedData) {
    AFL_VERIFY(serializedData);
    AFL_VERIFY(Array);
    AFL_VERIFY(Array->GetRecordsCount());
}

std::partial_ordering IChunkedArray::TFullDataAddress::Compare(
    const ui64 position, const TFullDataAddress& item, const ui64 itemPosition) const {
    AFL_VERIFY(Address.Contains(position))("pos", position)("start", Address.DebugString());
    AFL_VERIFY(item.Address.Contains(itemPosition))("pos", itemPosition)("start", item.Address.DebugString());
    return TComparator::TypedCompare<true>(*Array, Address.GetLocalIndex(position), *item.Array, item.Address.GetLocalIndex(itemPosition));
}

std::shared_ptr<arrow::Array> IChunkedArray::TFullDataAddress::CopyRecord(const ui64 recordIndex) const {
    return NArrow::CopyRecords(Array, { Address.GetLocalIndex(recordIndex) });
}

TString IChunkedArray::TFullDataAddress::DebugString(const ui64 position) const {
    return NArrow::DebugString(Array, Address.GetLocalIndex(position));
}

void IChunkedArray::TLocalDataAddress::Reallocate() {
    Array = NArrow::ReallocateArray(Array);
}

}   // namespace NKikimr::NArrow::NAccessor
