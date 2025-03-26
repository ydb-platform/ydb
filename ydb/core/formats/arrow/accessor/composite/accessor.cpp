#include "accessor.h"

#include <ydb/core/formats/arrow/arrow_filter.h>

#include <ydb/library/formats/arrow/arrow_helpers.h>
namespace NKikimr::NArrow::NAccessor {

namespace {
class TCompositeChunkAccessor {
private:
    const std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>>& Chunks;
    std::optional<IChunkedArray::TLocalChunkedArrayAddress>* ResultArrayAddress = nullptr;

public:
    TCompositeChunkAccessor(const std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>>& chunks,
        std::optional<IChunkedArray::TLocalChunkedArrayAddress>& result)
        : Chunks(chunks)
        , ResultArrayAddress(&result) {
    }
    ui64 GetChunksCount() const {
        return Chunks.size();
    }
    ui64 GetChunkLength(const ui32 idx) const {
        return Chunks[idx]->GetRecordsCount();
    }
    void OnArray(const ui32 chunkIdx, const ui32 startPosition) const {
        if (ResultArrayAddress) {
            *ResultArrayAddress = NArrow::NAccessor::IChunkedArray::TLocalChunkedArrayAddress(Chunks[chunkIdx], startPosition, chunkIdx);
        }
    }
};

}   // namespace

std::shared_ptr<IChunkedArray> ICompositeChunkedArray::DoISlice(const ui32 offset, const ui32 count) const {
    ui32 currentIndex = 0;
    std::optional<IChunkedArray::TFullChunkedArrayAddress> arrAddress;
    std::vector<std::shared_ptr<IChunkedArray>> chunks;
    while (currentIndex < offset + count) {
        arrAddress = GetArray(arrAddress, currentIndex, nullptr);
        const auto& arr = arrAddress->GetArray();
        if (currentIndex + arr->GetRecordsCount() < offset) {
        } else if (currentIndex >= offset && currentIndex + arr->GetRecordsCount() <= offset + count) {
            chunks.emplace_back(arr);
        } else {
            const ui32 localStart = std::max<ui32>(offset, currentIndex);
            const ui32 localFinish = std::min<ui32>(offset + count, currentIndex + arr->GetRecordsCount());
            AFL_VERIFY(localStart < localFinish)("start", localStart)("finish", localFinish);
            chunks.emplace_back(arrAddress->GetArray()->ISlice(localStart - currentIndex, localFinish - localStart));
        }
        currentIndex += arr->GetRecordsCount();
    }
    if (chunks.size() == 1) {
        return chunks.front();
    } else {
        return std::make_shared<TCompositeChunkedArray>(std::move(chunks), count, GetDataType());
    }
}

std::shared_ptr<IChunkedArray> ICompositeChunkedArray::DoApplyFilter(const TColumnFilter& filter) const {
    std::optional<IChunkedArray::TFullChunkedArrayAddress> arrAddress;
    std::vector<std::shared_ptr<IChunkedArray>> chunks;
    ui32 currentIndex = 0;
    while (currentIndex < GetRecordsCount()) {
        arrAddress = GetArray(arrAddress, currentIndex, nullptr);
        if (filter.CheckSlice(currentIndex, arrAddress->GetArray()->GetRecordsCount())) {
            auto sliceFilter = filter.Slice(currentIndex, arrAddress->GetArray()->GetRecordsCount());
            chunks.emplace_back(sliceFilter.Apply(arrAddress->GetArray()));
        }
        currentIndex += arrAddress->GetArray()->GetRecordsCount();
    }
    if (chunks.size() == 1) {
        return chunks.front();
    } else {
        return std::make_shared<TCompositeChunkedArray>(std::move(chunks), filter.GetFilteredCountVerified(), GetDataType());
    }
}

IChunkedArray::TLocalDataAddress TCompositeChunkedArray::DoGetLocalData(
    const std::optional<TCommonChunkAddress>& /*chunkCurrent*/, const ui64 /*position*/) const {
    AFL_VERIFY(false);
    return IChunkedArray::TLocalDataAddress(nullptr, 0, 0);
}

IChunkedArray::TLocalChunkedArrayAddress TCompositeChunkedArray::DoGetLocalChunkedArray(
    const std::optional<TCommonChunkAddress>& chunkCurrent, const ui64 position) const {
    std::optional<IChunkedArray::TLocalChunkedArrayAddress> result;
    TCompositeChunkAccessor accessor(Chunks, result);
    SelectChunk(chunkCurrent, position, accessor);
    AFL_VERIFY(result);
    return *result;
}

std::optional<bool> TCompositeChunkedArray::DoCheckOneValueAccessor(std::shared_ptr<arrow::Scalar>& value) const {
    std::optional<std::shared_ptr<arrow::Scalar>> result;
    for (auto&& i : Chunks) {
        std::shared_ptr<arrow::Scalar> valLocal;
        auto res = i->CheckOneValueAccessor(valLocal);
        if (!res || !*res) {
            return res;
        }
        if (!result) {
            result = valLocal;
        } else if (!NArrow::ScalarCompareNullable(*result, valLocal)) {
            return false;
        }
    }
    AFL_VERIFY(!!result);
    value = *result;
    return true;
}

}   // namespace NKikimr::NArrow::NAccessor
