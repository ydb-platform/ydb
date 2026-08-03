#pragma once

#include <ydb/core/formats/arrow/accessor/abstract/accessor.h>
#include <ydb/core/formats/arrow/accessor/common/additional_data.h>

#include <ydb/library/formats/arrow/splitter/similar_packer.h>

namespace NKikimr::NOlap::NChunks {

class TChunkedArraySerialized {
private:
    YDB_READONLY_DEF(std::shared_ptr<NArrow::NAccessor::IChunkedArray>, Array);
    YDB_READONLY_DEF(TString, SerializedData);
    YDB_READONLY_DEF(std::shared_ptr<NArrow::NAccessor::IAdditionalAccessorData>, Meta);

public:
    TChunkedArraySerialized(
        const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& array, NArrow::NAccessor::TBlobWithAdditionalAccessorData&& blobAndMeta);
};

template <class TSerializer>
std::vector<TChunkedArraySerialized> SplitBySizes(NArrow::NAccessor::IChunkedArray& array, const TSerializer& serialize,
    const TString& fullSerializedData, const std::vector<ui64>& splitSizes) {
    const std::vector<ui32> recordsCount =
        NArrow::NSplitter::TSimilarPacker::SizesToRecordsCount(array.GetRecordsCount(), fullSerializedData, splitSizes);
    std::vector<TChunkedArraySerialized> result;
    ui32 currentStartIndex = 0;
    for (auto&& i : recordsCount) {
        std::shared_ptr<NArrow::NAccessor::IChunkedArray> slice = array.ISlice(currentStartIndex, i);
        result.emplace_back(slice, serialize(slice));
        currentStartIndex += i;
    }
    return result;
}

}   // namespace NKikimr::NOlap::NChunks
