#include "constructor.h"
#include "partial.h"

#include <ydb/core/formats/arrow/accessor/plain/accessor.h>

#include <ydb/library/formats/arrow/simple_arrays_cache.h>

namespace NKikimr::NArrow::NAccessor {

void TSubColumnsPartialArray::InitOthers(const TString& blob, const TChunkConstructionData& externalInfo) {
    AFL_VERIFY(!OthersData);
    auto container = NSubColumns::TConstructor::BuildOthersContainer(blob, Header.GetAddressesProto(), externalInfo);
    OthersData = NSubColumns::TOthersData(Header.GetOtherStats(), container.DetachResult());
}

std::shared_ptr<IChunkedArray> TSubColumnsPartialArray::GetPathAccessor(const std::string_view svPath, const ui32 recordsCount) const {
    if (auto idx = Header.GetColumnStats().GetKeyIndexOptional(svPath)) {
        return PartialColumnsData.GetAccessorVerified(*idx);
    }
    if (OthersData) {
        return OthersData->GetPathAccessor(svPath, recordsCount);
    } else {
        AFL_VERIFY(!Header.GetOtherStats().GetKeyIndexOptional(svPath));
        return std::make_shared<TTrivialArray>(TThreadSimpleArraysCache::GetNull(arrow::utf8(), recordsCount));
    }
}

}   // namespace NKikimr::NArrow::NAccessor
