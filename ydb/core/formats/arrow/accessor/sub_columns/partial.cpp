#include "constructor.h"
#include "partial.h"

#include <ydb/core/formats/arrow/accessor/plain/accessor.h>

#include <ydb/library/formats/arrow/simple_arrays_cache.h>

namespace NKikimr::NArrow::NAccessor {

void TSubColumnsPartialArray::InitOthers(const TString& blob, const TChunkConstructionData& externalInfo,
    const std::shared_ptr<NArrow::TColumnFilter>& applyFilter, const bool deserialize) {
    AFL_VERIFY(!OthersData);
    auto container = NSubColumns::TConstructor::BuildOthersContainer(blob, Header.GetAddressesProto(), externalInfo, deserialize);
    OthersData = NSubColumns::TOthersData(Header.GetOtherStats(), container.DetachResult());
    if (applyFilter) {
        OthersData = OthersData->ApplyFilter(*applyFilter, Settings);
    }
    StoreOthersString = blob;
}

TConclusion<std::shared_ptr<NSubColumns::TJsonPathAccessor>> TSubColumnsPartialArray::GetPathAccessor(const std::string_view svPath, const ui32 recordsCount) const {
    auto jsonPathAccessorTrie = std::make_shared<NKikimr::NArrow::NAccessor::NSubColumns::TJsonPathAccessorTrie>();
    auto headerStats = Header.GetColumnStats();
    for (ui32 i = 0; i < headerStats.GetDataNamesCount(); ++i) {
        if (PartialColumnsData.HasColumn(i)) {
            auto insertResult = jsonPathAccessorTrie->Insert(NSubColumns::ToJsonPath(headerStats.GetColumnName(i)), PartialColumnsData.GetAccessorVerified(i));
            AFL_VERIFY(insertResult.IsSuccess())("error", insertResult.GetErrorMessage());
        }
    }

    auto accessorResult = jsonPathAccessorTrie->GetAccessor(svPath);
    if (accessorResult.IsSuccess() && accessorResult.GetResult()->IsValid()) {
        return accessorResult;
    }

    if (OthersData) {
        return OthersData->GetPathAccessor(svPath, recordsCount);
    }

    return std::shared_ptr<NSubColumns::TJsonPathAccessor>{};
}

}   // namespace NKikimr::NArrow::NAccessor
