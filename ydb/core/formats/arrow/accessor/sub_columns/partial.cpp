#include "constructor.h"
#include "partial.h"

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
            auto insertResult = jsonPathAccessorTrie->Insert(
                NSubColumns::ToJsonPath(headerStats.GetColumnName(i)), PartialColumnsData.GetAccessorVerified(i), headerStats.GetValueType(i));
            AFL_VERIFY(insertResult.IsSuccess())("error", insertResult.GetErrorMessage());
        }
    }

    auto columnsResult = jsonPathAccessorTrie->GetAccessor(svPath);
    if (columnsResult.IsFail()) {
        return columnsResult;
    }

    if (!OthersData) {
        // The fetch stage must have loaded Others if it has the best match.
        AFL_VERIFY(!GetBestPathSource(NSubColumns::ToSubcolumnName(svPath)).IsOther);
        return columnsResult;
    }
    auto othersResult = OthersData->GetPathAccessor(svPath, recordsCount);
    if (othersResult.IsFail()) {
        return othersResult;
    }
    return NSubColumns::TJsonPathAccessor::SelectBestMatch(columnsResult.DetachResult(), othersResult.DetachResult());
}

}   // namespace NKikimr::NArrow::NAccessor
