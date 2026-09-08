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
    auto headerStats = Header.GetColumnStats();
    // Resolve only among requested columns
    auto pathInfoResult = headerStats.ResolvePath(svPath, [this](const ui32 columnIndex) {
        return PartialColumnsData.HasColumn(columnIndex);
    });
    if (pathInfoResult.IsFail()) {
        return TConclusionStatus::Fail(pathInfoResult.GetErrorMessage());
    }
    auto pathInfo = pathInfoResult.DetachResult();
    const auto columnsAccessor = pathInfo
        ? std::make_shared<NSubColumns::TJsonPathAccessor>(PartialColumnsData.GetAccessorVerified(pathInfo->ColumnIndex),
              std::move(pathInfo->RemainingPath), pathInfo->ValueType)
        : std::make_shared<NSubColumns::TJsonPathAccessor>(nullptr, TString{}, NSubColumns::EValueType::BinaryJson);
    if (!OthersData) {
        AFL_VERIFY(!GetBestPathSource(svPath).IsOther);
        return columnsAccessor;
    }
    auto othersResult = OthersData->GetPathAccessor(svPath, recordsCount);
    if (othersResult.IsFail()) {
        return TConclusionStatus::Fail(othersResult.GetErrorMessage());
    }
    return NSubColumns::TJsonPathAccessor::SelectBestMatch(columnsAccessor, othersResult.DetachResult());
}

}   // namespace NKikimr::NArrow::NAccessor
