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
    auto parsedResult = NSubColumns::ParseJsonPath(svPath);
    if (parsedResult.IsFail()) {
        return TConclusionStatus::Fail(parsedResult.GetErrorMessage());
    }
    const auto parsedPath = parsedResult.DetachResult();
    auto headerStats = Header.GetColumnStats();
    auto columnsResult = headerStats.ResolvePath(parsedPath, [this](const ui32 columnIndex) {
        return PartialColumnsData.HasColumn(columnIndex);
    });
    if (columnsResult.IsFail()) {
        return TConclusionStatus::Fail(columnsResult.GetErrorMessage());
    }
    auto columnsPath = columnsResult.DetachResult();
    std::optional<NSubColumns::TDictStats::TResolvedPath> othersPath;
    if (OthersData) {
        auto othersResult = Header.GetOtherStats().ResolvePath(parsedPath);
        if (othersResult.IsFail()) {
            return TConclusionStatus::Fail(othersResult.GetErrorMessage());
        }
        othersPath = othersResult.DetachResult();
    }
    if (othersPath && (!columnsPath || !NSubColumns::TDictStats::TResolvedPath::IsBetterOrEqualMatchThan(*columnsPath, *othersPath))) {
        return OthersData->GetPathAccessor(std::move(*othersPath), recordsCount);
    }
    if (columnsPath) {
        return std::make_shared<NSubColumns::TJsonPathAccessor>(PartialColumnsData.GetAccessorVerified(columnsPath->ColumnIndex),
            std::move(columnsPath->RemainingPath), columnsPath->ValueType);
    }
    auto headerColumnsResult = Header.GetColumnStats().ResolvePath(parsedPath);
    if (headerColumnsResult.IsFail()) {
        return TConclusionStatus::Fail(headerColumnsResult.GetErrorMessage());
    }
    auto headerOthersResult = Header.GetOtherStats().ResolvePath(parsedPath);
    if (headerOthersResult.IsFail()) {
        return TConclusionStatus::Fail(headerOthersResult.GetErrorMessage());
    }
    // A matching path must be loaded before accessor creation.
    AFL_VERIFY(!headerColumnsResult.DetachResult());
    AFL_VERIFY(!headerOthersResult.DetachResult());
    return NSubColumns::TOthersData::BuildEmptyPathAccessor(recordsCount);
}

}   // namespace NKikimr::NArrow::NAccessor
