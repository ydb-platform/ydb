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
    auto headerStats = Header.GetColumnStats();
    // Resolve only among requested columns
    auto pathInfoResult = headerStats.ResolvePath(svPath, [this](const ui32 columnIndex) {
        return PartialColumnsData.HasColumn(columnIndex);
    });
    if (pathInfoResult.IsFail()) {
        return TConclusionStatus::Fail(pathInfoResult.GetErrorMessage());
    }
    auto pathInfo = pathInfoResult.DetachResult();
    if (pathInfo && PartialColumnsData.HasColumn(pathInfo->ColumnIndex)) {
        return std::make_shared<NSubColumns::TJsonPathAccessor>(PartialColumnsData.GetAccessorVerified(pathInfo->ColumnIndex),
            std::move(pathInfo->RemainingPath), pathInfo->ValueType);
    }

    if (OthersData) {
        return OthersData->GetPathAccessor(svPath, recordsCount);
    }

    AFL_VERIFY(!Header.GetOtherStats().GetKeyIndexOptional(svPath));
    return std::make_shared<NSubColumns::TJsonPathAccessor>(
        std::make_shared<TTrivialArray>(TThreadSimpleArraysCache::GetNull(arrow::binary(), recordsCount)), TString{},
        NSubColumns::EValueType::BinaryJson);
}

}   // namespace NKikimr::NArrow::NAccessor
