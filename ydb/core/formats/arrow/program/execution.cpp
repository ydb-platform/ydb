#include "execution.h"

#include <ydb/core/formats/arrow/accessor/common/chunk_data.h>
#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/accessor/plain/constructor.h>
#include <ydb/core/formats/arrow/serializer/native.h>

#include <ydb/library/formats/arrow/simple_arrays_cache.h>

namespace NKikimr::NArrow::NSSA {

void TSimpleDataSource::DoAssembleAccessor(const TProcessorContext& context, const ui32 columnId, const TString& subColumnName) {
    auto itBlob = Blobs.find(TBlobAddress(columnId, subColumnName));
    AFL_VERIFY(itBlob != Blobs.end());
    auto it = Info.find(columnId);
    AFL_VERIFY(it != Info.end());
    const auto cData = it->second;
    Info.erase(it);
    if (itBlob->second.empty()) {
        context.GetResources()->AddVerified(
            columnId, std::make_shared<NAccessor::TTrivialArray>(NArrow::TThreadSimpleArraysCache::GetNull(cData.GetColumnType(), 0)), true);
    } else {
        context.GetResources()->AddVerified(
            columnId, NAccessor::NPlain::TConstructor().DeserializeFromString(itBlob->second, cData).DetachResult(), true);
    }
}

void TSimpleDataSource::AddBlob(const ui32 columnId, const TString& subColumnName, const std::shared_ptr<arrow::Array>& data) {
    if (!data->length()) {
        Blobs.emplace(TBlobAddress(columnId, subColumnName), "");
        NAccessor::TChunkConstructionData cData(1, nullptr, data->type(), std::make_shared<NSerialization::TNativeSerializer>());
        AFL_VERIFY(Info.emplace(columnId, cData).second);
        return;
    }
    NAccessor::TChunkConstructionData cData(data->length(), nullptr, data->type(), std::make_shared<NSerialization::TNativeSerializer>());
    AFL_VERIFY(Info.emplace(columnId, cData).second);
    Blobs.emplace(TBlobAddress(columnId, subColumnName),
        NAccessor::NPlain::TConstructor().SerializeToString(std::make_shared<NAccessor::TTrivialArray>(data), cData));
}

}   // namespace NKikimr::NArrow::NSSA
