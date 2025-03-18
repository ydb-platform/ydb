#include "header.h"

#include <ydb/core/formats/arrow/serializer/abstract.h>

#include <ydb/library/formats/arrow/validation/validation.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

TConclusion<TSubColumnsHeader> TSubColumnsHeader::ReadHeader(const TString& originalData, const TChunkConstructionData& /*externalInfo*/) {
    TStringInput si(originalData);
    ui32 protoSize;
    si.Read(&protoSize, sizeof(protoSize));
    ui32 currentIndex = sizeof(protoSize);
    NKikimrArrowAccessorProto::TSubColumnsAccessor proto;
    if (!proto.ParseFromArray(originalData.data() + currentIndex, protoSize)) {
        return TConclusionStatus::Fail("cannot parse proto");
    }
    currentIndex += protoSize;
    TDictStats columnStats = [&]() {
        if (proto.GetColumnStatsSize()) {
            std::shared_ptr<arrow::RecordBatch> rbColumnStats =
                NArrow::DeserializeBatch(TString(originalData.data() + currentIndex, proto.GetColumnStatsSize()), TDictStats::GetStatsSchema());
            return TDictStats(rbColumnStats);
        } else {
            return TDictStats::BuildEmpty();
        }
    }();
    currentIndex += proto.GetColumnStatsSize();
    TDictStats otherStats = [&]() {
        if (proto.GetOtherStatsSize()) {
            std::shared_ptr<arrow::RecordBatch> rbOtherStats =
                NArrow::DeserializeBatch(TString(originalData.data() + currentIndex, proto.GetOtherStatsSize()), TDictStats::GetStatsSchema());
            return TDictStats(rbOtherStats);
        } else {
            return TDictStats::BuildEmpty();
        }
    }();
    currentIndex += proto.GetOtherStatsSize();
    return TSubColumnsHeader(std::move(columnStats), std::move(otherStats), std::move(proto), currentIndex);
}

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
