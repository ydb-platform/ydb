#include "constructor.h"
#include "meta.h"

#include <ydb/core/tx/schemeshard/olap/schema/schema.h>

namespace NKikimr::NOlap::NIndexes::NBloomNGramm {

std::shared_ptr<IIndexMeta> TIndexConstructor::DoCreateIndexMeta(
    const ui32 indexId, const TString& indexName, const NSchemeShard::TOlapSchema& currentSchema, NSchemeShard::IErrorCollector& errors) const {
    auto* columnInfo = currentSchema.GetColumns().GetByName(ColumnName);
    if (!columnInfo) {
        errors.AddError("no column with name " + ColumnName);
        return nullptr;
    }
    const ui32 columnId = columnInfo->GetId();
    return std::make_shared<TIndexMeta>(indexId, indexName, GetStorageId().value_or(NBlobOperations::TGlobal::DefaultStorageId), columnId,
        HashesCount, FilterSizeBytes, NGrammSize);
}

NKikimr::TConclusionStatus TIndexConstructor::DoDeserializeFromJson(const NJson::TJsonValue& jsonInfo) {
    if (!jsonInfo.Has("column_name")) {
        return TConclusionStatus::Fail("column_name have to be in bloom ngramm filter features");
    }
    if (!jsonInfo["column_name"].GetString(&ColumnName)) {
        return TConclusionStatus::Fail("column_name have to be string in bloom ngramm filter features");
    }
    if (!ColumnName) {
        return TConclusionStatus::Fail("empty column_name in bloom ngramm filter features");
    }

    if (!jsonInfo["ngramm_size"].IsUInteger()) {
        return TConclusionStatus::Fail("ngramm_size have to be in bloom filter features as uint field");
    }
    NGrammSize = jsonInfo["ngramm_size"].GetUInteger();
    if (NGrammSize < 3 || NGrammSize > 10) {
        return TConclusionStatus::Fail("ngramm_size have to be in bloom ngramm filter in interval [3, 10]");
    }

    if (!jsonInfo["filter_size_bytes"].IsUInteger()) {
        return TConclusionStatus::Fail("filter_size_bytes have to be in bloom filter features as uint field");
    }
    FilterSizeBytes = jsonInfo["filter_size_bytes"].GetUInteger();
    if (FilterSizeBytes < 128 || FilterSizeBytes > (1 << 20)) {
        return TConclusionStatus::Fail("filter_size_bytes have to be in bloom ngramm filter in interval [128, 1Mb]");
    }

    if (!jsonInfo["hashes_count"].IsUInteger()) {
        return TConclusionStatus::Fail("hashes_count have to be in bloom filter features as uint field");
    }
    HashesCount = jsonInfo["hashes_count"].GetUInteger();
    if (HashesCount < 1 || HashesCount > 10) {
        return TConclusionStatus::Fail("hashes_count have to be in bloom ngramm filter in interval [1, 10]");
    }
    return TConclusionStatus::Success();
}

NKikimr::TConclusionStatus TIndexConstructor::DoDeserializeFromProto(const NKikimrSchemeOp::TOlapIndexRequested& proto) {
    if (!proto.HasBloomNGrammFilter()) {
        const TString errorMessage = "not found BloomNGrammFilter section in proto: \"" + proto.DebugString() + "\"";
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("problem", errorMessage);
        return TConclusionStatus::Fail(errorMessage);
    }
    auto& bFilter = proto.GetBloomNGrammFilter();
    NGrammSize = bFilter.GetNGrammSize();
    if (NGrammSize < 3 || NGrammSize > 10) {
        return TConclusionStatus::Fail("NGrammSize have to be in [3, 10]");
    }
    FilterSizeBytes = bFilter.GetFilterSizeBytes();
    if (FilterSizeBytes < 128 || FilterSizeBytes > (1 << 20)) {
        return TConclusionStatus::Fail("FilterSizeBytes have to be in [128, 1Mb]");
    }
    HashesCount = bFilter.GetHashesCount();
    if (HashesCount < 1 || HashesCount > 10) {
        return TConclusionStatus::Fail("HashesCount size have to be in [3, 10]");
    }
    ColumnName = bFilter.GetColumnName();
    if (!ColumnName) {
        return TConclusionStatus::Fail("empty column name");
    }
    return TConclusionStatus::Success();
}

void TIndexConstructor::DoSerializeToProto(NKikimrSchemeOp::TOlapIndexRequested& proto) const {
    auto* filterProto = proto.MutableBloomNGrammFilter();
    filterProto->SetColumnName(ColumnName);
    filterProto->SetNGrammSize(NGrammSize);
    filterProto->SetFilterSizeBytes(FilterSizeBytes);
    filterProto->SetHashesCount(HashesCount);
}

}   // namespace NKikimr::NOlap::NIndexes::NBloomNGramm
