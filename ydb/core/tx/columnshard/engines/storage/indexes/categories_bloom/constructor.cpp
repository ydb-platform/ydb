#include "constructor.h"
#include "meta.h"

#include <ydb/core/tx/columnshard/engines/storage/indexes/portions/extractor/default.h>
#include <ydb/core/tx/schemeshard/olap/schema/schema.h>

namespace NKikimr::NOlap::NIndexes::NCategoriesBloom {

std::shared_ptr<IIndexMeta> TBloomIndexConstructor::DoCreateIndexMeta(
    const ui32 indexId, const TString& indexName, const NSchemeShard::TOlapSchema& currentSchema, NSchemeShard::IErrorCollector& errors) const {
    auto* columnInfo = currentSchema.GetColumns().GetByName(GetColumnName());
    if (!columnInfo) {
        errors.AddError("no column with name " + GetColumnName());
        return nullptr;
    }
    return std::make_shared<TIndexMeta>(indexId, indexName, GetStorageId().value_or(NBlobOperations::TGlobal::DefaultStorageId),
        columnInfo->GetId(), FalsePositiveProbability, std::make_shared<TDefaultDataExtractor>(), TBase::GetBitsStorageConstructor());
}

NKikimr::TConclusionStatus TBloomIndexConstructor::DoDeserializeFromJson(const NJson::TJsonValue& jsonInfo) {
    {
        auto conclusion = TBase::DoDeserializeFromJson(jsonInfo);
        if (conclusion.IsFail()) {
            return conclusion;
        }
    }
    if (!jsonInfo["false_positive_probability"].IsDouble()) {
        return TConclusionStatus::Fail("false_positive_probability have to be in bloom filter features as double field");
    }
    FalsePositiveProbability = jsonInfo["false_positive_probability"].GetDouble();
    if (FalsePositiveProbability < 0.01 || FalsePositiveProbability >= 1) {
        return TConclusionStatus::Fail("false_positive_probability have to be in bloom filter features as double field in interval [0.01, 1)");
    }
    return TConclusionStatus::Success();
}

NKikimr::TConclusionStatus TBloomIndexConstructor::DoDeserializeFromProto(const NKikimrSchemeOp::TOlapIndexRequested& proto) {
    if (!proto.HasBloomFilter()) {
        const TString errorMessage = "not found BloobFilter section in proto: \"" + proto.DebugString() + "\"";
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("problem", errorMessage);
        return TConclusionStatus::Fail(errorMessage);
    }
    auto& bFilter = proto.GetBloomFilter();
    {
        auto conclusion = TBase::DeserializeFromProtoImpl(bFilter);
        if (conclusion.IsFail()) {
            return conclusion;
        }
    }
    FalsePositiveProbability = bFilter.GetFalsePositiveProbability();
    if (FalsePositiveProbability < 0.01 || FalsePositiveProbability >= 1) {
        const TString errorMessage = "FalsePositiveProbability have to be in interval[0.01, 1)";
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("problem", errorMessage);
        return TConclusionStatus::Fail(errorMessage);
    }
    return TConclusionStatus::Success();
}

void TBloomIndexConstructor::DoSerializeToProto(NKikimrSchemeOp::TOlapIndexRequested& proto) const {
    auto* filterProto = proto.MutableBloomFilter();
    filterProto->SetFalsePositiveProbability(FalsePositiveProbability);
    TBase::SerializeToProtoImpl(*filterProto);
}

}   // namespace NKikimr::NOlap::NIndexes::NCategoriesBloom
