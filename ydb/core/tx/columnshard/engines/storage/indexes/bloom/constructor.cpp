#include "constructor.h"
#include "meta.h"

#include <ydb/core/tx/columnshard/engines/storage/indexes/helper/index_json_keys.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/portions/extractor/default.h>
#include <ydb/core/tx/schemeshard/olap/schema/schema.h>

namespace NKikimr::NOlap::NIndexes {

std::shared_ptr<IIndexMeta> TBloomIndexConstructor::DoCreateIndexMeta(
    const ui32 indexId, const TString& indexName, const NSchemeShard::TOlapSchema& currentSchema, NSchemeShard::IErrorCollector& errors) const {
    auto* columnInfo = currentSchema.GetColumns().GetByName(GetColumnName());
    if (!columnInfo) {
        errors.AddError("no column with name " + GetColumnName());
        return nullptr;
    }
    const ui32 columnId = columnInfo->GetId();
    return std::make_shared<TBloomIndexMeta>(indexId, indexName, GetStorageId().value_or(NBlobOperations::TGlobal::DefaultStorageId),
        GetInheritPortionStorage().value_or(false), columnId, FalsePositiveProbability, std::make_shared<TDefaultDataExtractor>(), CaseSensitive,
        TBase::GetBitsStorageConstructor());
}

TConclusionStatus TBloomIndexConstructor::ValidateValues() const {
    if (FalsePositiveProbability <= 0 || FalsePositiveProbability >= 1) {
        return TConclusionStatus::Fail("false_positive_probability have to be in bloom filter features as double field in interval (0, 1)");
    }

    return TConclusionStatus::Success();
}

NKikimr::TConclusionStatus TBloomIndexConstructor::DoDeserializeFromJson(const NJson::TJsonValue& jsonInfo) {
    {
        auto conclusion = TBase::DoDeserializeFromJson(jsonInfo);
        if (conclusion.IsFail()) {
            return conclusion;
        }
    }
    if (!jsonInfo[NJsonKeys::FalsePositiveProbability].IsDouble()) {
        return TConclusionStatus::Fail("false_positive_probability have to be in bloom filter features as double field");
    }
    FalsePositiveProbability = jsonInfo[NJsonKeys::FalsePositiveProbability].GetDouble();

    if (jsonInfo.Has(NJsonKeys::CaseSensitive)) {
        if (!jsonInfo[NJsonKeys::CaseSensitive].IsBoolean()) {
            return TConclusionStatus::Fail("case_sensitive have to be in bloom filter features as boolean field");
        }
        CaseSensitive = jsonInfo[NJsonKeys::CaseSensitive].GetBoolean();
    }

    return ValidateValues();
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
    if (bFilter.HasCaseSensitive()) {
        CaseSensitive = bFilter.GetCaseSensitive();
    }

    return ValidateValues();
}

void TBloomIndexConstructor::DoSerializeToProto(NKikimrSchemeOp::TOlapIndexRequested& proto) const {
    auto* filterProto = proto.MutableBloomFilter();
    TBase::SerializeToProtoImpl(*filterProto);
    filterProto->SetFalsePositiveProbability(FalsePositiveProbability);
    filterProto->SetCaseSensitive(CaseSensitive);
}

}   // namespace NKikimr::NOlap::NIndexes
