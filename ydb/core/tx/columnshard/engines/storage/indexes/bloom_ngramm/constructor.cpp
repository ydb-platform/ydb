#include "const.h"
#include "constructor.h"
#include "meta.h"

#include <ydb/core/tx/columnshard/engines/storage/indexes/helper/index_json_keys.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/portions/extractor/default.h>
#include <ydb/core/tx/schemeshard/olap/schema/schema.h>

namespace NKikimr::NOlap::NIndexes::NBloomNGramm {

using namespace NJsonKeys;

std::shared_ptr<IIndexMeta> TIndexConstructor::DoCreateIndexMeta(
    const ui32 indexId, const TString& indexName, const NSchemeShard::TOlapSchema& currentSchema, NSchemeShard::IErrorCollector& errors) const {
    auto* columnInfo = currentSchema.GetColumns().GetByName(GetColumnName());
    if (!columnInfo) {
        errors.AddError("no column with name " + GetColumnName());
        return nullptr;
    }

    const ui32 columnId = columnInfo->GetId();
    return std::make_shared<TIndexMeta>(indexId, indexName, GetStorageId().value_or(NBlobOperations::TGlobal::DefaultStorageId),
        GetInheritPortionStorage().value_or(false), columnId, GetDataExtractor(), FalsePositiveProbability, HashesCount, NGrammSize,
        TBase::GetBitsStorageConstructor(), CaseSensitive);
}

TConclusionStatus TIndexConstructor::ValidateValues() const {
    if (auto conclusion = TConstants::ValidateParams(FalsePositiveProbability, NGrammSize, HashesCount); conclusion.IsFail()) {
        return conclusion;
    }
    if (!ColumnName) {
        return TConclusionStatus::Fail("empty column name");
    }
    return TConclusionStatus::Success();
}

TConclusionStatus TIndexConstructor::DoDeserializeFromJson(const NJson::TJsonValue& jsonInfo) {
    {
        auto conclusion = TBase::DoDeserializeFromJson(jsonInfo);
        if (conclusion.IsFail()) {
            return conclusion;
        }
    }

    if (!jsonInfo[NJsonKeys::FalsePositiveProbability].IsDouble()) {
        return TConclusionStatus::Fail("false_positive_probability must be in bloom ngramm filter features as double field");
    }
    FalsePositiveProbability = jsonInfo[NJsonKeys::FalsePositiveProbability].GetDouble();

    if (!jsonInfo[NJsonKeys::NGrammSize].IsUInteger()) {
        return TConclusionStatus::Fail("ngramm_size have to be in bloom filter features as uint field");
    }
    NGrammSize = jsonInfo[NJsonKeys::NGrammSize].GetUInteger();

    if (jsonInfo.Has(NJsonKeys::HashesCount)) {
        if (!jsonInfo[NJsonKeys::HashesCount].IsUInteger()) {
            return TConclusionStatus::Fail("hashes_count have to be in bloom ngramm filter features as uint field");
        }
        HashesCount = jsonInfo[NJsonKeys::HashesCount].GetUInteger();
    }

    if (jsonInfo.Has(NJsonKeys::CaseSensitive)) {
        if (!jsonInfo[NJsonKeys::CaseSensitive].IsBoolean()) {
            return TConclusionStatus::Fail("case_sensitive have to be in bloom filter features as boolean field");
        }
        CaseSensitive = jsonInfo[NJsonKeys::CaseSensitive].GetBoolean();
    }

    return ValidateValues();
}

NKikimr::TConclusionStatus TIndexConstructor::DoDeserializeFromProto(const NKikimrSchemeOp::TOlapIndexRequested& proto) {
    if (!proto.HasBloomNGrammFilter()) {
        const TString errorMessage = "not found BloomNGrammFilter section in proto: \"" + proto.DebugString() + "\"";
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("problem", errorMessage);
        return TConclusionStatus::Fail(errorMessage);
    }

    auto& bFilter = proto.GetBloomNGrammFilter();

    {
        auto conclusion = TBase::DeserializeFromProtoBitsStorageOnly(bFilter);
        if (conclusion.IsFail()) {
            return conclusion;
        }
    }

    if (bFilter.HasCaseSensitive()) {
        CaseSensitive = bFilter.GetCaseSensitive();
    }
    NGrammSize = bFilter.GetNGrammSize();
    FalsePositiveProbability = bFilter.GetFalsePositiveProbability();
    HashesCount = bFilter.GetHashesCount();
    ColumnName = bFilter.GetColumnName();

    if (!DataExtractor.DeserializeFromProto(bFilter.GetDataExtractor())) {
        return TConclusionStatus::Fail("cannot parse data extractor from proto: " + bFilter.GetDataExtractor().DebugString());
    }

    return ValidateValues();
}

void TIndexConstructor::DoSerializeToProto(NKikimrSchemeOp::TOlapIndexRequested& proto) const {
    auto* filterProto = proto.MutableBloomNGrammFilter();
    TBase::SerializeToProtoBitsStorageOnly(*filterProto);
    filterProto->SetColumnName(GetColumnName());
    filterProto->SetCaseSensitive(CaseSensitive);
    filterProto->SetNGrammSize(NGrammSize);
    filterProto->SetHashesCount(HashesCount);
    filterProto->SetFalsePositiveProbability(FalsePositiveProbability);
    *filterProto->MutableDataExtractor() = GetDataExtractor().SerializeToProto();
}

}   // namespace NKikimr::NOlap::NIndexes::NBloomNGramm
