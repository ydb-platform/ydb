#include "const.h"
#include "constructor.h"
#include "meta.h"

#include <ydb/core/tx/columnshard/engines/storage/indexes/portions/extractor/default.h>
#include <ydb/core/tx/schemeshard/olap/schema/schema.h>

namespace NKikimr::NOlap::NIndexes::NBloomNGramm {

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

TConclusionStatus TIndexConstructor::DoDeserializeFromJson(const NJson::TJsonValue& jsonInfo) {
    {
        auto conclusion = TBase::DoDeserializeFromJson(jsonInfo);
        if (conclusion.IsFail()) {
            return conclusion;
        }
    }

    if (!jsonInfo["false_positive_probability"].IsDouble()) {
        return TConclusionStatus::Fail("false_positive_probability have to be in bloom ngramm filter features as double field");
    }

    FalsePositiveProbability = jsonInfo["false_positive_probability"].GetDouble();

    if (FalsePositiveProbability <= 0 || FalsePositiveProbability >= 1) {
        return TConclusionStatus::Fail(
            "false_positive_probability have to be in bloom ngramm filter features as double field in interval (0, 1)");
    }

    if (!jsonInfo["ngramm_size"].IsUInteger()) {
        return TConclusionStatus::Fail("ngramm_size have to be in bloom filter features as uint field");
    }

    NGrammSize = jsonInfo["ngramm_size"].GetUInteger();
    if (!TConstants::CheckNGrammSize(NGrammSize)) {
        return TConclusionStatus::Fail("ngramm_size have to be in bloom ngramm filter in interval " + TConstants::GetNGrammSizeIntervalString());
    }

    if (jsonInfo.Has("hashes_count")) {
        if (!jsonInfo["hashes_count"].IsUInteger()) {
            return TConclusionStatus::Fail("hashes_count have to be in bloom ngramm filter features as uint field");
        }

        HashesCount = jsonInfo["hashes_count"].GetUInteger();
        if (!TConstants::CheckHashesCount(HashesCount)) {
            return TConclusionStatus::Fail(
                "hashes_count have to be in bloom ngramm filter in interval " + TConstants::GetHashesCountIntervalString());
        }
    }

    if (jsonInfo.Has("case_sensitive")) {
        if (!jsonInfo["case_sensitive"].IsBoolean()) {
            return TConclusionStatus::Fail("case_sensitive have to be in bloom filter features as boolean field");
        }
        CaseSensitive = jsonInfo["case_sensitive"].GetBoolean();
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
    if (!TConstants::CheckNGrammSize(NGrammSize)) {
        return TConclusionStatus::Fail("NGrammSize have to be in " + TConstants::GetNGrammSizeIntervalString());
    }

    FalsePositiveProbability = bFilter.GetFalsePositiveProbability();
    if (FalsePositiveProbability <= 0 || FalsePositiveProbability >= 1) {
        return TConclusionStatus::Fail("FalsePositiveProbability have to be in interval (0, 1)");
    }

    HashesCount = bFilter.GetHashesCount();
    if (!TConstants::CheckHashesCount(HashesCount)) {
        return TConclusionStatus::Fail("HashesCount size have to be in " + TConstants::GetHashesCountIntervalString());
    }

    ColumnName = bFilter.GetColumnName();
    if (!ColumnName) {
        return TConclusionStatus::Fail("empty column name");
    }

    if (!DataExtractor.DeserializeFromProto(bFilter.GetDataExtractor())) {
        return TConclusionStatus::Fail("cannot parse data extractor from proto: " + bFilter.GetDataExtractor().DebugString());
    }

    return TConclusionStatus::Success();
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
