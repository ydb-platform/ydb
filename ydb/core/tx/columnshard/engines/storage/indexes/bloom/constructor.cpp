#include "constructor.h"
#include "meta.h"

#include <ydb/core/tx/columnshard/engines/storage/indexes/helper/index_parameters.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/portions/extractor/default.h>
#include <ydb/core/tx/schemeshard/olap/schema/schema.h>

namespace NKikimr::NOlap::NIndexes {

TConclusion<double> TBloomIndexConstructor::BuildCanonicalFalsePositiveProbability() const {
    const double fpp = Request.FalsePositiveProbability.value_or(NDefaults::FalsePositiveProbability);
    if (fpp <= 0 || fpp >= 1) {
        return TConclusionStatus::Fail("false_positive_probability have to be in bloom filter features as double field in interval (0, 1)");
    }

    return fpp;
}

std::shared_ptr<IIndexMeta> TBloomIndexConstructor::DoCreateIndexMeta(
    const ui32 indexId, const TString& indexName, const NSchemeShard::TOlapSchema& currentSchema, NSchemeShard::IErrorCollector& errors) const {
    auto* columnInfo = currentSchema.GetColumns().GetByName(GetColumnName());
    if (!columnInfo) {
        errors.AddError("no column with name " + GetColumnName());
        return nullptr;
    }

    auto canonicalFpp = BuildCanonicalFalsePositiveProbability();
    if (canonicalFpp.IsFail()) {
        errors.AddError(canonicalFpp.GetErrorMessage());
        return nullptr;
    }

    const ui32 columnId = columnInfo->GetId();
    return std::make_shared<TBloomIndexMeta>(indexId, indexName, GetStorageId().value_or(NBlobOperations::TGlobal::DefaultStorageId),
        GetInheritPortionStorage().value_or(false), columnId,
        *canonicalFpp,
        std::make_shared<TDefaultDataExtractor>(),
        TBase::GetBitsStorageConstructor());
}

std::shared_ptr<IIndexMeta> TBloomIndexConstructor::DoCreateOrPatchIndexMeta(const ui32 indexId, const TString& indexName,
    const NSchemeShard::TOlapSchema& currentSchema, NSchemeShard::IErrorCollector& errors,
    const IIndexMeta& existingMeta) const {
    if (!ColumnName.empty()) {
        return DoCreateIndexMeta(indexId, indexName, currentSchema, errors);
    }

    const auto colId = existingMeta.GetSingleColumnId();
    if (!colId) {
        errors.AddError("existing index has no single column; cannot determine column for ALTER INDEX");
        return nullptr;
    }

    const auto* col = currentSchema.GetColumns().GetById(*colId);
    if (!col) {
        errors.AddError(TStringBuilder() << "column id " << *colId << " not found in schema for ALTER INDEX");
        return nullptr;
    }

    TBloomIndexConstructor patched = *this;
    patched.ColumnName = col->GetName();
    return patched.DoCreateIndexMeta(indexId, indexName, currentSchema, errors);
}

TConclusionStatus TBloomIndexConstructor::ValidateValues() const {
    auto canonicalFpp = BuildCanonicalFalsePositiveProbability();
    if (canonicalFpp.IsFail()) {
        return TConclusionStatus::Fail(canonicalFpp.GetErrorMessage());
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

    if (!jsonInfo[NIndexParameters::FalsePositiveProbability].IsDouble()) {
        return TConclusionStatus::Fail("false_positive_probability have to be in bloom filter features as double field");
    }

    Request.FalsePositiveProbability = jsonInfo[NIndexParameters::FalsePositiveProbability].GetDouble();
    return ValidateValues();
}

NKikimr::TConclusionStatus TBloomIndexConstructor::DoDeserializeFromProto(const NKikimrSchemeOp::TOlapIndexRequested& proto) {
    if (!proto.HasBloomFilter()) {
        const TString errorMessage = "not found BloobFilter section in proto: \"" + proto.DebugString() + "\"";
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("problem", errorMessage);
        return TConclusionStatus::Fail(errorMessage);
    }

    const auto& bFilter = proto.GetBloomFilter();

    {
        auto conclusion = TBase::DeserializeFromProtoImpl(bFilter);
        if (conclusion.IsFail()) {
            return conclusion;
        }
    }

    Request.FalsePositiveProbability = bFilter.HasFalsePositiveProbability()
        ? std::optional<double>(bFilter.GetFalsePositiveProbability()) : std::nullopt;
    return ValidateValues();
}

void TBloomIndexConstructor::DoSerializeToProto(NKikimrSchemeOp::TOlapIndexRequested& proto) const {
    auto* filterProto = proto.MutableBloomFilter();
    TBase::SerializeToProtoImpl(*filterProto);
    if (Request.FalsePositiveProbability) {
        filterProto->SetFalsePositiveProbability(*Request.FalsePositiveProbability);
    }
}

}   // namespace NKikimr::NOlap::NIndexes
