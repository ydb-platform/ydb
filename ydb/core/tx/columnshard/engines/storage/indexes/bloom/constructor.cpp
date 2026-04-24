#include "constructor.h"
#include "meta.h"

#include <ydb/core/tx/columnshard/engines/storage/indexes/helper/index_parameters.h>
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

    if (auto c = ValidateRequest(Request); c.IsFail()) {
        errors.AddError(c.GetErrorMessage());
        return nullptr;
    }

    const ui32 columnId = columnInfo->GetId();
    return std::make_shared<TBloomIndexMeta>(indexId, indexName, GetStorageId().value_or(NBlobOperations::TGlobal::DefaultStorageId),
        GetInheritPortionStorage().value_or(false), columnId,
        Request,
        std::make_shared<TDefaultDataExtractor>(),
        TBase::GetBitsStorageConstructor());
}

std::shared_ptr<IIndexMeta> TBloomIndexConstructor::DoCreateOrPatchIndexMeta(const ui32 indexId, const TString& indexName,
    const NSchemeShard::TOlapSchema& currentSchema, NSchemeShard::IErrorCollector& errors,
    const IIndexMeta& existingMeta) const {
    if (!ColumnName.empty()) {
        return DoCreateIndexMeta(indexId, indexName, currentSchema, errors);
    }

    auto resolvedColumnName = ResolveColumnNameForAlterIndex(currentSchema, existingMeta);
    if (resolvedColumnName.IsFail()) {
        errors.AddError(resolvedColumnName.GetErrorMessage());
        return nullptr;
    }

    TBloomIndexConstructor patched = *this;
    patched.ColumnName = *resolvedColumnName;
    return patched.DoCreateIndexMeta(indexId, indexName, currentSchema, errors);
}

TConclusionStatus TBloomIndexConstructor::ValidateValues() const {
    return ValidateRequest(Request);
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

    Request = TRequestSettings::FromProtoFilter(bFilter);
    return ValidateValues();
}

void TBloomIndexConstructor::DoSerializeToProto(NKikimrSchemeOp::TOlapIndexRequested& proto) const {
    auto* filterProto = proto.MutableBloomFilter();
    TBase::SerializeToProtoImpl(*filterProto);
    Request.SerializeToProtoFilter(*filterProto);
}

}   // namespace NKikimr::NOlap::NIndexes
