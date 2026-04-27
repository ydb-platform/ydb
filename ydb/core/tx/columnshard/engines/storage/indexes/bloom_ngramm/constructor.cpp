#include "const.h"
#include "constructor.h"
#include "meta.h"

#include <ydb/core/tx/columnshard/engines/storage/indexes/helper/index_parameters.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/portions/extractor/default.h>
#include <ydb/core/tx/schemeshard/olap/schema/schema.h>

namespace NKikimr::NOlap::NIndexes::NBloomNGramm {

namespace {

bool IsSupportedColumnType(const NSchemeShard::TOlapColumnSchema& columnInfo, const TReadDataExtractorContainer& dataExtractor) {
    const auto extractorProto = dataExtractor.SerializeToProto();
    const auto typeId = columnInfo.GetType().GetTypeId();
    return ::NKikimr::NOlap::NIndexes::TColumnIndexConstructor::IsSupportedTypeForEquals(typeId) ||
        (typeId == NScheme::NTypeIds::JsonDocument && extractorProto.HasSubColumn());
}

} // namespace

std::shared_ptr<IIndexMeta> TIndexConstructor::DoCreateOrPatchIndexMeta(const ui32 indexId, const TString& indexName,
    const NSchemeShard::TOlapSchema& currentSchema, NSchemeShard::IErrorCollector& errors,
    const IIndexMeta& existingMeta) const {
    return DoCreateOrPatchSingleColumnIndexMeta<TIndexConstructor>(
        indexId, indexName, currentSchema, errors, existingMeta);
}

std::shared_ptr<IIndexMeta> TIndexConstructor::DoCreateIndexMeta(
    const ui32 indexId, const TString& indexName, const NSchemeShard::TOlapSchema& currentSchema, NSchemeShard::IErrorCollector& errors) const {
    auto* columnInfo = currentSchema.GetColumns().GetByName(GetColumnName());
    if (!columnInfo) {
        errors.AddError("no column with name " + GetColumnName());
        return nullptr;
    }

    if (!IsSupportedColumnType(*columnInfo, GetDataExtractor())) {
        errors.AddError(Sprintf("inappropriate column type for bloom ngramm index: %s", columnInfo->GetTypeName().c_str()));
        return nullptr;
    }

    if (auto c = TConstants::ValidateRequest(Request); c.IsFail()) {
        errors.AddError(c.GetErrorMessage());
        return nullptr;
    }

    const ui32 columnId = columnInfo->GetId();
    return std::make_shared<TIndexMeta>(indexId, indexName, GetStorageId().value_or(NBlobOperations::TGlobal::DefaultStorageId),
        GetInheritPortionStorage().value_or(false), columnId, GetDataExtractor(), TBase::GetBitsStorageConstructor(), Request);
}

TConclusionStatus TIndexConstructor::ValidateValues() const {
    if (Request.FalsePositiveProbability) {
        if (Request.DeprecatedHashesCount || Request.DeprecatedFilterSizeBytes || Request.DeprecatedRecordsCount) {
            return TConclusionStatus::Fail(
                "cannot mix false_positive_probability with filter_size_bytes or records_count or hashes_count in bloom ngramm filter features");
        }
    }

    if (auto c = TConstants::ValidateRequest(Request); c.IsFail()) {
        return c;
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

    if (auto c = FillRequestFromJson(jsonInfo); c.IsFail()) {
        return c;
    }

    return ValidateValues();
}

TConclusionStatus TIndexConstructor::FillRequestFromJson(const NJson::TJsonValue& jsonInfo) {
    auto c = NIndexParameters::ParseOptionalJsonFields(
        jsonInfo,
        NIndexParameters::MakeOptionalDoubleField(
            NIndexParameters::FalsePositiveProbability, Request.FalsePositiveProbability,
            "false_positive_probability must be in bloom ngramm filter features as double field"),
        NIndexParameters::MakeOptionalUintField(
            NIndexParameters::NGrammSize, Request.NGrammSize,
            "ngramm_size have to be in bloom filter features as uint field"),
        NIndexParameters::MakeOptionalUintField(
            NIndexParameters::HashesCount, Request.DeprecatedHashesCount,
            "hashes_count have to be in bloom filter features as uint field"),
        NIndexParameters::MakeOptionalUintField(
            NIndexParameters::FilterSizeBytes, Request.DeprecatedFilterSizeBytes,
            "filter_size_bytes have to be in bloom filter features as uint field"),
        NIndexParameters::MakeOptionalUintField(
            NIndexParameters::RecordsCount, Request.DeprecatedRecordsCount,
            "records_count have to be in bloom filter features as uint field"),
        NIndexParameters::MakeOptionalBoolField(
            NIndexParameters::CaseSensitive, Request.CaseSensitive,
            "case_sensitive have to be in bloom filter features as boolean field"));
    if (c.IsFail()) {
        return c;
    }

    return TConclusionStatus::Success();
}

void TIndexConstructor::FillRequestFromProtoFilter(const NKikimrSchemeOp::TRequestedBloomNGrammFilter& bFilter) {
    Request = TRequestSettings::FromProtoFilter(bFilter);
}

NKikimr::TConclusionStatus TIndexConstructor::DoDeserializeFromProto(const NKikimrSchemeOp::TOlapIndexRequested& proto) {
    if (!proto.HasBloomNGrammFilter()) {
        const TString errorMessage = "not found BloomNGrammFilter section in proto: \"" + proto.DebugString() + "\"";
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("problem", errorMessage);
        return TConclusionStatus::Fail(errorMessage);
    }

    const auto& bFilter = proto.GetBloomNGrammFilter();

    {
        auto conclusion = TBase::DeserializeFromProtoBitsStorageOnly(bFilter);
        if (conclusion.IsFail()) {
            return conclusion;
        }
    }

    FillRequestFromProtoFilter(bFilter);

    ColumnName = bFilter.GetColumnName();

    if (!DataExtractor.DeserializeFromProto(bFilter.GetDataExtractor())) {
        return TConclusionStatus::Fail("cannot parse data extractor from proto: " + bFilter.GetDataExtractor().DebugString());
    }

    return ValidateValues();
}

void TIndexConstructor::DoSerializeToProto(NKikimrSchemeOp::TOlapIndexRequested& proto) const {
    auto* filterProto = proto.MutableBloomNGrammFilter();
    TBase::SerializeToProtoBitsStorageOnly(*filterProto);
    if (!ColumnName.empty()) {
        filterProto->SetColumnName(ColumnName);
    }

    Request.SerializeToProtoFilterRaw(*filterProto);

    *filterProto->MutableDataExtractor() = GetDataExtractor().SerializeToProto();
}

}   // namespace NKikimr::NOlap::NIndexes::NBloomNGramm
