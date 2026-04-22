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
    const bool isUtf8Column = typeId == NScheme::NTypeIds::Utf8;
    const bool isJsonSubColumn = typeId == NScheme::NTypeIds::JsonDocument && extractorProto.HasSubColumn();
    return isUtf8Column || isJsonSubColumn;
}

} // namespace

std::shared_ptr<IIndexMeta> TIndexConstructor::DoCreateOrPatchIndexMeta(const ui32 indexId, const TString& indexName,
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

    TIndexConstructor patched = *this;
    patched.ColumnName = col->GetName();
    return patched.DoCreateIndexMeta(indexId, indexName, currentSchema, errors);
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

    const ui32 columnId = columnInfo->GetId();
    return std::make_shared<TIndexMeta>(indexId, indexName, GetStorageId().value_or(NBlobOperations::TGlobal::DefaultStorageId),
        GetInheritPortionStorage().value_or(false), columnId, GetDataExtractor(),
        FalsePositiveProbability.value_or(NDefaults::FalsePositiveProbability),
        NGrammSize.value_or(NDefaults::NGrammSize),
        TBase::GetBitsStorageConstructor(),
        CaseSensitive.value_or(NDefaults::CaseSensitive),
        UseDeprecatedSizing, DeprecatedFilterSizeBytes, DeprecatedRecordsCount, DeprecatedHashesCount);
}

TConclusionStatus TIndexConstructor::ValidateValues() const {
    if (FalsePositiveProbability) {
        if (DeprecatedHashesCount || DeprecatedFilterSizeBytes || DeprecatedRecordsCount) {
            return TConclusionStatus::Fail(
                "cannot mix false_positive_probability with filter_size_bytes or records_count or hashes_count in bloom ngramm filter features");
        }

        if (auto conclusion = TConstants::ValidateParams(*FalsePositiveProbability,
                NGrammSize.value_or(NDefaults::NGrammSize));
                conclusion.IsFail()) {
            return conclusion;
        }
    }

    if (NGrammSize && !TConstants::CheckNGrammSize(*NGrammSize)) {
        return TConclusionStatus::Fail("ngramm_size is out of allowed interval");
    }

    if (DeprecatedHashesCount && !TConstants::CheckHashesCount(*DeprecatedHashesCount)) {
        return TConclusionStatus::Fail("hashes_count have to be in bloom ngramm filter in interval " + TConstants::GetHashesCountIntervalString());
    }

    if (DeprecatedFilterSizeBytes && !TConstants::CheckFilterSizeBytes(*DeprecatedFilterSizeBytes)) {
        return TConclusionStatus::Fail("filter_size_bytes have to be in bloom ngramm filter in interval " + TConstants::GetFilterSizeBytesIntervalString());
    }

    if (DeprecatedRecordsCount && !TConstants::CheckRecordsCount(*DeprecatedRecordsCount)) {
        return TConclusionStatus::Fail("records_count have to be in bloom ngramm filter in interval " + TConstants::GetRecordsCountIntervalString());
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

    if (auto c = NIndexParameters::ParseOptionalJsonFields(
            jsonInfo,
            NIndexParameters::MakeOptionalJsonField<double>(
                NIndexParameters::FalsePositiveProbability, FalsePositiveProbability,
                [](const NJson::TJsonValue& v) { return v.IsDouble(); },
                [](const NJson::TJsonValue& v) { return v.GetDouble(); },
                "false_positive_probability must be in bloom ngramm filter features as double field"),
            NIndexParameters::MakeOptionalJsonField<ui32>(
                NIndexParameters::NGrammSize, NGrammSize,
                [](const NJson::TJsonValue& v) { return v.IsUInteger(); },
                [](const NJson::TJsonValue& v) { return v.GetUInteger(); },
                "ngramm_size have to be in bloom filter features as uint field"),
            NIndexParameters::MakeOptionalJsonField<ui32>(
                NIndexParameters::HashesCount, DeprecatedHashesCount,
                [](const NJson::TJsonValue& v) { return v.IsUInteger(); },
                [](const NJson::TJsonValue& v) { return v.GetUInteger(); },
                "hashes_count have to be in bloom filter features as uint field"),
            NIndexParameters::MakeOptionalJsonField<ui32>(
                NIndexParameters::FilterSizeBytes, DeprecatedFilterSizeBytes,
                [](const NJson::TJsonValue& v) { return v.IsUInteger(); },
                [](const NJson::TJsonValue& v) { return v.GetUInteger(); },
                "filter_size_bytes have to be in bloom filter features as uint field"),
            NIndexParameters::MakeOptionalJsonField<ui32>(
                NIndexParameters::RecordsCount, DeprecatedRecordsCount,
                [](const NJson::TJsonValue& v) { return v.IsUInteger(); },
                [](const NJson::TJsonValue& v) { return v.GetUInteger(); },
                "records_count have to be in bloom filter features as uint field"),
            NIndexParameters::MakeOptionalJsonField<bool>(
                NIndexParameters::CaseSensitive, CaseSensitive,
                [](const NJson::TJsonValue& v) { return v.IsBoolean(); },
                [](const NJson::TJsonValue& v) { return v.GetBoolean(); },
                "case_sensitive have to be in bloom filter features as boolean field"));
        c.IsFail()) {
        return c;
    }

    UseDeprecatedSizing = DeprecatedHashesCount || DeprecatedFilterSizeBytes || DeprecatedRecordsCount;
    return ValidateValues();
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

    FalsePositiveProbability = bFilter.HasFalsePositiveProbability()
        ? std::optional<double>(bFilter.GetFalsePositiveProbability()) : std::nullopt;
    NGrammSize = bFilter.HasNGrammSize()
        ? std::optional<ui32>(bFilter.GetNGrammSize()) : std::nullopt;
    CaseSensitive = bFilter.HasCaseSensitive()
        ? std::optional<bool>(bFilter.GetCaseSensitive()) : std::nullopt;

    UseDeprecatedSizing = bFilter.HasRecordsCount() && bFilter.GetRecordsCount() != 0;
    DeprecatedHashesCount.reset();
    DeprecatedFilterSizeBytes.reset();
    DeprecatedRecordsCount.reset();

    if (UseDeprecatedSizing) {
        if (bFilter.HasHashesCount()) {
            DeprecatedHashesCount = bFilter.GetHashesCount();
        }

        if (bFilter.HasFilterSizeBytes()) {
            DeprecatedFilterSizeBytes = bFilter.GetFilterSizeBytes();
        }

        DeprecatedRecordsCount = bFilter.GetRecordsCount();
    }

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

    NIndexParameters::SetProtoIfPresent(CaseSensitive, [&](bool v) { filterProto->SetCaseSensitive(v); });
    NIndexParameters::SetProtoIfPresent(NGrammSize, [&](ui32 v) { filterProto->SetNGrammSize(v); });

    if (UseDeprecatedSizing) {
        NIndexParameters::SetProtoIfPresent(DeprecatedHashesCount, [&](ui32 v) { filterProto->SetHashesCount(v); });
        NIndexParameters::SetProtoIfPresent(DeprecatedFilterSizeBytes, [&](ui32 v) { filterProto->SetFilterSizeBytes(v); });
        NIndexParameters::SetProtoIfPresent(DeprecatedRecordsCount, [&](ui32 v) { filterProto->SetRecordsCount(v); });
    } else {
        NIndexParameters::SetProtoIfPresent(FalsePositiveProbability, [&](double v) { filterProto->SetFalsePositiveProbability(v); });
    }

    *filterProto->MutableDataExtractor() = GetDataExtractor().SerializeToProto();
}

}   // namespace NKikimr::NOlap::NIndexes::NBloomNGramm
