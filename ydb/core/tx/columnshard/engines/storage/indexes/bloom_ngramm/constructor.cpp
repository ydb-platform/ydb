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
        GetInheritPortionStorage().value_or(false), columnId, GetDataExtractor(), FalsePositiveProbability, NGrammSize,
        TBase::GetBitsStorageConstructor(), CaseSensitive, UseDeprecatedSizing, DeprecatedFilterSizeBytes, DeprecatedRecordsCount,
        DeprecatedHashesCount);
}

TConclusionStatus TIndexConstructor::ValidateValues() const {
    if (auto conclusion = TConstants::ValidateParams(FalsePositiveProbability, NGrammSize); conclusion.IsFail()) {
        return conclusion;
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

    const bool hasFpp = jsonInfo.Has(NIndexParameters::FalsePositiveProbability);
    if (hasFpp) {
        if (!jsonInfo[NIndexParameters::FalsePositiveProbability].IsDouble()) {
            return TConclusionStatus::Fail("false_positive_probability must be in bloom ngramm filter features as double field");
        }

        FalsePositiveProbability = jsonInfo[NIndexParameters::FalsePositiveProbability].GetDouble();
    }

    const bool hasDeprecatedSizingParams = jsonInfo.Has(NIndexParameters::FilterSizeBytes) || jsonInfo.Has(NIndexParameters::RecordsCount) || jsonInfo.Has(NIndexParameters::HashesCount);
    if (hasFpp && hasDeprecatedSizingParams) {
        return TConclusionStatus::Fail(
            "cannot mix false_positive_probability with filter_size_bytes or records_count or hashes_count in bloom ngramm filter features");
    }

    if (!jsonInfo[NIndexParameters::NGrammSize].IsUInteger()) {
        return TConclusionStatus::Fail("ngramm_size have to be in bloom filter features as uint field");
    }

    NGrammSize = jsonInfo[NIndexParameters::NGrammSize].GetUInteger();
    std::optional<ui32> hashesCount;
    if (jsonInfo.Has(NIndexParameters::HashesCount)) {
        if (!jsonInfo[NIndexParameters::HashesCount].IsUInteger()) {
            return TConclusionStatus::Fail("hashes_count have to be in bloom filter features as uint field");
        }

        hashesCount = jsonInfo[NIndexParameters::HashesCount].GetUInteger();
        if (!TConstants::CheckHashesCount(*hashesCount)) {
            return TConclusionStatus::Fail("hashes_count have to be in bloom ngramm filter in interval " + TConstants::GetHashesCountIntervalString());
        }
    }

    std::optional<ui32> filterSizeBytes;
    if (jsonInfo.Has(NIndexParameters::FilterSizeBytes)) {
        if (!jsonInfo[NIndexParameters::FilterSizeBytes].IsUInteger()) {
            return TConclusionStatus::Fail("filter_size_bytes have to be in bloom filter features as uint field");
        }

        filterSizeBytes = jsonInfo[NIndexParameters::FilterSizeBytes].GetUInteger();
        if (!TConstants::CheckFilterSizeBytes(*filterSizeBytes)) {
            return TConclusionStatus::Fail("filter_size_bytes have to be in bloom ngramm filter in interval " + TConstants::GetFilterSizeBytesIntervalString());
        }
    }

    std::optional<ui32> recordsCount;
    if (jsonInfo.Has(NIndexParameters::RecordsCount)) {
        if (!jsonInfo[NIndexParameters::RecordsCount].IsUInteger()) {
            return TConclusionStatus::Fail("records_count have to be in bloom filter features as uint field");
        }

        recordsCount = jsonInfo[NIndexParameters::RecordsCount].GetUInteger();
        if (!TConstants::CheckRecordsCount(*recordsCount)) {
            return TConclusionStatus::Fail("records_count have to be in bloom ngramm filter in interval " + TConstants::GetRecordsCountIntervalString());
        }
    }

    if (!hasFpp && (hashesCount || filterSizeBytes || recordsCount)) {
        DeprecatedHashesCount = hashesCount;
        DeprecatedFilterSizeBytes = filterSizeBytes;
        DeprecatedRecordsCount = recordsCount;
        FalsePositiveProbability = TConstants::FalsePositiveProbabilityFromDeprecatedSizing(
            DeprecatedHashesCount, DeprecatedFilterSizeBytes, DeprecatedRecordsCount);
        UseDeprecatedSizing = true;
    }

    if (jsonInfo.Has(NIndexParameters::CaseSensitive)) {
        if (!jsonInfo[NIndexParameters::CaseSensitive].IsBoolean()) {
            return TConclusionStatus::Fail("case_sensitive have to be in bloom filter features as boolean field");
        }
        CaseSensitive = jsonInfo[NIndexParameters::CaseSensitive].GetBoolean();
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

    NGrammSize = bFilter.HasNGrammSize() ? bFilter.GetNGrammSize() : NDefaults::NGrammSize;
    DeprecatedHashesCount.reset();
    DeprecatedFilterSizeBytes.reset();
    DeprecatedRecordsCount.reset();
    UseDeprecatedSizing = false;

    if (bFilter.HasHashesCount()) {
        const ui32 v = bFilter.GetHashesCount();
        if (!TConstants::CheckHashesCount(v)) {
            return TConclusionStatus::Fail("hashes_count have to be in bloom ngramm filter in interval " + TConstants::GetHashesCountIntervalString());
        }

        DeprecatedHashesCount = v;
    }

    if (bFilter.HasFilterSizeBytes()) {
        const ui32 v = bFilter.GetFilterSizeBytes();
        if (!TConstants::CheckFilterSizeBytes(v)) {
            return TConclusionStatus::Fail("filter_size_bytes have to be in bloom ngramm filter in interval " + TConstants::GetFilterSizeBytesIntervalString());
        }

        DeprecatedFilterSizeBytes = v;
    }

    if (bFilter.HasRecordsCount()) {
        const ui32 v = bFilter.GetRecordsCount();
        if (v != 0) {
            if (!TConstants::CheckRecordsCount(v)) {
                return TConclusionStatus::Fail("records_count have to be in bloom ngramm filter in interval " + TConstants::GetRecordsCountIntervalString());
            }

            DeprecatedRecordsCount = v;
        }
    }

    const bool hasFpp = bFilter.HasFalsePositiveProbability();
    UseDeprecatedSizing = bFilter.HasRecordsCount() && bFilter.GetRecordsCount() != 0;
    if (UseDeprecatedSizing) {
        FalsePositiveProbability = hasFpp ? bFilter.GetFalsePositiveProbability()
                                          : TConstants::FalsePositiveProbabilityFromDeprecatedSizing(
                                                DeprecatedHashesCount, DeprecatedFilterSizeBytes, DeprecatedRecordsCount);
    } else if (hasFpp) {
        FalsePositiveProbability = bFilter.GetFalsePositiveProbability();
    } else {
        FalsePositiveProbability = NDefaults::FalsePositiveProbability;
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
    filterProto->SetColumnName(GetColumnName());
    filterProto->SetCaseSensitive(CaseSensitive);
    filterProto->SetNGrammSize(NGrammSize);
    filterProto->SetFalsePositiveProbability(FalsePositiveProbability);
    if (UseDeprecatedSizing) {
        filterProto->SetHashesCount(DeprecatedHashesCount.value_or(NDefaults::HashesCount));
        filterProto->SetFilterSizeBytes(
            DeprecatedFilterSizeBytes.value_or(TConstants::CalcDeprecatedFilterSizeBytes(FalsePositiveProbability)));
        filterProto->SetRecordsCount(DeprecatedRecordsCount.value_or(TConstants::DeprecatedRecordsCount));
    } else {
        filterProto->SetHashesCount(TConstants::CalcHashesCount(FalsePositiveProbability));
        filterProto->SetFilterSizeBytes(TConstants::CalcDeprecatedFilterSizeBytes(FalsePositiveProbability));
        filterProto->ClearRecordsCount();
    }
    *filterProto->MutableDataExtractor() = GetDataExtractor().SerializeToProto();
}

}   // namespace NKikimr::NOlap::NIndexes::NBloomNGramm
