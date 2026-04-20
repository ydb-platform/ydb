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
        TBase::GetBitsStorageConstructor(), CaseSensitive);
}

TConclusionStatus TIndexConstructor::ValidateValues() const {
    if (auto conclusion = TConstants::ValidateParams(FalsePositiveProbability, NGrammSize); conclusion.IsFail()) {
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
        const double k = static_cast<double>(hashesCount.value_or(NDefaults::HashesCount));
        const double m = static_cast<double>(filterSizeBytes.value_or(TConstants::CalcDeprecatedFilterSizeBytes(NDefaults::FalsePositiveProbability)) * 8);
        const double n = static_cast<double>(recordsCount.value_or(TConstants::DeprecatedRecordsCount));
        const double oneMinus = 1.0 - std::exp(-(k * n) / m);
        FalsePositiveProbability = std::pow(std::clamp(oneMinus, 0.0, 1.0), k);
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
    FalsePositiveProbability = bFilter.HasFalsePositiveProbability() ? bFilter.GetFalsePositiveProbability()
                                                                     : NDefaults::FalsePositiveProbability;
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
    filterProto->SetHashesCount(TConstants::CalcHashesCount(FalsePositiveProbability));
    filterProto->SetFilterSizeBytes(TConstants::CalcDeprecatedFilterSizeBytes(FalsePositiveProbability));
    filterProto->SetRecordsCount(TConstants::CalcDeprecatedRecordsCount(FalsePositiveProbability));
    filterProto->SetFalsePositiveProbability(FalsePositiveProbability);
    *filterProto->MutableDataExtractor() = GetDataExtractor().SerializeToProto();
}

}   // namespace NKikimr::NOlap::NIndexes::NBloomNGramm
