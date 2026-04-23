#pragma once

#include "const.h"

#include <optional>

#include <ydb/core/tx/columnshard/engines/storage/indexes/portions/meta.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/skip_index/meta.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/helper/index_defaults.h>

namespace NKikimr::NOlap::NIndexes::NBloomNGramm {

class TIndexMeta: public TSkipBitmapIndex {
public:
    static TString GetClassNameStatic() {
        return "BLOOM_NGRAMM_FILTER";
    }

private:
    using TBase = TSkipBitmapIndex;
    std::shared_ptr<arrow::Schema> ResultSchema;
    bool CaseSensitive = NDefaults::CaseSensitive;
    ui32 NGrammSize = NDefaults::NGrammSize;
    double FalsePositiveProbability = NDefaults::FalsePositiveProbability;
    ui32 RecordsCount = TConstants::DeprecatedRecordsCount;
    ui32 FilterSizeBytes = 0;
    ui32 HashesCount = 0;
    bool UseOldSizing = false;
    static inline auto Registrator = TFactory::TRegistrator<TIndexMeta>(GetClassNameStatic());
    [[nodiscard]] bool Initialize() {
        AFL_VERIFY(!ResultSchema);
        if (auto c = TConstants::ValidateParams(FalsePositiveProbability, NGrammSize, HashesCount, FilterSizeBytes); c.IsFail()) {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("index_init", c.GetErrorMessage());
            return false;
        }

        std::vector<std::shared_ptr<arrow::Field>> fields = { std::make_shared<arrow::Field>("", arrow::boolean()) };
        ResultSchema = std::make_shared<arrow::Schema>(fields);
        return true;
    }

    virtual bool DoIsAppropriateFor(const NArrow::NSSA::TIndexCheckOperation& op) const override {
        switch (op.GetOperation()) {
            case EOperation::Equals:
            case EOperation::StartsWith:
            case EOperation::EndsWith:
            case EOperation::Contains:
                return !CaseSensitive || op.GetCaseSensitive();
            default:
                return false;
        }
    }

protected:
    virtual TConclusionStatus DoCheckModificationCompatibility(const IIndexMeta& newMeta) const override {
        const auto* bMeta = dynamic_cast<const TIndexMeta*>(&newMeta);
        if (!bMeta) {
            return TConclusionStatus::Fail(
                "cannot read meta as appropriate class: " + GetClassName() + ". Meta said that class name is " + newMeta.GetClassName());
        }

        if (UseOldSizing && FalsePositiveProbability != bMeta->FalsePositiveProbability) {
            return TConclusionStatus::Fail(
                "cannot change false_positive_probability on a bloom ngram index created with deprecated sizing "
                "(filter_size_bytes/hashes_count/records_count); drop and recreate the index instead");
        }

        if (!UseOldSizing && bMeta->UseOldSizing) {
            return TConclusionStatus::Fail(
                "cannot switch bloom ngram index from false_positive_probability mode to deprecated sizing "
                "(filter_size_bytes/hashes_count/records_count) mode; drop and recreate the index instead");
        }

        return TBase::CheckSameColumnsForModification(newMeta);
    }
    virtual std::vector<std::shared_ptr<NChunks::TPortionIndexChunk>> DoBuildIndexImpl(
        TChunkedBatchReader& reader, const ui32 recordsCount) const override;

    virtual bool DoDeserializeFromProto(const NKikimrSchemeOp::TOlapIndexDescription& proto) override {
        AFL_VERIFY(TBase::DoDeserializeFromProto(proto));
        AFL_VERIFY(proto.HasBloomNGrammFilter());
        const auto& bFilter = proto.GetBloomNGrammFilter();

        {
            auto conclusion = TBase::DeserializeFromProtoImpl(bFilter);
            if (conclusion.IsFail()) {
                AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("index_parsing", conclusion.GetErrorMessage());
                return false;
            }
        }

        if (!MutableDataExtractor().DeserializeFromProto(bFilter.GetDataExtractor())) {
            return false;
        }

        if (!bFilter.HasColumnId() || !bFilter.GetColumnId()) {
            return false;
        }

        CaseSensitive = bFilter.HasCaseSensitive() ? bFilter.GetCaseSensitive() : NDefaults::CaseSensitive;
        NGrammSize = bFilter.HasNGrammSize() ? bFilter.GetNGrammSize() : NDefaults::NGrammSize;
        UseOldSizing = bFilter.HasRecordsCount() && bFilter.GetRecordsCount() != 0;
        FalsePositiveProbability = bFilter.HasFalsePositiveProbability()
            ? bFilter.GetFalsePositiveProbability()
            : (UseOldSizing
                ? TConstants::FalsePositiveProbabilityFromDeprecatedSizing(
                    bFilter.HasHashesCount() ? std::optional<ui32>(bFilter.GetHashesCount()) : std::nullopt,
                    bFilter.HasFilterSizeBytes() ? std::optional<ui32>(bFilter.GetFilterSizeBytes()) : std::nullopt,
                    bFilter.HasRecordsCount() ? std::optional<ui32>(bFilter.GetRecordsCount()) : std::nullopt)
                : NDefaults::FalsePositiveProbability);

        if (UseOldSizing) {
            HashesCount = bFilter.HasHashesCount() ? bFilter.GetHashesCount() : NDefaults::HashesCount;
            FilterSizeBytes = bFilter.HasFilterSizeBytes()
                ? bFilter.GetFilterSizeBytes()
                : TConstants::CalcDeprecatedFilterSizeBytes(FalsePositiveProbability);
            RecordsCount = bFilter.GetRecordsCount();
        } else {
            HashesCount = TConstants::CalcHashesCount(FalsePositiveProbability);
            FilterSizeBytes = TConstants::CalcDeprecatedFilterSizeBytes(FalsePositiveProbability);
        }

        AddColumnId(bFilter.GetColumnId());
        return Initialize();
    }
    virtual void DoSerializeToProto(NKikimrSchemeOp::TOlapIndexDescription& proto) const override {
        auto* filterProto = proto.MutableBloomNGrammFilter();
        TBase::SerializeToProtoImpl(*filterProto);
        AFL_VERIFY(TConstants::CheckNGrammSize(NGrammSize));
        filterProto->SetNGrammSize(NGrammSize);
        filterProto->SetColumnId(GetColumnId());
        filterProto->SetCaseSensitive(CaseSensitive);
        filterProto->SetFalsePositiveProbability(FalsePositiveProbability);

        filterProto->SetHashesCount(HashesCount);
        filterProto->SetFilterSizeBytes(FilterSizeBytes);
        if (UseOldSizing) {
            filterProto->SetRecordsCount(RecordsCount);
        } else {
            filterProto->ClearRecordsCount();
        }

        *filterProto->MutableDataExtractor() = GetDataExtractor().SerializeToProto();
    }

    bool DoCheckValueImpl(const IBitsStorageViewer& data, const std::optional<ui64> category, const std::shared_ptr<arrow::Scalar>& value,
        const NArrow::NSSA::TIndexCheckOperation& op, const TIndexInfo&) const override;

public:
    TIndexMeta() = default;

    static bool IsDeprecatedSizingMode(const std::optional<ui32>& deprecatedHashesCount,
        const std::optional<ui32>& deprecatedFilterSizeBytes,
        const std::optional<ui32>& deprecatedRecordsCount) {
        return deprecatedHashesCount || deprecatedFilterSizeBytes || deprecatedRecordsCount;
    }

    TIndexMeta(const ui32 indexId, const TString& indexName, const TString& storageId, const bool inheritPortionIndex, const ui32 columnId,
        const TReadDataExtractorContainer& dataExtractor, const double falsePositiveProbability, const ui32 nGrammSize,
        const std::shared_ptr<IBitsStorageConstructor>& bitsStorageConstructor, const bool caseSensitive,
        const bool useDeprecatedSizing = false,
        const std::optional<ui32> deprecatedFilterSizeBytes = std::nullopt,
        const std::optional<ui32> deprecatedRecordsCount = std::nullopt,
        const std::optional<ui32> deprecatedHashesCount = std::nullopt)
        : TBase(indexId, indexName, columnId, storageId, inheritPortionIndex, dataExtractor, bitsStorageConstructor)
        , CaseSensitive(caseSensitive)
        , NGrammSize(nGrammSize)
        , FalsePositiveProbability(falsePositiveProbability)
        , UseOldSizing(useDeprecatedSizing)
    {
        if (useDeprecatedSizing) {
            HashesCount = deprecatedHashesCount.value_or(NDefaults::HashesCount);
            FilterSizeBytes = deprecatedFilterSizeBytes.value_or(TConstants::CalcDeprecatedFilterSizeBytes(falsePositiveProbability));
            RecordsCount = deprecatedRecordsCount.value_or(TConstants::DeprecatedRecordsCount);
        } else {
            HashesCount = TConstants::CalcHashesCount(falsePositiveProbability);
            FilterSizeBytes = TConstants::CalcDeprecatedFilterSizeBytes(falsePositiveProbability);
        }

        AFL_VERIFY(Initialize());
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}   // namespace NKikimr::NOlap::NIndexes::NBloomNGramm
