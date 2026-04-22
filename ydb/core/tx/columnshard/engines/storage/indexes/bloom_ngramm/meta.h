#pragma once

#include "const.h"

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
    void Initialize() {
        AFL_VERIFY(!ResultSchema);
        std::vector<std::shared_ptr<arrow::Field>> fields = { std::make_shared<arrow::Field>("", arrow::boolean()) };
        ResultSchema = std::make_shared<arrow::Schema>(fields);
        AFL_VERIFY(FalsePositiveProbability > 0 && FalsePositiveProbability < 1);
        AFL_VERIFY(TConstants::CheckNGrammSize(NGrammSize));
        HashesCount = TConstants::CalcHashesCount(FalsePositiveProbability);
        AFL_VERIFY(TConstants::CheckHashesCount(HashesCount));
        FilterSizeBytes = TConstants::CalcDeprecatedFilterSizeBytes(FalsePositiveProbability);
        AFL_VERIFY(TConstants::CheckFilterSizeBytes(FilterSizeBytes));
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
        return TBase::CheckSameColumnsForModification(newMeta);
    }
    virtual std::vector<std::shared_ptr<NChunks::TPortionIndexChunk>> DoBuildIndexImpl(
        TChunkedBatchReader& reader, const ui32 recordsCount) const override;

    virtual bool DoDeserializeFromProto(const NKikimrSchemeOp::TOlapIndexDescription& proto) override {
        AFL_VERIFY(TBase::DoDeserializeFromProto(proto));
        AFL_VERIFY(proto.HasBloomNGrammFilter());
        auto& bFilter = proto.GetBloomNGrammFilter();

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

        if (bFilter.HasCaseSensitive()) {
            CaseSensitive = bFilter.GetCaseSensitive();
        }

        const bool hasFpp = bFilter.HasFalsePositiveProbability();
        const bool hasOldSizingParams = bFilter.HasFilterSizeBytes() || bFilter.HasRecordsCount() || bFilter.HasHashesCount();
        UseOldSizing = hasOldSizingParams && !hasFpp;
        std::optional<ui32> filterSizeBytes;
        std::optional<ui32> recordsCount;
        std::optional<ui32> hashesCount;
        NGrammSize = bFilter.HasNGrammSize() ? bFilter.GetNGrammSize() : NDefaults::NGrammSize;
        FalsePositiveProbability = bFilter.HasFalsePositiveProbability() ? bFilter.GetFalsePositiveProbability()
                                                                        : NDefaults::FalsePositiveProbability;

        {
            auto conclusion = TConstants::ValidateParams(FalsePositiveProbability, NGrammSize);
            if (conclusion.IsFail()) {
                AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("index_parsing", conclusion.GetErrorMessage());
                return false;
            }
        }

        if (bFilter.HasFilterSizeBytes()) {
            const ui32 value = bFilter.GetFilterSizeBytes();
            if (!TConstants::CheckFilterSizeBytes(value)) {
                AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("index_parsing", "incorrect filter_size_bytes value");
                return false;
            }

            filterSizeBytes = value;
        }

        if (bFilter.HasRecordsCount()) {
            const ui32 value = bFilter.GetRecordsCount();
            if (!TConstants::CheckRecordsCount(value)) {
                AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("index_parsing", "incorrect records_count value");
                return false;
            }

            recordsCount = value;
        }

        if (bFilter.HasHashesCount()) {
            const ui32 value = bFilter.GetHashesCount();
            if (!TConstants::CheckHashesCount(value)) {
                AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("index_parsing", "incorrect hashes_count value");
                return false;
            }

            hashesCount = value;
        }

        if (!bFilter.HasColumnId() || !bFilter.GetColumnId()) {
            return false;
        }

        AddColumnId(bFilter.GetColumnId());
        Initialize();
        if (UseOldSizing) {
            FilterSizeBytes = filterSizeBytes.value_or(TConstants::CalcDeprecatedFilterSizeBytes(FalsePositiveProbability));
            RecordsCount = recordsCount.value_or(TConstants::DeprecatedRecordsCount);
            HashesCount = hashesCount.value_or(NDefaults::HashesCount);
        }
        return true;
    }
    virtual void DoSerializeToProto(NKikimrSchemeOp::TOlapIndexDescription& proto) const override {
        auto* filterProto = proto.MutableBloomNGrammFilter();
        TBase::SerializeToProtoImpl(*filterProto);
        AFL_VERIFY(TConstants::CheckNGrammSize(NGrammSize));
        const ui32 hashesCountValue = UseOldSizing ? HashesCount : TConstants::CalcHashesCount(FalsePositiveProbability);
        const ui32 recordsCountValue = UseOldSizing ? RecordsCount : TConstants::CalcDeprecatedRecordsCount(FalsePositiveProbability);
        AFL_VERIFY(TConstants::CheckFilterSizeBytes(FilterSizeBytes));
        AFL_VERIFY(TConstants::CheckHashesCount(hashesCountValue));
        AFL_VERIFY(TConstants::CheckRecordsCount(recordsCountValue));
        filterProto->SetNGrammSize(NGrammSize);
        filterProto->SetHashesCount(hashesCountValue);
        filterProto->SetFilterSizeBytes(FilterSizeBytes);
        filterProto->SetRecordsCount(recordsCountValue);
        filterProto->SetFalsePositiveProbability(FalsePositiveProbability);
        filterProto->SetColumnId(GetColumnId());
        filterProto->SetCaseSensitive(CaseSensitive);
        *filterProto->MutableDataExtractor() = GetDataExtractor().SerializeToProto();
    }

    bool DoCheckValueImpl(const IBitsStorageViewer& data, const std::optional<ui64> category, const std::shared_ptr<arrow::Scalar>& value,
        const NArrow::NSSA::TIndexCheckOperation& op, const TIndexInfo&) const override;

public:
    TIndexMeta() = default;
    TIndexMeta(const ui32 indexId, const TString& indexName, const TString& storageId, const bool inheritPortionIndex, const ui32 columnId,
        const TReadDataExtractorContainer& dataExtractor, const double falsePositiveProbability, const ui32 nGrammSize,
        const std::shared_ptr<IBitsStorageConstructor>& bitsStorageConstructor, const bool caseSensitive)
        : TBase(indexId, indexName, columnId, storageId, inheritPortionIndex, dataExtractor, bitsStorageConstructor)
        , CaseSensitive(caseSensitive)
        , NGrammSize(nGrammSize)
        , FalsePositiveProbability(falsePositiveProbability)
    {
        Initialize();
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}   // namespace NKikimr::NOlap::NIndexes::NBloomNGramm
