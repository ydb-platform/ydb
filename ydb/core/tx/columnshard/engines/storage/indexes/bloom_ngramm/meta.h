#pragma once
#include <ydb/core/tx/columnshard/engines/storage/indexes/portions/meta.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/skip_index/meta.h>

namespace NKikimr::NOlap::NIndexes::NBloomNGramm {

class TIndexMeta: public TSkipBitmapIndex {
public:
    static TString GetClassNameStatic() {
        return "BLOOM_NGRAMM_FILTER";
    }

private:
    using TBase = TSkipBitmapIndex;
    std::shared_ptr<arrow::Schema> ResultSchema;
    ui32 NGrammSize = 3;
    ui32 FilterSizeBytes = 512;
    ui32 RecordsCount = 10000;
    ui32 HashesCount = 2;
    static inline auto Registrator = TFactory::TRegistrator<TIndexMeta>(GetClassNameStatic());
    void Initialize() {
        AFL_VERIFY(!ResultSchema);
        std::vector<std::shared_ptr<arrow::Field>> fields = { std::make_shared<arrow::Field>("", arrow::boolean()) };
        ResultSchema = std::make_shared<arrow::Schema>(fields);
        AFL_VERIFY(TConstants::CheckHashesCount(HashesCount));
        AFL_VERIFY(TConstants::CheckFilterSizeBytes(FilterSizeBytes));
        AFL_VERIFY(TConstants::CheckNGrammSize(NGrammSize));
        AFL_VERIFY(TConstants::CheckRecordsCount(RecordsCount));
    }

    virtual bool DoIsAppropriateFor(const TString& subColumnName, const EOperation op) const override {
        if (!!subColumnName) {
            return false;
        }
        switch (op) {
            case EOperation::Equals:
            case EOperation::StartsWith:
            case EOperation::EndsWith:
            case EOperation::Contains:
                return true;
        }

        return false;
    }

protected:
    virtual TConclusionStatus DoCheckModificationCompatibility(const IIndexMeta& newMeta) const override {
        const auto* bMeta = dynamic_cast<const TIndexMeta*>(&newMeta);
        if (!bMeta) {
            return TConclusionStatus::Fail(
                "cannot read meta as appropriate class: " + GetClassName() + ". Meta said that class name is " + newMeta.GetClassName());
        }
        if (HashesCount != bMeta->HashesCount) {
            return TConclusionStatus::Fail("cannot modify hashes count");
        }
        if (NGrammSize != bMeta->NGrammSize) {
            return TConclusionStatus::Fail("cannot modify ngramm size");
        }
        return TBase::CheckSameColumnsForModification(newMeta);
    }
    virtual TString DoBuildIndexImpl(TChunkedBatchReader& reader, const ui32 recordsCount) const override;

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
        if (bFilter.HasRecordsCount()) {
            RecordsCount = bFilter.GetRecordsCount();
            if (!TConstants::CheckRecordsCount(RecordsCount)) {
                return false;
            }
        }
        if (!MutableDataExtractor().DeserializeFromProto(bFilter.GetDataExtractor())) {
            return false;
        }
        HashesCount = bFilter.GetHashesCount();
        if (!TConstants::CheckHashesCount(HashesCount)) {
            return false;
        }
        NGrammSize = bFilter.GetNGrammSize();
        if (!TConstants::CheckNGrammSize(NGrammSize)) {
            return false;
        }
        FilterSizeBytes = bFilter.GetFilterSizeBytes();
        if (!TConstants::CheckFilterSizeBytes(FilterSizeBytes)) {
            return false;
        }
        if (!bFilter.HasColumnId() || !bFilter.GetColumnId()) {
            return false;
        }
        AddColumnId(bFilter.GetColumnId());
        Initialize();
        return true;
    }
    virtual void DoSerializeToProto(NKikimrSchemeOp::TOlapIndexDescription& proto) const override {
        auto* filterProto = proto.MutableBloomNGrammFilter();
        TBase::SerializeToProtoImpl(*filterProto);
        AFL_VERIFY(TConstants::CheckNGrammSize(NGrammSize));
        AFL_VERIFY(TConstants::CheckFilterSizeBytes(FilterSizeBytes));
        AFL_VERIFY(TConstants::CheckHashesCount(HashesCount));
        AFL_VERIFY(TConstants::CheckRecordsCount(RecordsCount));
        filterProto->SetRecordsCount(RecordsCount);
        filterProto->SetNGrammSize(NGrammSize);
        filterProto->SetFilterSizeBytes(FilterSizeBytes);
        filterProto->SetHashesCount(HashesCount);
        filterProto->SetColumnId(GetColumnId());
        *filterProto->MutableDataExtractor() = GetDataExtractor().SerializeToProto();
    }

    virtual bool DoCheckValueImpl(const IBitsStorage& data, const std::optional<ui64> category, const std::shared_ptr<arrow::Scalar>& value,
        const EOperation op) const override;

public:
    TIndexMeta() = default;
    TIndexMeta(const ui32 indexId, const TString& indexName, const TString& storageId, const ui32 columnId,
        const TReadDataExtractorContainer& dataExtractor, const ui32 hashesCount, const ui32 filterSizeBytes, const ui32 nGrammSize,
        const ui32 recordsCount, const std::shared_ptr<IBitsStorageConstructor>& bitsStorageConstructor)
        : TBase(indexId, indexName, columnId, storageId, dataExtractor, bitsStorageConstructor)
        , NGrammSize(nGrammSize)
        , FilterSizeBytes(filterSizeBytes)
        , RecordsCount(recordsCount)
        , HashesCount(hashesCount) {
        Initialize();
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}   // namespace NKikimr::NOlap::NIndexes::NBloomNGramm
