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
    TRequestSettings Request;
    static inline auto Registrator = TFactory::TRegistrator<TIndexMeta>(GetClassNameStatic());

    TConclusionStatus ValidateRequest() const {
        return TConstants::ValidateRequest(Request);
    }

    [[nodiscard]] bool Initialize() {
        AFL_VERIFY(!ResultSchema);
        if (auto c = ValidateRequest(); c.IsFail()) {
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
                return !Request.ResolvedCaseSensitive() || op.GetCaseSensitive();
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

        auto currentValidation = ValidateRequest();
        if (currentValidation.IsFail()) {
            return TConclusionStatus::Fail("current bloom ngram index parameters are invalid: " + currentValidation.GetErrorMessage());
        }

        auto nextValidation = bMeta->ValidateRequest();
        if (nextValidation.IsFail()) {
            return TConclusionStatus::Fail("new bloom ngram index parameters are invalid: " + nextValidation.GetErrorMessage());
        }

        if (Request.IsOldSizingMode() &&
            Request.ResolvedFalsePositiveProbability() != bMeta->Request.ResolvedFalsePositiveProbability()) {
            return TConclusionStatus::Fail(
                "cannot change false_positive_probability on a bloom ngram index created with deprecated sizing "
                "(filter_size_bytes/hashes_count/records_count); drop and recreate the index instead");
        }

        if (!Request.IsOldSizingMode() && bMeta->Request.IsOldSizingMode()) {
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

        Request = TRequestSettings::FromProtoFilter(bFilter);
        if (auto c = ValidateRequest(); c.IsFail()) {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("index_parsing", c.GetErrorMessage());
            return false;
        }

        AddColumnId(bFilter.GetColumnId());
        return Initialize();
    }

    virtual void DoSerializeToProto(NKikimrSchemeOp::TOlapIndexDescription& proto) const override {
        auto* filterProto = proto.MutableBloomNGrammFilter();
        TBase::SerializeToProtoImpl(*filterProto);
        Request.SerializeToProtoFilterRaw(*filterProto);
        filterProto->SetColumnId(GetColumnId());
        *filterProto->MutableDataExtractor() = GetDataExtractor().SerializeToProto();
    }

    bool DoCheckValueImpl(const IBitsStorageViewer& data, const std::optional<ui64> category, const std::shared_ptr<arrow::Scalar>& value,
        const NArrow::NSSA::TIndexCheckOperation& op, const TIndexInfo&) const override;

public:
    TIndexMeta() = default;

    TIndexMeta(const ui32 indexId, const TString& indexName, const TString& storageId, const bool inheritPortionIndex, const ui32 columnId,
        const TReadDataExtractorContainer& dataExtractor, const std::shared_ptr<IBitsStorageConstructor>& bitsStorageConstructor,
        const TRequestSettings& request)
        : TBase(indexId, indexName, columnId, storageId, inheritPortionIndex, dataExtractor, bitsStorageConstructor)
        , Request(request)
    {
        AFL_VERIFY(Initialize());
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}   // namespace NKikimr::NOlap::NIndexes::NBloomNGramm
