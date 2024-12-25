#pragma once
#include <ydb/core/tx/columnshard/engines/storage/indexes/portions/meta.h>
namespace NKikimr::NOlap::NIndexes::NBloomNGramm {

class TIndexMeta: public TIndexByColumns {
public:
    static TString GetClassNameStatic() {
        return "BLOOM_NGRAMM_FILTER";
    }
private:
    using TBase = TIndexByColumns;
    std::shared_ptr<arrow::Schema> ResultSchema;
    ui32 NGrammSize = 3;
    ui32 FilterSizeBytes = 512;
    ui32 HashesCount = 2;
    static inline auto Registrator = TFactory::TRegistrator<TIndexMeta>(GetClassNameStatic());
    void Initialize() {
        AFL_VERIFY(!ResultSchema);
        std::vector<std::shared_ptr<arrow::Field>> fields = {std::make_shared<arrow::Field>("", arrow::boolean())};
        ResultSchema = std::make_shared<arrow::Schema>(fields);
        AFL_VERIFY(HashesCount > 0);
        AFL_VERIFY(FilterSizeBytes > 0);
        AFL_VERIFY(NGrammSize > 2);
    }

    static const ui64 HashesConstructorP = ((ui64)2 << 31) - 1;
    static const ui64 HashesConstructorA = (ui64)2 << 16;

    template <class TActor>
    void BuildHashesSet(const ui64 originalHash, const TActor& actor) const {
        AFL_VERIFY(HashesCount < HashesConstructorP);
        for (ui32 b = 1; b <= HashesCount; ++b) {
            const ui64 hash = (HashesConstructorA * originalHash + b) % HashesConstructorP;
            actor(hash);
        }
    }

    template <class TContainer, class TActor>
    void BuildHashesSet(const TContainer& originalHashes, const TActor& actor) const {
        AFL_VERIFY(HashesCount < HashesConstructorP);
        for (auto&& hOriginal : originalHashes) {
            BuildHashesSet(hOriginal, actor);
        }
    }

protected:
    virtual TConclusionStatus DoCheckModificationCompatibility(const IIndexMeta& /*newMeta*/) const override {
        return TConclusionStatus::Fail("not supported");
    }
    virtual void DoFillIndexCheckers(const std::shared_ptr<NRequest::TDataForIndexesCheckers>& info, const NSchemeShard::TOlapSchema& schema) const override;

    virtual TString DoBuildIndexImpl(TChunkedBatchReader& reader) const override;

    virtual bool DoDeserializeFromProto(const NKikimrSchemeOp::TOlapIndexDescription& proto) override {
        AFL_VERIFY(TBase::DoDeserializeFromProto(proto));
        AFL_VERIFY(proto.HasBloomNGrammFilter());
        auto& bFilter = proto.GetBloomNGrammFilter();
        HashesCount = bFilter.GetHashesCount();
        if (HashesCount < 1 || 10 < HashesCount) {
            return false;
        }
        NGrammSize = bFilter.GetNGrammSize();
        if (NGrammSize < 3) {
            return false;
        }
        FilterSizeBytes = bFilter.GetFilterSizeBytes();
        if (FilterSizeBytes < 128) {
            return false;
        }
        if (!bFilter.HasColumnId() || !bFilter.GetColumnId()) {
            return false;
        }
        ColumnIds.emplace(bFilter.GetColumnId());
        Initialize();
        return true;
    }
    virtual void DoSerializeToProto(NKikimrSchemeOp::TOlapIndexDescription& proto) const override {
        auto* filterProto = proto.MutableBloomNGrammFilter();
        AFL_VERIFY(NGrammSize >= 3);
        AFL_VERIFY(FilterSizeBytes >= 128);
        AFL_VERIFY(HashesCount >= 1);
        AFL_VERIFY(ColumnIds.size() == 1);
        filterProto->SetNGrammSize(NGrammSize);
        filterProto->SetFilterSizeBytes(FilterSizeBytes);
        filterProto->SetHashesCount(HashesCount);
        filterProto->SetColumnId(*ColumnIds.begin());
    }

public:
    TIndexMeta() = default;
    TIndexMeta(const ui32 indexId, const TString& indexName, const TString& storageId, const ui32 columnId, const ui32 hashesCount,
        const ui32 filterSizeBytes, const ui32 nGrammSize)
        : TBase(indexId, indexName, { columnId }, storageId)
        , NGrammSize(nGrammSize)
        , FilterSizeBytes(filterSizeBytes)
        , HashesCount(hashesCount)
    {
        Initialize();
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}   // namespace NKikimr::NOlap::NIndexes
