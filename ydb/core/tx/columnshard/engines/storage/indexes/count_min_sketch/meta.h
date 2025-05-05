#pragma once
#include <ydb/core/tx/columnshard/engines/storage/indexes/portions/extractor/default.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/portions/meta.h>

namespace NKikimr::NOlap::NIndexes::NCountMinSketch {

class TIndexMeta: public TIndexByColumns {
public:
    static TString GetClassNameStatic() {
        return "COUNT_MIN_SKETCH";
    }

private:
    using TBase = TIndexByColumns;

    static inline auto Registrator = TFactory::TRegistrator<TIndexMeta>(GetClassNameStatic());

protected:
    virtual TConclusionStatus DoCheckModificationCompatibility(const IIndexMeta& newMeta) const override {
        const auto* bMeta = dynamic_cast<const TIndexMeta*>(&newMeta);
        if (!bMeta) {
            return TConclusionStatus::Fail(
                "cannot read meta as appropriate class: " + GetClassName() + ". Meta said that class name is " + newMeta.GetClassName());
        }
        return TBase::CheckSameColumnsForModification(newMeta);
    }

    virtual TString DoBuildIndexImpl(TChunkedBatchReader& reader, const ui32 recordsCount) const override;

    virtual bool DoDeserializeFromProto(const NKikimrSchemeOp::TOlapIndexDescription& proto) override {
        AFL_VERIFY(TBase::DoDeserializeFromProto(proto));
        AFL_VERIFY(proto.HasCountMinSketch());
        auto& sketch = proto.GetCountMinSketch();
        for (auto&& i : sketch.GetColumnIds()) {
            AddColumnId(i);
        }
        return true;
    }

    virtual void DoSerializeToProto(NKikimrSchemeOp::TOlapIndexDescription& proto) const override {
        auto* sketchProto = proto.MutableCountMinSketch();
        for (auto&& i : GetColumnIds()) {
            sketchProto->AddColumnIds(i);
        }
    }

public:
    TIndexMeta() = default;
    TIndexMeta(const ui32 indexId, const TString& indexName, const TString& storageId, const ui32 columnId)
        : TBase(indexId, indexName, columnId, storageId, std::make_shared<TDefaultDataExtractor>()) {
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}   // namespace NKikimr::NOlap::NIndexes::NCountMinSketch
