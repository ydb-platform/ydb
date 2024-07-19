#pragma once
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
            return TConclusionStatus::Fail("cannot read meta as appropriate class: " + GetClassName() + ". Meta said that class name is " + newMeta.GetClassName());
        }
        return TBase::CheckSameColumnsForModification(newMeta);
    }

    virtual void DoFillIndexCheckers(const std::shared_ptr<NRequest::TDataForIndexesCheckers>& info, const NSchemeShard::TOlapSchema& schema) const override;

    virtual TString DoBuildIndexImpl(TChunkedBatchReader& reader) const override;

    virtual bool DoDeserializeFromProto(const NKikimrSchemeOp::TOlapIndexDescription& proto) override {
        AFL_VERIFY(TBase::DoDeserializeFromProto(proto));
        AFL_VERIFY(proto.HasCountMinSketch());
        auto& sketch = proto.GetCountMinSketch();
        for (auto&& i : sketch.GetColumnIds()) {
            ColumnIds.emplace(i);
        }
        return true;
    }

    virtual void DoSerializeToProto(NKikimrSchemeOp::TOlapIndexDescription& proto) const override {
        auto* sketchProto = proto.MutableCountMinSketch();
        for (auto&& i : ColumnIds) {
            sketchProto->AddColumnIds(i);
        }
    }

public:
    TIndexMeta() = default;
    TIndexMeta(const ui32 indexId, const TString& indexName, const TString& storageId, std::set<ui32>& columnIds)
        : TBase(indexId, indexName, columnIds, storageId) {
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}   // namespace NKikimr::NOlap::NIndexes
