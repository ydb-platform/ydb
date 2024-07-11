#pragma once
#include <ydb/core/tx/columnshard/engines/storage/indexes/portions/meta.h>

namespace NKikimr::NOlap::NIndexes {

class TCountMinSketchIndexMeta: public TIndexByColumns {
public:
    static TString GetClassNameStatic() {
        return "COUNT_MIN_SKETCH";
    }
private:
    using TBase = TIndexByColumns;
    std::shared_ptr<arrow::Schema> ResultSchema;
    ui32 Width = 256;
    ui32 Depth = 8;

    static inline auto Registrator = TFactory::TRegistrator<TCountMinSketchIndexMeta>(GetClassNameStatic());

    void Initialize() {
        AFL_VERIFY(!ResultSchema);
        std::vector<std::shared_ptr<arrow::Field>> fields;
        fields.reserve(ColumnIds.size());
        for (auto columnId : ColumnIds) {
            fields.emplace_back(std::make_shared<arrow::Field>(std::to_string(columnId), arrow::TypeTraits<arrow::UInt32Type>::type_singleton()));
        }
        ResultSchema = std::make_shared<arrow::Schema>(fields);
    }

protected:
    virtual TConclusionStatus DoCheckModificationCompatibility(const IIndexMeta& newMeta) const override {
        const auto* bMeta = dynamic_cast<const TCountMinSketchIndexMeta*>(&newMeta);
        if (!bMeta) {
            return TConclusionStatus::Fail("cannot read meta as appropriate class: " + GetClassName() + ". Meta said that class name is " + newMeta.GetClassName());
        }
        return TBase::CheckSameColumnsForModification(newMeta);
    }

    virtual void DoFillIndexCheckers(const std::shared_ptr<NRequest::TDataForIndexesCheckers>& info, const NSchemeShard::TOlapSchema& schema) const override;

    virtual std::shared_ptr<arrow::RecordBatch> DoBuildIndexImpl(TChunkedBatchReader& reader) const override;

    virtual bool DoDeserializeFromProto(const NKikimrSchemeOp::TOlapIndexDescription& proto) override {
        AFL_VERIFY(TBase::DoDeserializeFromProto(proto));
        AFL_VERIFY(proto.HasCountMinSketch());
        auto& sketch = proto.GetCountMinSketch();
        Width = sketch.GetWidth();
        Depth = sketch.GetDepth();
        for (auto&& i : sketch.GetColumnIds()) {
            ColumnIds.emplace(i);
        }
        Initialize();
        return true;
    }

    virtual void DoSerializeToProto(NKikimrSchemeOp::TOlapIndexDescription& proto) const override {
        auto* sketchProto = proto.MutableCountMinSketch();
        sketchProto->SetWidth(Width);
        sketchProto->SetDepth(Depth);
        for (auto&& i : ColumnIds) {
            sketchProto->AddColumnIds(i);
        }
    }

public:
    TCountMinSketchIndexMeta() = default;
    TCountMinSketchIndexMeta(const ui32 indexId, const TString& indexName, std::set<ui32>& columnIds, const ui32 width, const ui32 depth)
        : TBase(indexId, indexName, columnIds)
        , Width(width)
        , Depth(depth) {
        Initialize();
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}   // namespace NKikimr::NOlap::NIndexes
