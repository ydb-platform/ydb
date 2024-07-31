#pragma once
#include <ydb/core/formats/arrow/arrow_filter.h>
#include <ydb/core/formats/arrow/reader/position.h>
#include <ydb/core/formats/arrow/reader/result_builder.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/abstract_scheme.h>
#include <ydb/library/conclusion/status.h>

namespace NKikimr::NOlap {

class IMerger {
private:
    NArrow::NMerger::TRWSortableBatchPosition IncomingPosition;

    virtual TConclusionStatus OnEqualKeys(const NArrow::NMerger::TSortableBatchPosition& exists, const NArrow::NMerger::TSortableBatchPosition& incoming) = 0;
    virtual TConclusionStatus OnIncomingOnly(const NArrow::NMerger::TSortableBatchPosition& incoming) = 0;
protected:
    std::shared_ptr<ISnapshotSchema> Schema;
    std::shared_ptr<arrow::RecordBatch> IncomingData;
    bool IncomingFinished = false;
public:
    IMerger(const std::shared_ptr<arrow::RecordBatch>& incoming, const std::shared_ptr<ISnapshotSchema>& actualSchema)
        : IncomingPosition(incoming, 0, actualSchema->GetPKColumnNames(), incoming->schema()->field_names(), false)
        , Schema(actualSchema)
        , IncomingData(incoming) {
        IncomingFinished = !IncomingPosition.InitPosition(0);
    }

    virtual ~IMerger() = default;

    virtual std::shared_ptr<arrow::RecordBatch> BuildResultBatch() = 0;

    TConclusionStatus Finish();

    TConclusionStatus AddExistsDataOrdered(const std::shared_ptr<arrow::Table>& data);
};

class TInsertMerger: public IMerger {
private:
    using TBase = IMerger;
    virtual TConclusionStatus OnEqualKeys(const NArrow::NMerger::TSortableBatchPosition& exists, const NArrow::NMerger::TSortableBatchPosition& /*incoming*/) override {
        return TConclusionStatus::Fail("Conflict with existing key. " + exists.GetSorting()->DebugJson(exists.GetPosition()).GetStringRobust());
    }
    virtual TConclusionStatus OnIncomingOnly(const NArrow::NMerger::TSortableBatchPosition& /*incoming*/) override {
        return TConclusionStatus::Success();
    }
public:
    using TBase::TBase;
    virtual std::shared_ptr<arrow::RecordBatch> BuildResultBatch() override {
        return IncomingData;
    }
};

class TReplaceMerger: public IMerger {
private:
    using TBase = IMerger;
    NArrow::TColumnFilter Filter = NArrow::TColumnFilter::BuildDenyFilter();
    virtual TConclusionStatus OnEqualKeys(const NArrow::NMerger::TSortableBatchPosition& /*exists*/, const NArrow::NMerger::TSortableBatchPosition& /*incoming*/) override {
        Filter.Add(true);
        return TConclusionStatus::Success();
    }
    virtual TConclusionStatus OnIncomingOnly(const NArrow::NMerger::TSortableBatchPosition& /*incoming*/) override {
        Filter.Add(false);
        return TConclusionStatus::Success();
    }
public:
    using TBase::TBase;

    virtual std::shared_ptr<arrow::RecordBatch> BuildResultBatch() override {
        auto result = IncomingData;
        AFL_VERIFY(Filter.Apply(result));
        return result;
    }
};

class TUpdateMerger: public IMerger {
private:
    using TBase = IMerger;
    NArrow::NMerger::TRecordBatchBuilder Builder;
    std::vector<std::optional<ui32>> IncomingColumnRemap;
    std::vector<std::shared_ptr<arrow::BooleanArray>> HasIncomingDataFlags;
    const std::optional<NArrow::NMerger::TSortableBatchPosition> DefaultExists;
    const TString InsertDenyReason;
    virtual TConclusionStatus OnEqualKeys(const NArrow::NMerger::TSortableBatchPosition& exists, const NArrow::NMerger::TSortableBatchPosition& incoming) override;
    virtual TConclusionStatus OnIncomingOnly(const NArrow::NMerger::TSortableBatchPosition& incoming) override {
        if (!!InsertDenyReason) {
            return TConclusionStatus::Fail("insertion is impossible: " + InsertDenyReason);
        }
        if (!DefaultExists) {
            return TConclusionStatus::Success();
        } else {
            return OnEqualKeys(*DefaultExists, incoming);
        }
    }
public:
    virtual std::shared_ptr<arrow::RecordBatch> BuildResultBatch() override {
        return Builder.Finalize();
    }

    TUpdateMerger(const std::shared_ptr<arrow::RecordBatch>& incoming, const std::shared_ptr<ISnapshotSchema>& actualSchema,
        const TString& insertDenyReason, const std::optional<NArrow::NMerger::TSortableBatchPosition>& defaultExists = {});
};

}
