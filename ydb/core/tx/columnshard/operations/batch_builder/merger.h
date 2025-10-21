#pragma once
#include <ydb/core/formats/arrow/arrow_filter.h>
#include <ydb/core/formats/arrow/reader/position.h>
#include <ydb/core/formats/arrow/reader/result_builder.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/abstract_scheme.h>
#include <ydb/library/conclusion/status.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

namespace NKikimr::NOlap {

class IMerger {
public:
    using TYdbConclusionStatus = TConclusionSpecialStatus<Ydb::StatusIds::StatusCode, Ydb::StatusIds::SUCCESS, Ydb::StatusIds::BAD_REQUEST>;
private:
    NArrow::NMerger::TRWSortableBatchPosition IncomingPosition;

    virtual TYdbConclusionStatus OnEqualKeys(const NArrow::NMerger::TSortableBatchPosition& exists, const NArrow::NMerger::TSortableBatchPosition& incoming) = 0;
    virtual TYdbConclusionStatus OnIncomingOnly(const NArrow::NMerger::TSortableBatchPosition& incoming) = 0;
protected:
    std::shared_ptr<ISnapshotSchema> Schema;
    NArrow::TContainerWithIndexes<arrow::RecordBatch> IncomingData;
    bool IncomingFinished = false;
public:
    IMerger(const NArrow::TContainerWithIndexes<arrow::RecordBatch>& incoming, const std::shared_ptr<ISnapshotSchema>& actualSchema)
        : IncomingPosition(incoming.GetContainer(), 0, actualSchema->GetPKColumnNames(), incoming->schema()->field_names(), false)
        , Schema(actualSchema)
        , IncomingData(incoming) {
        IncomingFinished = !IncomingPosition.InitPosition(0);
    }

    virtual ~IMerger() = default;

    virtual NArrow::TContainerWithIndexes<arrow::RecordBatch> BuildResultBatch() = 0;

    TYdbConclusionStatus Finish();

    TYdbConclusionStatus AddExistsDataOrdered(const std::shared_ptr<arrow::Table>& data);
};

class TInsertMerger: public IMerger {
private:
    using TBase = IMerger;
    virtual TYdbConclusionStatus OnEqualKeys(const NArrow::NMerger::TSortableBatchPosition& exists, const NArrow::NMerger::TSortableBatchPosition& /*incoming*/) override {
        return TYdbConclusionStatus::Fail(Ydb::StatusIds::PRECONDITION_FAILED, "Conflict with existing key. " + exists.GetSorting()->DebugJson(exists.GetPosition()).GetStringRobust());
    }
    virtual TYdbConclusionStatus OnIncomingOnly(const NArrow::NMerger::TSortableBatchPosition& /*incoming*/) override {
        return TYdbConclusionStatus::Success();
    }
public:
    using TBase::TBase;
    virtual NArrow::TContainerWithIndexes<arrow::RecordBatch> BuildResultBatch() override {
        return IncomingData;
    }
};

class TReplaceMerger: public IMerger {
private:
    using TBase = IMerger;
    NArrow::TColumnFilter Filter = NArrow::TColumnFilter::BuildDenyFilter();
    virtual TYdbConclusionStatus OnEqualKeys(const NArrow::NMerger::TSortableBatchPosition& /*exists*/, const NArrow::NMerger::TSortableBatchPosition& /*incoming*/) override {
        Filter.Add(true);
        return TYdbConclusionStatus::Success();
    }
    virtual TYdbConclusionStatus OnIncomingOnly(const NArrow::NMerger::TSortableBatchPosition& /*incoming*/) override {
        Filter.Add(false);
        return TYdbConclusionStatus::Success();
    }
public:
    using TBase::TBase;

    virtual NArrow::TContainerWithIndexes<arrow::RecordBatch> BuildResultBatch() override {
        auto result = IncomingData;
        Filter.Apply(result.MutableContainer());
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
    virtual TYdbConclusionStatus OnEqualKeys(const NArrow::NMerger::TSortableBatchPosition& exists, const NArrow::NMerger::TSortableBatchPosition& incoming) override;
    virtual TYdbConclusionStatus OnIncomingOnly(const NArrow::NMerger::TSortableBatchPosition& incoming) override {
        if (!!InsertDenyReason) {
            return TYdbConclusionStatus::Fail("insertion is impossible: " + InsertDenyReason);
        }
        if (!DefaultExists) {
            return TYdbConclusionStatus::Success();
        } else {
            return OnEqualKeys(*DefaultExists, incoming);
        }
    }
public:
    virtual NArrow::TContainerWithIndexes<arrow::RecordBatch> BuildResultBatch() override;

    TUpdateMerger(const NArrow::TContainerWithIndexes<arrow::RecordBatch>& incoming, const std::shared_ptr<ISnapshotSchema>& actualSchema,
        const TString& insertDenyReason, const std::optional<NArrow::NMerger::TSortableBatchPosition>& defaultExists = {});
};

}
