#pragma once
#include <ydb/core/formats/arrow/process_columns.h>
#include <ydb/core/formats/arrow/rows/view.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/counters/columnshard.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/abstract_scheme.h>
#include <ydb/core/tx/columnshard/operations/common/context.h>
#include <ydb/core/tx/conveyor/usage/abstract.h>
#include <ydb/core/tx/data_events/write_data.h>

namespace NKikimr::NOlap {

class TBuildBatchesTask: public NConveyor::ITask, public NColumnShard::TMonitoringObjectsCounter<TBuildBatchesTask> {
private:
    NEvWrite::TWriteData WriteData;
    const TWritingContext Context;
    void ReplyError(const TString& message, const NColumnShard::TEvPrivate::TEvWriteBlobsResult::EErrorClass errorClass);

protected:
    virtual void DoExecute(const std::shared_ptr<ITask>& taskPtr) override;

public:
    virtual TString GetTaskClassIdentifier() const override {
        return "Write::ConstructBatches";
    }

    TBuildBatchesTask(NEvWrite::TWriteData&& writeData, const TWritingContext& context)
        : WriteData(std::move(writeData))
        , Context(context)
    {
        WriteData.MutableWriteMeta().OnStage(NEvWrite::EWriteStage::BuildBatch);
    }
};

void StartWritingPreparedBatch(
    NEvWrite::TWriteData&& writeData, const NArrow::TContainerWithIndexes<arrow::RecordBatch>& batch, const TWritingContext& context);

class TStartWritingPreparedBatchTask: public NConveyor::ITask, public NColumnShard::TMonitoringObjectsCounter<TStartWritingPreparedBatchTask> {
private:
    NEvWrite::TWriteData WriteData;
    const NArrow::TContainerWithIndexes<arrow::RecordBatch> Batch;
    const TWritingContext Context;

protected:
    virtual void DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) override {
        StartWritingPreparedBatch(std::move(WriteData), Batch, Context);
    }

public:
    virtual TString GetTaskClassIdentifier() const override {
        return "Write::StartWritingPreparedBatch";
    }

    TStartWritingPreparedBatchTask(
        NEvWrite::TWriteData&& writeData, const NArrow::TContainerWithIndexes<arrow::RecordBatch>& batch, const TWritingContext& context)
        : WriteData(std::move(writeData))
        , Batch(batch)
        , Context(context)
    {
    }
};

class TEvPassWriteGate: public NActors::TEventLocal<TEvPassWriteGate, NColumnShard::TEvPrivate::EvPassWriteGate> {
private:
    NEvWrite::TWriteData WriteData;
    const NArrow::TContainerWithIndexes<arrow::RecordBatch> Batch;
    const TWritingContext Context;
    const NArrow::TSimpleRow MinKey;

public:
    const NEvWrite::TWriteData& GetWriteData() const {
        return WriteData;
    }

    NEvWrite::TWriteData& MutableWriteData() {
        return WriteData;
    }

    const NArrow::TContainerWithIndexes<arrow::RecordBatch>& GetBatch() const {
        return Batch;
    }

    const TWritingContext& GetContext() const {
        return Context;
    }

    const NArrow::TSimpleRow& GetMinKey() const {
        return MinKey;
    }

    TEvPassWriteGate(NEvWrite::TWriteData&& writeData, NArrow::TContainerWithIndexes<arrow::RecordBatch>&& batch, const TWritingContext& context,
        NArrow::TSimpleRow&& minKey)
        : WriteData(std::move(writeData))
        , Batch(std::move(batch))
        , Context(context)
        , MinKey(std::move(minKey))
    {
    }
};

}   // namespace NKikimr::NOlap
