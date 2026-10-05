#pragma once
#include "merger.h"

#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/common/path_id.h>
#include <ydb/core/tx/columnshard/data_reader/actor.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/abstract_scheme.h>
#include <ydb/core/tx/columnshard/operations/common/context.h>

namespace NKikimr::NOlap {

class TModificationRestoreTask: public NDataReader::IRestoreTask, public NColumnShard::TMonitoringObjectsCounter<TModificationRestoreTask> {
private:
    using TBase = NDataReader::IRestoreTask;
    NEvWrite::TWriteData WriteData;
    std::shared_ptr<IMerger> Merger;
    NArrow::TContainerWithIndexes<arrow::RecordBatch> IncomingData;
    const TWritingContext Context;
    bool ReadOnlyConflicts;
    virtual std::unique_ptr<TEvColumnShard::TEvInternalScan> DoBuildRequestInitiator() const override;

    using EErrorClass = NColumnShard::TEvPrivate::TEvWriteBlobsResult::EErrorClass;

    virtual TConclusionStatus DoOnDataChunk(const std::shared_ptr<arrow::Table>& data) override;
    virtual TConclusionStatus DoOnFinished() override;
    virtual void DoOnError(const Ydb::StatusIds::StatusCode status, const TString& errorMessage) override;
    void OnError(const TString& errorMessage, const EErrorClass errorClass);
    void SendErrorMessage(const TString& errorMessage, const EErrorClass errorClass);

public:
    virtual bool IsActive() const override {
        return Context.IsActive();
    }

    virtual TString GetErrorMessage() const override {
        return Context.GetErrorMessage();
    }

    virtual TDuration GetTimeout() const override;

    TModificationRestoreTask(NEvWrite::TWriteData&& writeData, const std::shared_ptr<IMerger>& merger,
        const NArrow::TContainerWithIndexes<arrow::RecordBatch>& incomingData, const TWritingContext& context, const bool readOnlyConflicts);
};

}   // namespace NKikimr::NOlap
