#pragma once

#include <ydb/core/tx/columnshard/counters/scan.h>
#include <ydb/core/tx/columnshard/private_events/events.h>
#include <ydb/core/tx/conveyor/usage/abstract.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/conclusion/result.h>

namespace NKikimr::NOlap::NReader {

class IDataReader;

class IApplyAction {
private:
    bool AppliedFlag = false;

protected:
    virtual bool DoApply(IDataReader& indexedDataRead) = 0;

public:
    bool Apply(IDataReader& indexedDataRead) {
        AFL_VERIFY(!AppliedFlag);
        AppliedFlag = true;
        return DoApply(indexedDataRead);
    }

    virtual ui64 GetSourceId() const {
        return 0;
    }

    virtual ui64 GetBlobBytes() const {
        return 0;
    }

    virtual ui64 GetRawBytes() const {
        return 0;
    }

    virtual ui32 GetFilteredRows() const {
        return 0;
    }

    virtual ui32 GetTotalRows() const {
        return 0;
    }

    virtual ui64 GetTotalReservedBytes() const {
        return 0;
    }

    virtual ~IApplyAction() = default;
};

class IDataTasksProcessor {
public:
    class ITask: public NConveyor::ITask, public IApplyAction {
    private:
        using TBase = NConveyor::ITask;
        const NActors::TActorId OwnerId;
        NColumnShard::TCounterGuard Guard;
        virtual TConclusion<bool> DoExecuteImpl() = 0;

    protected:
        virtual void DoExecute(const std::shared_ptr<NConveyor::ITask>& taskPtr) override final;
        virtual void DoOnCannotExecute(const TString& reason) override;

    public:
        using TPtr = std::shared_ptr<ITask>;
        virtual ~ITask() = default;

        ITask(const NActors::TActorId& ownerId, NColumnShard::TCounterGuard&& scanCounter)
            : OwnerId(ownerId)
            , Guard(std::move(scanCounter))
        {
        }
    };
};

}   // namespace NKikimr::NOlap::NReader

namespace NKikimr::NColumnShard {

class TEvPrivate::TEvTaskProcessedResult
    : public NActors::TEventLocal<TEvPrivate::TEvTaskProcessedResult, TEvPrivate::EEv::EvTaskProcessedResult> {
private:
    TConclusion<std::shared_ptr<NOlap::NReader::IApplyAction>> Result;
    TCounterGuard ScanCounter;
    ui64 SourceId = 0;
    ui64 BlobBytes = 0;
    ui64 RawBytes = 0;
    ui32 FilteredRows = 0;
    ui32 TotalRows = 0;
    ui64 TotalReservedBytes = 0;

public:
    TConclusion<std::shared_ptr<NOlap::NReader::IApplyAction>>& MutableResult() {
        return Result;
    }

    ui64 GetSourceId() const {
        return SourceId;
    }

    ui64 GetBlobBytes() const {
        return BlobBytes;
    }

    ui64 GetRawBytes() const {
        return RawBytes;
    }

    ui32 GetFilteredRows() const {
        return FilteredRows;
    }

    ui32 GetTotalRows() const {
        return TotalRows;
    }

    ui64 GetTotalReservedBytes() const {
        return TotalReservedBytes;
    }

    TEvTaskProcessedResult(TConclusion<std::shared_ptr<NOlap::NReader::IApplyAction>>&& result, TCounterGuard&& scanCounters, ui64 sourceId = 0,
        ui64 blobBytes = 0, ui64 rawBytes = 0, ui32 filteredRows = 0, ui32 totalRows = 0, ui64 totalReservedBytes = 0)
        : Result(std::move(result))
        , ScanCounter(std::move(scanCounters))
        , SourceId(sourceId)
        , BlobBytes(blobBytes)
        , RawBytes(rawBytes)
        , FilteredRows(filteredRows)
        , TotalRows(totalRows)
        , TotalReservedBytes(totalReservedBytes)
    {
    }
};

}   // namespace NKikimr::NColumnShard
