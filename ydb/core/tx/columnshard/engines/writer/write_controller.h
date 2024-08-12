#pragma once

#include "put_status.h"
#include "blob_constructor.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/core/tx/columnshard/defs.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/write.h>


namespace NKikimr::NColumnShard {

class TBlobPutResult: public NColumnShard::TPutStatus {
public:
    using TPtr = std::shared_ptr<TBlobPutResult>;

    TBlobPutResult(NKikimrProto::EReplyStatus status,
        THashSet<ui32>&& yellowMoveChannels,
        THashSet<ui32>&& yellowStopChannels)
    {
        SetPutStatus(status, std::move(yellowMoveChannels), std::move(yellowStopChannels));
    }

    TBlobPutResult(NKikimrProto::EReplyStatus status) {
        SetPutStatus(status);
    }
};

class IWriteController {
private:
    THashMap<TString, std::shared_ptr<NOlap::IBlobsWritingAction>> WaitingActions;
    NOlap::TWriteActionsCollection WritingActions;
    std::deque<NOlap::TBlobWriteInfo> WriteTasks;
protected:
    virtual void DoOnReadyResult(const NActors::TActorContext& ctx, const TBlobPutResult::TPtr& putResult) = 0;
    virtual void DoOnBlobWriteResult(const TEvBlobStorage::TEvPutResult& /*result*/) {

    }
    virtual void DoOnStartSending() {

    }

    NOlap::TBlobWriteInfo& AddWriteTask(NOlap::TBlobWriteInfo&& task);
    virtual void DoAbort(const TString& /*reason*/) {
    }
public:
    const NOlap::TWriteActionsCollection& GetBlobActions() const {
        return WritingActions;
    }

    TString DebugString() const {
        TStringBuilder sb;
        for (auto&& i : WritingActions) {
            sb << i.second->GetStorageId() << ",";
        }
        ui64 size = 0;
        for (auto&& i : WriteTasks) {
            size += i.GetBlobId().BlobSize();
        }

        return TStringBuilder() << "size=" << size << ";count=" << WriteTasks.size() << ";actions=" << sb << ";";
    }

    void Abort(const TString& reason) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "IWriteController aborted")("reason", reason);
        for (auto&& i : WritingActions) {
            i.second->Abort();
        }
        DoAbort(reason);
    }

    using TPtr = std::shared_ptr<IWriteController>;
    virtual ~IWriteController() {}

    void OnStartSending() {
        DoOnStartSending();
    }

    void OnReadyResult(const NActors::TActorContext& ctx, const TBlobPutResult::TPtr& putResult) {
        DoOnReadyResult(ctx, putResult);
    }

    void OnBlobWriteResult(const TEvBlobStorage::TEvPutResult& result);

    std::optional<NOlap::TBlobWriteInfo> Next() {
        if (WriteTasks.empty()) {
            return {};
        }
        auto result = std::move(WriteTasks.front());
        WriteTasks.pop_front();
        return result;

    }
    bool IsReady() const {
        return WaitingActions.empty();
    }
};

}
