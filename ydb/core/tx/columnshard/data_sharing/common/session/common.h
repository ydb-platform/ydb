#pragma once
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/data_sharing/common/context/context.h>
#include <ydb/core/tx/columnshard/data_locks/manager/manager.h>
#include <ydb/core/tablet_flat/tablet_flat_executor.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/conclusion/status.h>

namespace NKikimr::NColumnShard {
class TColumnShard;
}

namespace NKikimr::NOlap {
class TPortionInfo;
namespace NDataLocks {
class TManager;
}
}

namespace NKikimr::NOlap::NDataSharing {

class TCommonSession {
private:
    static ui64 GetNextRuntimeId() {
        static TAtomicCounter Counter = 0;
        return (ui64)Counter.Inc();
    }

    YDB_READONLY_DEF(TString, SessionId);
    const TString Info;
    YDB_READONLY(ui64, RuntimeId, GetNextRuntimeId());
    std::shared_ptr<NDataLocks::TManager::TGuard> LockGuard;
    bool IsStartedFlag = false;
    bool IsStartingFlag = false;
    bool IsFinishedFlag = false;
protected:
    TTransferContext TransferContext;
    virtual bool DoStart(const NColumnShard::TColumnShard& shard, const THashMap<ui64, std::vector<std::shared_ptr<TPortionInfo>>>& portions) = 0;
    virtual THashSet<ui64> GetPathIdsForStart() const = 0;
public:
    virtual ~TCommonSession() = default;

    TCommonSession(const TString& info)
        : Info(info)
    {

    }

    TCommonSession(const TString& sessionId, const TString& info, const TTransferContext& transferContext)
        : SessionId(sessionId)
        , Info(info)
        , TransferContext(transferContext) {
    }

    bool IsFinished() const {
        return IsFinishedFlag;
    }

    bool IsStarted() const {
        return IsStartedFlag;
    }

    bool IsStarting() const {
        return IsStartingFlag;
    }

    bool IsEqualTo(const TCommonSession& item) const {
        return SessionId == item.SessionId && TransferContext.IsEqualTo(item.TransferContext);
    }

    bool Start(const NColumnShard::TColumnShard& shard);
    void Finish(const std::shared_ptr<NDataLocks::TManager>& dataLocksManager);

    const TSnapshot& GetSnapshotBarrier() const {
        return TransferContext.GetSnapshotBarrier();
    }

    TString DebugString() const;

    template <class TProto>
    void SerializeToProto(TProto& proto) const {
        AFL_VERIFY(SessionId);
        *proto.MutableSessionId() = SessionId;
        *proto.MutableTransferContext() = TransferContext.SerializeToProto();
    }

    template <class TProto>
    TConclusionStatus DeserializeFromProto(const TProto& proto) {
        {
            SessionId = proto.GetSessionId();
            if (!SessionId) {
                return TConclusionStatus::Fail("SessionId not initialized in proto.");
            }
        }
        {
            if (!proto.HasTransferContext()) {
                return TConclusionStatus::Fail("TransferContext not initialized in proto.");
            }
            auto parsing = TransferContext.DeserializeFromProto(proto.GetTransferContext());
            if (!parsing) {
                return parsing;
            }
        }
        return TConclusionStatus::Success();
    }

};

}