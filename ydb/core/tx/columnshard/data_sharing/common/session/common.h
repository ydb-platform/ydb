#pragma once
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/data_sharing/common/context/context.h>
#include <ydb/core/tablet_flat/tablet_flat_executor.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/conclusion/status.h>

namespace NKikimr::NOlap::NDataSharing {

class TCommonSession {
private:
    static ui64 GetNextRuntimeId() {
        static TAtomicCounter Counter = 0;
        return (ui64)Counter.Inc();
    }

    YDB_READONLY_DEF(TString, SessionId);
    YDB_READONLY(ui64, RuntimeId, GetNextRuntimeId());
    TSnapshot SnapshotBarrier = TSnapshot::Zero();
protected:
    TTransferContext TransferContext;
public:
    TCommonSession() = default;

    TCommonSession(const TString& sessionId, const TTransferContext& transferContext)
        : SessionId(sessionId)
        , TransferContext(transferContext) {
    }

    bool IsEqualTo(const TCommonSession& item) const {
        return SessionId == item.SessionId && SnapshotBarrier == item.SnapshotBarrier;
    }

    const TSnapshot& GetSnapshotBarrier() const {
        return SnapshotBarrier;
    }

    TString DebugString() const;

    template <class TProto>
    void SerializeToProto(TProto& proto) const {
        AFL_VERIFY(SessionId);
        *proto.MutableSessionId() = SessionId;
        SnapshotBarrier.SerializeToProto(*proto.MutableSnapshotBarrier());
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
        {
            if (!proto.HasSnapshotBarrier()) {
                return TConclusionStatus::Fail("SnapshotBarrier not initialized in proto.");
            }
            auto snapshotParse = SnapshotBarrier.DeserializeFromProto(proto.GetSnapshotBarrier());
            if (!snapshotParse) {
                return snapshotParse;
            }
            if (!SnapshotBarrier.Valid()) {
                return TConclusionStatus::Fail("SnapshotBarrier must be valid in proto.");
            }
        }
        return TConclusionStatus::Success();
    }

};

}