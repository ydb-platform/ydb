#pragma once

#include <library/cpp/lwtrace/shuttle.h>

#include <ydb/core/base/events.h>
#include <ydb/core/protos/data_events.pb.h>
#include <ydb/core/scheme/scheme_tabledefs.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/actors/core/event_pb.h>

namespace NKikimr::NEvents {

struct TDataEvents {

    class TCoordinatorInfo {
        YDB_READONLY(ui64, MinStep, 0);
        YDB_READONLY(ui64, MaxStep, 0);
        YDB_READONLY_DEF(google::protobuf::RepeatedField<ui64>, DomainCoordinators);

    public:
        TCoordinatorInfo(const ui64 minStep, const ui64 maxStep, const google::protobuf::RepeatedField<ui64>& coordinators)
            : MinStep(minStep)
            , MaxStep(maxStep)
            , DomainCoordinators(coordinators) {}
    };

    enum EEventType {
        EvWrite = EventSpaceBegin(TKikimrEvents::ES_DATA_OPERATIONS),
        EvWriteResult,
        EvLockRows,
        EvLockRowsCancel,
        EvLockRowsResult,
        EvEnd
    };

    static_assert(EEventType::EvEnd < EventSpaceEnd(TKikimrEvents::ES_DATA_OPERATIONS), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_DATA_OPERATIONS)");

    struct TEvWrite : public NActors::TEventPB<TEvWrite, NKikimrDataEvents::TEvWrite, TDataEvents::EvWrite> {
    public:
        TEvWrite() = default;

        TEvWrite(const ui64 txId, NKikimrDataEvents::TEvWrite::ETxMode txMode);

        TEvWrite(NKikimrDataEvents::TEvWrite::ETxMode txMode);

        TEvWrite& SetTxId(const ui64 txId) {
            Record.SetTxId(txId);
            return *this;
        }

        TEvWrite& SetLockId(const ui64 lockTxId, const ui64 lockNodeId) {
            Record.SetLockTxId(lockTxId);
            Record.SetLockNodeId(lockNodeId);
            return *this;
        }

        NKikimrDataEvents::TEvWrite::TOperation& AddOperation(NKikimrDataEvents::TEvWrite_TOperation::EOperationType operationType,
            const TTableId& tableId, const std::vector<ui32>& columnIds,
            ui64 payloadIndex, NKikimrDataEvents::EDataFormat payloadFormat,
            const ui32 defaultFilledColumnCount = 0);

        // An unsafe truncate wipes the whole table on the shard, so it carries neither payload nor columns.
        NKikimrDataEvents::TEvWrite::TOperation& AddUnsafeTruncateOperation(const TTableId& tableId);

        ui64 GetTxId() const {
            return Record.GetTxId();
        }

        void SetOrbit(NLWTrace::TOrbit&& orbit) { Orbit = std::move(orbit); }
        NLWTrace::TOrbit& GetOrbit() { return Orbit; }
        NLWTrace::TOrbit&& MoveOrbit() { return std::move(Orbit); }
    private:
        NLWTrace::TOrbit Orbit;
    };

    struct TEvWriteResult : public NActors::TEventPB<TEvWriteResult, NKikimrDataEvents::TEvWriteResult, TDataEvents::EvWriteResult> {
    public:
        TEvWriteResult() = default;

        static std::unique_ptr<TEvWriteResult> BuildError(const ui64 origin, const ui64 txId, const NKikimrDataEvents::TEvWriteResult::EStatus& status, const TString& errorMsg);

        static std::unique_ptr<TEvWriteResult> BuildCompleted(const ui64 origin);

        static std::unique_ptr<TEvWriteResult> BuildCompleted(const ui64 origin, const ui64 txId);

        static std::unique_ptr<TEvWriteResult> BuildCompleted(const ui64 origin, const ui64 txId, const NKikimrDataEvents::TLock& lock);

        static std::unique_ptr<TEvWriteResult> BuildPrepared(const ui64 origin, const ui64 txId, const TCoordinatorInfo& transactionInfo);

        void AddTxLock(ui64 lockId, ui64 shard, ui32 generation, ui64 counter, ui64 ssId, ui64 pathId, bool hasWrites,
            ui64 writerIndex = 0, ui64 writeSeqNum = 0);

        TString GetError() const;

        NKikimrDataEvents::TEvWriteResult::EStatus GetStatus() const { return Record.GetStatus(); }

        bool IsPrepared() const { return GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED; }
        bool IsComplete() const { return GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED; }
        bool IsError() const { return !IsPrepared() && !IsComplete(); }

        void SetOrbit(NLWTrace::TOrbit&& orbit) { Orbit = std::move(orbit); }
        NLWTrace::TOrbit& GetOrbit() { return Orbit; }
        NLWTrace::TOrbit&& MoveOrbit() { return std::move(Orbit); }
    private:
        NLWTrace::TOrbit Orbit;
    };

    struct TEvLockRows : public NActors::TEventPB<TEvLockRows, NKikimrDataEvents::TEvLockRows, TDataEvents::EvLockRows> {
    public:
        TEvLockRows() = default;

        explicit TEvLockRows(ui64 requestId) {
            Record.SetRequestId(requestId);
        }

        TTableId GetTableId() const;

        void SetTableId(const TTableId& tableId);

        TSerializedCellMatrix GetCellMatrix() const;

        void SetCellMatrix(TString matrix) {
            auto payloadIndex = AddPayload(TRope(std::move(matrix)));
            Record.SetPayloadIndex(payloadIndex);
        }
    };

    struct TEvLockRowsCancel : public NActors::TEventPB<TEvLockRowsCancel, NKikimrDataEvents::TEvLockRowsCancel, TDataEvents::EvLockRowsCancel> {
    public:
        TEvLockRowsCancel() = default;

        explicit TEvLockRowsCancel(ui64 requestId) {
            Record.SetRequestId(requestId);
        }
    };

    struct TEvLockRowsResult : public NActors::TEventPB<TEvLockRowsResult, NKikimrDataEvents::TEvLockRowsResult, TDataEvents::EvLockRowsResult> {
    public:
        TEvLockRowsResult() = default;

        TEvLockRowsResult(ui64 tabletId, ui64 requestId, NKikimrDataEvents::TEvLockRowsResult::EStatus status);

        TEvLockRowsResult(ui64 tabletId, ui64 requestId, NKikimrDataEvents::TEvLockRowsResult::EStatus status, const TString& errorMsg);

        void AddLock(ui64 lockId, ui64 shard, ui32 generation, ui64 counter, ui64 ssId, ui64 pathId, bool hasWrites);
    };

};

}
