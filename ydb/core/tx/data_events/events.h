#pragma once

#include "write_data.h"

#include <library/cpp/lwtrace/shuttle.h>

#include <ydb/core/protos/data_events.pb.h>
#include <ydb/core/base/events.h>

#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/actors/core/log.h>


namespace NKikimr::NEvents {

struct TDataEvents {

    class TCoordinatorInfo {
        YDB_READONLY_DEF(ui64, TabletId);
        YDB_READONLY(ui64, MinStep, 0);
        YDB_READONLY(ui64, MaxStep, 0);
        YDB_READONLY_DEF(google::protobuf::RepeatedField<ui64>, DomainCoordinators);

    public:
        TCoordinatorInfo(const ui64 tabletId, const ui64 minStep, const ui64 maxStep, const google::protobuf::RepeatedField<ui64>& coordinators)
            : TabletId(tabletId)
            , MinStep(minStep)
            , MaxStep(maxStep)
            , DomainCoordinators(coordinators) {}
    };

    enum EEventType {
        EvWrite = EventSpaceBegin(TKikimrEvents::ES_DATA_OPERATIONS),
        EvWriteResult,
        EvEnd
    };

    static_assert(EEventType::EvEnd < EventSpaceEnd(TKikimrEvents::ES_DATA_OPERATIONS), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_DATA_OPERATIONS)");

    struct TEvWrite : public NActors::TEventPB<TEvWrite, NKikimrDataEvents::TEvWrite, TDataEvents::EvWrite> {
    public:
        TEvWrite() = default;

        TEvWrite(ui64 txId, NKikimrDataEvents::TEvWrite::ETxMode txMode) {
            Y_ABORT_UNLESS(txMode != NKikimrDataEvents::TEvWrite::MODE_UNSPECIFIED);

            Record.SetTxId(txId);
            Record.SetTxMode(txMode);
        }

        void AddOperation(NKikimrDataEvents::TEvWrite_TOperation::EOperationType operationType, ui64 tableId, 
            ui64 schemaVersion, const std::vector<std::pair<TString, NScheme::TTypeInfo>>& ydbSchema,
            ui64 payloadIndex, NKikimrDataEvents::EDataFormat payloadFormat) {
            Y_ABORT_UNLESS(operationType != NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UNSPECIFIED);
            Y_ABORT_UNLESS(payloadFormat != NKikimrDataEvents::FORMAT_UNSPECIFIED);

            auto operation = Record.AddOperations();
            operation->SetType(operationType);
            operation->SetPayloadFormat(payloadFormat);
            operation->SetPayloadIndex(payloadIndex);
            operation->MutableTableId()->SetTableId(tableId);
            operation->MutableTableId()->SetSchemaVersion(schemaVersion);
            for (ui32 i = 0; i < ydbSchema.size(); ++i) {
                operation->AddColumnIds(i + 1);
            }
        }

        ui64 GetTxId() const {
            Y_ABORT_UNLESS(Record.HasTxId());
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

        static std::unique_ptr<TEvWriteResult> BuildError(const ui64 txId, const NKikimrDataEvents::TEvWriteResult::EStatus& status, const TString& errorMsg) {
            auto result = std::make_unique<TEvWriteResult>();
            ACFL_ERROR("event", "ev_write_error")("status", NKikimrDataEvents::TEvWriteResult::EStatus_Name(status))("details", errorMsg)("tx_id", txId);
            result->Record.SetTxId(txId);
            result->Record.SetStatus(status);
            auto issue = result->Record.AddIssues();
            issue->set_message(errorMsg);
            return result;
        }

        static std::unique_ptr<TEvWriteResult> BuildCommited(const ui64 txId) {
            auto result = std::make_unique<TEvWriteResult>();
            result->Record.SetTxId(txId);
            result->Record.SetStatus(NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
            return result;
        }

        static std::unique_ptr<TEvWriteResult> BuildPrepared(const ui64 txId, const TCoordinatorInfo& transactionInfo) {
            auto result = std::make_unique<TEvWriteResult>();
            result->Record.SetTxId(txId);
            result->Record.SetStatus(NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED);

            result->Record.SetMinStep(transactionInfo.GetMinStep());
            result->Record.SetMaxStep(transactionInfo.GetMaxStep());
            result->Record.MutableDomainCoordinators()->CopyFrom(transactionInfo.GetDomainCoordinators());
            return result;
        }

        void SetOrbit(NLWTrace::TOrbit&& orbit) { Orbit = std::move(orbit); }
        NLWTrace::TOrbit& GetOrbit() { return Orbit; }
        NLWTrace::TOrbit&& MoveOrbit() { return std::move(Orbit); }
    private:
        NLWTrace::TOrbit Orbit;
    };

};

}
