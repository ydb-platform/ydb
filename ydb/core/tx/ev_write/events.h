#pragma once

#include "write_data.h"

#include <ydb/core/protos/ev_write.pb.h>
#include <ydb/core/base/events.h>

#include <library/cpp/actors/core/event_pb.h>
#include <library/cpp/actors/core/log.h>


namespace NKikimr::NEvents {


class IDataConstructor {
public:
    using TPtr = std::shared_ptr<IDataConstructor>;
    virtual ~IDataConstructor() {}
    virtual void Serialize(NKikimrDataEvents::TOperationData& proto) const = 0;
    virtual ui64 GetSchemaVersion() const = 0;
};

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
        TEvWrite() = default;

        TEvWrite(ui64 txId) {
            Record.SetTxId(txId);
        }

        void AddReplaceOp(const ui64 tableId, const IDataConstructor::TPtr& data) {
            Record.MutableTableId()->SetTableId(tableId);
            Y_VERIFY(data);
            Record.MutableTableId()->SetSchemaVersion(data->GetSchemaVersion());
            data->Serialize(*Record.MutableReplace());
        }

        ui64 GetTxId() const {
            Y_VERIFY(Record.HasTxId());
            return Record.GetTxId();
        }
    };

    struct TEvWriteResult : public NActors::TEventPB<TEvWriteResult, NKikimrDataEvents::TEvWriteResult, TDataEvents::EvWriteResult> {

        TEvWriteResult() = default;

        static std::unique_ptr<TEvWriteResult> BuildError(const ui64 txId, const NKikimrDataEvents::TEvWriteResult::EOperationStatus& status, const TString& errorMsg) {
            auto result = std::make_unique<TEvWriteResult>();
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "ev_write_error")("status", NKikimrDataEvents::TEvWriteResult::EOperationStatus_Name(status))
                ("details", errorMsg)("tx_id", txId);
            result->Record.SetTxId(txId);
            result->Record.SetStatus(status);
            result->Record.MutableIssueMessage()->set_message(errorMsg);
            return result;
        }

        static std::unique_ptr<TEvWriteResult> BuildCommited(const ui64 txId) {
            auto result = std::make_unique<TEvWriteResult>();
            result->Record.SetTxId(txId);
            result->Record.SetStatus(NKikimrDataEvents::TEvWriteResult::COMPLETED);
            return result;
        }

        static std::unique_ptr<TEvWriteResult> BuildPrepared(const ui64 txId, const TCoordinatorInfo& transactionInfo) {
            auto result = std::make_unique<TEvWriteResult>();
            result->Record.SetTxId(txId);
            result->Record.SetStatus(NKikimrDataEvents::TEvWriteResult::PREPARED);

            result->Record.SetMinStep(transactionInfo.GetMinStep());
            result->Record.SetMaxStep(transactionInfo.GetMaxStep());
            result->Record.MutableDomainCoordinators()->CopyFrom(transactionInfo.GetDomainCoordinators());
            return result;
        }
    };
};

}
