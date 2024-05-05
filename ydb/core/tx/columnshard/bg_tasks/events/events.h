#pragma once
#include <ydb/core/base/events.h>
#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/core/tx/columnshard/bg_tasks/abstract/control.h>
#include <ydb/core/tx/columnshard/bg_tasks/protos/data.pb.h>

namespace NKikimr::NOlap::NBackground {

class TEvents {
public:
    enum EEv {
        EvExecuteGeneralTransaction = EventSpaceBegin(TKikimrEvents::ES_TX_BACKGROUND),
        EvTransactionComplete,
        EvSessionControl,
        EvRemoveSession,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_TX_BACKGROUND), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_TX_BACKGROUND)");
};

class TEvExecuteGeneralTransaction: public TEventLocal<TEvExecuteGeneralTransaction, TEvents::EvExecuteGeneralTransaction> {
private:
    std::unique_ptr<NTabletFlatExecutor::ITransaction> Transaction;
public:
    TEvExecuteGeneralTransaction(std::unique_ptr<NTabletFlatExecutor::ITransaction>&& transaction)
        : Transaction(std::move(transaction))
    {
        AFL_VERIFY(!!Transaction);
    }

    std::unique_ptr<NTabletFlatExecutor::ITransaction> ExtractTransaction() {
        AFL_VERIFY(!!Transaction);
        return std::move(Transaction);
    }
};

class TEvTransactionCompleted: public TEventLocal<TEvTransactionCompleted, TEvents::EvTransactionComplete> {
private:
    const ui64 InternalTxId;
public:
    TEvTransactionCompleted(const ui64 internalTxId)
        : InternalTxId(internalTxId) {
    }

    ui64 GetInternalTxId() const {
        return InternalTxId;
    }
};

class TEvRemoveSession: public TEventLocal<TEvRemoveSession, TEvents::EvRemoveSession> {
private:
    YDB_READONLY_DEF(TString, ClassName);
    YDB_READONLY_DEF(TString, Identifier);
public:
    TEvRemoveSession(const TString& className, const TString& identifier)
        : ClassName(className)
        , Identifier(identifier)
    {
    }

};

struct TEvSessionControl: public TEventPB<TEvSessionControl, NKikimrTxBackgroundProto::TSessionControlContainer, TEvents::EvSessionControl> {
    TEvSessionControl() = default;

    TEvSessionControl(const TSessionControlContainer& container) {
        Record = container.SerializeToProto();
    }
};

}