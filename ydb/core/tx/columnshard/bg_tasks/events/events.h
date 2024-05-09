#pragma once
#include <ydb/core/base/events.h>
#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/core/tx/columnshard/bg_tasks/abstract/control.h>
#include <ydb/core/tx/columnshard/bg_tasks/protos/data.pb.h>

namespace NKikimr::NOlap::NBackground {

class TEvents {
public:
    enum EEv {
        EvExecuteGeneralLocalTransaction = EventSpaceBegin(TKikimrEvents::ES_TX_BACKGROUND),
        EvLocalTransactionComplete,
        EvSessionControl,
        EvRemoveSession,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_TX_BACKGROUND), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_TX_BACKGROUND)");
};

class TEvExecuteGeneralLocalTransaction: public TEventLocal<TEvExecuteGeneralLocalTransaction, TEvents::EvExecuteGeneralLocalTransaction> {
private:
    std::unique_ptr<NTabletFlatExecutor::ITransaction> Transaction;
public:
    TEvExecuteGeneralLocalTransaction(std::unique_ptr<NTabletFlatExecutor::ITransaction>&& transaction)
        : Transaction(std::move(transaction))
    {
        AFL_VERIFY(!!Transaction);
    }

    std::unique_ptr<NTabletFlatExecutor::ITransaction> ExtractTransaction() {
        AFL_VERIFY(!!Transaction);
        return std::move(Transaction);
    }
};

class TEvLocalTransactionCompleted: public TEventLocal<TEvLocalTransactionCompleted, TEvents::EvLocalTransactionComplete> {
private:
    const ui64 InternalTxId;
public:
    TEvLocalTransactionCompleted(const ui64 internalTxId)
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