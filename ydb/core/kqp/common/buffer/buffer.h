#pragma once

#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/kqp/common/kqp_tx_manager.h>

#include <ydb/library/aclib/user_context.h>

namespace NKikimr {
namespace NKqp {

struct TKqpBufferWriterSettings {
    TActorId SessionActorId;
    IKqpTransactionManagerPtr TxManager;
    NWilson::TTraceId TraceId;
    ui64 QuerySpanId = 0;
    TIntrusivePtr<TKqpCounters> Counters;
    TIntrusivePtr<NTxProxy::TTxProxyMon> TxProxyMon;
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
    TIntrusivePtr<NACLib::TUserContext> UserCtx;
};

NActors::IActor* CreateKqpBufferWriterActor(TKqpBufferWriterSettings&& settings);

}
}
