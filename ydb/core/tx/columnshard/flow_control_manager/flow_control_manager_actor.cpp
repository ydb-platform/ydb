#include "flow_control_manager_actor.h"

#include <ydb/core/tx/columnshard/overload_manager/overload_manager_service.h>

// TODO: remove this when DoLongTxWriteSameMailbox is not used
#include <ydb/core/tx/tx_proxy/upload_rows_common_impl.h>

namespace NKikimr::NColumnShard::NFlowControl {

TFlowControlManager::TFlowControlManager(TIntrusivePtr<::NMonitoring::TDynamicCounters> countersGroup)
    : TActor(&TThis::StateMain)
    , Counters(countersGroup)
{
}

void TFlowControlManager::Handle(const NFlowControl::TEvLongTxWrite::TPtr& ev, const TActorContext& ctx) {
    Counters.OnNewRequest();
    auto tx = ev->Get()->DetachLongTxWrite();
    NTxProxy::DoLongTxWriteSameMailbox(ctx, tx.GetReplyTo(), tx.GetLongTxId(), tx.GetDedupId(), tx.GetDatabaseName(), tx.GetPath(),
        tx.GetNavigateResult(), tx.GetBatch(), tx.GetIssues(), tx.GetUserCtx(), true);
}

}   // namespace NKikimr::NColumnShard::NFlowControl
