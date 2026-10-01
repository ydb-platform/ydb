#pragma once

#include <ydb/core/tx/columnshard/flow_control_manager/flow_control_manager_types.h>

#include <ydb/library/actors/core/actorsystem.h>

namespace NKikimr::NTxProxy {

// Flow-controlled entry into the LongTx write path: registers a helper on the caller's mailbox
// that asks the node's flow control manager for admission and only then starts the regular write.
//
// This lives in tx_proxy rather than in the flow control manager library because the helper has to
// call back into DoLongTxWriteSameMailbox once admitted; keeping it here makes the dependency
// one-directional (tx_proxy -> flow_control_manager) instead of a cycle.
void StartLongTxWriteFlowControlled(const TActorContext& ctx, NColumnShard::NFlowControl::TLongTxWrite&& longTxWrite);

}   // namespace NKikimr::NTxProxy
