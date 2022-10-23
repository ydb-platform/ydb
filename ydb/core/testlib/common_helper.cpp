#include "cs_helper.h"
#include <ydb/core/tx/schemeshard/schemeshard.h>

namespace NKikimr::Tests::NCommon {

void THelper::WaitForSchemeOperation(TActorId sender, ui64 txId) {
    auto& runtime = *Server.GetRuntime();
    auto& settings = Server.GetSettings();
    auto request = MakeHolder<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>();
    request->Record.SetTxId(txId);
    auto tid = ChangeStateStorage(Tests::SchemeRoot, settings.Domain);
    runtime.SendToPipe(tid, sender, request.Release(), 0, GetPipeConfigWithRetries());
    runtime.GrabEdgeEventRethrow<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult>(sender);
}

}
