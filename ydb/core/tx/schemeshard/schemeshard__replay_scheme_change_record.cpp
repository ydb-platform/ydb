#include "schemeshard_impl.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>

namespace NKikimr::NSchemeShard {

// Privileged re-execution of a persisted scheme change record body.
// Forwards the body through the normal TEvModifySchemeTransaction pipeline
// after injecting Internal=true + AllowAccessToPrivatePaths=true (flags the
// public DDL path does not expose) and rewriting WorkingDir to swap source
// path prefix with target prefix. The reply is a standard
// TEvModifySchemeTransactionResult for the tx id in the request.
void TSchemeShard::Handle(TEvSchemeShard::TEvReplaySchemeChangeRecord::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& req = ev->Get()->Record;
    const ui64 txId = req.GetTxId();

    auto sendError = [&](NKikimrScheme::EStatus status, const TString& reason) {
        auto result = MakeHolder<TEvSchemeShard::TEvModifySchemeTransactionResult>(
            status, txId, TabletID(), reason);
        ctx.Send(ev->Sender, result.Release());
    };

    NKikimrSchemeOp::TModifyScheme body;
    if (!body.ParseFromString(req.GetBody())) {
        sendError(NKikimrScheme::StatusInvalidParameter,
            "ReplaySchemeChangeRecord: failed to parse body");
        return;
    }

    const TString sourcePrefix = req.GetSourcePathPrefix();
    const TString targetPrefix = req.GetTargetPathPrefix();
    TString wd = body.GetWorkingDir();
    if (!wd.StartsWith(sourcePrefix)) {
        sendError(NKikimrScheme::StatusInvalidParameter,
            TStringBuilder() << "ReplaySchemeChangeRecord: WorkingDir '" << wd
                << "' does not start with SourcePathPrefix '" << sourcePrefix << "'");
        return;
    }
    body.SetWorkingDir(targetPrefix + wd.substr(sourcePrefix.size()));

    // These flags are the whole point of this RPC: the public DDL path
    // cannot set them, so replay of decomposed sub-operations (e.g. impl
    // tables under an index path) would otherwise be rejected.
    body.SetInternal(true);
    body.SetAllowAccessToPrivatePaths(true);

    auto modify = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(txId, TabletID());
    *modify->Record.AddTransaction() = std::move(body);

    // Forward through normal pipeline; reply flows directly to the original sender.
    ctx.Send(new IEventHandle(SelfId(), ev->Sender, modify.Release()));
}

} // namespace NKikimr::NSchemeShard
