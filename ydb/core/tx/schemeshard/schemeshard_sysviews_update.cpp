#include "schemeshard_sysviews_update.h"

#include <ydb/core/sys_view/common/events.h>

namespace NKikimr::NSchemeShard {

extern bool isSysDirCreateAllowed;

TString TModifySysViewRequestInfo::DebugString() const {
    TStringBuilder buf;
    switch (OperationType) {
    case NKikimrSchemeOp::ESchemeOpMkDir:
        buf << "mkdir";
        break;
    case NKikimrSchemeOp::ESchemeOpCreateSysView:
        buf << "create sys view";
        break;
    case NKikimrSchemeOp::ESchemeOpDropSysView:
        buf << "drop sys view";
        break;
    default:
        Y_ABORT("Unsupported operation type");
    }

    buf << " '" << JoinPath({WorkingDir, TargetName}) << "'";

    return buf;
}

class TSysViewsUpdate : public TActorBootstrapped<TSysViewsUpdate> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SCHEMESHARD_SYSVIEWS_UPDATE;
    }

    TSysViewsUpdate(uint64_t ssTabletId, TActorId ssActorId, THashMap<uint64_t, TModifySysViewRequestInfo> sysViewUpdates)
        : SSTabletId(ssTabletId)
        , SSActorId(ssActorId)
        , SysViewUpdates(std::move(sysViewUpdates))
    {
    }

    void Bootstrap() {
        for (const auto& [txId, request] : SysViewUpdates) {
            switch (request.OperationType) {
            case NKikimrSchemeOp::ESchemeOpMkDir:
                SendMakeSysViewDirTransaction(txId, request.WorkingDir, request.TargetName);
                break;
            case NKikimrSchemeOp::ESchemeOpCreateSysView:
                SendCreateSysViewTransaction(txId, request.WorkingDir, request.TargetName, *request.SysViewType);
                break;
            case NKikimrSchemeOp::ESchemeOpDropSysView:
                SendDropSysViewTransaction(txId, request.WorkingDir, request.TargetName);
                break;
            default:
                Y_UNREACHABLE();
            }
        }

        Become(&TSysViewsUpdate::StateUpdateSysViewFolder);
    }

    STFUNC(StateUpdateSysViewFolder) {
        switch(ev->GetTypeRewrite()) {
            hFunc(TEvSchemeShard::TEvModifySchemeTransactionResult, Handle);
            hFunc(TEvSchemeShard::TEvNotifyTxCompletionResult, Handle);
            IgnoreFunc(TEvSchemeShard::TEvNotifyTxCompletionRegistered);
            cFunc(NSysView::TEvSysView::TEvUpdateFinished::EventType, PassAway);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            default:
                LOG_CRIT(*TlsActivationContext, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TSysViewsUpdate StateDeleteObsoleteSysViews unexpected event 0x%08" PRIx32, ev->GetTypeRewrite());
        }
    }

private:

    void SendMakeSysViewDirTransaction(uint64_t txId, const TString& workingDir, const TString& name) const {
        const auto& ctx = TlsActivationContext->AsActorContext();
        auto request = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>();
        auto& record = request->Record;
        record.SetTxId(txId);

        auto& modifyScheme = *record.AddTransaction();
        modifyScheme.SetWorkingDir(workingDir);
        modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpMkDir);
        modifyScheme.MutableMkDir()->SetName(name);
        modifyScheme.SetFailOnExist(false);

        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "SysViewsUpdate# " << ctx.SelfID.ToString() <<
            " at schemeshard: " << SSTabletId <<
            " Send TEvModifySchemeTransaction:" <<
            " workdir: " << workingDir <<
            ", mkdir '" << name << "'");

            // TODO(n00bcracker) Delete after Check changes
            isSysDirCreateAllowed = true;

        Send(SSActorId, request.Release());
    }

    void SendCreateSysViewTransaction(uint64_t txId, const TString& workingDir, const TString& name,
                                      NKikimrSysView::ESysViewType type) const {
        const auto& ctx = TlsActivationContext->AsActorContext();
        auto request = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>();
        auto& record = request->Record;
        record.SetTxId(txId);

        auto& modifyScheme = *record.AddTransaction();
        modifyScheme.SetWorkingDir(workingDir);
        modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateSysView);
        auto& sysViewDescr = *modifyScheme.MutableCreateSysView();
        sysViewDescr.SetName(name);
        sysViewDescr.SetType(type);
        modifyScheme.SetFailOnExist(false);

        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "SysViewsUpdate# " << ctx.SelfID.ToString() <<
            " at schemeshard: " << SSTabletId <<
            " Send TEvModifySchemeTransaction:" <<
            " workdir: " << workingDir <<
            ", create sys view: '" << name << "'");

        Send(SSActorId, request.Release());
    }

    void SendDropSysViewTransaction(uint64_t txId, const TString& workingDir, const TString& name) const {
        const auto& ctx = TlsActivationContext->AsActorContext();
        auto request = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>();
        auto& record = request->Record;
        record.SetTxId(txId);

        auto& modifyScheme = *record.AddTransaction();
        modifyScheme.SetWorkingDir(workingDir);
        modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpDropSysView);
        auto& dropDescr = *modifyScheme.MutableDrop();
        dropDescr.SetName(name);

        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "SysViewsUpdate# " << ctx.SelfID.ToString() <<
            " at schemeshard: " << SSTabletId <<
            " Send TEvModifySchemeTransaction:" <<
            " workdir: " << workingDir <<
            ", drop sys view: '" << name << "'");

        Send(SSActorId, request.Release());
    }

    void SubscribeToCompletion(uint64_t txId) {
        const auto& ctx = TlsActivationContext->AsActorContext();
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "SysViewsUpdate# " << ctx.SelfID.ToString() <<
            " at schemeshard: " << SSTabletId <<
            " Send TEvNotifyTxCompletion" <<
            ", txId " << txId);


        Send(SSActorId, new TEvSchemeShard::TEvNotifyTxCompletion(txId));
    }

    void Handle(TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        const auto status = record.GetStatus();
        const auto txId = record.GetTxId();
        const auto& modifyInfo = SysViewUpdates.at(txId);

        const auto& ctx = TlsActivationContext->AsActorContext();
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "SysViewsUpdate# " << ctx.SelfID.ToString() <<
            " at schemeshard: " << SSTabletId <<
            " Handle TEvModifySchemeTransactionResult" <<
            ", " << modifyInfo.DebugString() <<
            ", status: " << status);

        // TODO(n00bcracker) Delete after Check changes
        if (modifyInfo.OperationType == NKikimrSchemeOp::ESchemeOpMkDir) {
            isSysDirCreateAllowed = false;
        }

        switch (status) {
        case NKikimrScheme::StatusSuccess:
            SysViewUpdates.erase(txId);
            break;
        case NKikimrScheme::StatusAccepted:
            SubscribeToCompletion(txId);
            break;
        default:
            LOG_ERROR_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "SysViewsUpdate# " << ctx.SelfID.ToString() <<
                " at schemeshard: " << SSTabletId <<
                ", failed to " << modifyInfo.DebugString() <<
                ", reason: " << record.GetReason());

            SysViewUpdates.erase(txId);
            break;
        }

        if (SysViewUpdates.empty()) {
            Send(ctx.SelfID, new NSysView::TEvSysView::TEvUpdateFinished());
        }
    }

    void Handle(TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        const auto txId = record.GetTxId();
        const auto& modifyInfo = SysViewUpdates.at(txId);

        const auto& ctx = TlsActivationContext->AsActorContext();
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "SysViewsUpdate# " << ctx.SelfID.ToString() <<
            " at schemeshard: " << SSTabletId <<
            " Handle TEvNotifyTxCompletionResult" <<
            ", " << modifyInfo.DebugString());

        SysViewUpdates.erase(txId);

        if (SysViewUpdates.empty()) {
            Send(ctx.SelfID, new NSysView::TEvSysView::TEvUpdateFinished());
        }
    }

private:
    const uint64_t SSTabletId;
    const TActorId SSActorId;
    THashMap<uint64_t, TModifySysViewRequestInfo> SysViewUpdates;
};

THolder<IActor> CreateSysViewsUpdate(ui64 ssTabletId, TActorId ssActorId,
                                     THashMap<uint64_t, TModifySysViewRequestInfo> sysViewUpdates) {
    return MakeHolder<TSysViewsUpdate>(ssTabletId, ssActorId, std::move(sysViewUpdates));
}

} // namespace NKikimr::NSchemeShard
