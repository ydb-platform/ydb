#include "schemeshard_sysviews_update.h"

#include <ydb/core/sys_view/common/events.h>

namespace NKikimr::NSchemeShard {

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

namespace {

    THolder<TEvSchemeShard::TEvModifySchemeTransaction> BuildMakeSysViewDirTransaction(TTxId txId, const TString& workingDir,
                                                                                       const TString& name) {
        auto request = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>();
        auto& record = request->Record;
        record.SetTxId(static_cast<ui64>(txId));
        record.SetUserToken(NACLib::TSystemUsers::Metadata().SerializeAsString());
        record.SetOwner(BUILTIN_ACL_METADATA);

        auto& modifyScheme = *record.AddTransaction();
        modifyScheme.SetWorkingDir(workingDir);
        modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpMkDir);
        modifyScheme.SetInternal(true);
        modifyScheme.SetFailOnExist(false);

        modifyScheme.MutableMkDir()->SetName(name);

        return request;
    }

    THolder<TEvSchemeShard::TEvModifySchemeTransaction> BuildCreateSysViewTransaction(TTxId txId, const TString& workingDir,
                                                                                      const TString& name,
                                                                                      NKikimrSysView::ESysViewType type) {
        auto request = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>();
        auto& record = request->Record;
        record.SetTxId(static_cast<ui64>(txId));
        record.SetUserToken(NACLib::TSystemUsers::Metadata().SerializeAsString());
        record.SetOwner(BUILTIN_ACL_METADATA);

        auto& modifyScheme = *record.AddTransaction();
        modifyScheme.SetWorkingDir(workingDir);
        modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateSysView);
        modifyScheme.SetInternal(true);
        modifyScheme.SetFailOnExist(false);

        auto& sysViewDescr = *modifyScheme.MutableCreateSysView();
        sysViewDescr.SetName(name);
        sysViewDescr.SetType(type);

        return request;
    }

    THolder<TEvSchemeShard::TEvModifySchemeTransaction> BuildDropSysViewTransaction(TTxId txId, const TString& workingDir,
                                                                                    const TString& name) {
        auto request = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>();
        auto& record = request->Record;
        record.SetTxId(static_cast<ui64>(txId));
        record.SetUserToken(NACLib::TSystemUsers::Metadata().SerializeAsString());
        record.SetOwner(BUILTIN_ACL_METADATA);

        auto& modifyScheme = *record.AddTransaction();
        modifyScheme.SetWorkingDir(workingDir);
        modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpDropSysView);
        modifyScheme.SetInternal(true);

        modifyScheme.MutableDrop()->SetName(name);

        return request;
    }

} // namespace

class TSysViewsRosterUpdate : public TActorBootstrapped<TSysViewsRosterUpdate> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SCHEMESHARD_SYSVIEWS_ROSTER_UPDATE;
    }

    TSysViewsRosterUpdate(TTabletId selfTabletId, TActorId selfActorId,
                          TVector<std::pair<TTxId, TModifySysViewRequestInfo>>&& sysViewUpdates)
        : SelfTabletId(selfTabletId)
        , SelfActorId(selfActorId)
        , SysViewUpdates(std::move(sysViewUpdates))
    {
    }

    void Bootstrap() {
        const auto& ctx = TlsActivationContext->AsActorContext();

        for (auto& [txId, requestInfo] : SysViewUpdates) {
            THolder<TEvSchemeShard::TEvModifySchemeTransaction> request;
            switch (requestInfo.OperationType) {
            case NKikimrSchemeOp::ESchemeOpMkDir:
                request = BuildMakeSysViewDirTransaction(txId, requestInfo.WorkingDir, requestInfo.TargetName);
                break;
            case NKikimrSchemeOp::ESchemeOpCreateSysView:
                request = BuildCreateSysViewTransaction(txId, requestInfo.WorkingDir, requestInfo.TargetName, *requestInfo.SysViewType);
                break;
            case NKikimrSchemeOp::ESchemeOpDropSysView:
                request = BuildDropSysViewTransaction(txId, requestInfo.WorkingDir, requestInfo.TargetName);
                break;
            default:
                Y_UNREACHABLE();
            }

            LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "SysViewsRosterUpdate# " << ctx.SelfID.ToString() <<
                " at schemeshard: " << SelfTabletId <<
                " Send TEvModifySchemeTransaction: " << request->Record.ShortDebugString());

            AwaitingModifySchemeRequests.emplace(txId, std::move(requestInfo));
            Send(SelfActorId, request.Release());
        }

        SysViewUpdates.clear();
        Become(&TSysViewsRosterUpdate::StateUpdateSysViewFolder);
    }

    STFUNC(StateUpdateSysViewFolder) {
        switch(ev->GetTypeRewrite()) {
            HFunc(TEvSchemeShard::TEvModifySchemeTransactionResult, Handle);
            HFunc(TEvSchemeShard::TEvNotifyTxCompletionResult, Handle);
            IgnoreFunc(TEvSchemeShard::TEvNotifyTxCompletionRegistered);
            cFunc(NSysView::TEvSysView::TEvRosterUpdateFinished::EventType, PassAway);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            default:
                LOG_CRIT(*TlsActivationContext, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TSysViewsRosterUpdate StateUpdateSysViewFolder unexpected event 0x%08" PRIx32, ev->GetTypeRewrite());
        }
    }

private:

    void SubscribeToCompletion(TTxId txId) const {
        const auto& ctx = TlsActivationContext->AsActorContext();
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "SysViewsRosterUpdate# " << ctx.SelfID.ToString() <<
            " at schemeshard: " << SelfTabletId <<
            " Send TEvNotifyTxCompletion" <<
            ", txId " << txId);


        Send(SelfActorId, new TEvSchemeShard::TEvNotifyTxCompletion(static_cast<ui64>(txId)));
    }

    void Handle(TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record;
        const auto status = record.GetStatus();
        const TTxId txId(record.GetTxId());
        const auto& modifyInfo = AwaitingModifySchemeRequests.at(txId);

        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "SysViewsRosterUpdate# " << ctx.SelfID.ToString() <<
            " at schemeshard: " << SelfTabletId <<
            " Handle TEvModifySchemeTransactionResult" <<
            ", " << modifyInfo.DebugString() <<
            ", status: " << status);

        switch (status) {
        case NKikimrScheme::StatusSuccess:
            AwaitingModifySchemeRequests.erase(txId);
            break;
        case NKikimrScheme::StatusAccepted:
            SubscribeToCompletion(txId);
            break;
        default:
            LOG_ERROR_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "SysViewsRosterUpdate# " << ctx.SelfID.ToString() <<
                " at schemeshard: " << SelfTabletId <<
                ", failed to " << modifyInfo.DebugString() <<
                ", reason: " << record.GetReason());

            AwaitingModifySchemeRequests.erase(txId);
            break;
        }

        if (AwaitingModifySchemeRequests.empty()) {
            LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "SysViewsRosterUpdate# " << ctx.SelfID.ToString() <<
                " at schemeshard: " << SelfTabletId <<
                " Send TEvRosterUpdateFinished");
            Send(ctx.SelfID, new NSysView::TEvSysView::TEvRosterUpdateFinished());
        }
    }

    void Handle(TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record;
        const TTxId txId(record.GetTxId());
        const auto& modifyInfo = AwaitingModifySchemeRequests.at(txId);

        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "SysViewsRosterUpdate# " << ctx.SelfID.ToString() <<
            " at schemeshard: " << SelfTabletId <<
            " Handle TEvNotifyTxCompletionResult" <<
            ", " << modifyInfo.DebugString());

        AwaitingModifySchemeRequests.erase(txId);

        if (AwaitingModifySchemeRequests.empty()) {
            LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "SysViewsRosterUpdate# " << ctx.SelfID.ToString() <<
                " at schemeshard: " << SelfTabletId <<
                " Send TEvRosterUpdateFinished");
            Send(ctx.SelfID, new NSysView::TEvSysView::TEvRosterUpdateFinished());
        }
    }

private:
    const TTabletId SelfTabletId;
    const TActorId SelfActorId;
    TVector<std::pair<TTxId, TModifySysViewRequestInfo>> SysViewUpdates;
    THashMap<TTxId, TModifySysViewRequestInfo> AwaitingModifySchemeRequests;
};

THolder<IActor> CreateSysViewsRosterUpdate(TTabletId selfTabletId, TActorId selfActorId,
                                           TVector<std::pair<TTxId, TModifySysViewRequestInfo>>&& sysViewUpdates) {
    return MakeHolder<TSysViewsRosterUpdate>(selfTabletId, selfActorId, std::move(sysViewUpdates));
}

} // namespace NKikimr::NSchemeShard
