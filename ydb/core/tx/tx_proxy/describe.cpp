#include "proxy.h"

#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/scheme/scheme_types_defs.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/sys_view/common/schema.h>
#include <ydb/core/protos/table_stats.pb.h>

#include <ydb/library/aclib/aclib.h>

#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr {
namespace NTxProxy {

class TDescribeReq : public TActor<TDescribeReq> {
    const TTxProxyServices Services;

    THolder<TEvTxProxyReq::TEvNavigateScheme> SchemeRequest;
    TIntrusivePtr<TTxProxyMon> TxProxyMon;

    TInstant WallClockStarted;

    TActorId Source;
    ui64 SourceCookie;

    TAutoPtr<const NACLib::TUserToken> UserToken;

    TString TextPath;

    void Die(const TActorContext &ctx) override {
        --*TxProxyMon->NavigateReqInFly;

        Send(Services.LeaderPipeCache, new TEvPipeCache::TEvUnlink(0));

        TActor::Die(ctx);
    }

    void ReportError(NKikimrScheme::EStatus status, const TString& reason, const TActorContext &ctx) {
        TAutoPtr<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResultBuilder> result =
                new NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResultBuilder();

        if (SchemeRequest != nullptr) {
            const auto &record = SchemeRequest->Ev->Get()->Record;
            if (record.GetDescribePath().HasPath()) {
                result->Record.SetPath(record.GetDescribePath().GetPath());
            }
        }

        result->Record.SetStatus(status);
        result->Record.SetReason(reason);

        ctx.Send(Source, result.Release(), 0, SourceCookie);
    }

    void FillRootDescr(NKikimrSchemeOp::TDirEntry* descr, const TString& name, ui64 schemeRootId) {
        descr->SetPathId(NSchemeShard::RootPathId);
        descr->SetName(name);
        descr->SetSchemeshardId(schemeRootId);
        descr->SetPathType(NKikimrSchemeOp::EPathType::EPathTypeDir);
        descr->SetCreateFinished(true);
        // TODO(xenoxeno): ?
        //descr->SetCreateTxId(0);
        //descr->SetCreateStep(0);
        //descr->SetOwner(BUILTIN_ACL_ROOT);
    }

    void FillSystemViewDescr(NKikimrSchemeOp::TDirEntry* descr, ui64 schemeShardId) {
        descr->SetSchemeshardId(schemeShardId);
        descr->SetPathId(InvalidLocalPathId);
        descr->SetParentPathId(InvalidLocalPathId);
        descr->SetCreateFinished(true);
        descr->SetCreateTxId(0);
        descr->SetCreateStep(0);
    }

    void SendSystemViewResult(const NSchemeCache::TSchemeCacheNavigate::TEntry& entry, const TString& path,
        const TActorContext& ctx)
    {
        auto schemeShardId = entry.DomainInfo->DomainKey.OwnerId;

        auto result = MakeHolder<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResultBuilder>(
            path, TPathId());

        auto* pathDescription = result->Record.MutablePathDescription();
        auto* self = pathDescription->MutableSelf();

        Y_ABORT_UNLESS(!entry.Path.empty());
        self->SetName(entry.Path.back());
        self->SetPathType(NKikimrSchemeOp::EPathTypeTable);
        FillSystemViewDescr(self, schemeShardId);

        auto* table = pathDescription->MutableTable();

        TVector<ui32> keyColumnIds(entry.Columns.size());
        size_t keySize = 0;

        table->SetName(entry.Path.back());
        table->MutableColumns()->Reserve(entry.Columns.size());

        for (const auto& [id, column] : entry.Columns) {
            auto* col = table->AddColumns();
            col->SetName(column.Name);
            col->SetType(NScheme::TypeName(column.PType, column.PTypeMod));
            auto columnType = NScheme::ProtoColumnTypeFromTypeInfoMod(column.PType, column.PTypeMod);
            col->SetTypeId(columnType.TypeId);
            if (columnType.TypeInfo) {
                *col->MutableTypeInfo() = *columnType.TypeInfo;
            }
            col->SetId(id);
            if (column.KeyOrder >= 0) {
                Y_ABORT_UNLESS((size_t)column.KeyOrder < keyColumnIds.size());
                keyColumnIds[column.KeyOrder] = id;
                ++keySize;
            }
        }

        table->MutableKeyColumnNames()->Reserve(keySize);
        table->MutableKeyColumnIds()->Reserve(keySize);

        for (size_t i = 0; i < keySize; ++i) {
            auto columnId = keyColumnIds[i];
            auto columnIt = entry.Columns.find(columnId);
            Y_ABORT_UNLESS(columnIt != entry.Columns.end());
            table->AddKeyColumnIds(columnId);
            table->AddKeyColumnNames(columnIt->second.Name);
        }

        auto* stats = pathDescription->MutableTableStats();
        stats->SetPartCount(0);

        LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY,
                "Actor# " << ctx.SelfID.ToString() <<
                " Send sysview TEvDescribeSchemeResult to# " << Source.ToString() <<
                " Cookie: " << SourceCookie <<
                " TEvDescribeSchemeResult: " << result->ToString());

        TxProxyMon->NavigateLatency->Collect((ctx.Now() - WallClockStarted).MilliSeconds());

        result->Record.SetStatus(NKikimrScheme::StatusSuccess);
        ctx.Send(Source, result.Release(), 0, SourceCookie);
        return Die(ctx);
    }

    void SendSystemViewFolderResult(const NSchemeCache::TSchemeCacheNavigate::TEntry& entry, const TString& path,
        const TActorContext& ctx)
    {
        auto schemeShardId = entry.DomainInfo->DomainKey.OwnerId;

        auto result = MakeHolder<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResultBuilder>(
            path, TPathId());

        auto* pathDescription = result->Record.MutablePathDescription();
        auto* self = pathDescription->MutableSelf();

        self->SetName(TString(NSysView::SysPathName));
        self->SetPathType(NKikimrSchemeOp::EPathTypeDir);
        FillSystemViewDescr(self, schemeShardId);

        if (entry.ListNodeEntry) {
            for (const auto& child : entry.ListNodeEntry->Children) {
                auto descr = pathDescription->AddChildren();
                descr->SetName(child.Name);
                descr->SetPathType(NKikimrSchemeOp::EPathTypeTable);
                FillSystemViewDescr(descr, schemeShardId);
            }
        };

        LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY,
                "Actor# " << ctx.SelfID.ToString() <<
                " Send sysview TEvDescribeSchemeResult to# " << Source.ToString() <<
                " Cookie: " << SourceCookie <<
                " TEvDescribeSchemeResult: " << result->ToString());

        TxProxyMon->NavigateLatency->Collect((ctx.Now() - WallClockStarted).MilliSeconds());

        result->Record.SetStatus(NKikimrScheme::StatusSuccess);
        ctx.Send(Source, result.Release(), 0, SourceCookie);
        return Die(ctx);
    }

    void Handle(TEvTxProxyReq::TEvNavigateScheme::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr &ev, const TActorContext &ctx);
    void Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr &ev, const TActorContext &ctx);

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TX_PROXY_NAVIGATE;
    }

    TDescribeReq(const TTxProxyServices &services, const TIntrusivePtr<TTxProxyMon>& txProxyMon)
        : TActor(&TThis::StateWaitInit)
        , Services(services)
        , TxProxyMon(txProxyMon)
    {
        ++*TxProxyMon->NavigateReqInFly;
    }

    STFUNC(StateWaitInit) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxProxyReq::TEvNavigateScheme, Handle);
        }
    }

    STFUNC(StateWaitResolve) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
        }
    }

    STFUNC(StateWaitExec) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            HFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
        }
    }
};

void TDescribeReq::Handle(TEvTxProxyReq::TEvNavigateScheme::TPtr &ev, const TActorContext &ctx) {
    TEvTxProxyReq::TEvNavigateScheme *msg = ev->Get();
    const auto &record = msg->Ev->Get()->Record;
    LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY, "Actor# " << ctx.SelfID.ToString() << " HANDLE EvNavigateScheme " << record.GetDescribePath().GetPath());

    WallClockStarted = ctx.Now();

    Source = msg->Ev->Sender;
    SourceCookie = msg->Ev->Cookie;

    if (record.GetDescribePath().HasPath()) {
        TDomainsInfo *domainsInfo = AppData(ctx)->DomainsInfo.Get();
        Y_ABORT_UNLESS(domainsInfo->Domain);

        if (record.GetDescribePath().GetPath() == "/") {
            // Special handling for enumerating roots
            TAutoPtr<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResultBuilder> result =
                new NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResultBuilder("/", TPathId(NSchemeShard::RootSchemeShardId, NSchemeShard::RootPathId));
            auto descr = result->Record.MutablePathDescription();
            FillRootDescr(descr->MutableSelf(), "/", NSchemeShard::RootSchemeShardId);
            auto entry = result->Record.MutablePathDescription()->AddChildren();
            auto *domain = domainsInfo->GetDomain();
            FillRootDescr(entry, domain->Name, domain->SchemeRoot);

            ctx.Send(Source, result.Release(), 0, SourceCookie);
            return Die(ctx);
        }
    }

    if (!record.GetUserToken().empty()) {
        UserToken = new NACLib::TUserToken(record.GetUserToken());
    }

    if (UserToken == nullptr && record.GetDescribePath().HasPathId()) {
        TAutoPtr<NSchemeShard::TEvSchemeShard::TEvDescribeScheme> req =
                new NSchemeShard::TEvSchemeShard::TEvDescribeScheme(
                                      record.GetDescribePath().GetSchemeshardId(),
                                      record.GetDescribePath().GetPathId());

        const ui64 shardToRequest = record.GetDescribePath().GetSchemeshardId();
        if (record.GetDescribePath().HasOptions()) {
            auto options = req->Record.MutableOptions();
            options->CopyFrom(record.GetDescribePath().GetOptions());
        }

        LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY, "Actor# " << ctx.SelfID.ToString()
            << " SEND to# " << shardToRequest << " shardToRequest " << req->ToString());

        Send(Services.LeaderPipeCache, new TEvPipeCache::TEvForward(req.Release(), shardToRequest, true), 0, SourceCookie);

        Become(&TThis::StateWaitExec);
        return;
    }

    TAutoPtr<NSchemeCache::TSchemeCacheNavigate> request(new NSchemeCache::TSchemeCacheNavigate());
    request->DatabaseName = record.GetDatabaseName();
    NSchemeCache::TSchemeCacheNavigate::TEntry entry;
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
    entry.SyncVersion = true;
    entry.ShowPrivatePath = record.GetDescribePath().GetOptions().GetShowPrivateTable();
    entry.Path = SplitPath(record.GetDescribePath().GetPath());
    if (entry.Path.empty()) {
        ReportError(NKikimrScheme::StatusInvalidParameter, "Invalid path", ctx);
        TxProxyMon->ResolveKeySetWrongRequest->Inc();
        return Die(ctx);
    }

    request->ResultSet.emplace_back(entry);

    ctx.Send(Services.SchemeCache, new TEvTxProxySchemeCache::TEvNavigateKeySet(request), 0, SourceCookie);

    SchemeRequest = ev->Release();
    Become(&TThis::StateWaitResolve);
}

void TDescribeReq::Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr &ev, const TActorContext &ctx) {
    TEvTxProxySchemeCache::TEvNavigateKeySetResult *msg = ev->Get();
    NSchemeCache::TSchemeCacheNavigate *navigate = msg->Request.Get();

    TxProxyMon->CacheRequestLatency->Collect((ctx.Now() - WallClockStarted).MilliSeconds());

    Y_ABORT_UNLESS(navigate->ResultSet.size() == 1);
    const auto& entry = navigate->ResultSet.front();

    LOG_LOG_S(ctx, (navigate->ErrorCount == 0 ? NActors::NLog::PRI_DEBUG : NActors::NLog::PRI_INFO),
        NKikimrServices::TX_PROXY,
        "Actor# " << ctx.SelfID.ToString()
        << " HANDLE EvNavigateKeySetResult TDescribeReq marker# P5 ErrorCount# " << navigate->ErrorCount);

    if (navigate->ErrorCount > 0) {
        switch (entry.Status) {
        case NSchemeCache::TSchemeCacheNavigate::EStatus::AccessDenied: {
            const ui32 access = NACLib::EAccessRights::DescribeSchema;
            LOG_ERROR_S(ctx, NKikimrServices::TX_PROXY,
                        "Access denied for " << (UserToken ? UserToken->GetUserSID() : "empty")
                        << " with access " << NACLib::AccessRightsToString(access)
                        << " to path " << JoinPath(entry.Path) << " because base path");
            ReportError(NKikimrScheme::StatusAccessDenied, "Access denied", ctx);
            break;
        }
        case NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown:
            ReportError(NKikimrScheme::StatusPathDoesNotExist, "Path not found", ctx);
            break;
        case NSchemeCache::TSchemeCacheNavigate::EStatus::RootUnknown:
            ReportError(NKikimrScheme::StatusPathDoesNotExist, "Root not found", ctx);
            TxProxyMon->ResolveKeySetWrongRequest->Inc();
            break;
        case NSchemeCache::TSchemeCacheNavigate::EStatus::RedirectLookupError:
            ReportError(NKikimrScheme::StatusNotAvailable, "Could not resolve redirected path", ctx);
            TxProxyMon->ResolveKeySetRedirectUnavaible->Inc();
            break;
        default:
            ReportError(NKikimrScheme::StatusNotAvailable, "Could not resolve path", ctx);
            TxProxyMon->ResolveKeySetFail->Inc();
            break;
        }

        return Die(ctx);
    }

    if (UserToken != nullptr) {
        ui32 access = NACLib::EAccessRights::DescribeSchema;
        if (entry.SecurityObject != nullptr && !entry.SecurityObject->CheckAccess(access, *UserToken)) {
            LOG_ERROR_S(ctx, NKikimrServices::TX_PROXY,
                        "Access denied for " << UserToken->GetUserSID()
                        << " with access " << NACLib::AccessRightsToString(access)
                        << " to path " << JoinPath(entry.Path));
            ReportError(NKikimrScheme::StatusAccessDenied, "Access denied", ctx);
            return Die(ctx);
        }
    }

    const auto& describePath = SchemeRequest->Ev->Get()->Record.GetDescribePath();

    if (entry.TableId.IsSystemView()) {
        // don't go to schemeshard
        const auto& path = describePath.GetPath();

        if (entry.TableId.SysViewInfo == NSysView::SysPathName) {
            return SendSystemViewFolderResult(entry, path, ctx);
        }

        return SendSystemViewResult(entry, path, ctx);
    }

    const ui64 shardToRequest = entry.DomainInfo->ExtractSchemeShard();

    TAutoPtr<NSchemeShard::TEvSchemeShard::TEvDescribeScheme> req(
        new NSchemeShard::TEvSchemeShard::TEvDescribeScheme(describePath));

    auto& record = req.Get()->Record;
    if (UserToken != nullptr) {
        auto options = record.MutableOptions();
        if (entry.SecurityObject != nullptr) {
            options->SetReturnBoundaries(false);
            options->SetReturnRangeKey(false);
            ui32 access = NACLib::EAccessRights::SelectRow;
            if (entry.SecurityObject->CheckAccess(access, *UserToken)) {
                options->SetReturnBoundaries(true);
                options->SetReturnRangeKey(true);
            }
        }
    }

    LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY, "Actor# " << ctx.SelfID.ToString()
        << " SEND to# " << shardToRequest << " shardToRequest " << req->ToString());

    Send(Services.LeaderPipeCache, new TEvPipeCache::TEvForward(req.Release(), shardToRequest, true), 0, SourceCookie);
    Become(&TThis::StateWaitExec);
}


void TDescribeReq::Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr &ev, const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::TX_PROXY,
                "Actor# " << ctx.SelfID.ToString() <<
                " Handle TEvDescribeSchemeResult" <<
                " Forward to# " << Source.ToString() <<
                " Cookie: " << ev->Cookie <<
                " TEvDescribeSchemeResult: " << ev->Get()->ToString());

    TxProxyMon->NavigateLatency->Collect((ctx.Now() - WallClockStarted).MilliSeconds());

    if (AppData()->FeatureFlags.GetEnableSystemViews()) {
        const auto& pathDescription = ev->Get()->GetRecord().GetPathDescription();
        const auto& self = pathDescription.GetSelf();

        const auto& domainsInfo = AppData()->DomainsInfo;

        bool needSysFolder = false;
        if (self.GetPathType() == NKikimrSchemeOp::EPathType::EPathTypeSubDomain ||
            self.GetPathType() == NKikimrSchemeOp::EPathType::EPathTypeColumnStore ||
            self.GetPathType() == NKikimrSchemeOp::EPathType::EPathTypeColumnTable)
        {
            needSysFolder = true;
        } else if (self.GetPathId() == NSchemeShard::RootPathId) {
            if (const auto& domain = domainsInfo->Domain; domain && domain->SchemeRoot == self.GetSchemeshardId()) {
                needSysFolder = true;
            }
        }

        if (needSysFolder) {
            bool hasSysFolder = false;

            const auto& children = pathDescription.GetChildren();
            if (!children.empty()) {
                auto size = children.size();
                if (children[size - 1].GetName() == NSysView::SysPathName) {
                    hasSysFolder = true;
                }
            }

            if (!hasSysFolder) {
                auto* record = ev->Get()->MutableRecord();
                auto* descr = record->MutablePathDescription()->AddChildren();
                descr->SetName(TString(NSysView::SysPathName));
                descr->SetPathType(NKikimrSchemeOp::EPathTypeDir);
                FillSystemViewDescr(descr, self.GetSchemeshardId());
            }
        }
    }

    ctx.ExecutorThread.Send(ev->Forward(Source));
    return Die(ctx);
}

void TDescribeReq::Handle(TEvPipeCache::TEvDeliveryProblem::TPtr &ev, const TActorContext &ctx) {
    Y_UNUSED(ev);
    ReportError(NKikimrScheme::StatusNotAvailable, "Schemeshard not available", ctx);
    return Die(ctx);
}

IActor* CreateTxProxyDescribeFlatSchemeReq(const TTxProxyServices &services, const TIntrusivePtr<TTxProxyMon>& txProxyMon) {
    return new TDescribeReq(services, txProxyMon);
}

}
}
