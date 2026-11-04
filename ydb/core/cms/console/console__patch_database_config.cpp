#include "console_configs_manager.h"
#include "console_impl.h"
#include "console_tenants_manager.h"
#include "console_audit.h"
#include "database_config_patch.h"

#include <ydb/core/base/path.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <yql/essentials/public/issue/protos/issue_severity.pb.h>

namespace NKikimr::NConsole {
namespace {

void ReplyPatchError(const TEvConsole::TEvPatchDatabaseConfigRequest::TPtr& request,
    Ydb::StatusIds::StatusCode status, const TString& reason, const TActorContext& ctx,
    const NKikimrConsole::TDatabaseFeatureFlagsChange& audit = {})
{
    AuditLogPatchDatabaseConfig(request->Get()->Record, audit, status, reason);
    auto response = MakeHolder<TEvConsole::TEvPatchDatabaseConfigResponse>();
    response->Record.SetYdbStatus(status);
    auto* issue = response->Record.AddIssues();
    issue->set_severity(NYql::TSeverityIds::S_ERROR);
    issue->set_message(reason);
    ctx.Send(request->Sender, response.Release(), 0, request->Cookie);
}

class TDatabaseConfigAuthorizationActor : public TActorBootstrapped<TDatabaseConfigAuthorizationActor> {
    using TThis = TDatabaseConfigAuthorizationActor;
public:
    TDatabaseConfigAuthorizationActor(TActorId manager,
        TEvConsole::TEvPatchDatabaseConfigRequest::TPtr request,
        NKikimrConsole::TDatabaseFeatureFlagsChange audit)
        : Manager(manager)
        , Request(std::move(request))
        , Audit(std::move(audit))
    {}

    void Bootstrap(const TActorContext& ctx) {
        const auto& record = Request->Get()->Record;
        auto navigate = std::make_unique<NSchemeCache::TSchemeCacheNavigate>();
        navigate->DatabaseName = record.GetDatabase();
        auto& entry = navigate->ResultSet.emplace_back();
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
        entry.Path = SplitPath(record.GetDatabase());
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(navigate.release()),
            IEventHandle::FlagTrackDelivery);
        auto deadline = ctx.Now() + TDuration::Seconds(30);
        if (record.GetDeadlineUnixMilliSeconds()) {
            deadline = Min(deadline, TInstant::MilliSeconds(record.GetDeadlineUnixMilliSeconds()));
        }
        Schedule(deadline > ctx.Now() ? deadline - ctx.Now() : TDuration::Zero(), new TEvents::TEvWakeup);
        Become(&TThis::StateWork);
    }

private:
    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) {
        const auto& result = *ev->Get()->Request;
        if (result.ErrorCount || result.ResultSet.size() != 1 || !result.ResultSet.front().Self
            || !result.ResultSet.front().DomainInfo)
        {
            ReplyPatchError(Request, Ydb::StatusIds::SCHEME_ERROR, "Cannot resolve the database", ctx, Audit);
        } else {
            const NACLib::TUserToken token(Request->Get()->Record.GetUserToken());
            if (!IsDatabaseAdministrator(&token, result.ResultSet.front().Self->Info.GetOwner())) {
                ReplyPatchError(Request, Ydb::StatusIds::UNAUTHORIZED,
                    "User is not a database administrator", ctx, Audit);
            } else {
                Send(Manager, new TConfigsManager::TEvPrivate::TEvPatchDatabaseConfigAuthorized(
                    std::move(Request), result.ResultSet.front().DomainInfo->DomainKey));
            }
        }
        PassAway();
    }

    void Timeout(const TActorContext& ctx) {
        ReplyPatchError(Request, Ydb::StatusIds::TIMEOUT, "Timed out authorizing database configuration change", ctx, Audit);
        PassAway();
    }

    void Handle(TEvents::TEvUndelivered::TPtr&, const TActorContext& ctx) {
        ReplyPatchError(Request, Ydb::StatusIds::UNAVAILABLE, "Database scheme cache is unavailable", ctx, Audit);
        PassAway();
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            HFunc(TEvents::TEvUndelivered, Handle);
            CFunc(TEvents::TSystem::Wakeup, Timeout);
        }
    }

    const TActorId Manager;
    TEvConsole::TEvPatchDatabaseConfigRequest::TPtr Request;
    const NKikimrConsole::TDatabaseFeatureFlagsChange Audit;
};

} // namespace

class TConfigsManager::TTxPatchDatabaseConfig : public TTransactionBase<TConfigsManager> {
public:
    TTxPatchDatabaseConfig(TConfigsManager* self, TEvConsole::TEvPatchDatabaseConfigRequest::TPtr& request,
        TPathId expectedDomainId)
        : TBase(self)
        , Request(std::move(request))
        , ExpectedDomainId(expectedDomainId)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        const auto& request = Request->Get()->Record;
        if (const auto it = Self->DatabaseYamlConfigs.find(request.GetDatabase()); it != Self->DatabaseYamlConfigs.end()) {
            OldConfig = it->second.Config;
            Version = it->second.Version;
        }
        try {
            Patch.Audit = DescribeDatabaseFeatureFlagPatch(OldConfig, request);
        } catch (const std::exception&) {
            // A malformed existing document must not hide the original rejection.
            // The requested changes are included in the audit independently.
        }
        Patch.Audit.SetVersion(Version);
        if (!IsAdministrator(AppData(ctx), request.GetUserToken())
            && (!ExpectedDomainId || !AppData(ctx)->FeatureFlags.GetEnableDatabaseAdmin()))
        {
            return Error(Ydb::StatusIds::UNAUTHORIZED, "Database administrator authorization is required");
        }
        if (request.GetDeadlineUnixMilliSeconds()
            && ctx.Now() >= TInstant::MilliSeconds(request.GetDeadlineUnixMilliSeconds()))
        {
            return Error(Ydb::StatusIds::TIMEOUT, "Database configuration change deadline has expired");
        }
        if (!AppData(ctx)->FeatureFlags.GetEnableAlterDatabase()) {
            return Error(Ydb::StatusIds::PRECONDITION_FAILED, "ALTER DATABASE is disabled");
        }
        if (!AppData(ctx)->FeatureFlags.GetDatabaseYamlConfigAllowed()
            || !AppData(ctx)->YamlConfigEnabled
            || Self->MainYamlConfig.empty() || Self->YamlDropped)
        {
            return Error(Ydb::StatusIds::PRECONDITION_FAILED, "Database YAML configuration must be enabled");
        }
        const auto tenant = Self->Self.TenantsManager->GetTenant(request.GetDatabase());
        if (!tenant) {
            return Error(Ydb::StatusIds::SCHEME_ERROR, "Target must be an existing tenant database");
        }
        if (ExpectedDomainId && tenant->DomainId != ExpectedDomainId) {
            return Error(Ydb::StatusIds::PRECONDITION_FAILED, "Database changed during authorization; retry the request");
        }
        if (tenant->SharedDomainId || tenant->AreResourcesShared) {
            return Error(Ydb::StatusIds::PRECONDITION_FAILED,
                "SQL feature flags require a database with dedicated resources");
        }

        try {
            Patch = PatchDatabaseFeatureFlags(OldConfig, request);
            Patch.Audit.SetVersion(Version);
            if (Patch.Modified) {
                if (Version == Max<ui32>()) {
                    return Error(Ydb::StatusIds::PRECONDITION_FAILED, "Database configuration version is exhausted");
                }
                TUpdateDatabaseConfigOpContext opCtx;
                Self->ReplaceDatabaseConfigMetadata(Patch.Config, false, opCtx);
                Self->ValidateDatabaseConfig(opCtx);
                if (opCtx.Error) {
                    return Error(Ydb::StatusIds::BAD_REQUEST, *opCtx.Error);
                }
                // Unknown fields already present in the stored YAML are retained. All
                // newly introduced keys came from the explicit SQL flag allowlist.
                Patch.Config = std::move(opCtx.UpdatedConfig);
                ++Version;
                Patch.Audit.SetVersion(Version);
                NIceDb::TNiceDb db(txc.DB);
                db.Table<Schema::DatabaseYamlConfigs>().Key(request.GetDatabase(), Version)
                    .Update<Schema::DatabaseYamlConfigs::Config>(Patch.Config);
                db.Table<Schema::DatabaseYamlConfigs>().Key(request.GetDatabase(), Version - 1).Delete();
            }

            NKikimrConsole::TLogRecordData history;
            history.AddAffectedKinds(NKikimrConsole::TConfigItem::DatabaseYamlConfigChangeItem);
            history.MutableDatabaseFeatureFlagsChange()->CopyFrom(Patch.Audit);
            auto* configChange = history.MutableDatabaseYamlConfigChange();
            configChange->SetDatabase(request.GetDatabase());
            configChange->SetOldYamlConfig(OldConfig);
            configChange->SetNewYamlConfig(Patch.Config);
            const NACLib::TUserToken token(request.GetUserToken());
            Self->Logger.DbLogData(token.GetUserSID(), history, txc, ctx);
        } catch (const std::exception& error) {
            return Error(Ydb::StatusIds::BAD_REQUEST, error.what());
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        const auto& request = Request->Get()->Record;
        if (Status == Ydb::StatusIds::SUCCESS && Patch.Modified) {
            Self->DatabaseYamlConfigs[request.GetDatabase()] = TDatabaseYamlConfig{
                .Config = Patch.Config,
                .Version = Version,
            };
            ctx.Send(Self->ConfigsProvider, new TConfigsProvider::TEvPrivate::TEvUpdateYamlConfig(
                Self->MainYamlConfig, Self->DatabaseYamlConfigs, Self->VolatileYamlConfigs, request.GetDatabase()));
        }
        if (Status != Ydb::StatusIds::SUCCESS) {
            Patch.Audit.MutableNewOverrides()->CopyFrom(Patch.Audit.GetOldOverrides());
        }
        AuditLogPatchDatabaseConfig(request, Patch.Audit, Status, Reason);
        auto response = MakeHolder<TEvConsole::TEvPatchDatabaseConfigResponse>();
        response->Record.SetYdbStatus(Status);
        response->Record.SetVersion(Version);
        if (!Reason.empty()) {
            auto* issue = response->Record.AddIssues();
            issue->set_severity(NYql::TSeverityIds::S_ERROR);
            issue->set_message(Reason);
        }
        ctx.Send(Request->Sender, response.Release(), 0, Request->Cookie);
        Self->TxProcessor->TxCompleted(this, ctx);
    }

private:
    bool Error(Ydb::StatusIds::StatusCode status, const TString& reason) {
        Status = status;
        Reason = reason;
        return true;
    }

    TEvConsole::TEvPatchDatabaseConfigRequest::TPtr Request;
    const TPathId ExpectedDomainId;
    Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::SUCCESS;
    TString Reason;
    TString OldConfig;
    TDatabaseConfigPatch Patch;
    ui32 Version = 0;
};

ITransaction* TConfigsManager::CreateTxPatchDatabaseConfig(TEvConsole::TEvPatchDatabaseConfigRequest::TPtr& ev,
    TPathId expectedDomainId)
{
    return new TTxPatchDatabaseConfig(this, ev, expectedDomainId);
}

void TConfigsManager::Handle(TEvConsole::TEvPatchDatabaseConfigRequest::TPtr& ev, const TActorContext& ctx) {
    auto& request = ev->Get()->Record;
    NACLibProto::TUserToken tokenProto;
    if (!tokenProto.ParseFromString(request.GetUserToken())) {
        return ReplyPatchError(ev, Ydb::StatusIds::UNAUTHORIZED, "Invalid user token", ctx);
    }
    if (request.GetDatabase().empty() || !request.GetDatabase().StartsWith('/')) {
        return ReplyPatchError(ev, Ydb::StatusIds::BAD_REQUEST, "An absolute database path is required", ctx);
    }
    request.SetDatabase(CanonizePath(request.GetDatabase()));
    NKikimrConsole::TDatabaseFeatureFlagsChange audit;
    if (!IsAdministrator(AppData(ctx), request.GetUserToken())) {
        const auto it = DatabaseYamlConfigs.find(request.GetDatabase());
        try {
            audit = DescribeDatabaseFeatureFlagPatch(it == DatabaseYamlConfigs.end() ? TString() : it->second.Config, request);
        } catch (const std::exception&) {
            // Audit the request even if its existing YAML cannot be inspected.
        }
        audit.SetVersion(it == DatabaseYamlConfigs.end() ? 0 : it->second.Version);
    }
    if (IsAdministrator(AppData(ctx), request.GetUserToken())) {
        TxProcessor->ProcessTx(CreateTxPatchDatabaseConfig(ev), ctx);
    } else if (!AppData(ctx)->FeatureFlags.GetEnableDatabaseAdmin()) {
        ReplyPatchError(ev, Ydb::StatusIds::UNAUTHORIZED, "User is not a cluster administrator", ctx, audit);
    } else {
        ctx.Register(new TDatabaseConfigAuthorizationActor(SelfId(), std::move(ev), std::move(audit)));
    }
}

void TConfigsManager::Handle(TEvPrivate::TEvPatchDatabaseConfigAuthorized::TPtr& ev, const TActorContext& ctx) {
    TxProcessor->ProcessTx(CreateTxPatchDatabaseConfig(ev->Get()->Request, ev->Get()->DomainId), ctx);
}

} // namespace NKikimr::NConsole
