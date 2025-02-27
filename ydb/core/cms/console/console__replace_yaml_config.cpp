#include "console_configs_manager.h"
#include "console_configs_provider.h"
#include "console_audit.h"

#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/config/validation/validators.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/yaml_config/yaml_config.h>
#include <yql/essentials/public/issue/protos/issue_severity.pb.h>
#include <ydb/core/base/appdata.h>

namespace NKikimr::NConsole {

using namespace NKikimrConsole;

class TConfigsManager::TTxReplaceYamlConfigBase
    : public TTransactionBase<TConfigsManager>
{
    template <class T>
    TTxReplaceYamlConfigBase(
        TConfigsManager *self,
        T &ev,
        bool force)
            : TBase(self)
            , Config(ev->Get()->Record.GetRequest().config())
            , Peer(ev->Get()->Record.GetPeerName())
            , Sender(ev->Sender)
            , UserToken(ev->Get()->Record.GetUserToken())
            , Force(force)
            , AllowUnknownFields(ev->Get()->Record.GetRequest().allow_unknown_fields())
            , DryRun(ev->Get()->Record.GetRequest().dry_run())
            , IngressDatabase(ev->Get()->Record.HasIngressDatabase() ? TMaybe<TString>{ev->Get()->Record.GetIngressDatabase()} : TMaybe<TString>{})
    {
    }

public:
    TTxReplaceYamlConfigBase(
        TConfigsManager *self,
        TEvConsole::TEvReplaceYamlConfigRequest::TPtr &ev)
            : TTxReplaceYamlConfigBase(self, ev, false)
    {
    }

    TTxReplaceYamlConfigBase(
        TConfigsManager *self,
        TEvConsole::TEvSetYamlConfigRequest::TPtr &ev)
            : TTxReplaceYamlConfigBase(self, ev, true)
    {
    }

    THolder<NActors::IEventHandle> FillResponse(const TUpdateConfigOpBaseContext& opCtx, auto& ev, auto errorLevel, const TActorContext &ctx) {
        for (auto& [path, info] : opCtx.UnknownFields) {
            auto *issue = ev->Record.AddIssues();
            issue->set_severity(errorLevel);
            issue->set_message(TStringBuilder{} << "Unknown key# " << info.first << " in proto# " << info.second << " found in path# " << path);
        }

        for (auto& [path, info] : opCtx.DeprecatedFields) {
            auto *issue = ev->Record.AddIssues();
            issue->set_severity(NYql::TSeverityIds::S_WARNING);
            issue->set_message(TStringBuilder{} << "Deprecated key# " << info.first << " in proto# " << info.second << " found in path# " << path);
        }

        return MakeHolder<NActors::IEventHandle>(Sender, ctx.SelfID, ev.Release());
    }

    void HandleError(const TString& error, const TActorContext& ctx) {
        Error = true;
        auto ev = MakeHolder<TEvConsole::TEvGenericError>();
        ev->Record.SetYdbStatus(Ydb::StatusIds::BAD_REQUEST);
        auto *issue = ev->Record.AddIssues();
        issue->set_severity(NYql::TSeverityIds::S_ERROR);
        issue->set_message(error);
        ErrorReason = error;
        Response = MakeHolder<NActors::IEventHandle>(Sender, ctx.SelfID, ev.Release());
    }

protected:
    const TString Config;
    const TString Peer;
    const TActorId Sender;
    const NACLib::TUserToken UserToken;
    const bool Force = false;
    const bool AllowUnknownFields = false;
    const bool DryRun = false;
    THolder<NActors::IEventHandle> Response;
    bool Error = false;
    TString ErrorReason;
    bool Modify = false;
    TSimpleSharedPtr<NYamlConfig::TBasicUnknownFieldsCollector> UnknownFieldsCollector = nullptr;
    TMaybe<TString> IngressDatabase;
    bool WarnDatabaseBypass = false;
};

class TConfigsManager::TTxReplaceMainYamlConfig
    : public TConfigsManager::TTxReplaceYamlConfigBase
{
public:
     TTxReplaceMainYamlConfig(
        TConfigsManager *self,
        TEvConsole::TEvReplaceYamlConfigRequest::TPtr &ev)
            : TTxReplaceYamlConfigBase(self, ev)
    {
    }

    TTxReplaceMainYamlConfig(
        TConfigsManager *self,
        TEvConsole::TEvSetYamlConfigRequest::TPtr &ev)
            : TTxReplaceYamlConfigBase(self, ev)
    {
    }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override
    {
        NIceDb::TNiceDb db(txc.DB);

        TUpdateConfigOpContext opCtx;
        Self->ReplaceMainConfigMetadata(Config, false, opCtx);
        Self->ValidateMainConfig(opCtx);

        bool hasForbiddenUnknown = !opCtx.UnknownFields.empty() && !AllowUnknownFields;
        if (opCtx.Error) {
            HandleError(opCtx.Error.value(), ctx);
            return true;
        }

        try {
            Version = opCtx.Version;
            UpdatedMainConfig = opCtx.UpdatedConfig;
            Cluster = opCtx.Cluster;
            Modify = opCtx.UpdatedConfig != Self->MainYamlConfig || Self->YamlDropped;

            if (IngressDatabase) {
                WarnDatabaseBypass = true;
            }

            if (!DryRun && !hasForbiddenUnknown) {
                DoInternalAudit(txc, ctx);

                db.Table<Schema::YamlConfig>().Key(Version + 1)
                    .Update<Schema::YamlConfig::Config>(UpdatedMainConfig)
                    // set config dropped by default to support rollback to previous versions
                    // where new config layout is not supported
                    // it will lead to ignoring config from new versions
                    .Update<Schema::YamlConfig::Dropped>(true);

                /* Later we shift this boundary to support rollback and history */
                db.Table<Schema::YamlConfig>().Key(Version)
                    .Delete();
            }

            if (hasForbiddenUnknown) {
                Error = true;
                auto ev = MakeHolder<TEvConsole::TEvGenericError>();
                ev->Record.SetYdbStatus(Ydb::StatusIds::BAD_REQUEST);
                ErrorReason = "Unknown keys in config.";
                Response = FillResponse(opCtx, ev, NYql::TSeverityIds::S_ERROR, ctx);
            } else if (!Force) {
                auto ev = MakeHolder<TEvConsole::TEvReplaceYamlConfigResponse>();
                Response = FillResponse(opCtx, ev, NYql::TSeverityIds::S_WARNING, ctx);
            } else {
                auto ev = MakeHolder<TEvConsole::TEvSetYamlConfigResponse>();
                Response = FillResponse(opCtx, ev, NYql::TSeverityIds::S_WARNING, ctx);
            }
        }
        catch (const yexception& ex) {
            HandleError(ex.what(), ctx);
        }

        return true;
    }

    void Complete(const TActorContext &ctx) noexcept override
    {
        LOG_DEBUG(ctx, NKikimrServices::CMS_CONFIGS, "TTxReplaceMainYamlConfig Complete");

        ctx.Send(Response.Release());

        if (!Error && Modify && !DryRun) {
            AuditLogReplaceConfigTransaction(
                /* peer = */ Peer,
                /* userSID = */ UserToken.GetUserSID(),
                /* sanitizedToken = */ UserToken.GetSanitizedToken(),
                /* oldConfig = */ Self->MainYamlConfig,
                /* newConfig = */ Config,
                /* reason = */ {},
                /* success = */ true);

            Self->YamlVersion = Version + 1;
            Self->MainYamlConfig = UpdatedMainConfig;
            Self->YamlDropped = false;

            Self->VolatileYamlConfigs.clear();

            auto resp = MakeHolder<TConfigsProvider::TEvPrivate::TEvUpdateYamlConfig>(Self->MainYamlConfig, Self->DatabaseYamlConfigs);
            ctx.Send(Self->ConfigsProvider, resp.Release());
        } else if (Error && !DryRun) {
            AuditLogReplaceConfigTransaction(
                /* peer = */ Peer,
                /* userSID = */ UserToken.GetUserSID(),
                /* sanitizedToken = */ UserToken.GetSanitizedToken(),
                /* oldConfig = */ Self->MainYamlConfig,
                /* newConfig = */ Config,
                /* reason = */ ErrorReason,
                /* success = */ false);
        }

        Self->TxProcessor->TxCompleted(this, ctx);
    }

    void DoInternalAudit(TTransactionContext &txc, const TActorContext &ctx)
    {
        auto logData = NKikimrConsole::TLogRecordData{};

        // for backward compatibility in ui
        logData.MutableAction()->AddActions()->MutableModifyConfigItem()->MutableConfigItem();
        logData.AddAffectedKinds(NKikimrConsole::TConfigItem::MainYamlConfigChangeItem);

        auto& yamlConfigChange = *logData.MutableMainYamlConfigChange();
        yamlConfigChange.SetOldYamlConfig(Self->MainYamlConfig);
        yamlConfigChange.SetNewYamlConfig(UpdatedMainConfig);
        for (auto& [id, config] : Self->VolatileYamlConfigs) {
            auto& oldVolatileConfig = *yamlConfigChange.AddOldVolatileYamlConfigs();
            oldVolatileConfig.SetId(id);
            oldVolatileConfig.SetConfig(config);
        }

        Self->Logger.DbLogData(UserToken.GetUserSID(), logData, txc, ctx);
    }

private:
    ui32 Version;
    TString Cluster;
    TString UpdatedMainConfig;
};

class TConfigsManager::TTxReplaceDatabaseYamlConfig
    : public TConfigsManager::TTxReplaceYamlConfigBase
{
public:
     TTxReplaceDatabaseYamlConfig(
        TConfigsManager *self,
        TEvConsole::TEvReplaceYamlConfigRequest::TPtr &ev)
            : TTxReplaceYamlConfigBase(self, ev)
    {
    }

    TTxReplaceDatabaseYamlConfig(
        TConfigsManager *self,
        TEvConsole::TEvSetYamlConfigRequest::TPtr &ev)
            : TTxReplaceYamlConfigBase(self, ev)
    {
    }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override
    {
        NIceDb::TNiceDb db(txc.DB);

        TUpdateDatabaseConfigOpContext opCtx;
        Self->ReplaceDatabaseConfigMetadata(Config, false, opCtx);
        Self->ValidateDatabaseConfig(opCtx);

        bool hasForbiddenUnknown = !opCtx.UnknownFields.empty() && !AllowUnknownFields;
        if (opCtx.Error) {
            HandleError(opCtx.Error.value(), ctx);
            return true;
        }

        try {
            Version = opCtx.Version;
            UpdatedDatabaseConfig = opCtx.UpdatedConfig;
            TargetDatabase = opCtx.TargetDatabase;
            TString currentConfig;
            if (auto it = Self->DatabaseYamlConfigs.find(TargetDatabase); it != Self->DatabaseYamlConfigs.end()) {
                currentConfig = it->second.Config;
            }
            Modify = opCtx.UpdatedConfig != currentConfig;

            if (IngressDatabase != TargetDatabase) {
                WarnDatabaseBypass = true;
            }

            if (!AppData(ctx)->FeatureFlags.GetDatabaseYamlConfigAllowed()) {
                Error = true;
                auto ev = MakeHolder<TEvConsole::TEvGenericError>();

                auto *issue = ev->Record.AddIssues();
                ErrorReason = "Per database config is disabled";
                issue->set_severity(NYql::TSeverityIds::S_ERROR);
                issue->set_message(ErrorReason);
                ev->Record.SetYdbStatus(Ydb::StatusIds::BAD_REQUEST);
                Response = MakeHolder<NActors::IEventHandle>(Sender, ctx.SelfID, ev.Release());
                return true;
            }

            if (!DryRun && !hasForbiddenUnknown) {
                DoInternalAudit(txc, ctx);

                db.Table<Schema::DatabaseYamlConfigs>().Key(TargetDatabase, Version + 1)
                    .Update<Schema::DatabaseYamlConfigs::Config>(Config);

                /* Later we shift this boundary to support rollback and history */
                db.Table<Schema::DatabaseYamlConfigs>().Key(TargetDatabase, Version)
                    .Delete();
            }

            if (hasForbiddenUnknown) {
                Error = true;
                auto ev = MakeHolder<TEvConsole::TEvGenericError>();
                ev->Record.SetYdbStatus(Ydb::StatusIds::BAD_REQUEST);
                ErrorReason = "Unknown keys in config.";
                Response = FillResponse(opCtx, ev, NYql::TSeverityIds::S_ERROR, ctx);
            } else if (!Force) {
                auto ev = MakeHolder<TEvConsole::TEvReplaceYamlConfigResponse>();
                Response = FillResponse(opCtx, ev, NYql::TSeverityIds::S_WARNING, ctx);
            } else {
                auto ev = MakeHolder<TEvConsole::TEvSetYamlConfigResponse>();
                Response = FillResponse(opCtx, ev, NYql::TSeverityIds::S_WARNING, ctx);
            }
        }
        catch (const yexception& ex) {
            HandleError(ex.what(), ctx);
        }

        return true;
    }

    void DoInternalAudit(TTransactionContext &txc, const TActorContext &ctx)
    {
        TString oldConfig;

        if (Self->DatabaseYamlConfigs.contains(TargetDatabase)) {
            oldConfig = Self->DatabaseYamlConfigs[TargetDatabase].Config;
        }

        auto logData = NKikimrConsole::TLogRecordData{};

        // for backward compatibility in ui
        logData.MutableAction()->AddActions()->MutableModifyConfigItem()->MutableConfigItem();
        logData.AddAffectedKinds(NKikimrConsole::TConfigItem::DatabaseYamlConfigChangeItem);

        auto& databaseConfigChange = *logData.MutableDatabaseYamlConfigChange();
        databaseConfigChange.SetDatabase(TargetDatabase);
        databaseConfigChange.SetOldYamlConfig(oldConfig);
        databaseConfigChange.SetNewYamlConfig(UpdatedDatabaseConfig);

        Self->Logger.DbLogData(UserToken.GetUserSID(), logData, txc, ctx);
    }

    void Complete(const TActorContext &ctx) noexcept override
    {
        LOG_DEBUG(ctx, NKikimrServices::CMS_CONFIGS, "TTxReplaceDatabaseYamlConfig Complete");

        ctx.Send(Response.Release());

        TString oldConfig;

        if (Self->DatabaseYamlConfigs.contains(TargetDatabase)) {
            oldConfig = Self->DatabaseYamlConfigs[TargetDatabase].Config;
        }

        if (!Error && Modify && !DryRun) {
            AuditLogReplaceDatabaseConfigTransaction(
                /* peer = */ Peer,
                /* userSID = */ UserToken.GetUserSID(),
                /* sanitizedToken = */ UserToken.GetSanitizedToken(),
                /* database =  */ TargetDatabase,
                /* oldConfig = */ oldConfig,
                /* newConfig = */ Config,
                /* reason = */ {},
                /* success = */ true);

            Self->DatabaseYamlConfigs[TargetDatabase] = TDatabaseYamlConfig {
                .Config = UpdatedDatabaseConfig,
                .Version = Version + 1,
            };

            auto resp = MakeHolder<TConfigsProvider::TEvPrivate::TEvUpdateYamlConfig>(
                Self->MainYamlConfig,
                Self->DatabaseYamlConfigs,
                Self->VolatileYamlConfigs,
                TargetDatabase);

            ctx.Send(Self->ConfigsProvider, resp.Release());
        } else if (Error && !DryRun) {
            AuditLogReplaceDatabaseConfigTransaction(
                /* peer = */ Peer,
                /* userSID = */ UserToken.GetUserSID(),
                /* sanitizedToken = */ UserToken.GetSanitizedToken(),
                /* database =  */ TargetDatabase,
                /* oldConfig = */ oldConfig,
                /* newConfig = */ Config,
                /* reason = */ ErrorReason,
                /* success = */ false);
        }

        Self->TxProcessor->TxCompleted(this, ctx);
    }

private:
    ui32 Version;
    TString TargetDatabase;
    TString UpdatedDatabaseConfig;
};

ITransaction *TConfigsManager::CreateTxReplaceMainYamlConfig(TEvConsole::TEvReplaceYamlConfigRequest::TPtr &ev)
{
    return new TTxReplaceMainYamlConfig(this, ev);
}

ITransaction *TConfigsManager::CreateTxSetMainYamlConfig(TEvConsole::TEvSetYamlConfigRequest::TPtr &ev)
{
    return new TTxReplaceMainYamlConfig(this, ev);
}

ITransaction *TConfigsManager::CreateTxReplaceDatabaseYamlConfig(TEvConsole::TEvReplaceYamlConfigRequest::TPtr &ev)
{
    return new TTxReplaceDatabaseYamlConfig(this, ev);
}

ITransaction *TConfigsManager::CreateTxSetDatabaseYamlConfig(TEvConsole::TEvSetYamlConfigRequest::TPtr &ev)
{
    return new TTxReplaceDatabaseYamlConfig(this, ev);
}

} // namespace NKikimr::NConsole
