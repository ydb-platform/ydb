#include "console_configs_manager.h"
#include "console_configs_provider.h"

#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/yql/public/issue/protos/issue_severity.pb.h>

namespace NKikimr::NConsole {

using namespace NKikimrConsole;

class TConfigsManager::TTxReplaceYamlConfig : public TTransactionBase<TConfigsManager> {
    template <class T>
    TTxReplaceYamlConfig(TConfigsManager *self,
                         T &ev,
                         bool force)
        : TBase(self)
        , Config(ev->Get()->Record.GetRequest().config())
        , Sender(ev->Sender)
        , UserSID(NACLib::TUserToken(ev->Get()->Record.GetUserToken()).GetUserSID())
        , Force(force)
        , AllowUnknownFields(ev->Get()->Record.GetRequest().allow_unknown_fields())
        , DryRun(ev->Get()->Record.GetRequest().dry_run())
    {
    }

public:
    TTxReplaceYamlConfig(TConfigsManager *self,
                         TEvConsole::TEvReplaceYamlConfigRequest::TPtr &ev)
        : TTxReplaceYamlConfig(self, ev, false)
    {
    }

    TTxReplaceYamlConfig(TConfigsManager *self,
                         TEvConsole::TEvSetYamlConfigRequest::TPtr &ev)
        : TTxReplaceYamlConfig(self, ev, true)
    {
    }

    void DoAudit(TTransactionContext &txc, const TActorContext &ctx)
    {
        auto logData = NKikimrConsole::TLogRecordData{};

        // for backward compatibility in ui
        logData.MutableAction()->AddActions()->MutableModifyConfigItem()->MutableConfigItem();
        logData.AddAffectedKinds(NKikimrConsole::TConfigItem::YamlConfigChangeItem);

        auto& yamlConfigChange = *logData.MutableYamlConfigChange();
        yamlConfigChange.SetOldYamlConfig(Self->YamlConfig);
        yamlConfigChange.SetNewYamlConfig(UpdatedConfig);
        for (auto& [id, config] : Self->VolatileYamlConfigs) {
            auto& oldVolatileConfig = *yamlConfigChange.AddOldVolatileYamlConfigs();
            oldVolatileConfig.SetId(id);
            oldVolatileConfig.SetConfig(config);
        }

        Self->Logger.DbLogData(UserSID, logData, txc, ctx);
    }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override
    {
        NIceDb::TNiceDb db(txc.DB);

        try {
            if (!Force) {
                auto metadata = NYamlConfig::GetMetadata(Config);
                Cluster = metadata.Cluster.value_or(TString("unknown"));
                Version = metadata.Version.value_or(0);
            } else {
               Cluster = Self->ClusterName;
               Version = Self->YamlVersion;
            }

            UpdatedConfig = NYamlConfig::ReplaceMetadata(Config, NYamlConfig::TMetadata{
                    .Version = Version + 1,
                    .Cluster = Cluster,
                });

            bool hasForbiddenUnknown = false;

            TMap<TString, std::pair<TString, TString>> deprecatedFields;
            TMap<TString, std::pair<TString, TString>> unknownFields;

            if (UpdatedConfig != Self->YamlConfig || Self->YamlDropped) {
                Modify = true;

                auto tree = NFyaml::TDocument::Parse(UpdatedConfig);
                auto resolved = NYamlConfig::ResolveAll(tree);

                if (Self->ClusterName != Cluster) {
                    ythrow yexception() << "ClusterName mismatch";
                }

                if (Version != Self->YamlVersion) {
                    ythrow yexception() << "Version mismatch";
                }

                UnknownFieldsCollector = new NYamlConfig::TBasicUnknownFieldsCollector;

                for (auto& [_, config] : resolved.Configs) {
                    auto cfg = NYamlConfig::YamlToProto(
                        config.second,
                        true,
                        true,
                        UnknownFieldsCollector);
                }

                const auto& deprecatedPaths = NKikimrConfig::TAppConfig::GetReservedChildrenPaths();

                for (const auto& [path, info] : UnknownFieldsCollector->GetUnknownKeys()) {
                    if (deprecatedPaths.contains(path)) {
                        deprecatedFields[path] = info;
                    } else {
                        unknownFields[path] = info;
                    }
                }

                hasForbiddenUnknown = !unknownFields.empty() && !AllowUnknownFields;

                if (!DryRun && !hasForbiddenUnknown) {
                    DoAudit(txc, ctx);

                    db.Table<Schema::YamlConfig>().Key(Version + 1)
                        .Update<Schema::YamlConfig::Config>(UpdatedConfig)
                        // set config dropped by default to support rollback to previous versions
                        // where new config layout is not supported
                        // it will lead to ignoring config from new versions
                        .Update<Schema::YamlConfig::Dropped>(true);

                    /* Later we shift this boundary to support rollback and history */
                    db.Table<Schema::YamlConfig>().Key(Version)
                        .Delete();
                }
            }

            auto fillResponse = [&](auto& ev, auto errorLevel){
                for (auto& [path, info] : unknownFields) {
                    auto *issue = ev->Record.AddIssues();
                        issue->set_severity(errorLevel);
                        issue->set_message(TStringBuilder{} << "Unknown key# " << info.first << " in proto# " << info.second << " found in path# " << path);
                }

                for (auto& [path, info] : deprecatedFields) {
                    auto *issue = ev->Record.AddIssues();
                        issue->set_severity(NYql::TSeverityIds::S_WARNING);
                        issue->set_message(TStringBuilder{} << "Deprecated key# " << info.first << " in proto# " << info.second << " found in path# " << path);
                }

                Response = MakeHolder<NActors::IEventHandle>(Sender, ctx.SelfID, ev.Release());
            };


            if (hasForbiddenUnknown) {
                Error = true;
                auto ev = MakeHolder<TEvConsole::TEvGenericError>();
                ev->Record.SetYdbStatus(Ydb::StatusIds::BAD_REQUEST);
                fillResponse(ev, NYql::TSeverityIds::S_ERROR);
            } else if (!Force) {
                auto ev = MakeHolder<TEvConsole::TEvReplaceYamlConfigResponse>();
                fillResponse(ev, NYql::TSeverityIds::S_WARNING);
            } else {
                auto ev = MakeHolder<TEvConsole::TEvSetYamlConfigResponse>();
                fillResponse(ev, NYql::TSeverityIds::S_WARNING);
            }
        } catch (const yexception& ex) {
            Error = true;

            auto ev = MakeHolder<TEvConsole::TEvGenericError>();
            ev->Record.SetYdbStatus(Ydb::StatusIds::BAD_REQUEST);
            auto *issue = ev->Record.AddIssues();
            issue->set_severity(NYql::TSeverityIds::S_ERROR);
            issue->set_message(ex.what());
            Response = MakeHolder<NActors::IEventHandle>(Sender, ctx.SelfID, ev.Release());
        }

        return true;
    }

    void Complete(const TActorContext &ctx) override
    {
        LOG_DEBUG(ctx, NKikimrServices::CMS_CONFIGS, "TTxReplaceYamlConfig Complete");

        ctx.Send(Response.Release());

        if (!Error && Modify && !DryRun) {
            Self->YamlVersion = Version + 1;
            Self->YamlConfig = UpdatedConfig;
            Self->YamlDropped = false;

            Self->VolatileYamlConfigs.clear();

            auto resp = MakeHolder<TConfigsProvider::TEvPrivate::TEvUpdateYamlConfig>(Self->YamlConfig);
            ctx.Send(Self->ConfigsProvider, resp.Release());
        }

        Self->TxProcessor->TxCompleted(this, ctx);
    }

private:
    const TString Config;
    const TActorId Sender;
    const TString UserSID;
    const bool Force = false;
    const bool AllowUnknownFields = false;
    const bool DryRun = false;
    THolder<NActors::IEventHandle> Response;
    bool Error = false;
    bool Modify = false;
    TSimpleSharedPtr<NYamlConfig::TBasicUnknownFieldsCollector> UnknownFieldsCollector = nullptr;
    ui32 Version;
    TString Cluster;
    TString UpdatedConfig;
};

ITransaction *TConfigsManager::CreateTxReplaceYamlConfig(TEvConsole::TEvReplaceYamlConfigRequest::TPtr &ev)
{
    return new TTxReplaceYamlConfig(this, ev);
}

ITransaction *TConfigsManager::CreateTxSetYamlConfig(TEvConsole::TEvSetYamlConfigRequest::TPtr &ev)
{
    return new TTxReplaceYamlConfig(this, ev);
}

} // namespace NKikimr::NConsole
