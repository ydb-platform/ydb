#include "service_table.h"
#include <ydb/core/grpc_services/base/base.h>
#include "rpc_calls.h"
#include "rpc_scheme_base.h"
#include "rpc_common/rpc_common.h"
#include "table_settings.h"

#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/protos/console_config.pb.h>
#include <ydb/core/ydb_convert/column_families.h>
#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/core/ydb_convert/table_profiles.h>

namespace NKikimr {
namespace NGRpcService {

using namespace NSchemeShard;
using namespace NActors;
using namespace NConsole;
using namespace Ydb;
using namespace Ydb::Table;

using TEvCreateTableRequest = TGrpcRequestOperationCall<Ydb::Table::CreateTableRequest,
    Ydb::Table::CreateTableResponse>;

class TCreateTableRPC : public TRpcSchemeRequestActor<TCreateTableRPC, TEvCreateTableRequest> {
    using TBase = TRpcSchemeRequestActor<TCreateTableRPC, TEvCreateTableRequest>;

public:
    TCreateTableRPC(IRequestOpCtx* msg)
        : TBase(msg) {}

    void Bootstrap(const TActorContext &ctx) {
        TBase::Bootstrap(ctx);

        SendConfigRequest(ctx);
        ctx.Schedule(TDuration::Seconds(15), new TEvents::TEvWakeup(WakeupTagGetConfig));
        Become(&TCreateTableRPC::StateGetConfig);
    }

private:
    void StateGetConfig(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvConfigsDispatcher::TEvGetConfigResponse, Handle);
            HFunc(TEvents::TEvUndelivered, Handle);
            HFunc(TEvents::TEvWakeup, HandleWakeup);
            default: TBase::StateFuncBase(ev);
        }
    }

    void StateWork(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            default: TBase::StateWork(ev);
        }
    }

    void Handle(TEvents::TEvUndelivered::TPtr &/*ev*/, const TActorContext &ctx)
    {
        LOG_CRIT_S(ctx, NKikimrServices::GRPC_PROXY,
                   "TCreateTableRPC: cannot deliver config request to Configs Dispatcher"
                   " (empty default profile is available only)");
        SendProposeRequest(ctx);
        Become(&TCreateTableRPC::StateWork);
    }

    void Handle(TEvConfigsDispatcher::TEvGetConfigResponse::TPtr &ev, const TActorContext &ctx) {
        auto &config = ev->Get()->Config->GetTableProfilesConfig();
        Profiles.Load(config);

        SendProposeRequest(ctx);
        Become(&TCreateTableRPC::StateWork);
    }

    void HandleWakeup(TEvents::TEvWakeup::TPtr &ev, const TActorContext &ctx) {
        switch (ev->Get()->Tag) {
            case WakeupTagGetConfig: {
                LOG_CRIT_S(ctx, NKikimrServices::GRPC_PROXY, "TCreateTableRPC: cannot get table profiles (timeout)");
                NYql::TIssues issues;
                issues.AddIssue(NYql::TIssue("Tables profiles config not available."));
                return Reply(StatusIds::UNAVAILABLE, issues, ctx);
            }
            default:
                TBase::HandleWakeup(ev, ctx);
        }
    }

    void SendConfigRequest(const TActorContext &ctx) {
        ui32 configKind = (ui32)NKikimrConsole::TConfigItem::TableProfilesConfigItem;
        ctx.Send(MakeConfigsDispatcherID(ctx.SelfID.NodeId()),
                 new TEvConfigsDispatcher::TEvGetConfigRequest(configKind),
                 IEventHandle::FlagTrackDelivery);
    }

    // Mutually exclusive settings
    void MEWarning(const TString& settingName) {
        Request_->RaiseIssue(
            NYql::TIssue(TStringBuilder() << "Table profile and " << settingName
                << " are set. They are mutually exclusive. Use either one of them.")
            .SetCode(NKikimrIssues::TIssuesIds::WARNING, NYql::TSeverityIds::S_WARNING)
        );
    }

    bool MakeCreateColumnTable(const Ydb::Table::CreateTableRequest& req, const TString& tableName,
                            NKikimrSchemeOp::TModifyScheme& schemaProto,
                            StatusIds::StatusCode& code, NYql::TIssues& issues) {
        schemaProto.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreateColumnTable);
        auto tableDesc = schemaProto.MutableCreateColumnTable();
        tableDesc->SetName(tableName);

        auto schema = tableDesc->MutableSchema();
        schema->SetEngine(NKikimrSchemeOp::EColumnTableEngine::COLUMN_ENGINE_REPLACING_TIMESERIES);

        TString error;
        if (!FillColumnDescription(*tableDesc, req.columns(), code, error)) {
            issues.AddIssue(NYql::TIssue(error));
            return false;
        }

        schema->MutableKeyColumnNames()->CopyFrom(req.primary_key());

        auto& hashSharding = *tableDesc->MutableSharding()->MutableHashSharding();
        hashSharding.SetFunction(NKikimrSchemeOp::TColumnTableSharding::THashSharding::HASH_FUNCTION_MODULO_N);

        if (req.has_partitioning_settings()) {
            auto& partitioningSettings = req.partitioning_settings();
            hashSharding.MutableColumns()->CopyFrom(partitioningSettings.partition_by());
            if (partitioningSettings.min_partitions_count()) {
                tableDesc->SetColumnShardCount(partitioningSettings.min_partitions_count());
            }
        }

        if (req.has_ttl_settings()) {
            if (!FillTtlSettings(*tableDesc->MutableTtlSettings()->MutableEnabled(), req.ttl_settings(), code, error)) {
                issues.AddIssue(NYql::TIssue(error));
                return false;
            }
        }
        tableDesc->MutableTtlSettings()->SetUseTiering(req.tiering());

        return true;
    }

    void SendProposeRequest(const TActorContext &ctx) {
        const auto req = GetProtoRequest();
        std::pair<TString, TString> pathPair;
        try {
            pathPair = SplitPath(Request_->GetDatabaseName(), req->path());
        } catch (const std::exception& ex) {
            Request_->RaiseIssue(NYql::ExceptionToIssue(ex));
            return Reply(StatusIds::BAD_REQUEST, ctx);
        }

        const auto& workingDir = pathPair.first;
        const auto& name = pathPair.second;
        if (!req->columnsSize()) {
            auto issue = NYql::TIssue("At least one column shoult be in table");
            Request_->RaiseIssue(issue);
            return Reply(StatusIds::BAD_REQUEST, ctx);
        }

        if (!req->primary_keySize()) {
            auto issue = NYql::TIssue("At least one primary key should be specified");
            Request_->RaiseIssue(issue);
            return Reply(StatusIds::BAD_REQUEST, ctx);
        }

        std::unique_ptr<TEvTxUserProxy::TEvProposeTransaction> proposeRequest = CreateProposeTransaction();
        NKikimrTxUserProxy::TEvProposeTransaction& record = proposeRequest->Record;
        NKikimrSchemeOp::TModifyScheme* modifyScheme = record.MutableTransaction()->MutableModifyScheme();
        modifyScheme->SetWorkingDir(workingDir);

        if (req->store_type() == Ydb::Table::StoreType::STORE_TYPE_COLUMN) {
            StatusIds::StatusCode code = StatusIds::SUCCESS;
            NYql::TIssues issues;
            if (MakeCreateColumnTable(*req, name, *modifyScheme, code, issues)) {
                ctx.Send(MakeTxProxyID(), proposeRequest.release());
            } else {
                Reply(code, issues, ctx);
            }
            return;
        }

        StatusIds::StatusCode code = StatusIds::SUCCESS;
        TString error;

        bool hasSerial = false;
        for (const auto& column : req->columns()) {
            switch (column.default_value_case()) {
                case Ydb::Table::ColumnMeta::kFromSequence: {
                    auto* seqDesc = modifyScheme->MutableCreateIndexedTable()->MutableSequenceDescription()->Add();
                    if (!FillSequenceDescription(*seqDesc, column.from_sequence(), code, error)) {
                        NYql::TIssues issues;
                        issues.AddIssue(NYql::TIssue(error));
                        return Reply(code, issues, ctx);
                    }
                    hasSerial = true;
                    break;
                }
                default: break;
            }
        }

        NKikimrSchemeOp::TTableDescription* tableDesc = nullptr;
        if (req->indexesSize() || hasSerial) {
            modifyScheme->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreateIndexedTable);
            tableDesc = modifyScheme->MutableCreateIndexedTable()->MutableTableDescription();
        } else {
            modifyScheme->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreateTable);
            tableDesc = modifyScheme->MutableCreateTable();
        }

        tableDesc->SetName(name);

        if (!FillColumnDescription(*tableDesc, req->columns(), code, error)) {
            NYql::TIssues issues;
            issues.AddIssue(NYql::TIssue(error));
            return Reply(code, issues, ctx);
        }

        tableDesc->MutableKeyColumnNames()->CopyFrom(req->primary_key());

        if (!FillIndexDescription(*modifyScheme->MutableCreateIndexedTable(), *req, code, error)) {
            NYql::TIssues issues;
            issues.AddIssue(NYql::TIssue(error));
            return Reply(code, issues, ctx);
        }

        bool tableProfileSet = false;
        if (req->has_profile()) {
            const auto& profile = req->profile();
            tableProfileSet = profile.preset_name() || profile.has_compaction_policy() || profile.has_execution_policy()
                || profile.has_partitioning_policy() || profile.has_storage_policy() || profile.has_replication_policy()
                || profile.has_caching_policy();
        }

        if (!Profiles.ApplyTableProfile(req->profile(), *tableDesc, code, error)) {
            NYql::TIssues issues;
            issues.AddIssue(NYql::TIssue(error));
            return Reply(code, issues, ctx);
        }

        TColumnFamilyManager families(tableDesc->MutablePartitionConfig());

        // Apply storage settings to the default column family
        if (req->has_storage_settings()) {
            if (tableProfileSet) {
                MEWarning("StorageSettings");
            }
            if (!families.ApplyStorageSettings(req->storage_settings(), &code, &error)) {
                NYql::TIssues issues;
                issues.AddIssue(NYql::TIssue(error));
                return Reply(code, issues, ctx);
            }
        }

        if (tableProfileSet && req->column_familiesSize()) {
            MEWarning("ColumnFamilies");
        }
        for (const auto& familySettings : req->column_families()) {
            if (!families.ApplyFamilySettings(familySettings, &code, &error)) {
                NYql::TIssues issues;
                issues.AddIssue(NYql::TIssue(error));
                return Reply(code, issues, ctx);
            }
        }

        if (families.Modified && !families.ValidateColumnFamilies(&code, &error)) {
            NYql::TIssues issues;
            issues.AddIssue(NYql::TIssue(error));
            return Reply(code, issues, ctx);
        }

        // Attributes
        for (auto [key, value] : req->attributes()) {
            auto& attr = *modifyScheme->MutableAlterUserAttributes()->AddUserAttributes();
            attr.SetKey(key);
            attr.SetValue(value);
        }

        TList<TString> warnings;
        if (!FillCreateTableSettingsDesc(*tableDesc, *req, Profiles, code, error, warnings)) {
            NYql::TIssues issues;
            issues.AddIssue(NYql::TIssue(error));
            return Reply(code, issues, ctx);
        }
        for (const auto& warning : warnings) {
            Request_->RaiseIssue(
                NYql::TIssue(warning)
                .SetCode(NKikimrIssues::TIssuesIds::WARNING, NYql::TSeverityIds::S_WARNING)
            );
        }

        ctx.Send(MakeTxProxyID(), proposeRequest.release());
    }

private:
    TTableProfiles Profiles;
};

void DoCreateTableRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TCreateTableRPC(p.release()));
}

template<>
IActor* TEvCreateTableRequest::CreateRpcActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
    return new TCreateTableRPC(msg);
}

} // namespace NGRpcService
} // namespace NKikimr
