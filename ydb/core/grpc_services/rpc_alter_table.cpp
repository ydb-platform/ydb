#include "service_table.h"
#include <ydb/core/grpc_services/base/base.h>

#include "rpc_scheme_base.h"
#include "rpc_common/rpc_common.h"
#include "operation_helpers.h"
#include "table_settings.h"
#include "service_table.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/protos/console_config.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard_build_index.h>
#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/ydb_convert/column_families.h>
#include <ydb/core/ydb_convert/table_description.h>

#include <util/generic/hash_set.h>

#define TXLOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_PROXY, LogPrefix << stream)
#define TXLOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TX_PROXY, LogPrefix << stream)
#define TXLOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::TX_PROXY, LogPrefix << stream)
#define TXLOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::TX_PROXY, LogPrefix << stream)
#define TXLOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::TX_PROXY, LogPrefix << stream)
#define TXLOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::TX_PROXY, LogPrefix << stream)

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;
using namespace NConsole;
using namespace Ydb;

static bool CheckAccess(const NACLib::TUserToken& userToken, const NSchemeCache::TSchemeCacheNavigate* navigate) {
    bool isDatabase = true; // first entry is always database

    using TEntry = NSchemeCache::TSchemeCacheNavigate::TEntry;

    for (const TEntry& entry : navigate->ResultSet) {
        if (!entry.SecurityObject) {
            continue;
        }

        const ui32 access = isDatabase ? NACLib::CreateDirectory | NACLib::CreateTable : NACLib::GenericRead | NACLib::GenericWrite;
        if (!entry.SecurityObject->CheckAccess(access, userToken)) {
            return false;
        }

        isDatabase = false;
    }

    return true;
}

static std::pair<StatusIds::StatusCode, TString> CheckAddIndexDesc(const Ydb::Table::TableIndex& desc) {
    if (!desc.name()) {
        return {StatusIds::BAD_REQUEST, "Index must have a name"};
    }

    if (!desc.index_columns_size()) {
        return {StatusIds::BAD_REQUEST, "At least one column must be specified"};
    }

    if (!desc.data_columns().empty() && !AppData()->FeatureFlags.GetEnableDataColumnForIndexTable()) {
        return {StatusIds::UNSUPPORTED, "Data column feature is not supported yet"};
    }

    return {StatusIds::SUCCESS, ""};
}

using TEvAlterTableRequest = TGrpcRequestOperationCall<Ydb::Table::AlterTableRequest,
    Ydb::Table::AlterTableResponse>;

class TAlterTableRPC : public TRpcSchemeRequestActor<TAlterTableRPC, TEvAlterTableRequest> {
    using TBase = TRpcSchemeRequestActor<TAlterTableRPC, TEvAlterTableRequest>;

    void PassAway() override {
        if (SSPipeClient) {
            NTabletPipe::CloseClient(SelfId(), SSPipeClient);
            SSPipeClient = TActorId();
        }
        IActor::PassAway();
    }

    enum class EOp {
        // columns, column families, storage, ttl
        Common,
        // add indices
        AddIndex,
        // drop indices
        DropIndex,
        // add/alter/drop attributes
        Attribute,
        // add changefeeds
        AddChangefeed,
        // drop changefeeds
        DropChangefeed,
        // rename index
        RenameIndex,
    };

    THashSet<EOp> GetOps() const {
        const auto& req = GetProtoRequest();
        THashSet<EOp> ops;

        if (req->add_columns_size() || req->drop_columns_size() || req->alter_columns_size()
            || req->ttl_action_case() != Ydb::Table::AlterTableRequest::TTL_ACTION_NOT_SET
            || req->tiering_action_case() != Ydb::Table::AlterTableRequest::TIERING_ACTION_NOT_SET
            || req->has_alter_storage_settings()
            || req->add_column_families_size() || req->alter_column_families_size()
            || req->set_compaction_policy() || req->has_alter_partitioning_settings()
            || req->set_key_bloom_filter() != Ydb::FeatureFlag::STATUS_UNSPECIFIED
            || req->has_set_read_replicas_settings()) {
            ops.emplace(EOp::Common);
        }

        if (req->add_indexes_size()) {
            ops.emplace(EOp::AddIndex);
        }

        if (req->drop_indexes_size()) {
            ops.emplace(EOp::DropIndex);
        }

        if (req->add_changefeeds_size()) {
            ops.emplace(EOp::AddChangefeed);
        }

        if (req->drop_changefeeds_size()) {
            ops.emplace(EOp::DropChangefeed);
        }

        if (req->alter_attributes_size()) {
            ops.emplace(EOp::Attribute);
        }

        if (req->rename_indexes_size()) {
            ops.emplace(EOp::RenameIndex);
        }

        return ops;
    }

public:
    TAlterTableRPC(IRequestOpCtx* msg)
        : TBase(msg) {}

    void Bootstrap(const TActorContext &ctx) {
        TBase::Bootstrap(ctx);

        const auto* req = GetProtoRequest();
        if (req->operation_params().has_forget_after() && req->operation_params().operation_mode() != Ydb::Operations::OperationParams::SYNC) {
            return Reply(StatusIds::UNSUPPORTED, "forget_after is not supported for this type of operation", NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
        }

        if (!Request_->GetSerializedToken().empty()) {
            UserToken = MakeHolder<NACLib::TUserToken>(Request_->GetSerializedToken());
        }

        auto ops = GetOps();
        if (!ops) {
            return Reply(StatusIds::BAD_REQUEST, "Empty alter",
                NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
        }
        if (ops.size() != 1) {
            return Reply(StatusIds::UNSUPPORTED, "Mixed alter is unsupported",
                NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
        }

        OpType = *ops.begin();
        switch (OpType) {
        case EOp::Common:
            // Altering table settings will need table profiles
            SendConfigRequest(ctx);
            ctx.Schedule(TDuration::Seconds(15), new TEvents::TEvWakeup(WakeupTagGetConfig));
            Become(&TAlterTableRPC::AlterStateGetConfig);
            return;

        case EOp::AddIndex:
            if (req->add_indexes_size() == 1) {
                const auto& index = req->add_indexes(0);
                auto [status, issues] = CheckAddIndexDesc(index);
                if (status == StatusIds::SUCCESS) {
                    PrepareAlterTableAddIndex();
                } else {
                    return Reply(status, issues, NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
                }
            } else {
                return Reply(StatusIds::UNSUPPORTED, "Only one index can be added by one operation",
                    NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
            }
            break;

        case EOp::DropIndex:
            if (req->drop_indexes_size() == 1) {
                DropIndex(ctx);
            } else {
                return Reply(StatusIds::UNSUPPORTED, "Only one index can be removed by one operation",
                    NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
            }
            break;

        case EOp::AddChangefeed:
            if (!AppData()->FeatureFlags.GetEnableChangefeeds()) {
                return Reply(StatusIds::UNSUPPORTED, "Changefeeds are not supported yet",
                    NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
            }
            if (req->add_changefeeds_size() == 1) {
                AddChangefeed(ctx);
            } else {
                return Reply(StatusIds::UNSUPPORTED, "Only one changefeed can be added by one operation",
                    NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
            }
            break;

        case EOp::DropChangefeed:
            if (req->drop_changefeeds_size() == 1) {
                DropChangefeed(ctx);
            } else {
                return Reply(StatusIds::UNSUPPORTED, "Only one changefeed can be removed by one operation",
                    NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
            }
            break;

        case EOp::Attribute:
            PrepareAlterUserAttrubutes();
            break;

        case EOp::RenameIndex:
            if (req->rename_indexes_size() == 1) {
                RenameIndex(ctx);
            } else {
                return Reply(StatusIds::UNSUPPORTED, "Only one index can be renamed by one operation",
                    NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
            }
            break;
        }

        Become(&TAlterTableRPC::AlterStateWork);
    }

private:
    void AlterStateWork(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
           HFunc(TEvTxUserProxy::TEvAllocateTxIdResult, Handle);
           HFunc(TEvTxUserProxy::TEvGetProxyServicesResponse, Handle);
           HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
           HFunc(NSchemeShard::TEvIndexBuilder::TEvCreateResponse, Handle);
           default: TBase::StateWork(ev);
        }
    }

    void AlterStateGetConfig(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvConfigsDispatcher::TEvGetConfigResponse, Handle);
            HFunc(TEvents::TEvUndelivered, Handle);
            HFunc(TEvents::TEvWakeup, HandleWakeup);
        default: TBase::StateFuncBase(ev);
        }
    }

    void Handle(TEvents::TEvUndelivered::TPtr &/*ev*/, const TActorContext &ctx)
    {
        LOG_CRIT_S(ctx, NKikimrServices::GRPC_PROXY,
            "TAlterTableRPC: cannot deliver config request to Configs Dispatcher"
            " (empty default profile is available only)");
        AlterTable(ctx);
        Become(&TAlterTableRPC::AlterStateWork);
    }

    void Handle(TEvConfigsDispatcher::TEvGetConfigResponse::TPtr &ev, const TActorContext &ctx) {
        auto &config = ev->Get()->Config->GetTableProfilesConfig();
        Profiles.Load(config);

        AlterTable(ctx);
        Become(&TAlterTableRPC::AlterStateWork);
    }

    void HandleWakeup(TEvents::TEvWakeup::TPtr &ev, const TActorContext &ctx) {
        switch (ev->Get()->Tag) {
        case WakeupTagGetConfig: {
            LOG_CRIT_S(ctx, NKikimrServices::GRPC_PROXY, "TAlterTableRPC: cannot get table profiles (timeout)");
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

    void PrepareAlterTableAddIndex() {
        using namespace NTxProxy;
        LogPrefix = TStringBuilder() << "[AlterTableAddIndexOp " << SelfId() << "] ";
        Send(MakeTxProxyID(), new TEvTxUserProxy::TEvAllocateTxId);
    }

    void Handle(TEvTxUserProxy::TEvAllocateTxIdResult::TPtr& ev, const TActorContext& ctx) {
        TXLOG_D("Handle TEvTxUserProxy::TEvAllocateTxIdResult");

        const auto* msg = ev->Get();
        TxId = msg->TxId;
        TxProxyMon = msg->TxProxyMon;
        LogPrefix = TStringBuilder() << "[AlterTableAddIndex " << SelfId() << " TxId# " << TxId << "] ";

        Navigate(msg->Services.SchemeCache, ctx);
    }

    void PrepareAlterUserAttrubutes() {
        using namespace NTxProxy;
        Send(MakeTxProxyID(), new TEvTxUserProxy::TEvGetProxyServicesRequest);
    }

    void Handle(TEvTxUserProxy::TEvGetProxyServicesResponse::TPtr& ev, const TActorContext& ctx) {
        Navigate(ev->Get()->Services.SchemeCache, ctx);
    }

    void Navigate(const TActorId& schemeCache, const TActorContext& ctx) {
        DatabaseName = Request_->GetDatabaseName()
            .GetOrElse(DatabaseFromDomain(AppData()));

        const auto& path = GetProtoRequest()->path();

        const auto paths = NKikimr::SplitPath(path);
        if (paths.empty()) {
            TString error = TStringBuilder() << "Failed to split table path " << path;
            Request_->RaiseIssue(NYql::TIssue(error));
            return Reply(Ydb::StatusIds::BAD_REQUEST, ctx);
        }

        auto ev = CreateNavigateForPath(DatabaseName);
        {
            auto& entry = static_cast<TEvTxProxySchemeCache::TEvNavigateKeySet*>(ev)->Request->ResultSet.emplace_back();
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
            entry.Path = paths;
        }

        Send(schemeCache, ev);
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) {
        TXLOG_D("Handle TEvTxProxySchemeCache::TEvNavigateKeySetResult"
                    << ", errors# " << ev->Get()->Request.Get()->ErrorCount);

        NSchemeCache::TSchemeCacheNavigate* resp = ev->Get()->Request.Get();

        if (resp->ErrorCount > 0 || resp->ResultSet.empty()) {
            TStringBuilder builder;
            builder << "Unable to navigate:";

            for (const auto& entry : resp->ResultSet) {
                if (entry.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
                    builder << " " << JoinPath(entry.Path) << " status: " << entry.Status;
                }
            }

            TString error(builder);
            TXLOG_E(error);
            Request_->RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR, error));
            return Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);
        }

        switch (OpType) {
        case EOp::AddIndex:
            return AlterTableAddIndexOp(resp, ctx);
        case EOp::Attribute:
            Y_VERIFY(!resp->ResultSet.empty());
            return AlterUserAttributes(resp->ResultSet.back().TableId.PathId, ctx);
        default:
            TXLOG_E("Got unexpected cache response");
            return Reply(Ydb::StatusIds::INTERNAL_ERROR, ctx);
        }
    }

    void AlterTableAddIndexOp(const NSchemeCache::TSchemeCacheNavigate* resp, const TActorContext& ctx) {
        if (UserToken && !CheckAccess(*UserToken, resp)) {
            TXLOG_W("Access check failed");
            return Reply(Ydb::StatusIds::UNAUTHORIZED, ctx);
        }

        auto domainInfo = resp->ResultSet.front().DomainInfo;
        if (!domainInfo) {
            TXLOG_E("Got empty domain info");
            return Reply(Ydb::StatusIds::INTERNAL_ERROR, ctx);
        }

        SchemeshardId = domainInfo->ExtractSchemeShard();
        SSPipeClient = CreatePipeClient(SchemeshardId, ctx);
        SendAddIndexOpToSS(ctx);
    }

    void SendAddIndexOpToSS(const TActorContext& ctx) {
        const auto& req = *GetProtoRequest();

        NKikimrIndexBuilder::TIndexBuildSettings settings;
        settings.set_source_path(req.path());
        auto tableIndex = settings.mutable_index();
        tableIndex->CopyFrom(req.add_indexes(0));
        auto ev = new NSchemeShard::TEvIndexBuilder::TEvCreateRequest(TxId, DatabaseName, std::move(settings));

        NTabletPipe::SendData(ctx, SSPipeClient, ev);
    }

    void Handle(NSchemeShard::TEvIndexBuilder::TEvCreateResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& response = ev->Get()->Record;
        const auto status = response.GetStatus();
        auto issuesProto = response.GetIssues();

        auto getDebugIssues = [issuesProto]() {
            NYql::TIssues issues;
            NYql::IssuesFromMessage(issuesProto, issues);
            return issues.ToString();
        };

        TXLOG_D("Handle TEvIndexBuilder::TEvCreateResponse"
            << ", status# " << status
            << ", issues# " << getDebugIssues()
            << ", Id# " << response.GetIndexBuild().GetId());

        if (status == Ydb::StatusIds::SUCCESS) {
            if (GetOperationMode() == Ydb::Operations::OperationParams::SYNC) {
                CreateSSOpSubscriber(SchemeshardId, TxId, DatabaseName, TOpType::BuildIndex, std::move(Request_), ctx);
                Die(ctx);
            } else {
                auto op = response.GetIndexBuild();
                Ydb::Operations::Operation operation;
                operation.set_id(NOperationId::ProtoToString(ToOperationId(op)));
                operation.set_ready(false);
                ReplyOperation(operation);
            }
        } else {
            Reply(status, issuesProto, ctx);
        }
    }

    void DropIndex(const TActorContext &ctx) {
        const auto req = GetProtoRequest();
        std::pair<TString, TString> pathPair;
        try {
            pathPair = SplitPath(req->path());
        } catch (const std::exception&) {
            return ReplyWithStatus(StatusIds::BAD_REQUEST, ctx);
        }

        const auto& workingDir = pathPair.first;
        const auto& name = pathPair.second;

        std::unique_ptr<TEvTxUserProxy::TEvProposeTransaction> proposeRequest = CreateProposeTransaction();
        NKikimrTxUserProxy::TEvProposeTransaction& record = proposeRequest->Record;
        NKikimrSchemeOp::TModifyScheme* modifyScheme = record.MutableTransaction()->MutableModifyScheme();
        modifyScheme->SetWorkingDir(workingDir);
        modifyScheme->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpDropIndex);

        for (const auto& drop : req->drop_indexes()) {
            auto desc = modifyScheme->MutableDropIndex();
            desc->SetIndexName(drop);
            desc->SetTableName(name);
        }

        ctx.Send(MakeTxProxyID(), proposeRequest.release());
    }

    void AddChangefeed(const TActorContext &ctx) {
        const auto req = GetProtoRequest();
        std::pair<TString, TString> pathPair;
        try {
            pathPair = SplitPath(req->path());
        } catch (const std::exception&) {
            return ReplyWithStatus(StatusIds::BAD_REQUEST, ctx);
        }

        const auto& workingDir = pathPair.first;
        const auto& name = pathPair.second;

        std::unique_ptr<TEvTxUserProxy::TEvProposeTransaction> proposeRequest = CreateProposeTransaction();
        NKikimrTxUserProxy::TEvProposeTransaction& record = proposeRequest->Record;
        NKikimrSchemeOp::TModifyScheme* modifyScheme = record.MutableTransaction()->MutableModifyScheme();
        modifyScheme->SetWorkingDir(workingDir);
        modifyScheme->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreateCdcStream);

        for (const auto& add : req->add_changefeeds()) {
            auto op = modifyScheme->MutableCreateCdcStream();
            op->SetTableName(name);
            if (add.has_retention_period()) {
                op->SetRetentionPeriodSeconds(add.retention_period().seconds());
            }

            StatusIds::StatusCode code;
            TString error;
            if (!FillChangefeedDescription(*op->MutableStreamDescription(), add, code, error)) {
                NYql::TIssues issues;
                issues.AddIssue(NYql::TIssue(error));
                return Reply(code, issues, ctx);
            }
        }

        ctx.Send(MakeTxProxyID(), proposeRequest.release());
    }

    void DropChangefeed(const TActorContext &ctx) {
        const auto req = GetProtoRequest();
        std::pair<TString, TString> pathPair;
        try {
            pathPair = SplitPath(req->path());
        } catch (const std::exception&) {
            return ReplyWithStatus(StatusIds::BAD_REQUEST, ctx);
        }

        const auto& workingDir = pathPair.first;
        const auto& name = pathPair.second;

        std::unique_ptr<TEvTxUserProxy::TEvProposeTransaction> proposeRequest = CreateProposeTransaction();
        NKikimrTxUserProxy::TEvProposeTransaction& record = proposeRequest->Record;
        NKikimrSchemeOp::TModifyScheme* modifyScheme = record.MutableTransaction()->MutableModifyScheme();
        modifyScheme->SetWorkingDir(workingDir);
        modifyScheme->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpDropCdcStream);

        for (const auto& drop : req->drop_changefeeds()) {
            auto op = modifyScheme->MutableDropCdcStream();
            op->SetStreamName(drop);
            op->SetTableName(name);
        }

        ctx.Send(MakeTxProxyID(), proposeRequest.release());
    }

    void AlterTable(const TActorContext &ctx) {
        const auto req = GetProtoRequest();
        std::pair<TString, TString> pathPair;
        try {
            pathPair = SplitPath(req->path());
        } catch (const std::exception&) {
            return ReplyWithStatus(StatusIds::BAD_REQUEST, ctx);
        }

        const auto& workingDir = pathPair.first;
        const auto& name = pathPair.second;

        std::unique_ptr<TEvTxUserProxy::TEvProposeTransaction> proposeRequest = CreateProposeTransaction();
        NKikimrTxUserProxy::TEvProposeTransaction& record = proposeRequest->Record;
        NKikimrSchemeOp::TModifyScheme* modifyScheme = record.MutableTransaction()->MutableModifyScheme();
        modifyScheme->SetWorkingDir(workingDir);
        modifyScheme->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpAlterTable);

        auto desc = modifyScheme->MutableAlterTable();
        desc->SetName(name);

        for (const auto& drop : req->drop_columns()) {
            desc->AddDropColumns()->SetName(drop);
        }

        StatusIds::StatusCode code = StatusIds::SUCCESS;
        TString error;

        if (!FillColumnDescription(*desc, req->add_columns(), code, error)) {
            NYql::TIssues issues;
            issues.AddIssue(NYql::TIssue(error));
            return Reply(code, issues, ctx);
        }

        for (const auto& alter : req->alter_columns()) {
            auto column = desc->AddColumns();
            column->SetName(alter.name());
            if (!alter.family().empty()) {
                column->SetFamilyName(alter.family());
            }
        }

        bool hadPartitionConfig = desc->HasPartitionConfig();
        TColumnFamilyManager families(desc->MutablePartitionConfig());

        // Apply storage settings to the default column family
        if (req->has_alter_storage_settings()) {
            Ydb::StatusIds::StatusCode code;
            TString error;
            if (!families.ApplyStorageSettings(req->alter_storage_settings(), &code, &error)) {
                NYql::TIssues issues;
                issues.AddIssue(NYql::TIssue(error));
                return Reply(code, issues, ctx);
            }
        }

        for (const auto& familySettings : req->add_column_families()) {
            Ydb::StatusIds::StatusCode code;
            TString error;
            if (!families.ApplyFamilySettings(familySettings, &code, &error)) {
                NYql::TIssues issues;
                issues.AddIssue(NYql::TIssue(error));
                return Reply(code, issues, ctx);
            }
        }

        for (const auto& familySettings : req->alter_column_families()) {
            Ydb::StatusIds::StatusCode code;
            TString error;
            if (!families.ApplyFamilySettings(familySettings, &code, &error)) {
                NYql::TIssues issues;
                issues.AddIssue(NYql::TIssue(error));
                return Reply(code, issues, ctx);
            }
        }

        // Avoid altering partition config unless we changed something
        if (!families.Modified && !hadPartitionConfig) {
            desc->ClearPartitionConfig();
        }

        if (!FillAlterTableSettingsDesc(*desc, *req, Profiles, code, error, AppData())) {
            NYql::TIssues issues;
            issues.AddIssue(NYql::TIssue(error));
            return Reply(code, issues, ctx);
        }

        ctx.Send(MakeTxProxyID(), proposeRequest.release());
    }

    void AlterUserAttributes(const TPathId& pathId, const TActorContext &ctx) {
        const auto req = GetProtoRequest();

        std::pair<TString, TString> pathPair;
        try {
            pathPair = SplitPath(req->path());
        } catch (const std::exception&) {
            return ReplyWithStatus(StatusIds::BAD_REQUEST, ctx);
        }

        const auto& workingDir = pathPair.first;
        const auto& name = pathPair.second;

        std::unique_ptr<TEvTxUserProxy::TEvProposeTransaction> proposeRequest = CreateProposeTransaction();
        auto& record = proposeRequest->Record;
        auto& modifyScheme = *record.MutableTransaction()->MutableModifyScheme();

        modifyScheme.SetWorkingDir(workingDir);
        modifyScheme.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpAlterUserAttributes);
        modifyScheme.AddApplyIf()->SetPathId(pathId.LocalPathId);

        auto& alter = *modifyScheme.MutableAlterUserAttributes();
        alter.SetPathName(name);

        for (auto [key, value] : req->alter_attributes()) {
            auto& attr = *alter.AddUserAttributes();
            attr.SetKey(key);
            if (value) {
                attr.SetValue(value);
            }
        }

        ctx.Send(MakeTxProxyID(), proposeRequest.release());
    }

    void RenameIndex(const TActorContext &ctx) {
        const auto req = GetProtoRequest();

        std::pair<TString, TString> pathPair;
        try {
            pathPair = SplitPath(req->path());
        } catch (const std::exception&) {
            return ReplyWithStatus(StatusIds::BAD_REQUEST, ctx);
        }

        const auto& workingDir = pathPair.first;

        std::unique_ptr<TEvTxUserProxy::TEvProposeTransaction> proposeRequest = CreateProposeTransaction();
        auto& record = proposeRequest->Record;
        auto& modifyScheme = *record.MutableTransaction()->MutableModifyScheme();

        modifyScheme.SetWorkingDir(workingDir);
        modifyScheme.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpMoveIndex);

        auto& alter = *modifyScheme.MutableMoveIndex();
        alter.SetTablePath(req->path());
        alter.SetSrcPath(req->rename_indexes(0).source_name());
        alter.SetDstPath(req->rename_indexes(0).destination_name());
        alter.SetAllowOverwrite(req->rename_indexes(0).replace_destination());

        ctx.Send(MakeTxProxyID(), proposeRequest.release());
    }

    void ReplyWithStatus(StatusIds::StatusCode status,
                         const TActorContext &ctx) {
        Request_->ReplyWithYdbStatus(status);
        Die(ctx);
    }

    ui64 TxId = 0;
    ui64 SchemeshardId = 0;
    TString DatabaseName;
    TIntrusivePtr<NTxProxy::TTxProxyMon> TxProxyMon;
    TString LogPrefix;
    TActorId SSPipeClient;
    THolder<const NACLib::TUserToken> UserToken;
    TTableProfiles Profiles;
    EOp OpType;
};

void DoAlterTableRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TAlterTableRPC(p.release()));
}

template<>
IActor* TEvAlterTableRequest::CreateRpcActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
    return new TAlterTableRPC(msg);
}

} // namespace NKikimr
} // namespace NGRpcService
