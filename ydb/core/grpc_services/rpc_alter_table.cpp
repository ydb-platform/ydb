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
#include <ydb/core/tx/schemeshard/schemeshard_forced_compaction.h>
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

using TEvAlterTableRequest = TGrpcRequestOperationCall<Ydb::Table::AlterTableRequest,
    Ydb::Table::AlterTableResponse>;

class TAlterTableRPC : public TRpcSchemeRequestActor<TAlterTableRPC, TEvAlterTableRequest> {
    using TBase = TRpcSchemeRequestActor<TAlterTableRPC, TEvAlterTableRequest>;
    using EOp = NKikimr::EAlterOperationKind;

public:
    TAlterTableRPC(IRequestOpCtx* msg)
        : TBase(msg)
        , DatabaseName(Request_->GetDatabaseName().GetOrElse(""))
    {}

    void Bootstrap(const TActorContext &ctx) {
        TBase::Bootstrap(ctx);

        const auto* req = GetProtoRequest();
        if (req->operation_params().has_forget_after() && req->operation_params().operation_mode() != Ydb::Operations::OperationParams::SYNC) {
            return Reply(StatusIds::UNSUPPORTED, "forget_after is not supported for this type of operation", NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
        }

        if (!Request_->GetSerializedToken().empty()) {
            UserToken = Request_->GetInternalToken();
        }

        auto ops = GetAlterOperationKinds(req);
        if (!ops) {
            return Reply(StatusIds::BAD_REQUEST, "Empty alter",
                NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
        }
        if (ops.size() != 1) {
            return Reply(StatusIds::UNSUPPORTED, "Mixed alter is unsupported",
                NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
        }

        OpType = *ops.begin();

        Ydb::StatusIds::StatusCode code;
        TString error;

        switch (OpType) {
        case EOp::Common:
            // Altering table settings will need table profiles
            SendConfigRequest(ctx);
            ctx.Schedule(TDuration::Seconds(15), new TEvents::TEvWakeup(WakeupTagGetConfig));
            Become(&TAlterTableRPC::AlterStateGetConfig);
            return;

        case EOp::AddIndex:
            if (!BuildAlterTableAddIndexRequest(req, &IndexBuildSettings, 0, code, error)) {
                Reply(code, error, NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
                return;
            }

            PrepareAlterTableWithTxId();
            break;

        case EOp::Attribute:
        case EOp::AddChangefeed:
        case EOp::DropChangefeed:
            Navigate(GetProtoRequest()->path());;
            break;

        case EOp::DropIndex:
        case EOp::RenameIndex:
            AlterTable(ctx);
            break;
        case EOp::Compact:
            if (!BuildAlterTableCompactRequest(req, &ForcedCompactionSettings, code, error)) {
                Reply(code, error, NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
                return;
            }

            PrepareAlterTableWithTxId();
            break;
        }

        Become(&TAlterTableRPC::AlterStateWork);
    }

private:
    void AlterStateWork(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
           hFunc(TEvTxUserProxy::TEvAllocateTxIdResult, Handle);
           HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
           HFunc(NSchemeShard::TEvIndexBuilder::TEvCreateResponse, Handle);
           HFunc(NSchemeShard::TEvIndexBuilder::TEvGetResponse, Handle);
           HFunc(NSchemeShard::TEvForcedCompaction::TEvCreateResponse, Handle);
           HFunc(NSchemeShard::TEvForcedCompaction::TEvGetResponse, Handle);
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

    void PrepareAlterTableWithTxId() {
        using namespace NTxProxy;
        LogPrefix = TStringBuilder() << "[AlterTable" << OpType << ' ' << SelfId() << "] ";
        Send(MakeTxProxyID(), new TEvTxUserProxy::TEvAllocateTxId);
    }

    void Handle(TEvTxUserProxy::TEvAllocateTxIdResult::TPtr& ev) {
        TXLOG_D("Handle TEvTxUserProxy::TEvAllocateTxIdResult");

        const auto* msg = ev->Get();
        TxId = msg->TxId;
        LogPrefix = TStringBuilder() << "[AlterTable" << OpType << ' ' << SelfId() << " TxId# " << TxId << "] ";

        Navigate(GetProtoRequest()->path());
    }

    void Navigate(const TString& path) {
        auto paths = NKikimr::SplitPath(path);
        if (paths.empty()) {
            auto& ctx = TlsActivationContext->AsActorContext();
            TString error = TStringBuilder() << "Failed to split table path " << path;
            Request_->RaiseIssue(NYql::TIssue(error));
            return Reply(Ydb::StatusIds::BAD_REQUEST, ctx);
        }

        auto navigate = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        navigate->DatabaseName = DatabaseName;

        auto& entry = navigate->ResultSet.emplace_back();
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
        entry.Path = std::move(paths);

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(navigate));
    }

    void Navigate(const TTableId& pathId) {
        auto navigate = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        navigate->DatabaseName = DatabaseName;

        auto& entry = navigate->ResultSet.emplace_back();
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpList;
        entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByTableId;
        entry.TableId = pathId;
        entry.ShowPrivatePath = true;

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(navigate));
    }

    static bool IsChangefeedOperation(EOp type) {
        switch (type) {
        case EOp::AddChangefeed:
        case EOp::DropChangefeed:
            return true;
        default:
            return false;
        }
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

        Y_ABORT_UNLESS(resp->ResultSet.size() == 1);
        const auto& entry = resp->ResultSet.front();

        switch (entry.Kind) {
        case NSchemeCache::TSchemeCacheNavigate::KindTable:
        case NSchemeCache::TSchemeCacheNavigate::KindColumnTable:
        case NSchemeCache::TSchemeCacheNavigate::KindExternalTable:
        case NSchemeCache::TSchemeCacheNavigate::KindExternalDataSource:
        case NSchemeCache::TSchemeCacheNavigate::KindView:
            break; // table
        case NSchemeCache::TSchemeCacheNavigate::KindIndex:
            if (IsChangefeedOperation(OpType)) {
                break;
            }
            [[fallthrough]];
        default:
            Request_->RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR, TStringBuilder()
                << "Unable to nagivate: " << JoinPath(entry.Path) << " status: PathNotTable"));
            return Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);
        }

        switch (OpType) {
        case EOp::AddIndex:
            return AlterTableOp(entry, ctx, [this]() {
                return std::make_unique<NSchemeShard::TEvIndexBuilder::TEvCreateRequest>(TxId, DatabaseName, std::move(IndexBuildSettings));
            });
        case EOp::Attribute:
            ResolvedPathId = resp->ResultSet.back().TableId.PathId;
            return AlterTable(ctx);
        case EOp::AddChangefeed:
        case EOp::DropChangefeed:
            if (entry.Kind != NSchemeCache::TSchemeCacheNavigate::KindIndex) {
                AlterTable(ctx);
            } else if (auto list = entry.ListNodeEntry) {
                if (list->Children.size() != 1) {
                    return Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);
                }

                const auto& child = list->Children.at(0);
                AlterTable(ctx, CanonizePath(ChildPath(NKikimr::SplitPath(GetProtoRequest()->path()), child.Name)));
            } else {
                Navigate(entry.TableId);
            }
            break;
        case EOp::Compact:
            return AlterTableOp(entry, ctx, [this]() {
                return std::make_unique<NSchemeShard::TEvForcedCompaction::TEvCreateRequest>(TxId, DatabaseName, std::move(ForcedCompactionSettings));
            });
        default:
            TXLOG_E("Got unexpected cache response");
            return Reply(Ydb::StatusIds::INTERNAL_ERROR, ctx);
        }
    }

    bool CheckAlterAccess(const NSchemeCache::TSchemeCacheNavigate::TEntry& entry, const TActorContext& ctx) {
        if (!UserToken || !entry.SecurityObject) {
            return true;
        }

        const ui32 access = NACLib::AlterSchema | NACLib::DescribeSchema;
        if (entry.SecurityObject->CheckAccess(access, *UserToken)) {
            return true;
        }

        TXLOG_W("Access check failed");
        Reply(Ydb::StatusIds::UNAUTHORIZED,
            TStringBuilder() << "Access denied"
                << " for# " << UserToken->GetUserSID()
                << ", path# " << CanonizePath(entry.Path)
                << ", access# " << NACLib::AccessRightsToString(access),
            NKikimrIssues::TIssuesIds::ACCESS_DENIED, ctx);
        return false;
    }

    void AlterTableOp(const NSchemeCache::TSchemeCacheNavigate::TEntry& entry, const TActorContext& ctx, auto createEventFn) {
        if (!CheckAlterAccess(entry, ctx)) {
            return;
        }

        const auto& domainInfo = entry.DomainInfo;
        if (!domainInfo) {
            TXLOG_E("Got empty domain info");
            return Reply(Ydb::StatusIds::INTERNAL_ERROR, ctx);
        }

        SendOpToSS(ctx, domainInfo->ExtractSchemeShard(), createEventFn);
    }

    void SendOpToSS(const TActorContext& ctx, ui64 schemeShardId, auto createEventFn) {
        SetSchemeShardId(schemeShardId);
        auto ev = createEventFn();
        if (UserToken) {
            ev->Record.SetUserSID(UserToken->GetUserSID());
        }
        ForwardToSchemeShard(ctx, std::move(ev));
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
            if (response.HasSchemeStatus() && response.GetSchemeStatus() == NKikimrScheme::EStatus::StatusAlreadyExists) {
                Reply(status, issuesProto, ctx);
            } else if (GetOperationMode() == Ydb::Operations::OperationParams::SYNC) {
                DoSubscribe(ctx);
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

    void Handle(NSchemeShard::TEvForcedCompaction::TEvCreateResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& response = ev->Get()->Record;
        const auto status = response.GetStatus();
        auto issuesProto = response.GetIssues();

        auto getDebugIssues = [issuesProto]() {
            NYql::TIssues issues;
            NYql::IssuesFromMessage(issuesProto, issues);
            return issues.ToString();
        };

        TXLOG_D("Handle TEvForcedCompaction::TEvCreateResponse"
            << ", status# " << status
            << ", issues# " << getDebugIssues()
            << ", Id# " << response.GetForcedCompaction().GetId());

        if (status == Ydb::StatusIds::SUCCESS) {
            if (GetOperationMode() == Ydb::Operations::OperationParams::SYNC) {
                DoSubscribe(ctx);
            } else {
                auto op = response.GetForcedCompaction();
                Ydb::Operations::Operation operation;
                operation.set_id(NOperationId::ProtoToString(ToOperationId(op)));
                operation.set_ready(false);
                ReplyOperation(operation);
            }
        } else {
            Reply(status, issuesProto, ctx);
        }
    }

    void GetIndexStatus(const TActorContext& ctx) {
        auto request = std::make_unique<NSchemeShard::TEvIndexBuilder::TEvGetRequest>(DatabaseName, TxId);
        ForwardToSchemeShard(ctx, std::move(request));
    }

    void GetCompactionStatus(const TActorContext& ctx) {
        auto request = std::make_unique<NSchemeShard::TEvForcedCompaction::TEvGetRequest>(DatabaseName, TxId);
        ForwardToSchemeShard(ctx, std::move(request));
    }

    void DoSubscribe(const TActorContext& ctx) {
        auto request = std::make_unique<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>(TxId);
        ForwardToSchemeShard(ctx, std::move(request));
    }

    void OnNotifyTxCompletionResult(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev, const TActorContext& ctx) override {
        if (OpType == EOp::AddIndex) {
            GetIndexStatus(ctx);
        } else if (OpType == EOp::Compact) {
            GetCompactionStatus(ctx);
        } else {
            TBase::OnNotifyTxCompletionResult(ev, ctx);
        }
    }

    void Handle(NSchemeShard::TEvIndexBuilder::TEvGetResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record;

        TXLOG_D("Handle TEvIndexBuilder::TEvGetResponse: record# " << record.ShortDebugString());

        if (record.GetStatus() != Ydb::StatusIds::SUCCESS) {
            Request_->ReplyWithYdbStatus(record.GetStatus());
        } else {
            Ydb::Operations::Operation op;
            ::NKikimr::NGRpcService::ToOperation(record.GetIndexBuild(), &op);
            Request_->SendOperation(op);
        }
        Die(ctx);
    }

    void Handle(NSchemeShard::TEvForcedCompaction::TEvGetResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record;

        TXLOG_D("Handle TEvForcedCompaction::TEvGetResponse: record# " << record.ShortDebugString());

        if (record.GetStatus() != Ydb::StatusIds::SUCCESS) {
            Request_->ReplyWithYdbStatus(record.GetStatus());
        } else {
            Ydb::Operations::Operation op;
            ::NKikimr::NGRpcService::ToOperation(record.GetForcedCompaction(), &op);
            Request_->SendOperation(op);
        }
        Die(ctx);
    }

    void AlterTable(const TActorContext &ctx, const TMaybe<TString>& overridePath = {}) {
        const auto req = GetProtoRequest();
        std::unique_ptr<TEvTxUserProxy::TEvProposeTransaction> proposeRequest = CreateProposeTransaction();
        auto modifyScheme = proposeRequest->Record.MutableTransaction()->MutableModifyScheme();
        modifyScheme->SetAllowAccessToPrivatePaths(overridePath.Defined());
        Ydb::StatusIds::StatusCode code;
        TString error;
        if (!BuildAlterTableModifyScheme(overridePath.GetOrElse(req->path()), req, modifyScheme, Profiles, ResolvedPathId, code, error)) {
            NYql::TIssues issues;
            issues.AddIssue(NYql::TIssue(error));
            return Reply(code, issues, ctx);
        }

        ctx.Send(MakeTxProxyID(), proposeRequest.release());
    }

    ui64 TxId = 0;
    const TString DatabaseName;
    TString LogPrefix;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    TPathId ResolvedPathId;
    TTableProfiles Profiles;
    EOp OpType;
    NKikimrIndexBuilder::TIndexBuildSettings IndexBuildSettings;
    NKikimrForcedCompaction::TForcedCompactionSettings ForcedCompactionSettings;
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
