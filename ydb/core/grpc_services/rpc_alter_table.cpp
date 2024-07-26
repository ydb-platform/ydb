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

using TEvAlterTableRequest = TGrpcRequestOperationCall<Ydb::Table::AlterTableRequest,
    Ydb::Table::AlterTableResponse>;

class TAlterTableRPC : public TRpcSchemeRequestActor<TAlterTableRPC, TEvAlterTableRequest> {
    using TBase = TRpcSchemeRequestActor<TAlterTableRPC, TEvAlterTableRequest>;
    using EOp = NKikimr::EAlterOperationKind;

public:
    TAlterTableRPC(IRequestOpCtx* msg)
        : TBase(msg)
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

            PrepareAlterTableAddIndex();
            break;

        case EOp::Attribute:
        case EOp::AddChangefeed:
        case EOp::DropChangefeed:
            GetProxyServices();
            break;

        case EOp::DropIndex:
        case EOp::RenameIndex:
            AlterTable(ctx);
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
           HFunc(NSchemeShard::TEvIndexBuilder::TEvGetResponse, Handle);
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
        LogPrefix = TStringBuilder() << "[AlterTableAddIndex " << SelfId() << " TxId# " << TxId << "] ";

        Navigate(msg->Services.SchemeCache, ctx);
    }

    void GetProxyServices() {
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
            entry.Path = paths;
        }

        Send(schemeCache, ev);
    }

    void Navigate(const TTableId& pathId) {
        DatabaseName = Request_->GetDatabaseName()
            .GetOrElse(DatabaseFromDomain(AppData()));

        auto ev = CreateNavigateForPath(DatabaseName);
        {
            auto& entry = static_cast<TEvTxProxySchemeCache::TEvNavigateKeySet*>(ev)->Request->ResultSet.emplace_back();
            entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByTableId;
            entry.TableId = pathId;
            entry.ShowPrivatePath = true;
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpList;
        }

        Send(MakeSchemeCacheID(), ev);
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

        Y_ABORT_UNLESS(!resp->ResultSet.empty());
        const auto& entry = resp->ResultSet.back();

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
            return AlterTableAddIndexOp(resp, ctx);
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

        SendAddIndexOpToSS(ctx, domainInfo->ExtractSchemeShard());
    }

    void SendAddIndexOpToSS(const TActorContext& ctx, ui64 schemeShardId) {
        SetSchemeShardId(schemeShardId);
        auto ev = std::make_unique<NSchemeShard::TEvIndexBuilder::TEvCreateRequest>(TxId, DatabaseName, std::move(IndexBuildSettings));
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

    void GetIndexStatus(const TActorContext& ctx) {
        auto request = std::make_unique<NSchemeShard::TEvIndexBuilder::TEvGetRequest>(DatabaseName, TxId);
        ForwardToSchemeShard(ctx, std::move(request));
    }

    void DoSubscribe(const TActorContext& ctx) {
        auto request = std::make_unique<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>(TxId);
        ForwardToSchemeShard(ctx, std::move(request));
    }

    void OnNotifyTxCompletionResult(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev, const TActorContext& ctx) override {
        if (OpType == EOp::AddIndex) {
            GetIndexStatus(ctx);
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
    TString DatabaseName;
    TString LogPrefix;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    TPathId ResolvedPathId;
    TTableProfiles Profiles;
    EOp OpType;
    NKikimrIndexBuilder::TIndexBuildSettings IndexBuildSettings;
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
