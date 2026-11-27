#include "ydb_database_management.h"

#include <ydb/core/base/path.h>
#include <ydb/core/base/rediscovery.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/rpc_scheme_base.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/library/grpc/server/grpc_method_setup.h>

namespace NKikimr::NGRpcService {
namespace {

using namespace Ydb;
using TEvTriggerRediscovery = TGrpcRequestOperationCall<DatabaseManagement::TriggerRediscoveryRequest, DatabaseManagement::TriggerRediscoveryResponse>;

class TTriggerRediscoveryRPC : public TRpcSchemeRequestActor<TTriggerRediscoveryRPC, TEvTriggerRediscovery> {
    using TBase = TRpcSchemeRequestActor<TTriggerRediscoveryRPC, TEvTriggerRediscovery>;

public:
    using TBase::TBase;

    void Bootstrap(const TActorContext& ctx) {
        TBase::Bootstrap(ctx);
        if (!AppData(ctx)->FeatureFlags.GetEnableDiscoveryLongPolling()) {
            return Reply(StatusIds::UNSUPPORTED);
        }
        if (Request_->GetSerializedToken().empty()) {
            return Reply(StatusIds::UNAUTHORIZED);
        }
        const auto name = Request_->GetDatabaseName();
        const auto database = name ? CanonizePath(*name) : TString{};
        if (!name || !name->StartsWith('/') || database.empty()) {
            return Reply(StatusIds::BAD_REQUEST);
        }
        if (Request_->IsClientLost() || Request_->GetDeadline() <= ctx.Now()) {
            return Reply(StatusIds::TIMEOUT);
        }

        Request_->AddAuditLogPart("target_database", database);
        auto request = CreateProposeTransaction();
        auto& tx = *request->Record.MutableTransaction()->MutableModifyScheme();
        tx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterUserAttributes);
        const auto parent = ExtractParent(database);
        tx.SetWorkingDir(parent.empty() ? "/" : TString(parent));
        auto& patch = *tx.MutableAlterUserAttributes();
        patch.SetPathName(TString(ExtractBase(database)));
        auto& marker = *patch.AddUserAttributes();
        marker.SetKey(RediscoveryAttribute);
        marker.SetValue("");
        ctx.Send(MakeTxProxyID(), request.release());
        Become(&TTriggerRediscoveryRPC::StateWork);
    }
};

void DoTriggerRediscovery(std::unique_ptr<IRequestOpCtx> request, const IFacilityProvider& facility) {
    facility.RegisterActor(new TTriggerRediscoveryRPC(request.release()));
}

} // namespace

void TGRpcDatabaseManagementService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    using namespace Ydb::DatabaseManagement;
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);
    SETUP_METHOD(TriggerRediscovery, DoTriggerRediscovery, RLSWITCH(Rps), UNSPECIFIED, database_management,
        TAuditMode::Modifying(TAuditMode::TLogClassConfig::DatabaseAdmin), EEmptyDatabaseMode::EmptyDatabaseForbidden);
}

} // namespace NKikimr::NGRpcService
