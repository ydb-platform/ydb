#include "service_scheme.h"

#include "rpc_scheme_base.h"
#include "rpc_common/rpc_common.h"
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/public/api/protos/ydb_scheme.pb.h>

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;
using namespace Ydb;

using TEvModifyPermissionsRequest = TGrpcRequestOperationCall<Ydb::Scheme::ModifyPermissionsRequest,
    Ydb::Scheme::ModifyPermissionsResponse>;

class TModifyPermissionsRPC : public TRpcSchemeRequestActor<TModifyPermissionsRPC, TEvModifyPermissionsRequest> {
    using TBase = TRpcSchemeRequestActor<TModifyPermissionsRPC, TEvModifyPermissionsRequest>;

public:
    TModifyPermissionsRPC(IRequestOpCtx* msg)
        : TBase(msg) {}

    void Bootstrap(const TActorContext &ctx) {
        TBase::Bootstrap(ctx);

        try {
            SendRequest(ctx);
        } catch (const std::exception& ex) {
            Request_->RaiseIssue(NYql::ExceptionToIssue(ex));
            return Reply(StatusIds::BAD_REQUEST, ctx);
        }

        Become(&TThis::StateWork);
    }

private:
    void SendRequest(const TActorContext &ctx) {
        const auto req = GetProtoRequest();

        const std::pair<TString, TString> pathPair = SplitPath(req->path());

        const auto& workingDir = pathPair.first;
        const auto& name = pathPair.second;

        std::unique_ptr<TEvTxUserProxy::TEvProposeTransaction> proposeRequest = CreateProposeTransaction();
        NKikimrTxUserProxy::TEvProposeTransaction& record = proposeRequest->Record;
        NKikimrSchemeOp::TModifyScheme* modifyScheme = record.MutableTransaction()->MutableModifyScheme();
        modifyScheme->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpModifyACL);
        modifyScheme->SetWorkingDir(workingDir);
        modifyScheme->MutableModifyACL()->SetName(name);

        NACLib::TDiffACL acl;
        if (req->clear_permissions()) {
            acl.ClearAccess();
        }
        if (req->has_interrupt_inheritance()) {
            acl.SetInterruptInheritance(req->interrupt_inheritance());
        }
        for (const auto& perm : req->actions()) {
            switch (perm.action_case()) {
                case Ydb::Scheme::PermissionsAction::kSet: {
                    acl.ClearAccessForSid(perm.set().subject());
                    for (const auto& n : perm.set().permission_names()) {
                        auto aclAttrs = ConvertYdbPermissionNameToACLAttrs(n);
                        acl.AddAccess(NACLib::EAccessType::Allow, aclAttrs.AccessMask, perm.set().subject(), aclAttrs.InheritanceType);
                    }
                }
                break;
                case Ydb::Scheme::PermissionsAction::kGrant:
                {
                    for (const auto& n : perm.grant().permission_names()) {
                        auto aclAttrs = ConvertYdbPermissionNameToACLAttrs(n);
                        acl.AddAccess(NACLib::EAccessType::Allow, aclAttrs.AccessMask, perm.grant().subject(), aclAttrs.InheritanceType);
                    }
                }
                break;
                case Ydb::Scheme::PermissionsAction::kRevoke: {
                    for (const auto& n : perm.revoke().permission_names()) {
                        auto aclAttrs = ConvertYdbPermissionNameToACLAttrs(n);
                        acl.RemoveAccess(NACLib::EAccessType::Allow, aclAttrs.AccessMask, perm.revoke().subject(), aclAttrs.InheritanceType);
                    }
                }
                break;
                case Ydb::Scheme::PermissionsAction::kChangeOwner: {
                    modifyScheme->MutableModifyACL()->SetNewOwner(perm.change_owner());
                }
                break;
                default: {
                    throw NYql::TErrorException(NKikimrIssues::TIssuesIds::DEFAULT_ERROR)
                        << "Unknown permission action";
                }
            }
        }
        modifyScheme->MutableModifyACL()->SetDiffACL(acl.SerializeAsString());
        ctx.Send(MakeTxProxyID(), proposeRequest.release());
    }
};

void DoModifyPermissionsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TModifyPermissionsRPC(p.release()));
}

template<>
IActor* TEvModifyPermissionsRequest::CreateRpcActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
    return new TModifyPermissionsRPC(msg);
}

} // namespace NKikimr
} // namespace NGRpcService
