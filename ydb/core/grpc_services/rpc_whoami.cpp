#include "service_discovery.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/core/security/ticket_parser.h>
#include <ydb/public/api/protos/ydb_discovery.pb.h>

namespace NKikimr {
namespace NGRpcService {

using TEvWhoAmIRequest = TGrpcRequestOperationCall<Ydb::Discovery::WhoAmIRequest,
    Ydb::Discovery::WhoAmIResponse>;

class TWhoAmIRPC : public TActorBootstrapped<TWhoAmIRPC> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ;
    }

    TWhoAmIRPC(IRequestOpCtx* request)
        : Request(request)
    {}

    void Bootstrap() {
        const TIntrusiveConstPtr<NACLib::TUserToken>& userToken = Request->GetInternalToken();
        if (userToken) {
            ReplyResult(*userToken);
        } else {
            ReplyError("No token provided");
        }
        PassAway();
    }

private:
    void ReplyResult(const NACLib::TUserToken& userToken) {
        auto* response = TEvWhoAmIRequest::AllocateResult<Ydb::Discovery::WhoAmIResult>(Request);
        response->set_user(userToken.GetUserSID());
        if (TEvWhoAmIRequest::GetProtoRequest(Request)->include_groups()) {
            for (const auto& group : userToken.GetGroupSIDs()) {
                response->add_groups(group);
            }
        }

        Request->SendResult(*response, Ydb::StatusIds::SUCCESS);
    }

    void ReplyError(const TString& error) {
        auto issue = NYql::TIssue(error);
        Request->RaiseIssue(issue);
        Request->ReplyWithYdbStatus(Ydb::StatusIds::GENERIC_ERROR);
    }

    std::unique_ptr<IRequestOpCtx> Request;
};

void DoWhoAmIRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TWhoAmIRPC(p.release()));
}

} // namespace NGRpcService
} // namespace NKikimr
