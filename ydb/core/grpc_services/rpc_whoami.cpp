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
        //TODO: Do we realy realy need to make call to the ticket parser here???
        //we have done it already in grpc_request_proxy
        auto req = dynamic_cast<TEvWhoAmIRequest*>(Request.get());
        Y_ABORT_UNLESS(req, "Unexpected request type for TWhoAmIRPC");
        TString ticket;
        if (TMaybe<TString> authToken = req->GetYdbToken()) {
            ticket = authToken.GetRef();
        } else if (TVector<TStringBuf> clientCert = Request->FindClientCert(); !clientCert.empty()) {
            ticket = TString(clientCert.front());
        } else {
            ReplyError("No token provided");
            PassAway();
            return;
        }

        TMaybe<TString> database = Request->GetDatabaseName();
        Send(MakeTicketParserID(), new TEvTicketParser::TEvAuthorizeTicket({
            .Database = database ? database.GetRef() : TString(),
            .Ticket = ticket,
            .PeerName = Request->GetPeerName()
        }));
        Become(&TThis::StateWaitForTicket);
    }

    STFUNC(StateWaitForTicket) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTicketParser::TEvAuthorizeTicketResult, Handle);
            hFunc(TEvents::TEvUndelivered, Handle);
        }
    }

private:
    void Handle(TEvTicketParser::TEvAuthorizeTicketResult::TPtr& ev) {
        const TEvTicketParser::TEvAuthorizeTicketResult& result(*ev->Get());
        auto *response = TEvWhoAmIRequest::AllocateResult<Ydb::Discovery::WhoAmIResult>(Request);
        if (!result.Error) {
            if (result.Token != nullptr) {
                response->set_user(result.Token->GetUserSID());
                if (TEvWhoAmIRequest::GetProtoRequest(Request)->include_groups()) {
                    for (const auto& group : result.Token->GetGroupSIDs()) {
                        response->add_groups(group);
                    }
                }
                Request->SendResult(*response, Ydb::StatusIds::SUCCESS);
            } else {
                ReplyError("Empty token in Staff response");
            }
        } else {
            ReplyError(result.Error.Message);
        }
        PassAway();
    }

    void Handle(TEvents::TEvUndelivered::TPtr&) {
        ReplyError("Error parsing token");
        PassAway();
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
