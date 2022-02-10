#include "grpc_request_proxy.h" 
#include "rpc_calls.h" 
 
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <ydb/core/security/ticket_parser.h>
 
 
namespace NKikimr { 
namespace NGRpcService { 
 
class TWhoAmIRPC : public TActorBootstrapped<TWhoAmIRPC> { 
public: 
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { 
        return NKikimrServices::TActivity::GRPC_REQ; 
    } 
 
    TWhoAmIRPC(TEvWhoAmIRequest::TPtr& request) 
        : Request(request->Release().Release()) 
    {} 
 
    void Bootstrap(const TActorContext& ctx) { 
        TMaybe<TString> authToken = Request->GetYdbToken(); 
        if (authToken) { 
            TMaybe<TString> database = Request->GetDatabaseName();
            ctx.Send(MakeTicketParserID(), new TEvTicketParser::TEvAuthorizeTicket({
                .Database = database ? database.GetRef() : TString(),
                .Ticket = authToken.GetRef(),
                .PeerName = Request->GetPeerName()
            }));
            Become(&TThis::StateWaitForTicket); 
        } else { 
            ReplyError("No token provided"); 
            PassAway(); 
        } 
    } 
 
    STFUNC(StateWaitForTicket) { 
        Y_UNUSED(ctx); 
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
                if (Request->GetProtoRequest()->include_groups()) { 
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
 
    THolder<TEvWhoAmIRequest> Request; 
}; 
 
void TGRpcRequestProxy::Handle(TEvWhoAmIRequest::TPtr& ev, const TActorContext& ctx) { 
    ctx.Register(new TWhoAmIRPC(ev)); 
} 
 
} // namespace NGRpcService 
} // namespace NKikimr 
