#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/base/ticket_parser.h>
#include "msgbus_server.h"
#include "msgbus_server_request.h"
#include "msgbus_securereq.h"

namespace NKikimr {
namespace NMsgBusProxy {

using namespace NActors;

class TMessageBusWhoAmI : public TMessageBusSecureRequest<TMessageBusServerRequestBase<TMessageBusWhoAmI>> {
    using TBase = TMessageBusSecureRequest<TMessageBusServerRequestBase<TMessageBusWhoAmI>>;
    THolder<TBusWhoAmI> Request;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::WHOAMI;
    }

    TMessageBusWhoAmI(NMsgBusProxy::TBusMessageContext& msg)
        : TMessageBusSecureRequest<TMessageBusServerRequestBase<TMessageBusWhoAmI>>(msg)
        , Request(static_cast<TBusWhoAmI*>(msg.ReleaseMessage()))
    {
        SetSecurityToken(Request->Record.GetSecurityToken());
    }

    void Bootstrap(const TActorContext& ctx) {
        THolder<NMsgBusProxy::TBusResponse> response(new NMsgBusProxy::TBusResponse());
        response->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);
        const TEvTicketParser::TEvAuthorizeTicketResult* result = GetAuthorizeTicketResult();
        if (result != nullptr) {
            if (result->Error) {
                response->Record.SetErrorReason(result->Error.Message);
                response->Record.SetStatus(NMsgBusProxy::MSTATUS_ERROR);
            } else {
                if (result->Token != nullptr) {
                    response->Record.SetUserName(result->Token->GetUserSID());
                    if (Request->Record.GetReturnGroups()) {
                        for (const auto& value : result->Token->GetGroupSIDs()) {
                            response->Record.AddGroups(value);
                        }
                    }
                }
            }
        }
        SendReply(response.Release());
        Die(ctx);
    }
};

IActor* CreateMessageBusWhoAmI(NMsgBusProxy::TBusMessageContext& msg) {
    return new TMessageBusWhoAmI(msg);
}

}
}
