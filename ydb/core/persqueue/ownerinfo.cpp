#include "ownerinfo.h"
#include <util/generic/guid.h>
#include <util/string/escape.h>

namespace NKikimr {
namespace NPQ {

    void TOwnerInfo::GenerateCookie(const TString& owner, const TActorId& pipeClient, const TActorId& sender,
                                            const TString& topicName, const TPartitionId& partition, const TActorContext& ctx) {
        TStringBuilder s;
        s << owner << "|" << CreateGuidAsString() << "_" << OwnerGeneration;
        ++OwnerGeneration;
        Y_ABORT_UNLESS(OwnerCookie != s);
        OwnerCookie = s;
        NextMessageNo = 0;
        NeedResetOwner = false;
        PipeClient = pipeClient;
        if (Sender) {
            THolder<TEvPersQueue::TEvResponse> response = MakeHolder<TEvPersQueue::TEvResponse>();
            response->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);
            response->Record.SetErrorCode(NPersQueue::NErrorCode::BAD_REQUEST);
            response->Record.SetErrorReason("ownership session is killed by another session with id " + OwnerCookie + " partition id " + partition.OriginalPartitionId);
            ctx.Send(Sender, response.Release());
        }
        Sender = sender;
        ReservedSize = 0;
        Requests.clear();
        //WaitToChageOwner not touched - they will wait for this owner to be dropped - this new owner must have force flag
        LOG_INFO_S(ctx, NKikimrServices::PERSQUEUE, "new Cookie " << s << " generated for partition " << partition << " topic '" << topicName << "' owner " << EscapeC(owner));
    }

    TStringBuf TOwnerInfo::GetOwnerFromOwnerCookie(const TString& cookie) {
        auto pos = cookie.rfind('|');
        if (pos == TString::npos)
            pos = cookie.size();
        TStringBuf res = TStringBuf(cookie.c_str(), pos);
        return res;
    }
} // NPQ
} // NKikimr
