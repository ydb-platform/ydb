#pragma once

#include <library/cpp/tvmauth/checked_service_ticket.h>
#include <library/cpp/tvmauth/checked_user_ticket.h>
#include <library/cpp/tvmauth/ticket_status.h>

#include <util/datetime/base.h>
#include <util/generic/fwd.h>

namespace NTvmAuth::NUtils {
    TString Bin2base64url(TStringBuf buf);
    TString Base64url2bin(TStringBuf buf);

    TString SignCgiParamsForTvm(TStringBuf secret, TStringBuf ts, TStringBuf dstTvmId, TStringBuf scopes);
}

namespace NTvmAuth::NInternal {
    class TCanningKnife {
    public:
        static TCheckedServiceTicket::TImpl* GetS(TCheckedServiceTicket& t) {
            return t.Impl_.Release();
        }

        static TCheckedUserTicket::TImpl* GetU(TCheckedUserTicket& t) {
            return t.Impl_.Release();
        }

        static TMaybe<TInstant> GetExpirationTime(TStringBuf ticket);
    };
}
