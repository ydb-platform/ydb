#pragma once
#include <ydb/core/base/defs.h>
#include <ydb/core/base/events.h>
#include <ydb/core/base/ticket_parser.h>

namespace NKikimr {

struct TEvLdapAuthProvider {
    enum EEv {
        // requests
        EvEnrichGroupsRequest = EventSpaceBegin(TKikimrEvents::ES_LDAP_AUTH_PROVIDER),
        EvAuthenticateRequest,

        // replies
        EvEnrichGroupsResponse = EventSpaceBegin(TKikimrEvents::ES_LDAP_AUTH_PROVIDER) + 512,
        EvAuthenticateResponse,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_LDAP_AUTH_PROVIDER), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_LDAP_AUTH_PROVIDER)");

    enum class EStatus {
        SUCCESS,
        UNAUTHORIZED,
        UNAVAILABLE,
        BAD_REQUEST,
    };

    struct TEvEnrichGroupsRequest : TEventLocal<TEvEnrichGroupsRequest, EvEnrichGroupsRequest> {
        TString Key;
        TString User;

        TEvEnrichGroupsRequest(const TString& key, const TString& user)
            : Key(key)
            , User(user)
        {}
    };

    struct TEvAuthenticateRequest : TEventLocal<TEvAuthenticateRequest, EvAuthenticateRequest> {
        TString Login;
        TString Password;

        TEvAuthenticateRequest(const TString& login, const TString& password)
            : Login(login)
            , Password(password)
        {}
    };

    using TError = TEvTicketParser::TError;

    template <typename TResponse, ui32 TEvent>
    struct TEvResponse : TEventLocal<TResponse, TEvent> {
        EStatus Status;
        TError Error;

        TEvResponse(const EStatus& status = EStatus::SUCCESS, const TError& error = {})
            : Status(status)
            , Error(error)
        {}
    };

    struct TEvEnrichGroupsResponse : TEvResponse<TEvEnrichGroupsResponse, EvEnrichGroupsResponse> {
        TString Key;
        TString User;
        std::vector<TString> Groups;

        TEvEnrichGroupsResponse(const TString& key, const TString& user, const std::vector<TString>& groups)
            : Key(key)
            , User(user)
            , Groups(groups)
        {}

        TEvEnrichGroupsResponse(const TString& key, const EStatus& status, const TError& error)
            : TEvResponse<TEvEnrichGroupsResponse, EvEnrichGroupsResponse>(status, error)
            , Key(key)
        {}
    };

    struct TEvAuthenticateResponse : TEvResponse<TEvAuthenticateResponse, EvAuthenticateResponse> {
        TEvAuthenticateResponse(const EStatus& status, const TError& error)
            : TEvResponse<TEvAuthenticateResponse, EvAuthenticateResponse>(status, error)
        {}
    };
};

inline NActors::TActorId MakeLdapAuthProviderID() {
    static const char name[12] = "ldapauthpdr";
    return NActors::TActorId(0, TStringBuf(name, 12));
}

IActor* CreateLdapAuthProvider(const NKikimrProto::TLdapAuthentication& settings);

}
