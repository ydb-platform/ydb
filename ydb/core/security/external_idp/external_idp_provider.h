#pragma once

#include <ydb/core/base/defs.h>
#include <ydb/core/base/events.h>
#include <ydb/core/base/ticket_parser.h>
#include <ydb/core/protos/auth.pb.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr {

struct TEvExternalIdpProvider {
    enum EEv {
        EvAuthenticateRequest = EventSpaceBegin(TKikimrEvents::ES_EXTERNAL_IDP_PROVIDER),
        EvAuthenticateResponse,

        EvEnd
    };

    static_assert(
        EvEnd < EventSpaceEnd(TKikimrEvents::ES_EXTERNAL_IDP_PROVIDER),
        "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_EXTERNAL_IDP_PROVIDER)"
    );

    enum class EStatus {
        SUCCESS,
        UNAUTHORIZED,    // signature mismatch / invalid claims (permanent)
        BAD_REQUEST,     // malformed token / no matching IdP (permanent)
        UNAVAILABLE,     // network or JWKS fetch error (retryable)
    };

    using TError = TEvTicketParser::TError;

    struct TEvAuthenticateRequest : TEventLocal<TEvAuthenticateRequest, EvAuthenticateRequest> {
        TString Key;
        TString Token;

        TEvAuthenticateRequest(TString key, TString token)
            : Key(std::move(key))
            , Token(std::move(token))
        {}
    };

    struct TEvAuthenticateResponse : TEventLocal<TEvAuthenticateResponse, EvAuthenticateResponse> {
        TString Key;
        EStatus Status = EStatus::SUCCESS;
        TError Error;

        // Populated on success.
        TString User;
        TVector<TString> Groups;
        TInstant ExpiresAt;

        explicit TEvAuthenticateResponse(TString key)
            : Key(std::move(key))
        {}

        TEvAuthenticateResponse(TString key, EStatus status, TError error)
            : Key(std::move(key))
            , Status(status)
            , Error(std::move(error))
        {}
    };
};

NActors::IActor* CreateExternalIdpProvider(
    const NKikimrProto::TExternalIdpConfig& config,
    const NActors::TActorId& httpProxyId
);

} // NKikimr
