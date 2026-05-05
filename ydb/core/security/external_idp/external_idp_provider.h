#pragma once

#include <ydb/core/base/defs.h>
#include <ydb/core/base/events.h>
#include <ydb/core/base/ticket_parser.h>
#include <ydb/core/protos/auth.pb.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr {

// TEvExternalIdpProvider is the public events interface for the ExternalIdpProvider actor.
//
// The actor is the analogue of AccessServiceValidator/LdapAuthProvider, but for external
// OIDC/OAuth2 IdPs. It owns the cache of OIDC discovery metadata and JSON Web Key Sets
// (JWKs) for the configured IdP and performs JWT signature & claim validation entirely
// in-process (without contacting the IdP for every request).
//
// Typical flow inside TicketParser:
//   1. Bearer JWT arrives -> TicketParser sends TEvAuthenticateRequest
//   2. ExternalIdpProvider decodes token, verifies signature against cached JWKs (fetching them
//      if missing/stale) and validates standard claims (exp, aud, iss).
//   3. Provider replies with TEvAuthenticateResponse containing the user subject
//      and group claims.
//   4. On periodic refresh TicketParser issues another TEvAuthenticateRequest
//      with the same Ticket; the same validation runs (cache hot-path).
struct TEvExternalIdpProvider {
    enum EEv {
        EvAuthenticateRequest = EventSpaceBegin(TKikimrEvents::ES_EXTERNAL_IDP_PROVIDER),

        EvAuthenticateResponse = EventSpaceBegin(TKikimrEvents::ES_EXTERNAL_IDP_PROVIDER) + 512,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_EXTERNAL_IDP_PROVIDER),
                  "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_EXTERNAL_IDP_PROVIDER)");

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
    const NActors::TActorId& httpProxyId);

} // NKikimr
