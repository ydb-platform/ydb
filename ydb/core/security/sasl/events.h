#pragma once

#include <ydb/core/base/events.h>
#include <ydb/library/actors/core/events.h>
#include <yql/essentials/public/issue/yql_issue.h>

namespace NKikimr::NSasl {

    struct TEvSasl {
        enum EEv {
            EvComputedHashes = EventSpaceBegin(TKikimrEvents::ES_SASL_AUTH),
            EvSaslPlainLoginResponse,
            EvSaslPlainLdapLoginResponse,
            EvSaslScramFirstServerResponse,
            EvSaslScramFinalClientRequest,
            EvSaslScramFinalServerResponse,
            EvEnd,
        };

        struct TEvComputedHashes : public TEventLocal<TEvComputedHashes, EvComputedHashes> {
            std::string Error;
            std::string Hashes;
            std::string ArgonHash;
        };

        struct TEvSaslPlainLoginResponse : public TEventLocal<TEvSaslPlainLoginResponse, EvSaslPlainLoginResponse> {
            NYql::TIssue Issue;
            std::string Token;
            std::string SanitizedToken;
            bool IsAdmin;
        };

        struct TEvSaslPlainLdapLoginResponse : public TEventLocal<TEvSaslPlainLdapLoginResponse, EvSaslPlainLdapLoginResponse> {
            NYql::TIssue Issue;
            std::string Reason;
            std::string Token;
            std::string SanitizedToken;
            bool IsAdmin;
        };

        struct TEvSaslScramFirstServerResponse : public TEventLocal<TEvSaslScramFirstServerResponse, EvSaslScramFirstServerResponse> {
            std::string Msg;
        };

        struct TEvSaslScramFinalClientRequest : public TEventLocal<TEvSaslScramFinalClientRequest, EvSaslScramFinalClientRequest> {
            std::string Msg;
        };

         struct TEvSaslScramFinalServerResponse : public TEventLocal<TEvSaslScramFinalServerResponse, EvSaslScramFinalServerResponse> {
            std::string Msg;
            NYql::TIssue Issue;
            std::string AuthcId;
            std::string Token;
            std::string SanitizedToken;
            bool IsAdmin;
        };
    };

} // namespace NKikimr::NSasl
