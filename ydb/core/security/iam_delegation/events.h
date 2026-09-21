#pragma once

#include <ydb/core/base/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <yql/essentials/public/issue/yql_issue.h>

#include <util/datetime/base.h>
#include <util/digest/multi.h>
#include <util/generic/string.h>

namespace NKikimr::NIamDelegation {

// Parameters of one IAM delegation: "the agent service account of YDB in cloud CloudId may impersonate
// ServiceAccountId; the delegation is referenced by ReferrerId". The IAM types of the cloud and of the
// referrer are constants of the service (TIamDelegationSettings), not part of a delegation's record.
struct TDelegationSpec {
    TString ServiceAccountId;
    TString CloudId;
    TString ReferrerId;

    TString ToString() const {
        return TStringBuilder() << "{sa: " << ServiceAccountId << ", cloud: " << CloudId << ", referrer: " << ReferrerId << "}";
    }
};

struct TDelegationResult {
    Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::SUCCESS;
    NYql::TIssues Issues;

    bool IsSuccess() const {
        return Status == Ydb::StatusIds::SUCCESS;
    }

    static TDelegationResult Success() {
        return {};
    }

    static TDelegationResult Error(Ydb::StatusIds::StatusCode status, const TString& message) {
        TDelegationResult result;
        result.Status = status;
        result.Issues.AddIssue(message);
        return result;
    }
};

// Key of a cached token: the delegated service account within a resource container (cloud).
struct TTokenKey {
    TString ServiceAccountId;
    TString CloudId;

    bool operator==(const TTokenKey&) const = default;

    size_t Hash() const {
        return MultiHash(ServiceAccountId, CloudId);
    }

    TString ToString() const {
        return TStringBuilder() << CloudId << "/" << ServiceAccountId;
    }
};

struct TEvIamDelegation {
    enum EEv {
        // delegation service
        EvSetupDelegation = EventSpaceBegin(TKikimrEvents::ES_IAM_DELEGATION),
        EvSetupDelegationResult,
        EvRevokeDelegation,
        EvRevokeDelegationResult,

        // token service
        EvGetToken,
        EvGetTokenResult,

        // cloud resolver
        EvResolveCloudResult,

        // private
        EvSystemTokenReady,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_IAM_DELEGATION), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_IAM_DELEGATION)");

    struct TEvSetupDelegation : NActors::TEventLocal<TEvSetupDelegation, EvSetupDelegation> {
        TDelegationSpec Spec;
        TString SubjectId; // IAM subject of the user on whose behalf the delegation is set up

        TEvSetupDelegation(TDelegationSpec spec, TString subjectId)
            : Spec(std::move(spec))
            , SubjectId(std::move(subjectId))
        {}
    };

    struct TEvSetupDelegationResult : NActors::TEventLocal<TEvSetupDelegationResult, EvSetupDelegationResult> {
        TDelegationResult Result;

        explicit TEvSetupDelegationResult(TDelegationResult result)
            : Result(std::move(result))
        {}
    };

    struct TEvRevokeDelegation : NActors::TEventLocal<TEvRevokeDelegation, EvRevokeDelegation> {
        TDelegationSpec Spec;

        explicit TEvRevokeDelegation(TDelegationSpec spec)
            : Spec(std::move(spec))
        {}
    };

    struct TEvRevokeDelegationResult : NActors::TEventLocal<TEvRevokeDelegationResult, EvRevokeDelegationResult> {
        TDelegationResult Result;

        explicit TEvRevokeDelegationResult(TDelegationResult result)
            : Result(std::move(result))
        {}
    };

    // Returns the current token for the key, minting one if needed. The reply carries the cookie of the request.
    struct TEvGetToken : NActors::TEventLocal<TEvGetToken, EvGetToken> {
        TTokenKey Key;

        explicit TEvGetToken(TTokenKey key)
            : Key(std::move(key))
        {}
    };

    struct TEvGetTokenResult : NActors::TEventLocal<TEvGetTokenResult, EvGetTokenResult> {
        TTokenKey Key;
        TString Token;
        TInstant ExpiresAt;
        Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::SUCCESS;
        NYql::TIssues Issues;

        bool IsSuccess() const {
            return Status == Ydb::StatusIds::SUCCESS;
        }
    };

    // Reply of the cloud resolver: the cloud (and folder) of the service account it was created for.
    struct TEvResolveCloudResult : NActors::TEventLocal<TEvResolveCloudResult, EvResolveCloudResult> {
        TDelegationResult Result;
        TString ServiceAccountId;
        TString FolderId;
        TString CloudId;
    };

    // Completion of ISystemTokenSource::RequestToken.
    struct TEvSystemTokenReady : NActors::TEventLocal<TEvSystemTokenReady, EvSystemTokenReady> {
        TString Token;
        TString Error;

        TEvSystemTokenReady(TString token, TString error)
            : Token(std::move(token))
            , Error(std::move(error))
        {}
    };
};

} // namespace NKikimr::NIamDelegation

template <>
struct THash<NKikimr::NIamDelegation::TTokenKey> {
    size_t operator()(const NKikimr::NIamDelegation::TTokenKey& key) const {
        return key.Hash();
    }
};
