#pragma once

#include <ydb/core/base/events.h>
#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <yql/essentials/public/issue/yql_issue.h>

#include <util/generic/string.h>
#include <util/string/builder.h>

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

struct TEvIamDelegation {
    enum EEv {
        // delegation service
        EvSetupDelegation = EventSpaceBegin(TKikimrEvents::ES_IAM_DELEGATION),
        EvSetupDelegationResult,
        EvRevokeDelegation,
        EvRevokeDelegationResult,

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

};

// Node-local service for privileged components performing SetupDelegation / RevokeDelegation.
inline NActors::TActorId MakeIamDelegationServiceId(ui32 nodeId = 0) {
    return NActors::TActorId(nodeId, "iam_dlg_srv");
}

} // namespace NKikimr::NIamDelegation
