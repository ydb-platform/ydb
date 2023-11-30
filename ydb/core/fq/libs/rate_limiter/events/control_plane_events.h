#pragma once
#include <ydb/core/fq/libs/events/event_subspace.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>

namespace NFq {

NActors::TActorId RateLimiterControlPlaneServiceId();

struct TEvRateLimiter {
    // Event ids.
    enum EEv : ui32 {
        EvCreateResource = YqEventSubspaceBegin(TYqEventSubspace::RateLimiter),
        EvCreateResourceResponse,
        EvDeleteResource,
        EvDeleteResourceResponse,
        EvEnd,
    };

    static_assert(EvEnd <= YqEventSubspaceEnd(TYqEventSubspace::RateLimiter), "All events must be in their subspace");

    struct TEvCreateResource : NActors::TEventLocal<TEvCreateResource, EEv::EvCreateResource> {
        TEvCreateResource(const TString& cloudId, const TString& scope, const TString& queryId, TMaybe<double> queryLimit)
            : CloudId(cloudId)
            , Scope(scope)
            , QueryId(queryId)
            , CloudLimit(Nothing())
            , QueryLimit(queryLimit)
        {
        }

        TEvCreateResource(const TString& cloudId, double cloudLimit)
            : CloudId(cloudId)
            , CloudLimit(cloudLimit)
        {
        }

        const TString CloudId;
        const TString Scope;
        const TString QueryId;
        const TMaybe<double> CloudLimit;
        const TMaybe<double> QueryLimit;
    };

    struct TEvCreateResourceResponse : NActors::TEventLocal<TEvCreateResourceResponse, EEv::EvCreateResourceResponse> {
        explicit TEvCreateResourceResponse(const NYql::TIssues& issues)
            : Success(false)
            , Issues(issues)
        {
        }

        explicit TEvCreateResourceResponse(const TString& rateLimiter, const NYql::TIssues& issues = {})
            : Success(true)
            , Issues(issues)
            , RateLimiterPath(rateLimiter)
        {
        }

        const bool Success;
        const NYql::TIssues Issues;
        const TString RateLimiterPath;
    };

    struct TEvDeleteResource : NActors::TEventLocal<TEvDeleteResource, EEv::EvDeleteResource> {
        explicit TEvDeleteResource(const TString& cloudId, const TString& scope, const TString& queryId)
            : CloudId(cloudId)
            , Scope(scope)
            , QueryId(queryId)
        {
        }

        const TString CloudId;
        const TString Scope;
        const TString QueryId;
    };

    struct TEvDeleteResourceResponse : NActors::TEventLocal<TEvDeleteResourceResponse, EEv::EvDeleteResourceResponse> {
        explicit TEvDeleteResourceResponse(bool success, const NYql::TIssues& issues = {})
            : Success(success)
            , Issues(issues)
        {
        }

        const bool Success;
        const NYql::TIssues Issues;
    };
};

} // namespace NFq
