#pragma once
#include <ydb/core/yq/libs/events/event_subspace.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

#include <library/cpp/actors/core/event_local.h>
#include <library/cpp/actors/core/events.h>

namespace NYq {

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
        explicit TEvCreateResource(const TString& cloudId, const TString& scope, const TString& queryId, double cloudLimit, TMaybe<double> queryLimit)
            : CloudId(cloudId)
            , Scope(scope)
            , QueryId(queryId)
            , CloudLimit(cloudLimit)
            , QueryLimit(queryLimit)
        {
        }

        const TString CloudId;
        const TString Scope;
        const TString QueryId;
        const double CloudLimit;
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

} // namespace NYq
