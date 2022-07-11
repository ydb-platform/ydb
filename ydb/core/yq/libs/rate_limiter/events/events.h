#pragma once
#include <ydb/core/yq/libs/events/event_subspace.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

#include <library/cpp/actors/core/event_local.h>
#include <library/cpp/actors/core/events.h>

namespace NYq {

struct TEvRateLimiter {
    // Event ids.
    enum EEv : ui32 {
        EvGetRateLimiterPath = YqEventSubspaceBegin(NYq::TYqEventSubspace::RateLimiter),
        EvRateLimiterPath,
        EvCreateResource,
        EvCreateResourceResponse,
        EvEnd,
    };

    static_assert(EvEnd <= YqEventSubspaceEnd(NYq::TYqEventSubspace::RateLimiter), "All events must be in their subspace");

private:
    struct TEvGetRateLimiterPath : NActors::TEventLocal<TEvGetRateLimiterPath, EEv::EvGetRateLimiterPath> {
        explicit TEvGetRateLimiterPath(const TString& cloudId)
            : CloudId(cloudId)
        {
        }

        const TString CloudId;
    };

    struct TEvRateLimiterPath : NActors::TEventLocal<TEvRateLimiterPath, EEv::EvRateLimiterPath> {
        explicit TEvRateLimiterPath(const TString& path)
            : Path(path)
        {
        }

        const TString Path;
    };

    struct TEvCreateResource : NActors::TEventLocal<TEvCreateResource, EEv::EvCreateResource> {
        explicit TEvCreateResource(const TString& rateLimiterPath, const TString& resourcePath)
            : RateLimiterPath(rateLimiterPath)
            , ResourcePath(resourcePath)
        {
        }

        const TString RateLimiterPath;
        const TString ResourcePath;
    };

    struct TEvCreateResourceResponse : NActors::TEventLocal<TEvCreateResourceResponse, EEv::EvCreateResourceResponse> {
        explicit TEvCreateResourceResponse(bool success, const NYql::TIssues& issues = {})
            : Success(success)
            , Issues(issues)
        {
        }

        const bool Success;
        const NYql::TIssues Issues;
    };
};

} // namespace NYq
