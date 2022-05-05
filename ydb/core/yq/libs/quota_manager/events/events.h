#pragma once

#include <memory>

#include <util/generic/maybe.h>
#include <util/generic/string.h>

#include <ydb/core/yq/libs/events/event_subspace.h>

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/event_local.h>

namespace NYq {

constexpr auto SUBJECT_TYPE_CLOUD = "cloud"; 
constexpr auto QUOTA_RESULT_LIMIT = "fq.queryResultLimit.bytes"; 

struct IQuotaCalculator {
    // ui64 GetQuotaUsage(const TString& quota, const TString& subjectType, const TString& subjectId) = 0;
};

struct TQuotaDescription {
    TString SubjectType;
    TString MetricName;
    ui64 DefaultLimit;
    std::shared_ptr<IQuotaCalculator> QuotaCalculator;
    TQuotaDescription(const TString& subjectType, const TString& metricName, ui64 defaultLimit)
        : SubjectType(subjectType)
        , MetricName(metricName)
        , DefaultLimit(defaultLimit)
    {
    }
};

struct TQuotaUsage {
    ui64 Limit;
    TMaybe<ui64> Usage;
    TQuotaUsage(ui64 limit) : Limit(limit) {}
    TQuotaUsage(ui64 limit, ui64 usage) : Limit(limit), Usage(usage) {}
};

using TQuotaMap = THashMap<TString, TQuotaUsage>;

struct TEvQuotaService {
    // Event ids.
    enum EEv : ui32 {
        EvQuotaGetRequest = YqEventSubspaceBegin(NYq::TYqEventSubspace::QuotaService),
        EvQuotaGetResponse,
        EvEnd,
    };

    static_assert(EvEnd <= YqEventSubspaceEnd(NYq::TYqEventSubspace::QuotaService), "All events must be in their subspace");

    struct TQuotaGetRequest : public NActors::TEventLocal<TQuotaGetRequest, EvQuotaGetRequest> {
        TString SubjectType;
        TString SubjectId;
    };

    // Quota request never fails, if no quota exist (i.e. SubjectType is incorrect) empty list will be returned
    struct TQuotaGetResponse : public NActors::TEventLocal<TQuotaGetResponse, EvQuotaGetResponse> {
        TQuotaMap Quotas;
    };
};

} /* NYq */
