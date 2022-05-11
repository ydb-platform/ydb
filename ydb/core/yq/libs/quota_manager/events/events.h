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
constexpr auto QUOTA_COUNT_LIMIT  = "fq.queryLimit.count"; 

struct TQuotaInfo {
    ui64 DefaultLimit;
    NActors::TActorId UsageUpdater;
    TQuotaInfo(ui64 defaultLimit, NActors::TActorId usageUpdater = {})
        : DefaultLimit(defaultLimit)
        , UsageUpdater(usageUpdater)
    {
    }
};

struct TQuotaDescription {
    TString SubjectType;
    TString MetricName;
    TQuotaInfo Info;
    TQuotaDescription(const TString& subjectType, const TString& metricName, ui64 defaultLimit, NActors::TActorId usageUpdater = {})
        : SubjectType(subjectType)
        , MetricName(metricName)
        , Info(defaultLimit, usageUpdater)
    {
    }
};

struct TQuotaUsage {
    ui64 Limit;
    TMaybe<ui64> Usage;
    TInstant UpdatedAt;
    TQuotaUsage(ui64 limit) : Limit(limit), UpdatedAt(TInstant::Zero()) {}
    TQuotaUsage(ui64 limit, ui64 usage, const TInstant& updatedAt = Now())
      : Limit(limit), Usage(usage), UpdatedAt(updatedAt) {}
};

using TQuotaMap = THashMap<TString, TQuotaUsage>;

struct TEvQuotaService {
    // Event ids.
    enum EEv : ui32 {
        EvQuotaGetRequest = YqEventSubspaceBegin(NYq::TYqEventSubspace::QuotaService),
        EvQuotaGetResponse,
        EvQuotaChangeNotification,
        EvQuotaUsageRequest,
        EvQuotaUsageResponse,
        EvEnd,
    };

    static_assert(EvEnd <= YqEventSubspaceEnd(NYq::TYqEventSubspace::QuotaService), "All events must be in their subspace");

    struct TQuotaGetRequest : public NActors::TEventLocal<TQuotaGetRequest, EvQuotaGetRequest> {
        TString SubjectType;
        TString SubjectId;
        bool AllowStaleUsage;
        TQuotaGetRequest(const TString& subjectType, const TString& subjectId, bool allowStaleUsage = false)
            : SubjectType(subjectType), SubjectId(subjectId), AllowStaleUsage(allowStaleUsage)
        {}
    };

    // Quota request never fails, if no quota exist (i.e. SubjectType is incorrect) empty list will be returned
    struct TQuotaGetResponse : public NActors::TEventLocal<TQuotaGetResponse, EvQuotaGetResponse> {
        TQuotaMap Quotas;
    };

    struct TQuotaChangeNotification : public NActors::TEventLocal<TQuotaChangeNotification, EvQuotaChangeNotification> {
        TString SubjectType;
        TString SubjectId;
        TString MetricName;
        TQuotaChangeNotification(const TString& subjectType, const TString& subjectId, const TString& metricName)
            : SubjectType(subjectType), SubjectId(subjectId), MetricName(metricName)
        {}
    };

    struct TQuotaUsageRequest : public NActors::TEventLocal<TQuotaUsageRequest, EvQuotaUsageRequest> {
        TString SubjectType;
        TString SubjectId;
        TString MetricName;
        TQuotaUsageRequest(const TString& subjectType, const TString& subjectId, const TString& metricName)
            : SubjectType(subjectType), SubjectId(subjectId), MetricName(metricName)
        {}
    };

    struct TQuotaUsageResponse : public NActors::TEventLocal<TQuotaUsageResponse, EvQuotaUsageResponse> {
        TString SubjectType;
        TString SubjectId;
        TString MetricName;
        ui64 Usage;
        TQuotaUsageResponse(const TString& subjectType, const TString& subjectId, const TString& metricName, ui64 usage)
            : SubjectType(subjectType), SubjectId(subjectId), MetricName(metricName), Usage(usage)
        {}
    };
};

} /* NYq */
