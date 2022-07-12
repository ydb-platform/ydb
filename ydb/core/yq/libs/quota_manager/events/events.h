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
    ui64 HardLimit;
    NActors::TActorId UsageUpdater;
    TQuotaInfo(ui64 defaultLimit, ui64 hardLimit = 0, NActors::TActorId usageUpdater = {})
        : DefaultLimit(defaultLimit)
        , HardLimit(hardLimit)
        , UsageUpdater(usageUpdater)
    {
    }
};

struct TQuotaDescription {
    TString SubjectType;
    TString MetricName;
    TQuotaInfo Info;
    TQuotaDescription(const TString& subjectType, const TString& metricName, ui64 defaultLimit, ui64 hardLimit = 0, NActors::TActorId usageUpdater = {})
        : SubjectType(subjectType)
        , MetricName(metricName)
        , Info(defaultLimit, hardLimit, usageUpdater)
    {
    }
};

struct TQuotaUsage {
    ui64 Limit;
    TMaybe<ui64> Usage;
    TInstant UpdatedAt;
    TQuotaUsage() = default;
    TQuotaUsage(const TQuotaUsage&) = default;
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
        EvQuotaSetRequest,
        EvQuotaSetResponse,
        EvQuotaLimitChangeRequest,
        EvQuotaLimitChangeResponse,
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
        TString SubjectType;
        TString SubjectId;
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

    struct TQuotaSetRequest : public NActors::TEventLocal<TQuotaSetRequest, EvQuotaSetRequest> {
        TString SubjectType;
        TString SubjectId;
        THashMap<TString, ui64> Limits;
        TQuotaSetRequest(const TString& subjectType, const TString& subjectId)
            : SubjectType(subjectType), SubjectId(subjectId)
        {}
    };

    struct TQuotaSetResponse : public NActors::TEventLocal<TQuotaSetResponse, EvQuotaSetResponse> {
        TString SubjectType;
        TString SubjectId;
        THashMap<TString, ui64> Limits;
        TQuotaSetResponse(const TString& subjectType, const TString& subjectId)
            : SubjectType(subjectType), SubjectId(subjectId)
        {}
    };

    struct TQuotaLimitChangeRequest : public NActors::TEventLocal<TQuotaLimitChangeRequest, EvQuotaLimitChangeRequest> {
        TString SubjectType;
        TString SubjectId;
        TQuotaUsage Quota;
        ui64 LimitRequested;
        TQuotaLimitChangeRequest(const TString& subjectType, const TString& subjectId)
            : SubjectType(subjectType), SubjectId(subjectId)
        {}
    };

    struct TQuotaLimitChangeResponse : public NActors::TEventLocal<TQuotaLimitChangeResponse, EvQuotaLimitChangeResponse> {
        TString SubjectType;
        TString SubjectId;
        TQuotaUsage Quota;
        ui64 LimitRequested;
        TQuotaLimitChangeResponse(const TString& subjectType, const TString& subjectId)
            : SubjectType(subjectType), SubjectId(subjectId)
        {}
    };

};

} /* NYq */
