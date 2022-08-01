#pragma once

#include <memory>

#include <grpc++/support/status.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>

#include <ydb/core/yq/libs/events/event_subspace.h>

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/event_local.h>

#include <ydb/core/yq/libs/quota_manager/proto/quota_internal.pb.h>

namespace NYq {

constexpr auto SUBJECT_TYPE_CLOUD = "cloud"; 
constexpr auto QUOTA_RESULT_LIMIT = "fq.queryResultLimit.bytes"; 
constexpr auto QUOTA_COUNT_LIMIT  = "fq.queryLimit.count";
constexpr auto QUOTA_TIME_LIMIT = "fq.queryLimit.ttl";

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

template <typename T>
struct TTimedValue {
    T Value;
    TInstant UpdatedAt;
    TTimedValue() = default;
    TTimedValue(const TTimedValue&) = default;
    TTimedValue(T value, const TInstant& updatedAt = TInstant::Zero()) : Value(value), UpdatedAt(updatedAt) {}
};

using TTimedUint64 = TTimedValue<ui64>;

struct TQuotaUsage {
    TTimedUint64 Limit;
    TMaybe<TTimedUint64> Usage;
    TQuotaUsage() = default;
    TQuotaUsage(const TQuotaUsage&) = default;
    TQuotaUsage(ui64 limit, const TInstant& limitUpdatedAt = Now()) : Limit(limit, limitUpdatedAt) {}
    TQuotaUsage(ui64 limit, const TInstant& limitUpdatedAt, ui64 usage, const TInstant& usageUpdatedAt = Now())
      : Limit(limit, limitUpdatedAt), Usage(NMaybe::TInPlace{}, usage, usageUpdatedAt) {}
    void Merge(const TQuotaUsage& other);
    TString ToString() {
        return (Usage ? std::to_string(Usage->Value) : "*") + "/" + std::to_string(Limit.Value);
    }
    TString ToString(const TString& subjectType, const TString& subjectId, const TString& metricName) {
        TStringBuilder builder;
        builder << subjectType << "." << subjectId << "." << metricName << "=" << ToString();
        return builder;
    }
    TString ToString(const TString& metricName) {
        TStringBuilder builder;
        builder << metricName << "=" << ToString();
        return builder;
    }
};

using TQuotaMap = THashMap<TString, TQuotaUsage>;

struct TEvQuotaService {
    // Event ids.
    enum EEv : ui32 {
        EvQuotaProxyGetRequest = YqEventSubspaceBegin(NYq::TYqEventSubspace::QuotaService),
        EvQuotaProxyGetResponse,
        EvQuotaProxySetRequest,
        EvQuotaProxySetResponse,
        EvQuotaProxyErrorResponse,
        EvQuotaGetRequest,
        EvQuotaGetResponse,
        EvQuotaChangeNotification,
        EvQuotaUsageRequest,
        EvQuotaUsageResponse,
        EvQuotaSetRequest,
        EvQuotaSetResponse,
        EvQuotaLimitChangeRequest,
        EvQuotaLimitChangeResponse,
        EvQuotaUpdateNotification,
        EvEnd,
    };

    static_assert(EvEnd <= YqEventSubspaceEnd(NYq::TYqEventSubspace::QuotaService), "All events must be in their subspace");

    struct TQuotaProxyGetRequest : public NActors::TEventLocal<TQuotaProxyGetRequest, EvQuotaProxyGetRequest> {
        TString User;
        bool PermissionExists;
        TString SubjectType;
        TString SubjectId;

        TQuotaProxyGetRequest(const TString& user, bool permissionExists, const TString& subjectType, const TString& subjectId)
            : User(user), PermissionExists(permissionExists), SubjectType(subjectType), SubjectId(subjectId) {
        }
    };

    struct TQuotaProxyGetResponse : public NActors::TEventLocal<TQuotaProxyGetResponse, EvQuotaProxyGetResponse> {
        TString SubjectType;
        TString SubjectId;
        TQuotaMap Quotas;

        TQuotaProxyGetResponse(const TString& subjectType, const TString& subjectId, const TQuotaMap& quotas)
            : SubjectType(subjectType), SubjectId(subjectId), Quotas(quotas) {
        }
    };

    struct TQuotaProxySetRequest : public NActors::TEventLocal<TQuotaProxySetRequest, EvQuotaProxySetRequest> {
        TString User;
        bool PermissionExists;
        TString SubjectType;
        TString SubjectId;
        THashMap<TString, ui64> Limits;

        TQuotaProxySetRequest(const TString& user, bool permissionExists, const TString& subjectType, const TString& subjectId, THashMap<TString, ui64>& limits)
            : User(user), PermissionExists(permissionExists), SubjectType(subjectType), SubjectId(subjectId), Limits(limits) {
        }
    };

    struct TQuotaProxySetResponse : public NActors::TEventLocal<TQuotaProxySetResponse, EvQuotaProxySetResponse> {
        TString SubjectType;
        TString SubjectId;
        THashMap<TString, ui64> Limits;

        TQuotaProxySetResponse(const TString& subjectType, const TString& subjectId, THashMap<TString, ui64>& limits)
            : SubjectType(subjectType), SubjectId(subjectId), Limits(limits) {
        }
    };

    struct TQuotaProxyErrorResponse : public NActors::TEventLocal<TQuotaProxyErrorResponse, EvQuotaProxyErrorResponse> {
        grpc::StatusCode Code;
        const TString Message;
        const TString Details;

        TQuotaProxyErrorResponse(grpc::StatusCode code, const TString& message, const TString& details = "")
            : Code(code), Message(message), Details(details) {
        }
    };

    struct TQuotaGetRequest : public NActors::TEventLocal<TQuotaGetRequest, EvQuotaGetRequest> {
        TString SubjectType;
        TString SubjectId;
        bool AllowStaleUsage;
        TQuotaGetRequest(const TString& subjectType, const TString& subjectId, bool allowStaleUsage = false)
            : SubjectType(subjectType), SubjectId(subjectId), AllowStaleUsage(allowStaleUsage) {
        }
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
        TQuotaSetRequest(const TString& subjectType, const TString& subjectId, THashMap<TString, ui64> limits)
            : SubjectType(subjectType), SubjectId(subjectId), Limits(limits)
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

    struct TEvQuotaUpdateNotification : public NActors::TEventPB<TEvQuotaUpdateNotification,
        Fq::Quota::EvQuotaUpdateNotification, EvQuotaUpdateNotification> {

        TEvQuotaUpdateNotification() = default;
        TEvQuotaUpdateNotification(const Fq::Quota::EvQuotaUpdateNotification& protoMessage)
            : NActors::TEventPB<TEvQuotaUpdateNotification, Fq::Quota::EvQuotaUpdateNotification, EvQuotaUpdateNotification>(protoMessage) {

        }
    };
};

} /* NYq */
