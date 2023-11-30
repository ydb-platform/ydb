#pragma once

#include <ydb/core/fq/libs/events/event_subspace.h>
#include <ydb/core/fq/libs/quota_manager/events/events.h>
#include <ydb/core/fq/libs/compute/common/config.h>

#include <ydb/public/api/protos/draft/fq.pb.h>

#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/interconnect/events_local.h>

#include <ydb/library/yql/public/issue/yql_issue.h>

#include <util/digest/multi.h>

namespace NFq {

enum TenantState : ui64 {
    Active = 0,
    Pending = 1,
    Idle = 2,
};

struct TTenantInfo {

    using TPtr = std::shared_ptr<TTenantInfo>;

    THashMap<TString /* subject type */, THashMap<TString /* subject id */, TString /* vtenant */>> SubjectMapping;
    TVector<TString> CommonVTenants;
    THashMap<TString /* vtenant */, TString /* tenant */> TenantMapping;
    THashMap<TString /* tenant */, ui32 /* state */> TenantState;
    TInstant StateTime;
    NFq::TComputeConfig ComputeConfig;

    TTenantInfo() = default;

    TTenantInfo(const NFq::NConfig::TComputeConfig& computeConfig)
        : ComputeConfig(computeConfig)
    {}

    // this method must be thread safe
    TString Assign(const TString& cloudId, const TString& scope, FederatedQuery::QueryContent::QueryType queryType, const TString& DefaultTenantName = "") const {
        auto pinTenants = ComputeConfig.GetPinTenantNames(queryType, scope);
        if (pinTenants) {
            return pinTenants[MultiHash(cloudId) % pinTenants.size()];
        }

        auto it = SubjectMapping.find(SUBJECT_TYPE_CLOUD);
        auto vTenant = it == SubjectMapping.end() ? "" : it->second.Value(cloudId, "");
        if (!vTenant && CommonVTenants.size()) {
            vTenant = CommonVTenants[MultiHash(cloudId) % CommonVTenants.size()];
        }

        auto tenant = vTenant ? TenantMapping.Value(vTenant, DefaultTenantName) : DefaultTenantName;
        // CPS_LOG_D("AssignTenantName: {" << cloudId << ", " << scope << "} => " << tenant);
        // Cerr << "AssignTenantName: {" << cloudId << ", " << scope << "} => " << tenant << Endl;
        return tenant;
    }
};

struct TEvControlPlaneConfig {
    // Event ids.
    enum EEv : ui32 {
        EvGetTenantInfoRequest = YqEventSubspaceBegin(NFq::TYqEventSubspace::ControlPlaneConfig),
        EvGetTenantInfoResponse,
        EvEnd,
    };

    static_assert(EvEnd <= YqEventSubspaceEnd(NFq::TYqEventSubspace::ControlPlaneConfig), "All events must be in their subspace");

    struct TEvGetTenantInfoRequest : NActors::TEventLocal<TEvGetTenantInfoRequest, EvGetTenantInfoRequest> {
    };

    struct TEvGetTenantInfoResponse : NActors::TEventLocal<TEvGetTenantInfoResponse, EvGetTenantInfoResponse> {
        TEvGetTenantInfoResponse(TTenantInfo::TPtr tenantInfo) : TenantInfo(tenantInfo) {
        }
        TTenantInfo::TPtr TenantInfo;
    };
};

}
