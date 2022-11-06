#pragma once

#include <ydb/core/yq/libs/events/event_subspace.h>
#include <ydb/core/yq/libs/quota_manager/events/events.h>

#include <ydb/public/api/protos/yq.pb.h>

#include <library/cpp/actors/core/event_pb.h>
#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/interconnect/events_local.h>

#include <ydb/library/yql/public/issue/yql_issue.h>

#include <util/digest/multi.h>

namespace NYq {

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

    TString Assign(const TString& cloudId, const TString& /* scope */, const TString& DefaultTenantName = "") {
        
        auto vTenant = SubjectMapping[SUBJECT_TYPE_CLOUD].Value(cloudId, "");
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
        EvGetTenantInfoRequest = YqEventSubspaceBegin(NYq::TYqEventSubspace::ControlPlaneConfig),
        EvGetTenantInfoResponse,
        EvEnd,
    };

    static_assert(EvEnd <= YqEventSubspaceEnd(NYq::TYqEventSubspace::ControlPlaneConfig), "All events must be in their subspace");

    struct TEvGetTenantInfoRequest : NActors::TEventLocal<TEvGetTenantInfoRequest, EvGetTenantInfoRequest> {
    };

    struct TEvGetTenantInfoResponse : NActors::TEventLocal<TEvGetTenantInfoResponse, EvGetTenantInfoResponse> {
        TEvGetTenantInfoResponse(TTenantInfo::TPtr tenantInfo) : TenantInfo(tenantInfo) {
        }
        TTenantInfo::TPtr TenantInfo;
    };
};

}
