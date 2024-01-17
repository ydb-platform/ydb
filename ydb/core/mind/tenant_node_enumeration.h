#pragma once
#include "defs.h"
#include <ydb/library/actors/core/event_local.h>

#include <util/generic/vector.h>

namespace NKikimr {

    struct TEvTenantNodeEnumerator {
        enum EEv {
            EvLookupResult = EventSpaceBegin(TKikimrEvents::ES_TENANT_NODE_ENUMERATOR),

            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_TENANT_NODE_ENUMERATOR),
            "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_TENANT_NODE_ENUMERATOR)");

        struct TEvLookupResult : public TEventLocal<TEvLookupResult, EvLookupResult> {
            const TString Tenant;
            const bool Success;
            TVector<ui32> AssignedNodes;

            TEvLookupResult(const TString &tenant, TVector<ui32> &&assignedNodes)
                : Tenant(tenant)
                , Success(true)
                , AssignedNodes(std::move(assignedNodes))
            {}

            TEvLookupResult(const TString &tenant, bool success)
                : Tenant(tenant)
                , Success(success)
            {}
        };
    };

    IActor* CreateTenantNodeEnumerationPublisher();
    IActor* CreateTenantNodeEnumerationLookup(TActorId replyTo, const TString &tenantName);
} // namespace NKikimr
