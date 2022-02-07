#pragma once
#include "defs.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/protos/tenant_slot_broker.pb.h>

namespace NKikimr {
namespace NTenantSlotBroker {

static const TString ANY_DATA_CENTER = "";
constexpr char ANY_SLOT_TYPE[] = "";

constexpr char PIN_DATA_CENTER[] = "pinned";
constexpr char PIN_SLOT_TYPE[] = "pinned";

struct TEvTenantSlotBroker {
    enum EEv {
        // requests
        EvGetTenantState = EventSpaceBegin(TKikimrEvents::ES_TENANT_SLOT_BROKER),
        EvAlterTenant,
        EvRegisterPool,
        EvListTenants,

        // responses
        EvTenantState,
        EvTenantsList,

        // slot stats
        EvGetSlotStats,
        EvSlotStats,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_TENANT_SLOT_BROKER),
                  "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_TENANT_SLOT_BROKER)");

    struct TEvGetTenantState : public TEventPB<TEvGetTenantState, NKikimrTenantSlotBroker::TGetTenantState, EvGetTenantState> {};

    struct TEvAlterTenant : public TEventPB<TEvAlterTenant, NKikimrTenantSlotBroker::TAlterTenant, EvAlterTenant> {};

    struct TEvRegisterPool : public TEventPB<TEvRegisterPool, NKikimrTenantSlotBroker::TRegisterPool, EvRegisterPool> {};

    struct TEvListTenants : public TEventPB<TEvListTenants, NKikimrTenantSlotBroker::TListTenants, EvListTenants> {};

    struct TEvTenantState : public TEventPB<TEvTenantState, NKikimrTenantSlotBroker::TTenantState, EvTenantState> {};

    struct TEvTenantsList : public TEventPB<TEvTenantsList, NKikimrTenantSlotBroker::TTenantsList, EvTenantsList> {};

    struct TEvGetSlotStats : public TEventPB<TEvGetSlotStats, NKikimrTenantSlotBroker::TGetSlotStats, EvGetSlotStats> {};

    struct TEvSlotStats : public TEventPB<TEvSlotStats, NKikimrTenantSlotBroker::TSlotStats, EvSlotStats> {};

};

IActor *CreateTenantSlotBroker(const TActorId &tablet, TTabletStorageInfo *info);

} // NTenantSlotBroker
} // namespace NKikimr
