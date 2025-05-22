#pragma once
#include "defs.h"
#include "local.h"

#include <ydb/core/protos/tenant_pool.pb.h>

#include <util/generic/hash.h>
#include <util/generic/ptr.h>

namespace NKikimrConfig{
    class TMonitoringConfig;
}

namespace NKikimr {

struct TEvTenantPool {
    enum EEv {
        EvGetStatus = EventSpaceBegin(TKikimrEvents::ES_TENANT_POOL),
        EvTenantPoolStatus,
        EvConfigureSlot,
        EvConfigureSlotResult,
        EvTakeOwnership,
        EvLostOwnership,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_TENANT_POOL),
        "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_TENANT_POOL)");

    struct TEvGetStatus : public TEventPB<TEvGetStatus, NKikimrTenantPool::TGetStatus, EvGetStatus> {
        TEvGetStatus(bool listStatic = false)
        {
            Record.SetListStaticSlots(listStatic);
        }
    };

    struct TEvTenantPoolStatus : public TEventPB<TEvTenantPoolStatus, NKikimrTenantPool::TTenantPoolStatus, EvTenantPoolStatus> {};

    struct TEvConfigureSlot : public TEventPB<TEvConfigureSlot, NKikimrTenantPool::TConfigureSlot, EvConfigureSlot> {
        TEvConfigureSlot() {}

        TEvConfigureSlot(const TString &slotId, const TString &tenantName, const TString &label)
        {
            Record.SetSlotId(slotId);
            Record.SetAssignedTenant(tenantName);
            Record.SetLabel(label);
        }
    };

    struct TEvConfigureSlotResult : public TEventPB<TEvConfigureSlotResult, NKikimrTenantPool::TConfigureSlotResult, EvConfigureSlotResult> {};

    struct TEvTakeOwnership : public TEventPB<TEvTakeOwnership, NKikimrTenantPool::TTakeOwnership, EvTakeOwnership> {
        TEvTakeOwnership() = default;

        TEvTakeOwnership(ui64 generation, ui64 seqNo = Max<ui64>()) {
            Record.SetGeneration(generation);
            Record.SetSeqNo(seqNo);
        }
    };

    struct TEvLostOwnership : public TEventPB<TEvLostOwnership, NKikimrTenantPool::TLostOwnership, EvLostOwnership> {};
};

class TTenantPoolConfig : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<TTenantPoolConfig>;

    TTenantPoolConfig(TLocalConfig::TPtr localConfig = nullptr);
    TTenantPoolConfig(const NKikimrTenantPool::TTenantPoolConfig &config,
                      TLocalConfig::TPtr localConfig = nullptr);
    TTenantPoolConfig(const NKikimrTenantPool::TTenantPoolConfig &config,
                      const NKikimrConfig::TMonitoringConfig &monCfg,
                      TLocalConfig::TPtr localConfig = nullptr);

    void AddStaticSlot(const NKikimrTenantPool::TSlotConfig &slot);
    void AddStaticSlot(const TString &tenant,
                       const NKikimrTabletBase::TMetrics &limit = NKikimrTabletBase::TMetrics());

    bool IsEnabled = true;
    TString NodeType;
    THashMap<TString, NKikimrTenantPool::TSlotConfig> StaticSlots;
    TLocalConfig::TPtr LocalConfig;
    TString StaticSlotLabel;
};

IActor* CreateTenantPool(TTenantPoolConfig::TPtr config);

inline TActorId MakeTenantPoolID(ui32 node = 0) {
    char x[12] = { 't', 'e', 'n', 'a', 'n', 't', 'p', 'o', 'o', 'l' };
    x[10] = static_cast<char>(1);
    return TActorId(node, TStringBuf(x, 12));
}

inline TActorId MakeTenantPoolRootID() {
    char x[12] = { 't', 'e', 'n', 'a', 'n', 't', 'p', 'o', 'o', 'l', 'r', 't' };
    return TActorId(0, TStringBuf(x, 12));
}

} // namespace NKikimr
