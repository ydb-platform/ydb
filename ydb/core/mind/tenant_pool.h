#pragma once
#include <ydb/core/mind/events/tenant_pool.h>
#include "local.h"

#include <ydb/core/protos/tenant_pool.pb.h>

#include <util/generic/hash.h>
#include <util/generic/ptr.h>

namespace NKikimrConfig{
    class TMonitoringConfig;
}

namespace NKikimr {

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

} // namespace NKikimr
