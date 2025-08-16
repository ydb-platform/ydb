#pragma once

#include <util/stream/format.h>
#include <ydb/core/base/memory_controller_iface.h>
#include <ydb/core/mon_alloc/memory_info.h>
#include <ydb/core/protos/memory_controller_config.pb.h>
#include <ydb/library/actors/core/defs.h>
#include <ydb/library/actors/core/actor.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr::NMemory {

::NFormatPrivate::THumanReadableSize HumanReadableBytes(ui64 bytes);

TString HumanReadableBytes(std::optional<ui64> bytes);

struct TResourceBrokerConfig {
    ui64 LimitBytes = 0;
    TMap<TString, ui64> QueueLimits;

    auto operator<=>(const TResourceBrokerConfig&) const = default;

    TString ToString() const noexcept {
        TStringBuilder result;
        result << "LimitBytes: " << HumanReadableBytes(LimitBytes);
        for (auto& [name, limitBytes] : QueueLimits) {
            result << " " << name << ": " << HumanReadableBytes(limitBytes);
        }
        return result;
    }
};

NActors::IActor* CreateMemoryController(
    TDuration interval,
    TIntrusiveConstPtr<IProcessMemoryInfoProvider> processMemoryInfoProvider,
    const NKikimrConfig::TMemoryControllerConfig& config,
    const TResourceBrokerConfig& resourceBrokerSelfConfig,
    const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters);

}
