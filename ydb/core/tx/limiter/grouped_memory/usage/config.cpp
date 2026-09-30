#include "config.h"
#include <util/string/builder.h>

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/tx/columnshard/common/limits.h>

namespace NKikimr::NOlap::NGroupedMemoryManager {

bool TConfig::DeserializeFromProto(const NKikimrConfig::TGroupedMemoryLimiterConfig& config) {
    CountBuckets = config.GetCountBuckets() ? config.GetCountBuckets() : 1;

    if (config.HasMemoryLimit()) {
        MemoryLimit = config.GetMemoryLimit() / CountBuckets;
    }

    if (config.HasHardMemoryLimit()) {
        HardMemoryLimit = config.GetHardMemoryLimit() / CountBuckets;
    }

    Enabled = config.GetEnabled();
    MaxUnrestrictedGroupsPerScope = config.GetMaxUnrestrictedGroupsPerScope();
    if (config.HasUnconstrainedSoftLimitCoefficient()) {
        const double coefficient = config.GetUnconstrainedSoftLimitCoefficient();
        if (coefficient < TGlobalLimits::GroupedMemoryLimiterSoftLimitCoefficient || coefficient > 1.0) {
            return false;
        }
        UnconstrainedSoftLimitCoefficient = coefficient;
    }

    return true;
}

std::optional<ui64> TConfig::MakeUnconstrainedSoftBytes(const std::optional<ui64>& hardBytes) const {
    if (!UnconstrainedSoftLimitCoefficient || !hardBytes) {
        return std::nullopt;
    }
    return static_cast<ui64>(static_cast<double>(*hardBytes) * *UnconstrainedSoftLimitCoefficient);
}

TString TConfig::DebugString() const {
    TStringBuilder sb;
    sb << "MemoryLimit=" << MemoryLimit.value_or(0)
       << ";HardMemoryLimit=" << HardMemoryLimit.value_or(0)
       << ";Enabled=" << Enabled
       << ";CountBuckets=" << CountBuckets
       << ";UnconstrainedSoftLimitCoefficient=" << UnconstrainedSoftLimitCoefficient.value_or(0)
       << ";MaxUnrestrictedGroupsPerScope=" << MaxUnrestrictedGroupsPerScope
       << ";";
    return sb;
}

}
