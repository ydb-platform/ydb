#include "config.h"
#include <util/string/builder.h>

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/tx/columnshard/common/limits.h>

namespace NKikimr::NOlap::NGroupedMemoryManager {

bool TConfig::DeserializeFromProto(const NKikimrConfig::TGroupedMemoryLimiterConfig& config) {
    // Validate first so a failed call leaves the object untouched.
    std::optional<double> coefficient;
    if (config.HasUnrestrictedSoftLimitCoefficient()) {
        const double value = config.GetUnrestrictedSoftLimitCoefficient();
        if (!(value >= TGlobalLimits::GroupedMemoryLimiterSoftLimitCoefficient && value <= 1.0)) {
            return false;
        }
        if (config.GetMaxUnrestrictedGroupsPerScope() == 0) {
            return false;
        }
        coefficient = value;
    }

    CountBuckets = config.GetCountBuckets() ? config.GetCountBuckets() : 1;

    if (config.HasMemoryLimit()) {
        MemoryLimit = config.GetMemoryLimit() / CountBuckets;
    }

    if (config.HasHardMemoryLimit()) {
        HardMemoryLimit = config.GetHardMemoryLimit() / CountBuckets;
    }

    Enabled = config.GetEnabled();
    MaxUnrestrictedGroupsPerScope = config.GetMaxUnrestrictedGroupsPerScope();
    UnrestrictedSoftLimitCoefficient = coefficient;

    return true;
}

std::optional<ui64> TConfig::MakeUnrestrictedSoftBytes(const std::optional<ui64>& hardBytes) const {
    if (!UnrestrictedSoftLimitCoefficient || !hardBytes) {
        return std::nullopt;
    }
    return static_cast<ui64>(static_cast<double>(*hardBytes) * *UnrestrictedSoftLimitCoefficient);
}

TString TConfig::DebugString() const {
    TStringBuilder sb;
    sb << "MemoryLimit=" << MemoryLimit.value_or(0)
       << ";HardMemoryLimit=" << HardMemoryLimit.value_or(0)
       << ";Enabled=" << Enabled
       << ";CountBuckets=" << CountBuckets
       << ";UnrestrictedSoftLimitCoefficient=" << UnrestrictedSoftLimitCoefficient.value_or(0)
       << ";MaxUnrestrictedGroupsPerScope=" << MaxUnrestrictedGroupsPerScope
       << ";";
    return sb;
}

}
