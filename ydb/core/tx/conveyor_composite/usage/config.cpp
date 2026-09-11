#include "config.h"

#include <ydb/library/actors/core/log.h>

#include <util/generic/serialized_enum.h>
#include <util/string/builder.h>
#include <util/string/join.h>

namespace NKikimr::NConveyorComposite::NConfig {

TConclusionStatus TConfig::DeserializeFromProto(const NKikimrConfig::TCompositeConveyorConfig& config) {
    if (!config.HasEnabled()) {
        EnabledFlag = true;
    } else {
        EnabledFlag = config.GetEnabled();
    }
    for (auto&& i : GetEnumAllValues<ESpecialTaskCategory>()) {
        Categories.emplace_back(TCategory(i));
    }
    TWorkersPool* defWorkersPool = nullptr;
    WorkerPools.reserve(1 + config.GetWorkerPools().size());
    if ((ui32)config.GetCategories().size() != GetEnumAllValues<ESpecialTaskCategory>().size()) {
        TWorkersPool wp(WorkerPools.size());
        WorkerPools.emplace_back(std::move(wp));
        defWorkersPool = &WorkerPools.front();
    }
    std::set<ESpecialTaskCategory> usedCategories;
    for (auto&& i : config.GetCategories()) {
        TCategory cat(ESpecialTaskCategory::Insert);
        auto conclusion = cat.DeserializeFromProto(i);
        if (conclusion.IsFail()) {
            return conclusion;
        }
        if (!usedCategories.emplace(cat.GetCategory()).second) {
            return TConclusionStatus::Fail("category " + ::ToString(cat.GetCategory()) + " duplication");
        }
        Categories[(ui64)cat.GetCategory()] = std::move(cat);
    }
    THashSet<TString> poolNames;
    for (auto&& i : config.GetWorkerPools()) {
        TWorkersPool wp(WorkerPools.size());
        auto conclusion = wp.DeserializeFromProto(i);
        if (conclusion.IsFail()) {
            return conclusion;
        }
        if (!poolNames.emplace(wp.GetName()).second) {
            return TConclusionStatus::Fail("pool name duplication: '" + wp.GetName() + "'");
        }
        WorkerPools.emplace_back(std::move(wp));
        for (auto&& link : WorkerPools.back().GetLinks()) {
            AFL_VERIFY((ui64)link.GetCategory() < Categories.size());
            auto& cat = Categories[(ui64)link.GetCategory()];
            if (!cat.AddWorkerPool(WorkerPools.back().GetWorkersPoolId())) {
                return TConclusionStatus::Fail("double link for category: " + ::ToString(link.GetCategory()));
            }
        }
    }
    for (auto&& i : Categories) {
        if (i.GetWorkerPools().empty()) {
            AFL_VERIFY(defWorkersPool);
            AFL_VERIFY(defWorkersPool->AddLink(i.GetCategory()));
            AFL_VERIFY(i.AddWorkerPool(defWorkersPool->GetWorkersPoolId()));
        }
    }
    return TConclusionStatus::Success();
}

double TWorkersPool::GetWorkerCPUUsage(const ui32 workerIdx, const ui32 totalThreadsCount) const {
    const double workersCountDouble = WorkersCountInfo.GetCPUUsageDouble(totalThreadsCount);
    double wholePart;
    const double fractionalPart = std::modf(workersCountDouble, &wholePart);
    if (workerIdx + 1 <= wholePart) {
        return 1;
    } else {
        AFL_VERIFY(workerIdx == wholePart);
        AFL_VERIFY(fractionalPart)("count", workersCountDouble);
        return fractionalPart;
    }
}

const TCategory& TConfig::GetCategoryConfig(const ESpecialTaskCategory cat) const {
    AFL_VERIFY((ui64)cat < Categories.size());
    return Categories[(ui64)cat];
}

TString TConfig::DebugString() const {
    TStringBuilder sb;
    sb << "{";
    sb << "{Categories:[";
    for (auto&& c : Categories) {
        sb << c.DebugString() << ";";
    }
    sb << "]};";
    sb << "{WorkerPools:[";
    for (auto&& wp : WorkerPools) {
        sb << wp.DebugString() << ";";
    }
    sb << "]};";
    sb << "Enabled=" << EnabledFlag << ";";
    sb << "}";
    return sb;
}

TConfig TConfig::BuildDefault() {
    TConfig result;
    ui32 idx = 0;
    for (auto&& i : GetEnumAllValues<ESpecialTaskCategory>()) {
        result.Categories.emplace_back(i);
        result.WorkerPools.emplace_back(idx, std::nullopt, 0.33);
        AFL_VERIFY(result.WorkerPools.back().AddLink(i));
        AFL_VERIFY(result.Categories.back().AddWorkerPool(idx));
        ++idx;
    }
    return result;
}

TConclusionStatus THeavyLimit::DeserializeFromProto(const NKikimrConfig::TCompositeConveyorConfig::THeavyLimit& proto) {
    if (!proto.HasCpuLimitUs() || !proto.GetCpuLimitUs()) {
        return TConclusionStatus::Fail("heavy_limits cpu_limit_us must be greater than 0");
    }
    if (!proto.HasThreadLimit() || !proto.GetThreadLimit()) {
        return TConclusionStatus::Fail("heavy_limits thread_limit must be greater than 0");
    }
    CpuLimit = TDuration::MicroSeconds(proto.GetCpuLimitUs());
    ThreadLimit = proto.GetThreadLimit();
    return TConclusionStatus::Success();
}

TString THeavyLimit::DebugString() const {
    TStringBuilder sb;
    sb << "{cpu_us=" << CpuLimit.MicroSeconds() << ";threads=" << ThreadLimit << "}";
    return sb;
}

TConclusion<bool> ParseActorSystemPoolName(const TString& name) {
    if (name == "User") {
        return false;
    }
    if (name == "Batch") {
        return true;
    }
    return TConclusionStatus::Fail("unknown actor system pool name '" + name + "', expected User or Batch");
}

TConclusion<NKikimrConfig::TCompositeConveyorConfig> TConfig::OverlayYamlOnDefaults(
    const NKikimrConfig::TCompositeConveyorConfig& defaults, const NKikimrConfig::TCompositeConveyorConfig& yaml) {
    bool allHaveLinks = yaml.GetWorkerPools().size() > 0;
    for (const auto& pool : yaml.GetWorkerPools()) {
        if (!pool.GetLinks().size()) {
            allHaveLinks = false;
            break;
        }
    }
    if (allHaveLinks) {
        return yaml;
    }

    NKikimrConfig::TCompositeConveyorConfig result = defaults;
    if (yaml.HasEnabled()) {
        result.SetEnabled(yaml.GetEnabled());
    }
    if (yaml.GetCategories().size()) {
        result.ClearCategories();
        result.MutableCategories()->CopyFrom(yaml.GetCategories());
    }

    for (const auto& yamlPool : yaml.GetWorkerPools()) {
        if (!yamlPool.HasName() || yamlPool.GetName().empty()) {
            return TConclusionStatus::Fail("worker pool overlay requires a name");
        }
        NKikimrConfig::TCompositeConveyorConfig::TWorkersPool* existing = nullptr;
        for (auto& pool : *result.MutableWorkerPools()) {
            if (pool.GetName() == yamlPool.GetName()) {
                existing = &pool;
                break;
            }
        }
        if (yamlPool.GetLinks().size()) {
            if (existing) {
                auto merged = yamlPool;
                if (!merged.HasWorkersCount() && existing->HasWorkersCount()) {
                    merged.SetWorkersCount(existing->GetWorkersCount());
                }
                if (!merged.HasDefaultFractionOfThreadsCount() && existing->HasDefaultFractionOfThreadsCount()) {
                    merged.SetDefaultFractionOfThreadsCount(existing->GetDefaultFractionOfThreadsCount());
                }
                if (!merged.HasMaxBatchSize() && existing->HasMaxBatchSize()) {
                    merged.SetMaxBatchSize(existing->GetMaxBatchSize());
                }
                if (!merged.GetHeavyLimits().size() && existing->GetHeavyLimits().size()) {
                    merged.MutableHeavyLimits()->CopyFrom(existing->GetHeavyLimits());
                }
                *existing = merged;
            } else {
                *result.AddWorkerPools() = yamlPool;
            }
            continue;
        }
        if (!existing) {
            return TConclusionStatus::Fail("unknown worker pool name for overlay: '" + yamlPool.GetName() + "'");
        }
        if (yamlPool.GetHeavyLimits().size()) {
            existing->MutableHeavyLimits()->CopyFrom(yamlPool.GetHeavyLimits());
        }
        if (yamlPool.HasWorkersCount()) {
            existing->SetWorkersCount(yamlPool.GetWorkersCount());
        }
        if (yamlPool.HasDefaultFractionOfThreadsCount()) {
            existing->SetDefaultFractionOfThreadsCount(yamlPool.GetDefaultFractionOfThreadsCount());
        }
        if (yamlPool.HasMaxBatchSize()) {
            existing->SetMaxBatchSize(yamlPool.GetMaxBatchSize());
        }
    }
    return result;
}

TWorkersPool::TWorkersPool(const ui32 wpId, const std::optional<double> workersCountDouble, const std::optional<double> workersFraction)
    : WorkersPoolId(wpId)
    , WorkersCountInfo(workersCountDouble, workersFraction) {
    PoolName = "WP::UNDEFINED:" + ::ToString(wpId);
}

TConclusionStatus TWorkersPool::DeserializeFromProto(const NKikimrConfig::TCompositeConveyorConfig::TWorkersPool& proto) {
    if (!proto.GetLinks().size()) {
        return TConclusionStatus::Fail("no categories for workers pool");
    }
    if (proto.HasName()) {
        PoolName = proto.GetName();
    }
    std::set<TString> categories;
    for (auto&& c : proto.GetLinks()) {
        TWorkerPoolCategoryUsage link;
        auto conclusion = link.DeserializeFromProto(c);
        if (conclusion.IsFail()) {
            return conclusion;
        }
        categories.emplace(::ToString(link.GetCategory()));
        Links.emplace_back(std::move(link));
    }
    if (!PoolName || PoolName == "WP::DEFAULT") {
        PoolName = "WP::" + JoinSeq("-", categories);
    }
    if (Links.empty()) {
        return TConclusionStatus::Fail("no links for workers pool");
    }
    {
        auto parseConclusion = WorkersCountInfo.DeserializeFromProto(proto);
        if (parseConclusion.IsFail()) {
            return parseConclusion;
        }
    }
    if (proto.HasMaxBatchSize()) {
        MaxBatchSize = proto.GetMaxBatchSize();
    }
    for (const auto& protoLimit : proto.GetHeavyLimits()) {
        THeavyLimit limit;
        auto conclusion = limit.DeserializeFromProto(protoLimit);
        if (conclusion.IsFail()) {
            return conclusion;
        }
        if (!HeavyLimits.empty()) {
            if (limit.GetCpuLimit() <= HeavyLimits.back().GetCpuLimit()) {
                return TConclusionStatus::Fail("heavy_limits cpu_limit_us must be strictly increasing");
            }
            if (limit.GetThreadLimit() >= HeavyLimits.back().GetThreadLimit()) {
                return TConclusionStatus::Fail("heavy_limits thread_limit must be strictly decreasing");
            }
        }
        HeavyLimits.emplace_back(std::move(limit));
    }

    return TConclusionStatus::Success();
}

TString TWorkersPool::DebugString() const {
    TStringBuilder sb;
    sb << "{";
    sb << "id=" << WorkersPoolId << ";";
    sb << "threads=" << WorkersCountInfo.DebugString() << ";";
    TStringBuilder sbLinks;
    sbLinks << "[";
    for (auto&& l : Links) {
        sbLinks << l.DebugString() << ";";
    }
    sbLinks << "]";
    sb << "links=" << sbLinks << ";";
    if (HeavyLimits.size()) {
        TStringBuilder sbLimits;
        sbLimits << "[";
        for (auto&& l : HeavyLimits) {
            sbLimits << l.DebugString() << ";";
        }
        sbLimits << "]";
        sb << "heavy_limits=" << sbLimits << ";";
    }
    sb << "}";
    return sb;
}

ui32 TWorkersPool::GetWorkersCount(const ui32 totalThreadsCount) const {
    return WorkersCountInfo.GetThreadsCount(totalThreadsCount);
}

const TString& TWorkersPool::GetName() const {
    AFL_VERIFY(!!PoolName);
    return PoolName;
}

TString TCategory::DebugString() const {
    TStringBuilder sb;
    sb << "{";
    sb << "category=" << Category << ";";
    sb << "queue_limit=" << QueueSizeLimit << ";";
    sb << "pools=" << JoinSeq(",", WorkerPools) << ";";
    sb << "}";
    return sb;
}

TString TWorkerPoolCategoryUsage::DebugString() const {
    TStringBuilder sb;
    sb << "{";
    sb << "c=" << Category << ";";
    sb << "w=" << Weight << ";";
    sb << "}";
    return sb;
}

TThreadsCountInfo::TThreadsCountInfo(const std::optional<double> count, const std::optional<double> fraction)
    : Count(count)
    , Fraction(fraction) {
    AFL_VERIFY(Count || Fraction);
}

double TThreadsCountInfo::GetCPUUsageDouble(const ui32 totalThreadsCount) const {
    if (Count) {
        return *Count;
    }
    AFL_VERIFY(Fraction);
    const double result = *Fraction * totalThreadsCount;
    if (!result) {
        return 1;
    }
    return result;
}

NKikimr::TConclusionStatus TThreadsCountInfo::DeserializeFromProto(const NKikimrConfig::TCompositeConveyorConfig::TWorkersPool& poolInfo) {
    if (poolInfo.HasWorkersCount()) {
        Count = poolInfo.GetWorkersCount();
        if (*Count <= 0) {
            return TConclusionStatus::Fail("incorrect threads count: " + ::ToString(*Count));
        }
        Fraction.reset();
    } else if (poolInfo.HasDefaultFractionOfThreadsCount()) {
        Fraction = poolInfo.GetDefaultFractionOfThreadsCount();
        if (*Fraction <= 0 || 1 < *Fraction) {
            return TConclusionStatus::Fail("incorrect threads count fraction: " + ::ToString(*Fraction));
        }
        Count.reset();
    }
    return TConclusionStatus::Success();
}

TString TThreadsCountInfo::DebugString() const {
    TStringBuilder sb;
    sb << "{";
    if (Count) {
        sb << "c=" << *Count << ";";
    }
    if (Fraction) {
        sb << "f=" << *Fraction << ";";
    }
    sb << "}";
    return sb;
}

}   // namespace NKikimr::NConveyorComposite::NConfig

namespace NKikimr::NConveyorComposite {
TCPULimitsConfig::TCPULimitsConfig(const double cpuGroupThreadsLimit, const double weight)
    : CPUGroupThreadsLimit(cpuGroupThreadsLimit)
    , Weight(weight) {
}

TConclusionStatus TCPULimitsConfig::DeserializeFromProto(const NKikimrTxDataShard::TEvKqpScan& config) {
    if (config.HasCpuGroupThreadsLimit()) {
        CPUGroupThreadsLimit = config.GetCpuGroupThreadsLimit();
        CPUGroupName = config.GetCpuGroupName();
    }
    return TConclusionStatus::Success();
}

TString TCPULimitsConfig::DebugString() const {
    TStringBuilder sb;
    if (CPUGroupThreadsLimit) {
        sb << "CPUGroupThreadsLimit=" << *CPUGroupThreadsLimit << ";";
    } else {
        sb << "Disabled;";
    }
    return sb;
}

}   // namespace NKikimr::NConveyorComposite
