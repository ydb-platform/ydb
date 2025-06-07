#pragma once
#include "common.h"

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/tx/conveyor/usage/config.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/conclusion/status.h>

namespace NKikimr::NConveyorComposite::NConfig {

class TWorkerPoolCategoryUsage {
private:
    YDB_READONLY(ESpecialTaskCategory, Category, ESpecialTaskCategory::Insert);
    YDB_READONLY(double, Weight, 1);

public:
    TWorkerPoolCategoryUsage() = default;

    TWorkerPoolCategoryUsage(const ESpecialTaskCategory cat)
        : Category(cat) {
    }

    TString DebugString() const;

    [[nodiscard]] TConclusionStatus DeserializeFromProto(const NKikimrConfig::TCompositeConveyorConfig::TWorkerPoolCategoryLink& proto) {
        if (!TryFromString<ESpecialTaskCategory>(proto.GetCategory(), Category)) {
            return TConclusionStatus::Fail("cannot parse category link: " + proto.GetCategory());
        }
        if (proto.HasWeight()) {
            if (proto.GetWeight() <= 0) {
                return TConclusionStatus::Fail("incorrect category link weight: " + ::ToString(proto.GetWeight()));
            }
            Weight = proto.GetWeight();
        }
        return TConclusionStatus::Success();
    }
};

class TThreadsCountInfo {
private:
    YDB_READONLY_DEF(std::optional<double>, Count);
    YDB_READONLY_DEF(std::optional<double>, Fraction, 0.33);

public:
    TThreadsCountInfo(const std::optional<double> count, const std::optional<double> fraction)
        : Count(count)
        , Fraction(fraction) {
        AFL_VERIFY(Count || Fraction);
    }

    ui32 GetThreadsCount(const ui32 totalThreadsCount) const {
        return std::ceil(GetCPUUsageDouble(totalThreadsCount));
    }

    double GetCPUUsageDouble(const ui32 totalThreadsCount) const {
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

    TConclusionStatus DeserializeFromProto(const NKikimrConfig::TCompositeConveyorConfig::TWorkersPool& poolInfo) {
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

};

class TWorkersPool {
private:
    YDB_READONLY(ui32, WorkersPoolId, 0);
    YDB_READONLY_DEF(TThreadsCountInfo, WorkersCountInfo);
    YDB_READONLY_DEF(std::vector<TWorkerPoolCategoryUsage>, Links);

public:
    double GetWorkerCPUUsage(const ui32 workerIdx) const;
    ui32 GetWorkersCount() const;

    bool AddLink(const ESpecialTaskCategory cat) {
        for (auto&& i : Links) {
            if (i.GetCategory() == cat) {
                return false;
            }
        }
        Links.emplace_back(TWorkerPoolCategoryUsage(cat));
        return true;
    }

    TString DebugString() const;

    TWorkersPool(const ui32 wpId)
        : WorkersPoolId(wpId) {
    }

    TWorkersPool(const ui32 wpId, const std::optional<double> workersCountDouble, const std::optional<double> workersFraction);

    [[nodiscard]] TConclusionStatus DeserializeFromProto(
        const NKikimrConfig::TCompositeConveyorConfig::TWorkersPool& proto, const ui64 usableThreadsCount);
};

class TCategory {
private:
    YDB_READONLY(ESpecialTaskCategory, Category, ESpecialTaskCategory::Insert);
    YDB_READONLY(ui32, QueueSizeLimit, 256 * 1024);
    YDB_READONLY_DEF(std::vector<ui32>, WorkerPools);
    YDB_READONLY_FLAG(Default, true);

public:
    TString DebugString() const;

    [[nodiscard]] bool AddWorkerPool(const ui32 id) {
        for (auto&& i : WorkerPools) {
            if (i == id) {
                return false;
            }
        }
        WorkerPools.emplace_back(id);
        return true;
    }

    [[nodiscard]] TConclusionStatus DeserializeFromProto(const NKikimrConfig::TCompositeConveyorConfig::TCategory& proto) {
        if (!TryFromString<ESpecialTaskCategory>(proto.GetName(), Category)) {
            return TConclusionStatus::Fail("cannot parse category: " + proto.GetName());
        }
        if (proto.HasQueueSizeLimit()) {
            QueueSizeLimit = proto.GetQueueSizeLimit();
        }
        DefaultFlag = false;
        return TConclusionStatus::Success();
    }

    TCategory(const ESpecialTaskCategory cat)
        : Category(cat) {
    }
};

class TConfig {
private:
    YDB_READONLY_DEF(std::vector<TCategory>, Categories);
    YDB_READONLY_DEF(std::vector<TWorkersPool>, WorkerPools);
    YDB_READONLY_FLAG(Enabled, true);

public:
    static TConfig BuildDefault() {
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

    const TCategory& GetCategoryConfig(const ESpecialTaskCategory cat) const;

    [[nodiscard]] TConclusionStatus DeserializeFromProto(const NKikimrConfig::TCompositeConveyorConfig& config);

    TString DebugString() const;
};

}   // namespace NKikimr::NConveyorComposite::NConfig

namespace NKikimr::NConveyorComposite {
class TCPULimitsConfig {
    YDB_OPT(double, CPUGroupThreadsLimit);
    YDB_READONLY(double, Weight, 1);

public:
    TCPULimitsConfig() = default;
    TCPULimitsConfig(const double cpuGroupThreadsLimit, const double weight = 1);

    TConclusionStatus DeserializeFromProto(const NKikimrTxDataShard::TEvKqpScan& config);
    TString DebugString() const;
};

}
