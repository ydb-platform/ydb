#pragma once
#include "common.h"

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/tx/conveyor/usage/config.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/conclusion/result.h>
#include <ydb/library/conclusion/status.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/system/yassert.h>

#include <cmath>

namespace NKikimr::NConveyorComposite::NConfig {

inline constexpr TStringBuf DefaultPoolId = "__DEFAULT_POOL__";

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
    YDB_READONLY(std::optional<double>, Fraction, 0.33);

public:
    TThreadsCountInfo() = default;
    TThreadsCountInfo(const std::optional<double> count, const std::optional<double> fraction);

    TString DebugString() const;

    ui64 GetThreadsCount(const ui64 totalThreadsCount) const {
        return std::ceil(GetCPUUsageDouble(totalThreadsCount));
    }

    double GetCPUUsageDouble(const ui64 totalThreadsCount) const;

    TConclusionStatus DeserializeFromProto(const NKikimrConfig::TCompositeConveyorConfig::TWorkersPool& poolInfo);
};

class TWorkersPool {
private:
    TString PoolName;
    YDB_READONLY_DEF(TThreadsCountInfo, WorkersCountInfo);
    YDB_READONLY_DEF(std::vector<TWorkerPoolCategoryUsage>, Links);
    YDB_READONLY(ui64, MaxBatchSize, 30);

public:
    struct THash {
        size_t operator()(const TWorkersPool& pool) const {
            return ::THash<TString>()(pool.GetName());
        }
    };

    const TString& GetName() const;

    double GetWorkerCPUUsage(const ui64 workerIdx, const ui64 totalThreadsCount) const;
    ui64 GetWorkersCount(const ui64 totalThreadsCount) const;

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

    TWorkersPool() = default;

    explicit TWorkersPool(const TString& poolName)
        : PoolName(poolName) {
    }

    bool operator==(const TWorkersPool& other) const {
        return PoolName == other.PoolName;
    }

    [[nodiscard]] TConclusionStatus DeserializeFromProto(const NKikimrConfig::TCompositeConveyorConfig::TWorkersPool& proto);
};

class TCategory {
private:
    YDB_READONLY(ESpecialTaskCategory, Category, ESpecialTaskCategory::Insert);
    YDB_READONLY(ui64, QueueSizeLimit, 256 * 1024);
    YDB_READONLY_DEF(THashSet<TString>, WorkerPools);

public:
    TString DebugString() const;

    [[nodiscard]] bool AddWorkerPool(const TString& id) {
        return WorkerPools.emplace(id).second;
    }

    [[nodiscard]] TConclusionStatus DeserializeFromProto(const NKikimrConfig::TCompositeConveyorConfig::TCategory& proto) {
        if (!TryFromString<ESpecialTaskCategory>(proto.GetName(), Category)) {
            return TConclusionStatus::Fail("cannot parse category: " + proto.GetName());
        }
        if (proto.HasQueueSizeLimit()) {
            QueueSizeLimit = proto.GetQueueSizeLimit();
        }
        return TConclusionStatus::Success();
    }

    TCategory(const ESpecialTaskCategory cat)
        : Category(cat) {
    }
};

class TConfig {
private:
    YDB_READONLY_DEF(std::vector<TCategory>, Categories);
    THashSet<TWorkersPool, TWorkersPool::THash> WorkerPools;
    YDB_READONLY_FLAG(Enabled, true);

    TConfig() = default;
    [[nodiscard]] TConclusionStatus DeserializeFromProto(const NKikimrConfig::TCompositeConveyorConfig& config);

public:
    const THashSet<TWorkersPool, TWorkersPool::THash>& GetWorkerPools() const {
        return WorkerPools;
    }

    const TWorkersPool& GetWorkersPoolVerified(const TString& poolId) const {
        const auto it = WorkerPools.find(TWorkersPool(poolId));
        Y_ABORT_UNLESS(it != WorkerPools.end(), "unknown workers pool: %s", poolId.c_str());
        return *it;
    }

    static NKikimrConfig::TCompositeConveyorConfig BuildDefaultProto();
    static TConfig BuildDefault();

    static TConclusion<TConfig> BuildFromProto(const NKikimrConfig::TCompositeConveyorConfig& protoConfig) {
        TConfig config;
        auto conclusion = config.DeserializeFromProto(protoConfig);
        if (conclusion.IsFail()) {
            return conclusion;
        }
        return config;
    }

    const TCategory& GetCategoryConfig(const ESpecialTaskCategory cat) const;

    TString DebugString() const;
};

}   // namespace NKikimr::NConveyorComposite::NConfig

namespace NKikimr::NConveyorComposite {
class TCPULimitsConfig {
    YDB_OPT(double, CPUGroupThreadsLimit);
    YDB_OPT(TString, CPUGroupName);
    YDB_READONLY(double, Weight, 1);

public:
    TCPULimitsConfig() = default;
    TCPULimitsConfig(const double cpuGroupThreadsLimit, const double weight = 1);

    TConclusionStatus DeserializeFromProto(const NKikimrTxDataShard::TEvKqpScan& config);
    TString DebugString() const;
};

}   // namespace NKikimr::NConveyorComposite
