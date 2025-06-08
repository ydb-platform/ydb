#pragma once
#include "common.h"

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/tx/conveyor/usage/config.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/conclusion/result.h>
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
    YDB_READONLY(std::optional<double>, Fraction, 0.33);

public:
    TThreadsCountInfo() = default;
    TThreadsCountInfo(const std::optional<double> count, const std::optional<double> fraction);

    TString DebugString() const;

    ui32 GetThreadsCount(const ui32 totalThreadsCount) const {
        return std::ceil(GetCPUUsageDouble(totalThreadsCount));
    }

    double GetCPUUsageDouble(const ui32 totalThreadsCount) const;

    TConclusionStatus DeserializeFromProto(const NKikimrConfig::TCompositeConveyorConfig::TWorkersPool& poolInfo);
};

class TWorkersPool {
private:
    TString PoolName;
    YDB_READONLY(ui32, WorkersPoolId, 0);
    YDB_READONLY_DEF(TThreadsCountInfo, WorkersCountInfo);
    YDB_READONLY_DEF(std::vector<TWorkerPoolCategoryUsage>, Links);

public:
    const TString& GetName() const;

    double GetWorkerCPUUsage(const ui32 workerIdx, const ui32 totalThreadsCount) const;
    ui32 GetWorkersCount(const ui32 totalThreadsCount) const;

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
        PoolName = "WP::DEFAULT";
    }

    TWorkersPool(const ui32 wpId, const std::optional<double> workersCountDouble, const std::optional<double> workersFraction);

    [[nodiscard]] TConclusionStatus DeserializeFromProto(const NKikimrConfig::TCompositeConveyorConfig::TWorkersPool& proto);
};

class TCategory {
private:
    YDB_READONLY(ESpecialTaskCategory, Category, ESpecialTaskCategory::Insert);
    YDB_READONLY(ui32, QueueSizeLimit, 256 * 1024);
    YDB_READONLY_DEF(std::vector<ui32>, WorkerPools);

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

    TConfig() = default;
    [[nodiscard]] TConclusionStatus DeserializeFromProto(const NKikimrConfig::TCompositeConveyorConfig& config);

public:
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
    YDB_READONLY(double, Weight, 1);

public:
    TCPULimitsConfig() = default;
    TCPULimitsConfig(const double cpuGroupThreadsLimit, const double weight = 1);

    TConclusionStatus DeserializeFromProto(const NKikimrTxDataShard::TEvKqpScan& config);
    TString DebugString() const;
};

}   // namespace NKikimr::NConveyorComposite
