#pragma once

#include <util/system/types.h>
#include <util/generic/fwd.h>
#include <util/generic/map.h>

#include <array>
#include <optional>
#include <vector>

namespace NKikimrConfig {

    class TActorSystemConfig;
    class TGRpcConfig;

}

namespace NKikimr::NAutoConfigInitializer {

    enum class ELogicalPoolKind : ui8 {
        System = 0,
        User = 1,
        Batch = 2,
        IO = 3,
        IC = 4,
    };

    struct TPoolConfig {
        i16 ThreadCount = 0;
        i16 MaxThreadCount = 0;
    };

    enum class EExecutorPoolKind : ui8 {
        Common = 0,
        System = 1,
        User = 2,
        Batch = 3,
        IO = 4,
        IC = 5,
    };

    static constexpr size_t PoolKindsCount = 5;
    static_assert(static_cast<size_t>(ELogicalPoolKind::IC) + 1 == PoolKindsCount);
    static constexpr i16 MaxPreparedCpuCount = 30;

    struct TAdjacentPoolConfig {
        std::array<ui8, PoolKindsCount> Pools{};
        ui8 Count = 0;
    };

    struct TExecutorPoolConfig {
        EExecutorPoolKind Kind = EExecutorPoolKind::Common;
        i16 ThreadCount = 0;
        i16 MaxThreadCount = 0;
        ui8 Priority = 0;
        std::optional<bool> HasSharedThread;
        std::optional<ui64> SpinThreshold;
        std::optional<ui32> TimePerMailboxMicroSecs;
        std::optional<i32> MaxAvgPingDeviation;
        std::optional<ui32> ForcedForeignSlots;
        std::optional<TAdjacentPoolConfig> AdjacentPools;
    };

    struct TExecutorPoolLayout {
        static constexpr auto CommonPoolName = "Common";
        static constexpr auto SystemPoolName = "System";
        static constexpr auto UserPoolName = "User";
        static constexpr auto BatchPoolName = "Batch";
        static constexpr auto IOPoolName = "IO";
        static constexpr auto ICPoolName = "IC";

        ui8 SystemPoolId = 0;
        ui8 UserPoolId = 1;
        ui8 BatchPoolId = 2;
        ui8 IOPoolId = 3;
        ui8 ICPoolId = 4;

        ui8 GetExecutorPoolCount() const {
            return 1 + Max(SystemPoolId, UserPoolId, BatchPoolId, IOPoolId, ICPoolId);
        }

        std::vector<ui8> GetIndices() const {
            return {
                SystemPoolId,
                UserPoolId,
                BatchPoolId,
                IOPoolId,
                ICPoolId
            };
        }

        std::vector<TString> GetExecutorPoolNames() const {
            switch (GetExecutorPoolCount()) {
            case 1:
                return {CommonPoolName};
            case 2:
                return {CommonPoolName, IOPoolName};
            case 3:
                return {CommonPoolName, BatchPoolName, IOPoolName};
            case 4:
                return {CommonPoolName, BatchPoolName, IOPoolName, ICPoolName};
            default:
                return {SystemPoolName, UserPoolName, BatchPoolName, IOPoolName, ICPoolName};
            }
        }

        std::vector<ui8> GetPriorities() const {
            switch (GetExecutorPoolCount()) {
            case 1:
                return {40};
            case 2:
                return {40, 0};
            case 3:
                return {40, 10, 0,};
            case 4:
                return {30, 10, 0, 40};
            default:
                return {30, 20, 10, 0, 40};
            }
        }
    };

    struct TCpuTableRow {
        std::array<TExecutorPoolConfig, PoolKindsCount> ExecutorPools{};
        std::array<ui8, PoolKindsCount> LogicalPoolToExecutorPool{};
        ui8 ExecutorPoolCount = 0;
    };

    struct ICpuTable {
        virtual ~ICpuTable() = default;
        virtual TCpuTableRow operator[](i16 cpuCount) const = 0;
    };

    struct TDefaultCpuTable : ICpuTable {
        std::array<TCpuTableRow, MaxPreparedCpuCount + 1> Rows{};
        // Smallest prepared row that may participate in chunked scaling above MaxPreparedCpuCount.
        i16 MinScalableRowCpuCount = 1;

        TDefaultCpuTable() = default;

        TDefaultCpuTable(
            std::array<TCpuTableRow, MaxPreparedCpuCount + 1> rows,
            i16 minScalableRowCpuCount = 1)
            : Rows(rows)
            , MinScalableRowCpuCount(minScalableRowCpuCount)
        {}

        TCpuTableRow operator[](i16 cpuCount) const override;

        TCpuTableRow& GetPreparedRow(size_t idx) {
            return Rows[idx];
        }

        const TCpuTableRow& GetPreparedRow(size_t idx) const {
            return Rows[idx];
        }
    };

    struct TAutoConfigOptions {
        bool IsDynamicNode = false;
        bool ForceTinyConfiguration = false;
        const ICpuTable* CpuTable = nullptr;
    };

    TExecutorPoolLayout GetExecutorPoolLayout(i16 cpuCount = 0);

    TExecutorPoolLayout GetExecutorPoolLayout(const NKikimrConfig::TActorSystemConfig &config, bool useAutoConfig);

    TMap<TString, ui32> GetServicePools(const NKikimrConfig::TActorSystemConfig &config, bool useAutoConfig);

    void ApplyAutoConfig(NKikimrConfig::TActorSystemConfig *config, const TAutoConfigOptions& options);

    void ApplyAutoConfig(NKikimrConfig::TGRpcConfig *config, const NKikimrConfig::TActorSystemConfig &asConfig);

} // NKikimr::NActorSystemInitializer

namespace NKikimr {
    bool NeedToUseAutoConfig(const NKikimrConfig::TActorSystemConfig& config);
}
