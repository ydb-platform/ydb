#include "auto_config_initializer.h"

#include <ydb/core/protos/config.pb.h>

#include <ydb/library/actors/util/affinity.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <util/system/info.h>

namespace {

    using namespace NKikimr::NAutoConfigInitializer;

    i16 GetCpuCount() {
        TAffinity affinity;
        affinity.Current();
        TCpuMask cpuMask = static_cast<TCpuMask>(affinity);
        if (cpuMask.Size() && cpuMask.CpuCount()) {
            return cpuMask.CpuCount();
        }
        return NSystemInfo::CachedNumberOfCpus();
    }

    constexpr i16 GRpcWorkerCountInMaxPreparedCpuCase = 4;
    constexpr i16 GrpcProxyCountInMaxPreparedCpuCase = 4;
    constexpr i16 CpuCountForEachGRpcWorker = MaxPreparedCpuCount / GRpcWorkerCountInMaxPreparedCpuCase;
    constexpr i16 CpuCountForEachGRpcProxy = MaxPreparedCpuCount / GrpcProxyCountInMaxPreparedCpuCase;
    constexpr i16 GRpcHandlersPerCompletionQueueInMaxPreparedCpuCase = 1000;
    constexpr i16 GRpcHandlersPerCompletionQueuePerCpu = GRpcHandlersPerCompletionQueueInMaxPreparedCpuCase / MaxPreparedCpuCount;

    constexpr i16 SchedulerTinyCoresThreshold = 4;

    constexpr ::arc_ui64 SchedulerDefaultResolution = 64;
    constexpr ::arc_ui64 SchedulerTinyResolution = 1024;

    constexpr TExecutorPoolLayout GetDefaultExecutorPoolLayoutForCpuCount(i16 cpuCount) {
        if (cpuCount >= 4) {
            return TExecutorPoolLayout{};
        } else if (cpuCount == 3) {
            return TExecutorPoolLayout{.SystemPoolId = 0, .UserPoolId = 0, .BatchPoolId = 1, .IOPoolId = 2, .ICPoolId = 3};
        } else {
            return TExecutorPoolLayout{.SystemPoolId = 0, .UserPoolId = 0, .BatchPoolId = 0, .IOPoolId = 1, .ICPoolId = 0};
        }
    }

    constexpr std::array<ui8, PoolKindsCount> MakeLogicalToExecutorPool(
        ui8 system,
        ui8 user,
        ui8 batch,
        ui8 io,
        ui8 ic)
    {
        return {{system, user, batch, io, ic}};
    }

    constexpr TCpuTableRow MakeRowWithFiveExecutorPools(
        TPoolConfig system,
        TPoolConfig user,
        TPoolConfig batch,
        TPoolConfig io,
        TPoolConfig ic)
    {
        return TCpuTableRow{
            .ExecutorPools = {{
                TExecutorPoolConfig{
                    .Kind = EExecutorPoolKind::System,
                    .ThreadCount = system.ThreadCount,
                    .MaxThreadCount = system.MaxThreadCount,
                    .Priority = 30,
                },
                TExecutorPoolConfig{
                    .Kind = EExecutorPoolKind::User,
                    .ThreadCount = user.ThreadCount,
                    .MaxThreadCount = user.MaxThreadCount,
                    .Priority = 20,
                },
                TExecutorPoolConfig{
                    .Kind = EExecutorPoolKind::Batch,
                    .ThreadCount = batch.ThreadCount,
                    .MaxThreadCount = batch.MaxThreadCount,
                    .Priority = 10,
                },
                TExecutorPoolConfig{
                    .Kind = EExecutorPoolKind::IO,
                    .ThreadCount = io.ThreadCount,
                    .MaxThreadCount = io.MaxThreadCount,
                },
                TExecutorPoolConfig{
                    .Kind = EExecutorPoolKind::IC,
                    .ThreadCount = ic.ThreadCount,
                    .MaxThreadCount = ic.MaxThreadCount,
                    .Priority = 40,
                },
            }},
            .LogicalPoolToExecutorPool = MakeLogicalToExecutorPool(0, 1, 2, 3, 4),
            .ExecutorPoolCount = 5,
        };
    }

    constexpr TCpuTableRow MakeRowWithFourExecutorPools(
        TPoolConfig common,
        TPoolConfig batch,
        TPoolConfig io,
        TPoolConfig ic)
    {
        return TCpuTableRow{
            .ExecutorPools = {{
                TExecutorPoolConfig{
                    .Kind = EExecutorPoolKind::Common,
                    .ThreadCount = common.ThreadCount,
                    .MaxThreadCount = common.MaxThreadCount,
                    .Priority = 30,
                },
                TExecutorPoolConfig{
                    .Kind = EExecutorPoolKind::Batch,
                    .ThreadCount = batch.ThreadCount,
                    .MaxThreadCount = batch.MaxThreadCount,
                    .Priority = 10,
                },
                TExecutorPoolConfig{
                    .Kind = EExecutorPoolKind::IO,
                    .ThreadCount = io.ThreadCount,
                    .MaxThreadCount = io.MaxThreadCount,
                },
                TExecutorPoolConfig{
                    .Kind = EExecutorPoolKind::IC,
                    .ThreadCount = ic.ThreadCount,
                    .MaxThreadCount = ic.MaxThreadCount,
                    .Priority = 40,
                },
                {},
            }},
            .LogicalPoolToExecutorPool = MakeLogicalToExecutorPool(0, 0, 1, 2, 3),
            .ExecutorPoolCount = 4,
        };
    }

    constexpr TCpuTableRow MakeRowWithTwoExecutorPools(
        TPoolConfig common,
        TPoolConfig io)
    {
        return TCpuTableRow{
            .ExecutorPools = {{
                TExecutorPoolConfig{
                    .Kind = EExecutorPoolKind::Common,
                    .ThreadCount = common.ThreadCount,
                    .MaxThreadCount = common.MaxThreadCount,
                    .Priority = 40,
                },
                TExecutorPoolConfig{
                    .Kind = EExecutorPoolKind::IO,
                    .ThreadCount = io.ThreadCount,
                    .MaxThreadCount = io.MaxThreadCount,
                },
                {},
                {},
                {},
            }},
            .LogicalPoolToExecutorPool = MakeLogicalToExecutorPool(0, 0, 0, 1, 0),
            .ExecutorPoolCount = 2,
        };
    }

    constexpr TCpuTableRow MakeEmptyRow() {
        return TCpuTableRow{};
    }

    constexpr TCpuTableRow MakeTinyRow(i16 cpuCount) {
        TCpuTableRow row{
            .ExecutorPools = {{
                TExecutorPoolConfig{
                    .Kind = EExecutorPoolKind::System,
                    .Priority = 30,
                    .HasSharedThread = false,
                    .SpinThreshold = ui64{0},
                    .ForcedForeignSlots = ui32{0},
                },
                TExecutorPoolConfig{
                    .Kind = EExecutorPoolKind::User,
                    .Priority = 20,
                    .HasSharedThread = false,
                    .SpinThreshold = ui64{0},
                    .ForcedForeignSlots = ui32{0},
                },
                TExecutorPoolConfig{
                    .Kind = EExecutorPoolKind::Batch,
                    .Priority = 10,
                    .HasSharedThread = false,
                    .SpinThreshold = ui64{0},
                    .ForcedForeignSlots = ui32{0},
                },
                TExecutorPoolConfig{
                    .Kind = EExecutorPoolKind::IO,
                    .ThreadCount = 1,
                    .MaxThreadCount = 1,
                },
                TExecutorPoolConfig{
                    .Kind = EExecutorPoolKind::IC,
                    .ThreadCount = 1,
                    .MaxThreadCount = 1,
                    .Priority = 40,
                    .HasSharedThread = true,
                    .SpinThreshold = ui64{0},
                },
            }},
            .LogicalPoolToExecutorPool = MakeLogicalToExecutorPool(1, 0, 2, 3, 4),
            .ExecutorPoolCount = 5,
        };

        if (cpuCount == 1) {
            row.ExecutorPools[4].AdjacentPools = TAdjacentPoolConfig{
                .Pools = {{0, 1, 2, 0, 0}},
                .Count = 3,
            };
            return row;
        }

        row.ExecutorPools[1].ThreadCount = 1;
        row.ExecutorPools[1].MaxThreadCount = 1;
        row.ExecutorPools[1].HasSharedThread = true;
        row.ExecutorPools[1].ForcedForeignSlots = 1;
        row.ExecutorPools[1].AdjacentPools = TAdjacentPoolConfig{
            .Pools = {{2, 0, 0, 0, 0}},
            .Count = 1,
        };

        row.ExecutorPools[0].ForcedForeignSlots = 1;
        row.ExecutorPools[4].ForcedForeignSlots = 1;

        if (cpuCount == 2) {
            row.ExecutorPools[4].AdjacentPools = TAdjacentPoolConfig{
                .Pools = {{0, 0, 0, 0, 0}},
                .Count = 1,
            };
            return row;
        }

        row.ExecutorPools[0].ThreadCount = 1;
        row.ExecutorPools[0].MaxThreadCount = 1;
        row.ExecutorPools[0].HasSharedThread = true;
        row.ExecutorPools[1].ForcedForeignSlots = 2;

        return row;
    }

    const TDefaultCpuTable ComputeCpuTable({{
        MakeEmptyRow(),                                                  // 0
        MakeRowWithTwoExecutorPools({1, 1}, {0, 0}),                                  // 1
        MakeRowWithTwoExecutorPools({2, 2}, {0, 0}),                                  // 2
        MakeRowWithFourExecutorPools({1, 3}, {1, 1}, {0, 0}, {1, 1}),                 // 3
        MakeRowWithFiveExecutorPools({1, 3},  {1, 4},   {1, 1}, {0, 0}, {1, 2}),      // 4
        MakeRowWithFiveExecutorPools({1, 3},  {2, 5},   {1, 1}, {0, 0}, {1, 2}),      // 5
        MakeRowWithFiveExecutorPools({1, 3},  {3, 6},   {1, 1}, {0, 0}, {1, 3}),      // 6
        MakeRowWithFiveExecutorPools({2, 4},  {3, 7},   {1, 2}, {0, 0}, {1, 3}),      // 7
        MakeRowWithFiveExecutorPools({2, 4},  {4, 8},   {1, 2}, {0, 0}, {1, 4}),      // 8
        MakeRowWithFiveExecutorPools({2, 5},  {4, 9},   {2, 3}, {0, 0}, {1, 4}),      // 9
        MakeRowWithFiveExecutorPools({2, 5},  {5, 10},  {2, 3}, {0, 0}, {1, 5}),      // 10
        MakeRowWithFiveExecutorPools({2, 6},  {5, 11},  {2, 3}, {0, 0}, {2, 5}),      // 11
        MakeRowWithFiveExecutorPools({2, 6},  {6, 12},  {2, 3}, {0, 0}, {2, 6}),      // 12
        MakeRowWithFiveExecutorPools({3, 7},  {6, 13},  {2, 3}, {0, 0}, {2, 6}),      // 13
        MakeRowWithFiveExecutorPools({3, 7},  {6, 14},  {2, 3}, {0, 0}, {3, 7}),      // 14
        MakeRowWithFiveExecutorPools({3, 8},  {7, 15},  {2, 4}, {0, 0}, {3, 7}),      // 15
        MakeRowWithFiveExecutorPools({3, 8},  {8, 16},  {2, 4}, {0, 0}, {3, 8}),      // 16
        MakeRowWithFiveExecutorPools({3, 9},  {9, 17},  {2, 4}, {0, 0}, {3, 8}),      // 17
        MakeRowWithFiveExecutorPools({3, 9},  {9, 18},  {3, 5}, {0, 0}, {3, 9}),      // 18
        MakeRowWithFiveExecutorPools({3, 10}, {9, 19},  {3, 5}, {0, 0}, {4, 9}),      // 19
        MakeRowWithFiveExecutorPools({4, 10}, {9, 20},  {3, 5}, {0, 0}, {4, 10}),     // 20
        MakeRowWithFiveExecutorPools({4, 11}, {10, 21}, {3, 5}, {0, 0}, {4, 10}),     // 21
        MakeRowWithFiveExecutorPools({4, 11}, {11, 22}, {3, 5}, {0, 0}, {4, 11}),     // 22
        MakeRowWithFiveExecutorPools({4, 12}, {12, 23}, {3, 6}, {0, 0}, {4, 11}),     // 23
        MakeRowWithFiveExecutorPools({4, 12}, {12, 24}, {3, 6}, {0, 0}, {5, 12}),     // 24
        MakeRowWithFiveExecutorPools({5, 13}, {12, 25}, {3, 6}, {0, 0}, {5, 12}),     // 25
        MakeRowWithFiveExecutorPools({5, 13}, {12, 26}, {4, 7}, {0, 0}, {5, 13}),     // 26
        MakeRowWithFiveExecutorPools({5, 14}, {13, 27}, {4, 7}, {0, 0}, {5, 13}),     // 27
        MakeRowWithFiveExecutorPools({5, 14}, {14, 28}, {4, 7}, {0, 0}, {5, 14}),     // 28
        MakeRowWithFiveExecutorPools({5, 15}, {14, 29}, {4, 8}, {0, 0}, {6, 14}),     // 29
        MakeRowWithFiveExecutorPools({5, 15}, {15, 30}, {4, 8}, {0, 0}, {6, 15}),     // 30
    }}, 4);

    const TDefaultCpuTable HybridCpuTable({{
        MakeEmptyRow(),                                                   // 0
        MakeRowWithTwoExecutorPools({1, 1}, {0, 0}),                                   // 1
        MakeRowWithTwoExecutorPools({2, 2}, {0, 0}),                                   // 2
        MakeRowWithFourExecutorPools({1, 3}, {1, 1}, {0, 0}, {1, 1}),                  // 3
        MakeRowWithFiveExecutorPools({1, 3},   {1, 4},   {1, 1}, {0, 0}, {1, 2}),      // 4
        MakeRowWithFiveExecutorPools({1, 3},   {2, 5},   {1, 1}, {0, 0}, {1, 2}),      // 5
        MakeRowWithFiveExecutorPools({1, 3},   {2, 6},   {1, 1}, {0, 0}, {2, 3}),      // 6
        MakeRowWithFiveExecutorPools({2, 3},   {2, 7},   {1, 2}, {0, 0}, {2, 3}),      // 7
        MakeRowWithFiveExecutorPools({2, 3},   {3, 8},   {1, 2}, {0, 0}, {2, 4}),      // 8
        MakeRowWithFiveExecutorPools({2, 4},   {3, 9},   {1, 2}, {0, 0}, {3, 4}),      // 9
        MakeRowWithFiveExecutorPools({3, 4},   {3, 10},  {1, 2}, {0, 0}, {3, 5}),      // 10
        MakeRowWithFiveExecutorPools({3, 5},   {4, 11},  {1, 2}, {0, 0}, {3, 5}),      // 11
        MakeRowWithFiveExecutorPools({3, 5},   {4, 12},  {1, 3}, {0, 0}, {4, 6}),      // 12
        MakeRowWithFiveExecutorPools({4, 6},   {4, 13},  {1, 3}, {0, 0}, {4, 6}),      // 13
        MakeRowWithFiveExecutorPools({4, 6},   {5, 14},  {1, 3}, {0, 0}, {4, 7}),      // 14
        MakeRowWithFiveExecutorPools({4, 7},   {5, 15},  {1, 3}, {0, 0}, {5, 7}),      // 15
        MakeRowWithFiveExecutorPools({5, 7},   {5, 16},  {1, 3}, {0, 0}, {5, 8}),      // 16
        MakeRowWithFiveExecutorPools({5, 8},   {6, 17},  {1, 4}, {0, 0}, {5, 8}),      // 17
        MakeRowWithFiveExecutorPools({5, 8},   {6, 18},  {1, 4}, {0, 0}, {6, 9}),      // 18
        MakeRowWithFiveExecutorPools({6, 9},   {6, 19},  {1, 4}, {0, 0}, {6, 9}),      // 19
        MakeRowWithFiveExecutorPools({6, 9},   {7, 20},  {1, 4}, {0, 0}, {6, 10}),     // 20
        MakeRowWithFiveExecutorPools({6, 10},  {7, 21},  {1, 4}, {0, 0}, {7, 10}),     // 21
        MakeRowWithFiveExecutorPools({7, 10},  {7, 22},  {1, 5}, {0, 0}, {7, 11}),     // 22
        MakeRowWithFiveExecutorPools({7, 11},  {8, 23},  {1, 5}, {0, 0}, {7, 11}),     // 23
        MakeRowWithFiveExecutorPools({7, 11},  {8, 24},  {1, 5}, {0, 0}, {8, 12}),     // 24
        MakeRowWithFiveExecutorPools({8, 12},  {8, 25},  {1, 5}, {0, 0}, {8, 12}),     // 25
        MakeRowWithFiveExecutorPools({8, 12},  {9, 26},  {1, 5}, {0, 0}, {8, 13}),     // 26
        MakeRowWithFiveExecutorPools({8, 13},  {9, 27},  {1, 6}, {0, 0}, {9, 13}),     // 27
        MakeRowWithFiveExecutorPools({9, 13},  {9, 28},  {1, 6}, {0, 0}, {9, 14}),     // 28
        MakeRowWithFiveExecutorPools({9, 14},  {10, 29}, {1, 6}, {0, 0}, {9, 14}),     // 29
        MakeRowWithFiveExecutorPools({9, 14},  {10, 30}, {1, 6}, {0, 0}, {10, 15}),    // 30
    }}, 4);

    const TDefaultCpuTable StorageCpuTable({{
        MakeEmptyRow(),                                                  // 0
        MakeRowWithTwoExecutorPools({1, 1}, {0, 0}),                                  // 1
        MakeRowWithTwoExecutorPools({2, 2}, {0, 0}),                                  // 2
        MakeRowWithFourExecutorPools({1, 3}, {1, 1}, {0, 0}, {1, 1}),                 // 3
        MakeRowWithFiveExecutorPools({1, 4},   {1, 4},  {1, 1}, {0, 0}, {1, 2}),      // 4
        MakeRowWithFiveExecutorPools({2, 5},   {1, 5},  {1, 1}, {0, 0}, {1, 2}),      // 5
        MakeRowWithFiveExecutorPools({3, 6},   {1, 6},  {1, 1}, {0, 0}, {1, 3}),      // 6
        MakeRowWithFiveExecutorPools({4, 7},   {1, 7},  {1, 2}, {0, 0}, {1, 3}),      // 7
        MakeRowWithFiveExecutorPools({5, 8},   {1, 8},  {1, 2}, {0, 0}, {1, 4}),      // 8
        MakeRowWithFiveExecutorPools({5, 9},   {1, 9},  {1, 2}, {0, 0}, {2, 4}),      // 9
        MakeRowWithFiveExecutorPools({6, 10},  {1, 10}, {1, 2}, {0, 0}, {2, 5}),      // 10
        MakeRowWithFiveExecutorPools({6, 11},  {1, 11}, {2, 3}, {0, 0}, {2, 5}),      // 11
        MakeRowWithFiveExecutorPools({7, 12},  {1, 12}, {2, 3}, {0, 0}, {2, 6}),      // 12
        MakeRowWithFiveExecutorPools({8, 13},  {1, 13}, {2, 3}, {0, 0}, {2, 6}),      // 13
        MakeRowWithFiveExecutorPools({8, 14},  {1, 14}, {2, 3}, {0, 0}, {3, 7}),      // 14
        MakeRowWithFiveExecutorPools({9, 15},  {1, 15}, {2, 4}, {0, 0}, {3, 7}),      // 15
        MakeRowWithFiveExecutorPools({10, 16}, {1, 16}, {2, 4}, {0, 0}, {3, 8}),      // 16
        MakeRowWithFiveExecutorPools({11, 17}, {1, 17}, {2, 4}, {0, 0}, {3, 8}),      // 17
        MakeRowWithFiveExecutorPools({11, 18}, {1, 18}, {3, 5}, {0, 0}, {3, 9}),      // 18
        MakeRowWithFiveExecutorPools({11, 19}, {1, 19}, {3, 5}, {0, 0}, {4, 9}),      // 19
        MakeRowWithFiveExecutorPools({12, 20}, {1, 20}, {3, 5}, {0, 0}, {4, 10}),     // 20
        MakeRowWithFiveExecutorPools({13, 21}, {1, 21}, {3, 5}, {0, 0}, {4, 10}),     // 21
        MakeRowWithFiveExecutorPools({14, 22}, {1, 22}, {3, 6}, {0, 0}, {4, 11}),     // 22
        MakeRowWithFiveExecutorPools({15, 23}, {1, 23}, {3, 6}, {0, 0}, {4, 11}),     // 23
        MakeRowWithFiveExecutorPools({15, 24}, {1, 24}, {3, 6}, {0, 0}, {5, 12}),     // 24
        MakeRowWithFiveExecutorPools({16, 25}, {1, 25}, {3, 6}, {0, 0}, {5, 12}),     // 25
        MakeRowWithFiveExecutorPools({16, 26}, {1, 26}, {4, 7}, {0, 0}, {5, 13}),     // 26
        MakeRowWithFiveExecutorPools({17, 27}, {1, 27}, {4, 7}, {0, 0}, {5, 13}),     // 27
        MakeRowWithFiveExecutorPools({18, 28}, {1, 28}, {4, 7}, {0, 0}, {5, 14}),     // 28
        MakeRowWithFiveExecutorPools({18, 29}, {1, 29}, {4, 7}, {0, 0}, {6, 14}),     // 29
        MakeRowWithFiveExecutorPools({19, 30}, {1, 30}, {4, 8}, {0, 0}, {6, 15}),     // 30
    }}, 4);

    const TDefaultCpuTable TinyCpuTable = [] {
        TDefaultCpuTable table{};
        for (i16 cpuCount = 1; cpuCount <= 3; ++cpuCount) {
            table.GetPreparedRow(cpuCount) = MakeTinyRow(cpuCount);
        }
        return table;
    }();

    i16 GetIOThreadCount(i16 cpuCount) {
        return (cpuCount - 1) / (MaxPreparedCpuCount * 2) + 1;
    }

    size_t GetPoolIdx(ELogicalPoolKind pool) {
        return static_cast<size_t>(pool);
    }

    ui8 GetPoolId(const TCpuTableRow& row, ELogicalPoolKind poolKind) {
        return row.LogicalPoolToExecutorPool[GetPoolIdx(poolKind)];
    }

    TExecutorPoolLayout GetExecutorPoolLayoutForRow(const TCpuTableRow& row) {
        return TExecutorPoolLayout{
            .SystemPoolId = GetPoolId(row, ELogicalPoolKind::System),
            .UserPoolId = GetPoolId(row, ELogicalPoolKind::User),
            .BatchPoolId = GetPoolId(row, ELogicalPoolKind::Batch),
            .IOPoolId = GetPoolId(row, ELogicalPoolKind::IO),
            .ICPoolId = GetPoolId(row, ELogicalPoolKind::IC),
        };
    }

    TExecutorPoolConfig GetExecutorPoolConfig(ui8 executorPoolId, const TCpuTableRow& row) {
        return row.ExecutorPools[executorPoolId];
    }

    bool IsPreparedRow(const TCpuTableRow& row) {
        return row.ExecutorPoolCount > 0;
    }

    TString GetExecutorPoolName(EExecutorPoolKind poolKind) {
        switch (poolKind) {
            case EExecutorPoolKind::Common:
                return TExecutorPoolLayout::CommonPoolName;
            case EExecutorPoolKind::System:
                return TExecutorPoolLayout::SystemPoolName;
            case EExecutorPoolKind::User:
                return TExecutorPoolLayout::UserPoolName;
            case EExecutorPoolKind::Batch:
                return TExecutorPoolLayout::BatchPoolName;
            case EExecutorPoolKind::IO:
                return TExecutorPoolLayout::IOPoolName;
            case EExecutorPoolKind::IC:
                return TExecutorPoolLayout::ICPoolName;
        }
        Y_ABORT("Unexpected executor pool kind: %d", static_cast<int>(poolKind));
    }

    const TDefaultCpuTable& GetDefaultCpuTable(NKikimrConfig::TActorSystemConfig::ENodeType nodeType) {
        switch (nodeType) {
            case NKikimrConfig::TActorSystemConfig::STORAGE:
                return StorageCpuTable;
            case NKikimrConfig::TActorSystemConfig::COMPUTE:
                return ComputeCpuTable;
            case NKikimrConfig::TActorSystemConfig::HYBRID:
                return HybridCpuTable;
        }
        Y_ABORT("Unexpected actor system node type: %d", static_cast<int>(nodeType));
    }

    const TDefaultCpuTable& GetTinyCpuTable() {
        return TinyCpuTable;
    }

    void ValidatePoolIdLayout(const TExecutorPoolLayout& layout, ui8 executorPoolCount) {
        Y_VERIFY_S(executorPoolCount > 0 && executorPoolCount <= PoolKindsCount, "executorPoolCount# " << executorPoolCount);
        Y_VERIFY_S(layout.GetExecutorPoolCount() == executorPoolCount,
            "layout.GetExecutorPoolCount()# " << layout.GetExecutorPoolCount() << " executorPoolCount# " << executorPoolCount);
        std::array<bool, PoolKindsCount> usedPoolIds{};
        for (ui8 poolId : layout.GetIndices()) {
            Y_VERIFY_S(poolId < executorPoolCount, "poolId# " << poolId << " executorPoolCount# " << executorPoolCount);
            usedPoolIds[poolId] = true;
        }
        for (ui8 poolId = 0; poolId < executorPoolCount; ++poolId) {
            Y_VERIFY_S(usedPoolIds[poolId], "poolId# " << poolId << " is not used");
        }
    }

    void ValidateLogicalToExecutorPoolMapping(const TCpuTableRow& row) {
        for (ELogicalPoolKind logicalPoolKind : {ELogicalPoolKind::System, ELogicalPoolKind::User, ELogicalPoolKind::Batch, ELogicalPoolKind::IO, ELogicalPoolKind::IC}) {
            const ui8 executorPoolId = GetPoolId(row, logicalPoolKind);
            Y_VERIFY_S(executorPoolId < row.ExecutorPoolCount,
                "pool# " << static_cast<int>(logicalPoolKind)
                << " executorPoolId# " << executorPoolId
                << " executorPoolCount# " << row.ExecutorPoolCount);

            const EExecutorPoolKind executorPoolKind = row.ExecutorPools[executorPoolId].Kind;
            switch (executorPoolKind) {
                case EExecutorPoolKind::Common:
                case EExecutorPoolKind::System:
                case EExecutorPoolKind::User:
                case EExecutorPoolKind::Batch:
                case EExecutorPoolKind::IC:
                    Y_VERIFY_S(logicalPoolKind != ELogicalPoolKind::IO,
                        "basic pool can't be used for IO executor, executorPoolId# " << executorPoolId);
                    break;
                case EExecutorPoolKind::IO:
                    Y_VERIFY_S(logicalPoolKind == ELogicalPoolKind::IO,
                        "io pool can only map io executor, executorPoolId# " << executorPoolId);
                    break;
            }
        }
    }

    void ValidateExecutorPools(const TCpuTableRow& row, i16 cpuCount) {
        i16 cpuSum = 0;
        for (ui8 executorPoolId = 0; executorPoolId < row.ExecutorPoolCount; ++executorPoolId) {
            const TExecutorPoolConfig cfg = GetExecutorPoolConfig(executorPoolId, row);
            Y_VERIFY_S(cfg.ThreadCount >= 0,
                "executorPoolId# " << executorPoolId << " threadCount# " << cfg.ThreadCount);
            Y_VERIFY_S(cfg.MaxThreadCount >= cfg.ThreadCount,
                "executorPoolId# " << executorPoolId
                << " threadCount# " << cfg.ThreadCount
                << " maxThreadCount# " << cfg.MaxThreadCount);
            if (cfg.AdjacentPools) {
                Y_VERIFY_S(cfg.AdjacentPools->Count <= row.ExecutorPoolCount,
                    "executorPoolId# " << executorPoolId
                    << " adjacentPoolCount# " << cfg.AdjacentPools->Count
                    << " executorPoolCount# " << row.ExecutorPoolCount);
                for (ui8 adjacentPoolIdx = 0; adjacentPoolIdx < cfg.AdjacentPools->Count; ++adjacentPoolIdx) {
                    Y_VERIFY_S(cfg.AdjacentPools->Pools[adjacentPoolIdx] < row.ExecutorPoolCount,
                        "executorPoolId# " << executorPoolId
                        << " adjacentPool# " << cfg.AdjacentPools->Pools[adjacentPoolIdx]
                        << " executorPoolCount# " << row.ExecutorPoolCount);
                }
            }

            if (cfg.Kind != EExecutorPoolKind::IO) {
                cpuSum += cfg.ThreadCount;
            }
        }
        Y_VERIFY_S(cpuSum == cpuCount, "cpuSum# " << cpuSum << " cpuCount# " << cpuCount);
    }

    void ValidateCpuTable(const ICpuTable& cpuTable, i16 cpuCount) {
        const TCpuTableRow row = cpuTable[cpuCount];
        const TExecutorPoolLayout layout = GetExecutorPoolLayoutForRow(row);
        ValidatePoolIdLayout(layout, row.ExecutorPoolCount);
        ValidateLogicalToExecutorPoolMapping(row);
        ValidateExecutorPools(row, cpuCount);
    }

    void ValidateCustomCpuTable(const ICpuTable& cpuTable, i16 cpuCount) {
        if (cpuCount <= MaxPreparedCpuCount) {
            return;
        }

        const auto* defaultCpuTable = dynamic_cast<const TDefaultCpuTable*>(&cpuTable);
        if (!defaultCpuTable) {
            return;
        }

        Y_VERIFY_S(defaultCpuTable->MinScalableRowCpuCount > 0,
            "minScalableRowCpuCount# " << defaultCpuTable->MinScalableRowCpuCount);
        Y_VERIFY_S(defaultCpuTable->MinScalableRowCpuCount <= MaxPreparedCpuCount,
            "minScalableRowCpuCount# " << defaultCpuTable->MinScalableRowCpuCount
            << " maxPreparedCpuCount# " << MaxPreparedCpuCount);

        for (i16 rowCpuCount = defaultCpuTable->MinScalableRowCpuCount; rowCpuCount <= MaxPreparedCpuCount; ++rowCpuCount) {
            Y_VERIFY_S(IsPreparedRow(defaultCpuTable->GetPreparedRow(rowCpuCount)),
                "custom cpu table is sparse, row for cpuCount# " << rowCpuCount
                << " must be defined to scale beyond " << MaxPreparedCpuCount);
        }
    }

    void AddExecutorFromPoolConfig(
        NKikimrConfig::TActorSystemConfig* config,
        const TExecutorPoolConfig& executorPoolConfig,
        i16 cpuCount,
        bool useSharedThreads,
        bool useUnitedPool)
    {
        auto* executor = config->AddExecutor();
        const TString poolName = GetExecutorPoolName(executorPoolConfig.Kind);

        if (executorPoolConfig.Kind == EExecutorPoolKind::IO) {
            executor->SetType(NKikimrConfig::TActorSystemConfig::TExecutor::IO);
            ui32 ioThreadCount = executorPoolConfig.ThreadCount > 0 ? executorPoolConfig.ThreadCount : GetIOThreadCount(cpuCount);
            if (config->HasForceIOPoolThreads()) {
                ioThreadCount = config->GetForceIOPoolThreads();
            }
            executor->SetThreads(ioThreadCount);
            executor->SetName(poolName);
            return;
        }

        const bool hasSharedThread = executorPoolConfig.HasSharedThread.value_or(useSharedThreads);
        if (hasSharedThread) {
            executor->SetHasSharedThread(true);
        }
        executor->SetType(NKikimrConfig::TActorSystemConfig::TExecutor::BASIC);
        executor->SetThreads(executorPoolConfig.ThreadCount);
        executor->SetMaxThreads(Max(executorPoolConfig.MaxThreadCount, executorPoolConfig.ThreadCount));
        executor->SetPriority(executorPoolConfig.Priority);
        executor->SetName(poolName);
        executor->SetAllThreadsAreShared(useUnitedPool);

        if (executorPoolConfig.SpinThreshold) {
            executor->SetSpinThreshold(*executorPoolConfig.SpinThreshold);
        } else if (executorPoolConfig.Kind == EExecutorPoolKind::Common) {
            executor->SetSpinThreshold(0);
        } else if (executorPoolConfig.Kind == EExecutorPoolKind::IC) {
            executor->SetSpinThreshold(10);
        } else {
            executor->SetSpinThreshold(1);
        }

        if (executorPoolConfig.TimePerMailboxMicroSecs) {
            executor->SetTimePerMailboxMicroSecs(*executorPoolConfig.TimePerMailboxMicroSecs);
        } else if (executorPoolConfig.Kind == EExecutorPoolKind::Common || executorPoolConfig.Kind == EExecutorPoolKind::IC) {
            executor->SetTimePerMailboxMicroSecs(100);
        }

        if (executorPoolConfig.MaxAvgPingDeviation) {
            executor->SetMaxAvgPingDeviation(*executorPoolConfig.MaxAvgPingDeviation);
        } else if (executorPoolConfig.Kind == EExecutorPoolKind::IC) {
            executor->SetMaxAvgPingDeviation(500);
        }

        if (executorPoolConfig.ForcedForeignSlots) {
            executor->SetForcedForeignSlots(*executorPoolConfig.ForcedForeignSlots);
        }
        if (executorPoolConfig.AdjacentPools) {
            for (ui8 adjacentPoolIdx = 0; adjacentPoolIdx < executorPoolConfig.AdjacentPools->Count; ++adjacentPoolIdx) {
                executor->AddAdjacentPools(executorPoolConfig.AdjacentPools->Pools[adjacentPoolIdx]);
            }
        }

        if (config->HasMinLocalQueueSize()) {
            executor->SetMinLocalQueueSize(config->GetMinLocalQueueSize());
        }
        if (config->HasMaxLocalQueueSize()) {
            executor->SetMaxLocalQueueSize(config->GetMaxLocalQueueSize());
        }
    }

} // anonymous

namespace NKikimr::NAutoConfigInitializer {

    TCpuTableRow TDefaultCpuTable::operator[](i16 cpuCount) const {
        Y_ABORT_UNLESS(cpuCount >= 0);
        if (cpuCount <= MaxPreparedCpuCount) {
            return Rows[cpuCount];
        }

        Y_ABORT_UNLESS(MinScalableRowCpuCount > 0);
        Y_ABORT_UNLESS(MinScalableRowCpuCount <= MaxPreparedCpuCount);

        TCpuTableRow result{};
        bool initialized = false;
        i16 remaining = cpuCount;

        while (remaining > 0) {
            i16 chunkCpuCount = remaining;
            if (chunkCpuCount > MaxPreparedCpuCount) {
                const i16 tail = remaining - MaxPreparedCpuCount;
                if (tail == 0 || tail >= MinScalableRowCpuCount) {
                    chunkCpuCount = MaxPreparedCpuCount;
                } else {
                    chunkCpuCount = MaxPreparedCpuCount - (MinScalableRowCpuCount - tail);
                }
            }

            const TCpuTableRow& chunk = Rows[chunkCpuCount];
            Y_VERIFY_S(IsPreparedRow(chunk),
                "cpu table row for cpuCount# " << chunkCpuCount << " is not defined");
            if (!initialized) {
                result = chunk;
                initialized = true;
            } else {
                Y_ABORT_UNLESS(result.ExecutorPoolCount == chunk.ExecutorPoolCount);
                Y_ABORT_UNLESS(result.LogicalPoolToExecutorPool == chunk.LogicalPoolToExecutorPool);
                for (ui8 executorPoolId = 0; executorPoolId < result.ExecutorPoolCount; ++executorPoolId) {
                    Y_ABORT_UNLESS(result.ExecutorPools[executorPoolId].Kind == chunk.ExecutorPools[executorPoolId].Kind);
                    result.ExecutorPools[executorPoolId].ThreadCount += chunk.ExecutorPools[executorPoolId].ThreadCount;
                    result.ExecutorPools[executorPoolId].MaxThreadCount += chunk.ExecutorPools[executorPoolId].MaxThreadCount;
                }
            }

            remaining -= chunkCpuCount;
        }

        return result;
    }

    TExecutorPoolLayout GetExecutorPoolLayout(i16 cpuCount) {
        Y_ABORT_UNLESS(cpuCount >= 0);
        if (cpuCount == 0) {
            cpuCount = GetCpuCount();
        }
        Y_ABORT_UNLESS(cpuCount > 0, "Can't read cpu count of this system");
        return GetDefaultExecutorPoolLayoutForCpuCount(cpuCount);
    }

    TExecutorPoolLayout GetExecutorPoolLayout(const NKikimrConfig::TActorSystemConfig &config, bool useAutoConfig) {
        const bool hasExplicitPoolIds =
            config.HasSysExecutor() ||
            config.HasUserExecutor() ||
            config.HasBatchExecutor() ||
            config.HasIoExecutor() ||
            config.ServiceExecutorSize();
        const bool hasMaterializedExecutors = config.ExecutorSize() > 0;

        if (useAutoConfig && (!hasExplicitPoolIds || !hasMaterializedExecutors)) {
            i16 cpuCount = (config.HasCpuCount() ? config.GetCpuCount() : 0);
            return GetExecutorPoolLayout(cpuCount);
        } else {
            ui8 icPoolId = 0;
            for (ui32 i = 0; i < config.ServiceExecutorSize(); ++i) {
                auto item = config.GetServiceExecutor(i);
                const TString service = item.GetServiceName();
                if (service == "Interconnect") {
                    icPoolId = static_cast<ui8>(item.GetExecutorId());
                    break;
                }
            }

            return TExecutorPoolLayout {
                .SystemPoolId = static_cast<ui8>(config.HasSysExecutor() ? config.GetSysExecutor() : 0),
                .UserPoolId = static_cast<ui8>(config.HasUserExecutor() ? config.GetUserExecutor() : 0),
                .BatchPoolId = static_cast<ui8>(config.HasBatchExecutor() ? config.GetBatchExecutor() : 0),
                .IOPoolId = static_cast<ui8>(config.HasIoExecutor() ? config.GetIoExecutor() : 0),
                .ICPoolId = icPoolId,
            };
        }
    }

    TMap<TString, ui32> GetServicePools(const NKikimrConfig::TActorSystemConfig &config, bool useAutoConfig) {
        TMap<TString, ui32> servicePools;
        if (useAutoConfig) {
            TExecutorPoolLayout pools = GetExecutorPoolLayout(config, useAutoConfig);
            servicePools =  {{"Interconnect", pools.ICPoolId}};
        }
        for (ui32 i = 0; i < config.ServiceExecutorSize(); ++i) {
            auto item = config.GetServiceExecutor(i);
            const TString service = item.GetServiceName();
            if (servicePools.count(service)) {
                continue;
            }
            const ui32 pool = item.GetExecutorId();
            servicePools.insert(std::pair<TString, ui32>(service, pool));
        }
        return servicePools;
    }

    void ApplyAutoConfig(NKikimrConfig::TActorSystemConfig *config, const TAutoConfigOptions& options) {
        config->SetUseAutoConfig(true);
        config->ClearExecutor();

        i16 cpuCount = config->HasCpuCount() ? config->GetCpuCount() : GetCpuCount();
        Y_ABORT_UNLESS(cpuCount);
        config->SetCpuCount(cpuCount);

        bool useSharedThreads = config->GetUseSharedThreads();
        bool useUnitedPool = config->GetUseUnitedPool();
        const bool useLowCpuProfile = options.ForceTinyConfiguration || (cpuCount >= 1 && cpuCount <= SchedulerTinyCoresThreshold);

        if (!config->HasScheduler()) {
            auto *scheduler = config->MutableScheduler();
            scheduler->SetResolution(useLowCpuProfile ? SchedulerTinyResolution : SchedulerDefaultResolution);

            scheduler->SetSpinThreshold(0);
            scheduler->SetProgressThreshold(10'000);
        }

        if (!config->HasActorSystemProfile()) {
            if (useLowCpuProfile) {
                config->SetActorSystemProfile(NKikimrConfig::TActorSystemConfig::LOW_CPU_CONSUMPTION);
            } else {
                config->SetActorSystemProfile(NKikimrConfig::TActorSystemConfig::LOW_LATENCY);
            }
        }

        if (!config->HasNodeType()) {
            config->SetNodeType(options.IsDynamicNode ? NKikimrConfig::TActorSystemConfig::COMPUTE : NKikimrConfig::TActorSystemConfig::STORAGE);
        }

        const bool useTinyExecutorConfiguration = useLowCpuProfile && useSharedThreads;
        const ICpuTable& cpuTable = options.CpuTable
            ? *options.CpuTable
            : useTinyExecutorConfiguration && cpuCount <= 3
                ? GetTinyCpuTable()
                : GetDefaultCpuTable(config->GetNodeType());
        if (options.CpuTable) {
            ValidateCustomCpuTable(cpuTable, cpuCount);
        }
        ValidateCpuTable(cpuTable, cpuCount);

        const TCpuTableRow row = cpuTable[cpuCount];
        const TExecutorPoolLayout pools = GetExecutorPoolLayoutForRow(row);
        const ui8 poolCount = row.ExecutorPoolCount;

        config->SetSysExecutor(pools.SystemPoolId);
        config->SetUserExecutor(pools.UserPoolId);
        config->SetBatchExecutor(pools.BatchPoolId);
        config->SetIoExecutor(pools.IOPoolId);

        auto *serviceExecutor = [&]() -> decltype(config->AddServiceExecutor()) {
            for (ui32 i = 0; i < config->ServiceExecutorSize(); ++i) {
                auto* existing = config->MutableServiceExecutor(i);
                if (existing->GetServiceName() == "Interconnect") {
                    return existing;
                }
            }

            auto* added = config->AddServiceExecutor();
            added->SetServiceName("Interconnect");
            return added;
        }();
        serviceExecutor->SetExecutorId(pools.ICPoolId);

        for (ui32 poolIdx = 0; poolIdx < poolCount; ++poolIdx) {
            AddExecutorFromPoolConfig(config, GetExecutorPoolConfig(poolIdx, row), cpuCount, useSharedThreads, useUnitedPool);
        }
    }

    void ApplyAutoConfig(NKikimrConfig::TGRpcConfig *config, const NKikimrConfig::TActorSystemConfig &asConfig) {
        i16 cpuCount = asConfig.HasCpuCount() ? asConfig.GetCpuCount() : GetCpuCount();

        if (!config->HasWorkerThreads()) {
            config->SetWorkerThreads(Max(2, cpuCount / CpuCountForEachGRpcWorker));
        }
        if (!config->HasHandlersPerCompletionQueue()) {
            config->SetHandlersPerCompletionQueue(GRpcHandlersPerCompletionQueuePerCpu * cpuCount);
        }
        if (!config->HasGRpcProxyCount()) {
            config->SetGRpcProxyCount(Max(2, cpuCount / CpuCountForEachGRpcProxy));
        }
    }

} // NKikimr::NActorSystemInitializer

namespace NKikimr {
    bool NeedToUseAutoConfig(const NKikimrConfig::TActorSystemConfig& config) {
        bool hasSpecialFields = config.HasNodeType() || config.HasCpuCount();
        if (!config.HasUseAutoConfig() && hasSpecialFields) {
            return true;
        }
        return config.GetUseAutoConfig();
    }
}
