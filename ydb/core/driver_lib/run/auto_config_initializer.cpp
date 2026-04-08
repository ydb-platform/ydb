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

    constexpr TASPools GetDefaultASPoolsForCpuCount(i16 cpuCount) {
        if (cpuCount >= 4) {
            return TASPools{};
        } else if (cpuCount == 3) {
            return TASPools{.SystemPoolId = 0, .UserPoolId = 0, .BatchPoolId = 1, .IOPoolId = 2, .ICPoolId = 3};
        } else {
            return TASPools{.SystemPoolId = 0, .UserPoolId = 0, .BatchPoolId = 0, .IOPoolId = 1, .ICPoolId = 0};
        }
    }

    constexpr std::array<ui8, PoolKindsCount> MakeLogicalToRealPool(
        ui8 system,
        ui8 user,
        ui8 batch,
        ui8 io,
        ui8 ic)
    {
        return {{system, user, batch, io, ic}};
    }

    constexpr TCpuTableRow MakeFivePoolRow(
        TPoolConfig system,
        TPoolConfig user,
        TPoolConfig batch,
        TPoolConfig io,
        TPoolConfig ic)
    {
        return TCpuTableRow{
            .RealPools = {{
                TRealPoolConfig{
                    .Kind = ERealPoolKind::System,
                    .ThreadCount = system.ThreadCount,
                    .MaxThreadCount = system.MaxThreadCount,
                    .Priority = 30,
                },
                TRealPoolConfig{
                    .Kind = ERealPoolKind::User,
                    .ThreadCount = user.ThreadCount,
                    .MaxThreadCount = user.MaxThreadCount,
                    .Priority = 20,
                },
                TRealPoolConfig{
                    .Kind = ERealPoolKind::Batch,
                    .ThreadCount = batch.ThreadCount,
                    .MaxThreadCount = batch.MaxThreadCount,
                    .Priority = 10,
                },
                TRealPoolConfig{
                    .Kind = ERealPoolKind::IO,
                    .ThreadCount = io.ThreadCount,
                    .MaxThreadCount = io.MaxThreadCount,
                },
                TRealPoolConfig{
                    .Kind = ERealPoolKind::IC,
                    .ThreadCount = ic.ThreadCount,
                    .MaxThreadCount = ic.MaxThreadCount,
                    .Priority = 40,
                },
            }},
            .LogicalToRealPool = MakeLogicalToRealPool(0, 1, 2, 3, 4),
            .RealPoolCount = 5,
        };
    }

    constexpr TCpuTableRow MakeFourPoolRow(
        TPoolConfig common,
        TPoolConfig batch,
        TPoolConfig io,
        TPoolConfig ic)
    {
        return TCpuTableRow{
            .RealPools = {{
                TRealPoolConfig{
                    .Kind = ERealPoolKind::Common,
                    .ThreadCount = common.ThreadCount,
                    .MaxThreadCount = common.MaxThreadCount,
                    .Priority = 30,
                },
                TRealPoolConfig{
                    .Kind = ERealPoolKind::Batch,
                    .ThreadCount = batch.ThreadCount,
                    .MaxThreadCount = batch.MaxThreadCount,
                    .Priority = 10,
                },
                TRealPoolConfig{
                    .Kind = ERealPoolKind::IO,
                    .ThreadCount = io.ThreadCount,
                    .MaxThreadCount = io.MaxThreadCount,
                },
                TRealPoolConfig{
                    .Kind = ERealPoolKind::IC,
                    .ThreadCount = ic.ThreadCount,
                    .MaxThreadCount = ic.MaxThreadCount,
                    .Priority = 40,
                },
                {},
            }},
            .LogicalToRealPool = MakeLogicalToRealPool(0, 0, 1, 2, 3),
            .RealPoolCount = 4,
        };
    }

    constexpr TCpuTableRow MakeTwoPoolRow(
        TPoolConfig common,
        TPoolConfig io)
    {
        return TCpuTableRow{
            .RealPools = {{
                TRealPoolConfig{
                    .Kind = ERealPoolKind::Common,
                    .ThreadCount = common.ThreadCount,
                    .MaxThreadCount = common.MaxThreadCount,
                    .Priority = 40,
                },
                TRealPoolConfig{
                    .Kind = ERealPoolKind::IO,
                    .ThreadCount = io.ThreadCount,
                    .MaxThreadCount = io.MaxThreadCount,
                },
                {},
                {},
                {},
            }},
            .LogicalToRealPool = MakeLogicalToRealPool(0, 0, 0, 1, 0),
            .RealPoolCount = 2,
        };
    }

    constexpr TCpuTableRow MakeEmptyRow() {
        return TCpuTableRow{};
    }

    constexpr TCpuTableRow MakeTinyRow1() {
        return TCpuTableRow{
            .RealPools = {{
                TRealPoolConfig{
                    .Kind = ERealPoolKind::System,
                    .Priority = 30,
                    .HasSharedThread = false,
                    .SpinThreshold = ui64{0},
                    .ForcedForeignSlots = ui32{0},
                },
                TRealPoolConfig{
                    .Kind = ERealPoolKind::User,
                    .Priority = 20,
                    .HasSharedThread = false,
                    .SpinThreshold = ui64{0},
                    .ForcedForeignSlots = ui32{0},
                },
                TRealPoolConfig{
                    .Kind = ERealPoolKind::Batch,
                    .Priority = 10,
                    .HasSharedThread = false,
                    .SpinThreshold = ui64{0},
                    .ForcedForeignSlots = ui32{0},
                },
                TRealPoolConfig{
                    .Kind = ERealPoolKind::IO,
                    .ThreadCount = 1,
                    .MaxThreadCount = 1,
                },
                TRealPoolConfig{
                    .Kind = ERealPoolKind::IC,
                    .ThreadCount = 1,
                    .MaxThreadCount = 1,
                    .Priority = 40,
                    .HasSharedThread = true,
                    .SpinThreshold = ui64{0},
                    .ForcedForeignSlots = ui32{0},
                    .AdjacentPools = TAdjacentPoolConfig{
                        .Pools = {{0, 1, 2, 0, 0}},
                        .Count = 3,
                    },
                },
            }},
            .LogicalToRealPool = MakeLogicalToRealPool(1, 0, 2, 3, 4),
            .RealPoolCount = 5,
        };
    }

    constexpr TCpuTableRow MakeTinyRow2() {
        return TCpuTableRow{
            .RealPools = {{
                TRealPoolConfig{
                    .Kind = ERealPoolKind::System,
                    .Priority = 30,
                    .HasSharedThread = false,
                    .SpinThreshold = ui64{0},
                    .ForcedForeignSlots = ui32{1},
                },
                TRealPoolConfig{
                    .Kind = ERealPoolKind::User,
                    .ThreadCount = 1,
                    .MaxThreadCount = 1,
                    .Priority = 20,
                    .HasSharedThread = true,
                    .SpinThreshold = ui64{0},
                    .ForcedForeignSlots = ui32{1},
                    .AdjacentPools = TAdjacentPoolConfig{
                        .Pools = {{2, 0, 0, 0, 0}},
                        .Count = 1,
                    },
                },
                TRealPoolConfig{
                    .Kind = ERealPoolKind::Batch,
                    .Priority = 10,
                    .HasSharedThread = false,
                    .SpinThreshold = ui64{0},
                    .ForcedForeignSlots = ui32{0},
                },
                TRealPoolConfig{
                    .Kind = ERealPoolKind::IO,
                    .ThreadCount = 1,
                    .MaxThreadCount = 1,
                },
                TRealPoolConfig{
                    .Kind = ERealPoolKind::IC,
                    .ThreadCount = 1,
                    .MaxThreadCount = 1,
                    .Priority = 40,
                    .HasSharedThread = true,
                    .SpinThreshold = ui64{0},
                    .ForcedForeignSlots = ui32{1},
                    .AdjacentPools = TAdjacentPoolConfig{
                        .Pools = {{0, 0, 0, 0, 0}},
                        .Count = 1,
                    },
                },
            }},
            .LogicalToRealPool = MakeLogicalToRealPool(1, 0, 2, 3, 4),
            .RealPoolCount = 5,
        };
    }

    constexpr TCpuTableRow MakeTinyRow3() {
        return TCpuTableRow{
            .RealPools = {{
                TRealPoolConfig{
                    .Kind = ERealPoolKind::System,
                    .ThreadCount = 1,
                    .MaxThreadCount = 1,
                    .Priority = 30,
                    .HasSharedThread = true,
                    .SpinThreshold = ui64{0},
                    .ForcedForeignSlots = ui32{1},
                },
                TRealPoolConfig{
                    .Kind = ERealPoolKind::User,
                    .ThreadCount = 1,
                    .MaxThreadCount = 1,
                    .Priority = 20,
                    .HasSharedThread = true,
                    .SpinThreshold = ui64{0},
                    .ForcedForeignSlots = ui32{2},
                    .AdjacentPools = TAdjacentPoolConfig{
                        .Pools = {{2, 0, 0, 0, 0}},
                        .Count = 1,
                    },
                },
                TRealPoolConfig{
                    .Kind = ERealPoolKind::Batch,
                    .Priority = 10,
                    .HasSharedThread = false,
                    .SpinThreshold = ui64{0},
                    .ForcedForeignSlots = ui32{0},
                },
                TRealPoolConfig{
                    .Kind = ERealPoolKind::IO,
                    .ThreadCount = 1,
                    .MaxThreadCount = 1,
                },
                TRealPoolConfig{
                    .Kind = ERealPoolKind::IC,
                    .ThreadCount = 1,
                    .MaxThreadCount = 1,
                    .Priority = 40,
                    .HasSharedThread = true,
                    .SpinThreshold = ui64{0},
                    .ForcedForeignSlots = ui32{1},
                },
            }},
            .LogicalToRealPool = MakeLogicalToRealPool(1, 0, 2, 3, 4),
            .RealPoolCount = 5,
        };
    }

    const TDefaultCpuTable ComputeCpuTable({{
        MakeEmptyRow(),                                                  // 0
        MakeTwoPoolRow({1, 1}, {0, 0}),                                  // 1
        MakeTwoPoolRow({2, 2}, {0, 0}),                                  // 2
        MakeFourPoolRow({1, 3}, {1, 1}, {0, 0}, {1, 1}),                 // 3
        MakeFivePoolRow({1, 3},  {1, 4},   {1, 1}, {0, 0}, {1, 2}),      // 4
        MakeFivePoolRow({1, 3},  {2, 5},   {1, 1}, {0, 0}, {1, 2}),      // 5
        MakeFivePoolRow({1, 3},  {3, 6},   {1, 1}, {0, 0}, {1, 3}),      // 6
        MakeFivePoolRow({2, 4},  {3, 7},   {1, 2}, {0, 0}, {1, 3}),      // 7
        MakeFivePoolRow({2, 4},  {4, 8},   {1, 2}, {0, 0}, {1, 4}),      // 8
        MakeFivePoolRow({2, 5},  {4, 9},   {2, 3}, {0, 0}, {1, 4}),      // 9
        MakeFivePoolRow({2, 5},  {5, 10},  {2, 3}, {0, 0}, {1, 5}),      // 10
        MakeFivePoolRow({2, 6},  {5, 11},  {2, 3}, {0, 0}, {2, 5}),      // 11
        MakeFivePoolRow({2, 6},  {6, 12},  {2, 3}, {0, 0}, {2, 6}),      // 12
        MakeFivePoolRow({3, 7},  {6, 13},  {2, 3}, {0, 0}, {2, 6}),      // 13
        MakeFivePoolRow({3, 7},  {6, 14},  {2, 3}, {0, 0}, {3, 7}),      // 14
        MakeFivePoolRow({3, 8},  {7, 15},  {2, 4}, {0, 0}, {3, 7}),      // 15
        MakeFivePoolRow({3, 8},  {8, 16},  {2, 4}, {0, 0}, {3, 8}),      // 16
        MakeFivePoolRow({3, 9},  {9, 17},  {2, 4}, {0, 0}, {3, 8}),      // 17
        MakeFivePoolRow({3, 9},  {9, 18},  {3, 5}, {0, 0}, {3, 9}),      // 18
        MakeFivePoolRow({3, 10}, {9, 19},  {3, 5}, {0, 0}, {4, 9}),      // 19
        MakeFivePoolRow({4, 10}, {9, 20},  {3, 5}, {0, 0}, {4, 10}),     // 20
        MakeFivePoolRow({4, 11}, {10, 21}, {3, 5}, {0, 0}, {4, 10}),     // 21
        MakeFivePoolRow({4, 11}, {11, 22}, {3, 5}, {0, 0}, {4, 11}),     // 22
        MakeFivePoolRow({4, 12}, {12, 23}, {3, 6}, {0, 0}, {4, 11}),     // 23
        MakeFivePoolRow({4, 12}, {12, 24}, {3, 6}, {0, 0}, {5, 12}),     // 24
        MakeFivePoolRow({5, 13}, {12, 25}, {3, 6}, {0, 0}, {5, 12}),     // 25
        MakeFivePoolRow({5, 13}, {12, 26}, {4, 7}, {0, 0}, {5, 13}),     // 26
        MakeFivePoolRow({5, 14}, {13, 27}, {4, 7}, {0, 0}, {5, 13}),     // 27
        MakeFivePoolRow({5, 14}, {14, 28}, {4, 7}, {0, 0}, {5, 14}),     // 28
        MakeFivePoolRow({5, 15}, {14, 29}, {4, 8}, {0, 0}, {6, 14}),     // 29
        MakeFivePoolRow({5, 15}, {15, 30}, {4, 8}, {0, 0}, {6, 15}),     // 30
    }}, 4);

    const TDefaultCpuTable HybridCpuTable({{
        MakeEmptyRow(),                                                   // 0
        MakeTwoPoolRow({1, 1}, {0, 0}),                                   // 1
        MakeTwoPoolRow({2, 2}, {0, 0}),                                   // 2
        MakeFourPoolRow({1, 3}, {1, 1}, {0, 0}, {1, 1}),                  // 3
        MakeFivePoolRow({1, 3},   {1, 4},   {1, 1}, {0, 0}, {1, 2}),      // 4
        MakeFivePoolRow({1, 3},   {2, 5},   {1, 1}, {0, 0}, {1, 2}),      // 5
        MakeFivePoolRow({1, 3},   {2, 6},   {1, 1}, {0, 0}, {2, 3}),      // 6
        MakeFivePoolRow({2, 3},   {2, 7},   {1, 2}, {0, 0}, {2, 3}),      // 7
        MakeFivePoolRow({2, 3},   {3, 8},   {1, 2}, {0, 0}, {2, 4}),      // 8
        MakeFivePoolRow({2, 4},   {3, 9},   {1, 2}, {0, 0}, {3, 4}),      // 9
        MakeFivePoolRow({3, 4},   {3, 10},  {1, 2}, {0, 0}, {3, 5}),      // 10
        MakeFivePoolRow({3, 5},   {4, 11},  {1, 2}, {0, 0}, {3, 5}),      // 11
        MakeFivePoolRow({3, 5},   {4, 12},  {1, 3}, {0, 0}, {4, 6}),      // 12
        MakeFivePoolRow({4, 6},   {4, 13},  {1, 3}, {0, 0}, {4, 6}),      // 13
        MakeFivePoolRow({4, 6},   {5, 14},  {1, 3}, {0, 0}, {4, 7}),      // 14
        MakeFivePoolRow({4, 7},   {5, 15},  {1, 3}, {0, 0}, {5, 7}),      // 15
        MakeFivePoolRow({5, 7},   {5, 16},  {1, 3}, {0, 0}, {5, 8}),      // 16
        MakeFivePoolRow({5, 8},   {6, 17},  {1, 4}, {0, 0}, {5, 8}),      // 17
        MakeFivePoolRow({5, 8},   {6, 18},  {1, 4}, {0, 0}, {6, 9}),      // 18
        MakeFivePoolRow({6, 9},   {6, 19},  {1, 4}, {0, 0}, {6, 9}),      // 19
        MakeFivePoolRow({6, 9},   {7, 20},  {1, 4}, {0, 0}, {6, 10}),     // 20
        MakeFivePoolRow({6, 10},  {7, 21},  {1, 4}, {0, 0}, {7, 10}),     // 21
        MakeFivePoolRow({7, 10},  {7, 22},  {1, 5}, {0, 0}, {7, 11}),     // 22
        MakeFivePoolRow({7, 11},  {8, 23},  {1, 5}, {0, 0}, {7, 11}),     // 23
        MakeFivePoolRow({7, 11},  {8, 24},  {1, 5}, {0, 0}, {8, 12}),     // 24
        MakeFivePoolRow({8, 12},  {8, 25},  {1, 5}, {0, 0}, {8, 12}),     // 25
        MakeFivePoolRow({8, 12},  {9, 26},  {1, 5}, {0, 0}, {8, 13}),     // 26
        MakeFivePoolRow({8, 13},  {9, 27},  {1, 6}, {0, 0}, {9, 13}),     // 27
        MakeFivePoolRow({9, 13},  {9, 28},  {1, 6}, {0, 0}, {9, 14}),     // 28
        MakeFivePoolRow({9, 14},  {10, 29}, {1, 6}, {0, 0}, {9, 14}),     // 29
        MakeFivePoolRow({9, 14},  {10, 30}, {1, 6}, {0, 0}, {10, 15}),    // 30
    }}, 4);

    const TDefaultCpuTable StorageCpuTable({{
        MakeEmptyRow(),                                                  // 0
        MakeTwoPoolRow({1, 1}, {0, 0}),                                  // 1
        MakeTwoPoolRow({2, 2}, {0, 0}),                                  // 2
        MakeFourPoolRow({1, 3}, {1, 1}, {0, 0}, {1, 1}),                 // 3
        MakeFivePoolRow({1, 4},   {1, 4},  {1, 1}, {0, 0}, {1, 2}),      // 4
        MakeFivePoolRow({2, 5},   {1, 5},  {1, 1}, {0, 0}, {1, 2}),      // 5
        MakeFivePoolRow({3, 6},   {1, 6},  {1, 1}, {0, 0}, {1, 3}),      // 6
        MakeFivePoolRow({4, 7},   {1, 7},  {1, 2}, {0, 0}, {1, 3}),      // 7
        MakeFivePoolRow({5, 8},   {1, 8},  {1, 2}, {0, 0}, {1, 4}),      // 8
        MakeFivePoolRow({5, 9},   {1, 9},  {1, 2}, {0, 0}, {2, 4}),      // 9
        MakeFivePoolRow({6, 10},  {1, 10}, {1, 2}, {0, 0}, {2, 5}),      // 10
        MakeFivePoolRow({6, 11},  {1, 11}, {2, 3}, {0, 0}, {2, 5}),      // 11
        MakeFivePoolRow({7, 12},  {1, 12}, {2, 3}, {0, 0}, {2, 6}),      // 12
        MakeFivePoolRow({8, 13},  {1, 13}, {2, 3}, {0, 0}, {2, 6}),      // 13
        MakeFivePoolRow({8, 14},  {1, 14}, {2, 3}, {0, 0}, {3, 7}),      // 14
        MakeFivePoolRow({9, 15},  {1, 15}, {2, 4}, {0, 0}, {3, 7}),      // 15
        MakeFivePoolRow({10, 16}, {1, 16}, {2, 4}, {0, 0}, {3, 8}),      // 16
        MakeFivePoolRow({11, 17}, {1, 17}, {2, 4}, {0, 0}, {3, 8}),      // 17
        MakeFivePoolRow({11, 18}, {1, 18}, {3, 5}, {0, 0}, {3, 9}),      // 18
        MakeFivePoolRow({11, 19}, {1, 19}, {3, 5}, {0, 0}, {4, 9}),      // 19
        MakeFivePoolRow({12, 20}, {1, 20}, {3, 5}, {0, 0}, {4, 10}),     // 20
        MakeFivePoolRow({13, 21}, {1, 21}, {3, 5}, {0, 0}, {4, 10}),     // 21
        MakeFivePoolRow({14, 22}, {1, 22}, {3, 6}, {0, 0}, {4, 11}),     // 22
        MakeFivePoolRow({15, 23}, {1, 23}, {3, 6}, {0, 0}, {4, 11}),     // 23
        MakeFivePoolRow({15, 24}, {1, 24}, {3, 6}, {0, 0}, {5, 12}),     // 24
        MakeFivePoolRow({16, 25}, {1, 25}, {3, 6}, {0, 0}, {5, 12}),     // 25
        MakeFivePoolRow({16, 26}, {1, 26}, {4, 7}, {0, 0}, {5, 13}),     // 26
        MakeFivePoolRow({17, 27}, {1, 27}, {4, 7}, {0, 0}, {5, 13}),     // 27
        MakeFivePoolRow({18, 28}, {1, 28}, {4, 7}, {0, 0}, {5, 14}),     // 28
        MakeFivePoolRow({18, 29}, {1, 29}, {4, 7}, {0, 0}, {6, 14}),     // 29
        MakeFivePoolRow({19, 30}, {1, 30}, {4, 8}, {0, 0}, {6, 15}),     // 30
    }}, 4);

    const TDefaultCpuTable TinyCpuTable = [] {
        TDefaultCpuTable table{};
        table.GetPreparedRow(1) = MakeTinyRow1();
        table.GetPreparedRow(2) = MakeTinyRow2();
        table.GetPreparedRow(3) = MakeTinyRow3();
        return table;
    }();

    i16 GetIOThreadCount(i16 cpuCount) {
        return (cpuCount - 1) / (MaxPreparedCpuCount * 2) + 1;
    }

    size_t GetPoolIdx(EPoolKind pool) {
        return static_cast<size_t>(pool);
    }

    ui8 GetPoolId(const TCpuTableRow& row, EPoolKind poolKind) {
        return row.LogicalToRealPool[GetPoolIdx(poolKind)];
    }

    TASPools GetASPoolsForRow(const TCpuTableRow& row) {
        return TASPools{
            .SystemPoolId = GetPoolId(row, EPoolKind::System),
            .UserPoolId = GetPoolId(row, EPoolKind::User),
            .BatchPoolId = GetPoolId(row, EPoolKind::Batch),
            .IOPoolId = GetPoolId(row, EPoolKind::IO),
            .ICPoolId = GetPoolId(row, EPoolKind::IC),
        };
    }

    TRealPoolConfig GetRealPoolCfg(ui8 realPoolId, const TCpuTableRow& row) {
        return row.RealPools[realPoolId];
    }

    bool IsPreparedRow(const TCpuTableRow& row) {
        return row.RealPoolCount > 0;
    }

    TString GetRealPoolName(ERealPoolKind poolKind) {
        switch (poolKind) {
            case ERealPoolKind::Common:
                return TASPools::CommonPoolName;
            case ERealPoolKind::System:
                return TASPools::SystemPoolName;
            case ERealPoolKind::User:
                return TASPools::UserPoolName;
            case ERealPoolKind::Batch:
                return TASPools::BatchPoolName;
            case ERealPoolKind::IO:
                return TASPools::IOPoolName;
            case ERealPoolKind::IC:
                return TASPools::ICPoolName;
        }
        Y_ABORT("Unexpected real pool kind: %d", static_cast<int>(poolKind));
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

    void ValidateASPools(const TASPools& pools, ui8 realPoolCount) {
        Y_VERIFY_S(realPoolCount > 0 && realPoolCount <= PoolKindsCount, "realPoolCount# " << realPoolCount);
        Y_VERIFY_S(pools.GetRealPoolCount() == realPoolCount,
            "pools.GetRealPoolCount()# " << pools.GetRealPoolCount() << " realPoolCount# " << realPoolCount);
        std::array<bool, PoolKindsCount> usedPoolIds{};
        for (ui8 poolId : pools.GetIndeces()) {
            Y_VERIFY_S(poolId < realPoolCount, "poolId# " << poolId << " realPoolCount# " << realPoolCount);
            usedPoolIds[poolId] = true;
        }
        for (ui8 poolId = 0; poolId < realPoolCount; ++poolId) {
            Y_VERIFY_S(usedPoolIds[poolId], "poolId# " << poolId << " is not used");
        }
    }

    void ValidateLogicalToRealPoolMapping(const TCpuTableRow& row) {
        for (EPoolKind logicalPoolKind : {EPoolKind::System, EPoolKind::User, EPoolKind::Batch, EPoolKind::IO, EPoolKind::IC}) {
            const ui8 realPoolId = GetPoolId(row, logicalPoolKind);
            Y_VERIFY_S(realPoolId < row.RealPoolCount,
                "pool# " << static_cast<int>(logicalPoolKind)
                << " realPoolId# " << realPoolId
                << " realPoolCount# " << row.RealPoolCount);

            const ERealPoolKind realPoolKind = row.RealPools[realPoolId].Kind;
            switch (realPoolKind) {
                case ERealPoolKind::Common:
                case ERealPoolKind::System:
                case ERealPoolKind::User:
                case ERealPoolKind::Batch:
                case ERealPoolKind::IC:
                    Y_VERIFY_S(logicalPoolKind != EPoolKind::IO,
                        "basic pool can't be used for IO executor, realPoolId# " << realPoolId);
                    break;
                case ERealPoolKind::IO:
                    Y_VERIFY_S(logicalPoolKind == EPoolKind::IO,
                        "io pool can only map io executor, realPoolId# " << realPoolId);
                    break;
            }
        }
    }

    void ValidateRealPools(const TCpuTableRow& row, i16 cpuCount) {
        i16 cpuSum = 0;
        for (ui8 realPoolId = 0; realPoolId < row.RealPoolCount; ++realPoolId) {
            const TRealPoolConfig cfg = GetRealPoolCfg(realPoolId, row);
            Y_VERIFY_S(cfg.ThreadCount >= 0,
                "realPoolId# " << realPoolId << " threadCount# " << cfg.ThreadCount);
            Y_VERIFY_S(cfg.MaxThreadCount >= cfg.ThreadCount,
                "realPoolId# " << realPoolId
                << " threadCount# " << cfg.ThreadCount
                << " maxThreadCount# " << cfg.MaxThreadCount);
            if (cfg.AdjacentPools) {
                Y_VERIFY_S(cfg.AdjacentPools->Count <= row.RealPoolCount,
                    "realPoolId# " << realPoolId
                    << " adjacentPoolCount# " << cfg.AdjacentPools->Count
                    << " realPoolCount# " << row.RealPoolCount);
                for (ui8 adjacentPoolIdx = 0; adjacentPoolIdx < cfg.AdjacentPools->Count; ++adjacentPoolIdx) {
                    Y_VERIFY_S(cfg.AdjacentPools->Pools[adjacentPoolIdx] < row.RealPoolCount,
                        "realPoolId# " << realPoolId
                        << " adjacentPool# " << cfg.AdjacentPools->Pools[adjacentPoolIdx]
                        << " realPoolCount# " << row.RealPoolCount);
                }
            }

            if (cfg.Kind != ERealPoolKind::IO) {
                cpuSum += cfg.ThreadCount;
            }
        }
        Y_VERIFY_S(cpuSum == cpuCount, "cpuSum# " << cpuSum << " cpuCount# " << cpuCount);
    }

    void ValidateCpuTable(const ICpuTable& cpuTable, i16 cpuCount) {
        const TCpuTableRow row = cpuTable[cpuCount];
        const TASPools pools = GetASPoolsForRow(row);
        ValidateASPools(pools, row.RealPoolCount);
        ValidateLogicalToRealPoolMapping(row);
        ValidateRealPools(row, cpuCount);
    }

    void ValidateCustomCpuTable(const ICpuTable& cpuTable, i16 cpuCount) {
        if (cpuCount <= MaxPreparedCpuCount) {
            return;
        }

        const auto* defaultCpuTable = dynamic_cast<const TDefaultCpuTable*>(&cpuTable);
        if (!defaultCpuTable) {
            return;
        }

        Y_VERIFY_S(defaultCpuTable->MinScaledRowCpuCount > 0,
            "minScaledRowCpuCount# " << defaultCpuTable->MinScaledRowCpuCount);
        Y_VERIFY_S(defaultCpuTable->MinScaledRowCpuCount <= MaxPreparedCpuCount,
            "minScaledRowCpuCount# " << defaultCpuTable->MinScaledRowCpuCount
            << " maxPreparedCpuCount# " << MaxPreparedCpuCount);

        for (i16 rowCpuCount = defaultCpuTable->MinScaledRowCpuCount; rowCpuCount <= MaxPreparedCpuCount; ++rowCpuCount) {
            Y_VERIFY_S(IsPreparedRow(defaultCpuTable->GetPreparedRow(rowCpuCount)),
                "custom cpu table is sparse, row for cpuCount# " << rowCpuCount
                << " must be defined to scale beyond " << MaxPreparedCpuCount);
        }
    }

    void AddExecutorFromRealPool(
        NKikimrConfig::TActorSystemConfig* config,
        const TRealPoolConfig& realPoolConfig,
        i16 cpuCount,
        bool useSharedThreads,
        bool useUnitedPool)
    {
        auto* executor = config->AddExecutor();
        const TString poolName = GetRealPoolName(realPoolConfig.Kind);

        if (realPoolConfig.Kind == ERealPoolKind::IO) {
            executor->SetType(NKikimrConfig::TActorSystemConfig::TExecutor::IO);
            ui32 ioThreadCount = realPoolConfig.ThreadCount > 0 ? realPoolConfig.ThreadCount : GetIOThreadCount(cpuCount);
            if (config->HasForceIOPoolThreads()) {
                ioThreadCount = config->GetForceIOPoolThreads();
            }
            executor->SetThreads(ioThreadCount);
            executor->SetName(poolName);
            return;
        }

        const bool hasSharedThread = realPoolConfig.HasSharedThread.value_or(useSharedThreads);
        if (hasSharedThread) {
            executor->SetHasSharedThread(true);
        }
        executor->SetType(NKikimrConfig::TActorSystemConfig::TExecutor::BASIC);
        executor->SetThreads(realPoolConfig.ThreadCount);
        executor->SetMaxThreads(Max(realPoolConfig.MaxThreadCount, realPoolConfig.ThreadCount));
        executor->SetPriority(realPoolConfig.Priority);
        executor->SetName(poolName);
        executor->SetAllThreadsAreShared(useUnitedPool);

        if (realPoolConfig.SpinThreshold) {
            executor->SetSpinThreshold(*realPoolConfig.SpinThreshold);
        } else if (realPoolConfig.Kind == ERealPoolKind::Common) {
            executor->SetSpinThreshold(0);
        } else if (realPoolConfig.Kind == ERealPoolKind::IC) {
            executor->SetSpinThreshold(10);
        } else {
            executor->SetSpinThreshold(1);
        }

        if (realPoolConfig.TimePerMailboxMicroSecs) {
            executor->SetTimePerMailboxMicroSecs(*realPoolConfig.TimePerMailboxMicroSecs);
        } else if (realPoolConfig.Kind == ERealPoolKind::Common || realPoolConfig.Kind == ERealPoolKind::IC) {
            executor->SetTimePerMailboxMicroSecs(100);
        }

        if (realPoolConfig.MaxAvgPingDeviation) {
            executor->SetMaxAvgPingDeviation(*realPoolConfig.MaxAvgPingDeviation);
        } else if (realPoolConfig.Kind == ERealPoolKind::IC) {
            executor->SetMaxAvgPingDeviation(500);
        }

        if (realPoolConfig.ForcedForeignSlots) {
            executor->SetForcedForeignSlots(*realPoolConfig.ForcedForeignSlots);
        }
        if (realPoolConfig.AdjacentPools) {
            for (ui8 adjacentPoolIdx = 0; adjacentPoolIdx < realPoolConfig.AdjacentPools->Count; ++adjacentPoolIdx) {
                executor->AddAdjacentPools(realPoolConfig.AdjacentPools->Pools[adjacentPoolIdx]);
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

        Y_ABORT_UNLESS(MinScaledRowCpuCount > 0);
        Y_ABORT_UNLESS(MinScaledRowCpuCount <= MaxPreparedCpuCount);

        TCpuTableRow result{};
        bool initialized = false;
        i16 remaining = cpuCount;

        while (remaining > 0) {
            i16 chunkCpuCount = remaining;
            if (chunkCpuCount > MaxPreparedCpuCount) {
                const i16 tail = remaining - MaxPreparedCpuCount;
                if (tail == 0 || tail >= MinScaledRowCpuCount) {
                    chunkCpuCount = MaxPreparedCpuCount;
                } else {
                    chunkCpuCount = MaxPreparedCpuCount - (MinScaledRowCpuCount - tail);
                }
            }

            const TCpuTableRow& chunk = Rows[chunkCpuCount];
            Y_VERIFY_S(IsPreparedRow(chunk),
                "cpu table row for cpuCount# " << chunkCpuCount << " is not defined");
            if (!initialized) {
                result = chunk;
                initialized = true;
            } else {
                Y_ABORT_UNLESS(result.RealPoolCount == chunk.RealPoolCount);
                Y_ABORT_UNLESS(result.LogicalToRealPool == chunk.LogicalToRealPool);
                for (ui8 realPoolId = 0; realPoolId < result.RealPoolCount; ++realPoolId) {
                    Y_ABORT_UNLESS(result.RealPools[realPoolId].Kind == chunk.RealPools[realPoolId].Kind);
                    result.RealPools[realPoolId].ThreadCount += chunk.RealPools[realPoolId].ThreadCount;
                    result.RealPools[realPoolId].MaxThreadCount += chunk.RealPools[realPoolId].MaxThreadCount;
                }
            }

            remaining -= chunkCpuCount;
        }

        return result;
    }

    TASPools GetASPools(i16 cpuCount) {
        Y_ABORT_UNLESS(cpuCount >= 0);
        if (cpuCount == 0) {
            cpuCount = GetCpuCount();
        }
        Y_ABORT_UNLESS(cpuCount > 0, "Can't read cpu count of this system");
        return GetDefaultASPoolsForCpuCount(cpuCount);
    }

    TASPools GetASPools(const NKikimrConfig::TActorSystemConfig &config, bool useAutoConfig) {
        const bool hasExplicitPoolIds =
            config.HasSysExecutor() ||
            config.HasUserExecutor() ||
            config.HasBatchExecutor() ||
            config.HasIoExecutor() ||
            config.ServiceExecutorSize();
        const bool hasMaterializedExecutors = config.ExecutorSize() > 0;

        if (useAutoConfig && (!hasExplicitPoolIds || !hasMaterializedExecutors)) {
            i16 cpuCount = (config.HasCpuCount() ? config.GetCpuCount() : 0);
            return GetASPools(cpuCount);
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

            return TASPools {
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
            TASPools pools = GetASPools(config, useAutoConfig);
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
        const bool useTiny = options.ForceTinyConfiguration || (cpuCount >= 1 && cpuCount <= SchedulerTinyCoresThreshold);

        if (!config->HasScheduler()) {
            auto *scheduler = config->MutableScheduler();
            scheduler->SetResolution(useTiny ? SchedulerTinyResolution : SchedulerDefaultResolution);

            scheduler->SetSpinThreshold(0);
            scheduler->SetProgressThreshold(10'000);
        }

        if (!config->HasActorSystemProfile()) {
            if (useTiny) {
                config->SetActorSystemProfile(NKikimrConfig::TActorSystemConfig::LOW_CPU_CONSUMPTION);
            } else {
                config->SetActorSystemProfile(NKikimrConfig::TActorSystemConfig::LOW_LATENCY);
            }
        }

        if (!config->HasNodeType()) {
            config->SetNodeType(options.IsDynamicNode ? NKikimrConfig::TActorSystemConfig::COMPUTE : NKikimrConfig::TActorSystemConfig::STORAGE);
        }

        const bool useTinyConfiguration = useTiny && useSharedThreads;
        const ICpuTable& cpuTable = options.CpuTable
            ? *options.CpuTable
            : useTinyConfiguration && cpuCount <= 3
                ? GetTinyCpuTable()
                : GetDefaultCpuTable(config->GetNodeType());
        if (options.CpuTable) {
            ValidateCustomCpuTable(cpuTable, cpuCount);
        }
        ValidateCpuTable(cpuTable, cpuCount);

        const TCpuTableRow row = cpuTable[cpuCount];
        const TASPools pools = GetASPoolsForRow(row);
        const ui8 poolCount = row.RealPoolCount;

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
            AddExecutorFromRealPool(config, GetRealPoolCfg(poolIdx, row), cpuCount, useSharedThreads, useUnitedPool);
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
