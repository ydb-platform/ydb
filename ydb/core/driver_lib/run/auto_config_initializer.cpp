#include "auto_config_initializer.h"

#include <ydb/core/protos/config.pb.h>

#include <library/cpp/actors/util/affinity.h>
#include <util/system/info.h>

namespace {

    i16 GetCpuCount() {
        TAffinity affinity;
        affinity.Current();
        TCpuMask cpuMask = static_cast<TCpuMask>(affinity);
        if (cpuMask.Size() && cpuMask.CpuCount()) {
            return cpuMask.CpuCount();
        }
        return NSystemInfo::CachedNumberOfCpus();
    }

    enum class EPoolKind : i8 {
        System = 0,
        User = 1,
        Batch = 2,
        IO = 3,
        IC = 4,
    };

    struct TShortPoolCfg {
        i16 ThreadCount;
        i16 MaxThreadCount;
    };

    constexpr i16 MaxPreparedCpuCount = 31;
    constexpr i16 GRpcWorkerCountInMaxPreparedCpuCase = 4;
    constexpr i16 GrpcProxyCountInMaxPreparedCpuCase = 4;
    constexpr i16 CpuCountForEachGRpcWorker = MaxPreparedCpuCount / GRpcWorkerCountInMaxPreparedCpuCase;
    constexpr i16 CpuCountForEachGRpcProxy = MaxPreparedCpuCount / GrpcProxyCountInMaxPreparedCpuCase;
    constexpr i16 GRpcHandlersPerCompletionQueueInMaxPreparedCpuCase = 1000;
    constexpr i16 GRpcHandlersPerCompletionQueuePerCpu = GRpcHandlersPerCompletionQueueInMaxPreparedCpuCase / MaxPreparedCpuCount;

    TShortPoolCfg ComputeCpuTable[MaxPreparedCpuCount + 1][5] {
        {  {0, 0},  {0, 0},   {0, 0}, {0, 0}, {0, 0} },     // 0
        {  {1, 1},  {0, 1},   {0, 1}, {0, 0}, {0, 0} },     // 1
        {  {1, 1},  {0, 2},   {0, 1}, {0, 0}, {1, 1} },     // 2
        {  {1, 2},  {0, 3},   {1, 1}, {0, 0}, {1, 1} },     // 3
        {  {1, 2},  {1, 4},   {1, 1}, {0, 0}, {1, 2} },     // 4
        {  {1, 3},  {2, 5},   {1, 1}, {0, 0}, {1, 2} },     // 5
        {  {1, 3},  {3, 6},   {1, 1}, {0, 0}, {1, 2} },     // 6
        {  {2, 4},  {3, 7},   {1, 2}, {0, 0}, {1, 3} },     // 7
        {  {2, 4},  {4, 8},   {1, 2}, {0, 0}, {1, 3} },     // 8
        {  {2, 5},  {4, 9},   {2, 3}, {0, 0}, {1, 3} },     // 9
        {  {2, 5},  {5, 10},  {2, 3}, {0, 0}, {1, 3} },     // 10
        {  {2, 6},  {6, 11},  {2, 3}, {0, 0}, {2, 4} },     // 11
        {  {2, 6},  {7, 12},  {2, 3}, {0, 0}, {2, 5} },     // 12
        {  {3, 7},  {7, 13},  {2, 3}, {0, 0}, {2, 5} },     // 13
        {  {3, 7},  {7, 14},  {2, 3}, {0, 0}, {3, 6} },     // 14
        {  {3, 8},  {8, 15},  {2, 4}, {0, 0}, {3, 6} },     // 15
        {  {3, 8},  {9, 16},  {2, 4}, {0, 0}, {3, 6} },     // 16
        {  {3, 9},  {10, 17}, {2, 4}, {0, 0}, {3, 7} },     // 17
        {  {3, 9},  {10, 18}, {3, 5}, {0, 0}, {3, 7} },     // 18
        {  {4, 10}, {10, 19}, {3, 5}, {0, 0}, {4, 8} },     // 19
        {  {4, 10}, {10, 20}, {3, 5}, {0, 0}, {4, 8} },     // 20
        {  {4, 11}, {11, 21}, {3, 5}, {0, 0}, {4, 8} },     // 21
        {  {4, 11}, {12, 22}, {3, 5}, {0, 0}, {4, 9} },     // 22
        {  {4, 12}, {13, 23}, {3, 6}, {0, 0}, {4, 9} },     // 23
        {  {4, 12}, {13, 24}, {3, 6}, {0, 0}, {5, 10} },    // 24
        {  {5, 13}, {13, 25}, {3, 6}, {0, 0}, {5, 10} },    // 25
        {  {5, 13}, {13, 26}, {4, 7}, {0, 0}, {5, 10} },    // 26
        {  {5, 14}, {14, 27}, {4, 7}, {0, 0}, {5, 11} },    // 27
        {  {5, 14}, {14, 28}, {4, 7}, {0, 0}, {5, 11} },    // 28
        {  {5, 15}, {15, 29}, {4, 8}, {0, 0}, {6, 12} },    // 29
        {  {5, 15}, {16, 30}, {4, 8}, {0, 0}, {6, 12} },    // 30
        {  {6, 18}, {16, 31}, {4, 8}, {0, 0}, {6, 12} },    // 31
    };

    TShortPoolCfg HybridCpuTable[MaxPreparedCpuCount + 1][5] {
        {  {0, 0},   {0, 0},   {0, 0}, {0, 0}, {0, 0} },     // 0
        {  {1, 1},   {0, 1},   {0, 1}, {0, 0}, {0, 0} },     // 1
        {  {1, 1},   {0, 2},   {0, 1}, {0, 0}, {1, 1} },     // 2
        {  {1, 2},   {0, 3},   {1, 1}, {0, 0}, {1, 1} },     // 3
        {  {1, 2},   {1, 4},   {1, 1}, {0, 0}, {1, 2} },     // 4
        {  {1, 2},   {2, 5},   {1, 1}, {0, 0}, {1, 2} },     // 5
        {  {1, 2},   {2, 6},   {1, 1}, {0, 0}, {2, 2} },     // 6
        {  {2, 3},   {2, 7},   {1, 2}, {0, 0}, {2, 3} },     // 7
        {  {2, 3},   {3, 8},   {1, 2}, {0, 0}, {2, 3} },     // 8
        {  {2, 4},   {3, 9},   {1, 2}, {0, 0}, {3, 4} },     // 9
        {  {3, 4},   {3, 10},  {1, 2}, {0, 0}, {3, 4} },     // 10
        {  {3, 5},   {4, 11},  {1, 2}, {0, 0}, {3, 5} },     // 11
        {  {3, 5},   {4, 12},  {1, 3}, {0, 0}, {4, 5} },     // 12
        {  {4, 6},   {4, 13},  {1, 3}, {0, 0}, {4, 6} },     // 13
        {  {4, 6},   {5, 14},  {1, 3}, {0, 0}, {4, 6} },     // 14
        {  {4, 7},   {5, 15},  {1, 3}, {0, 0}, {5, 7} },     // 15
        {  {5, 7},   {5, 16},  {1, 3}, {0, 0}, {5, 7} },     // 16
        {  {5, 8},   {6, 17},  {1, 4}, {0, 0}, {5, 8} },     // 17
        {  {5, 8},   {6, 18},  {1, 4}, {0, 0}, {6, 8} },     // 18
        {  {6, 9},   {6, 19},  {1, 4}, {0, 0}, {6, 9} },     // 19
        {  {6, 9},   {7, 20},  {1, 4}, {0, 0}, {6, 9} },     // 20
        {  {6, 10},  {7, 21},  {1, 4}, {0, 0}, {7, 10} },     // 21
        {  {7, 10},  {7, 22},  {1, 5}, {0, 0}, {7, 10} },     // 22
        {  {7, 11},  {8, 23},  {1, 5}, {0, 0}, {7, 11} },     // 23
        {  {7, 11},  {8, 24},  {1, 5}, {0, 0}, {8, 11} },    // 24
        {  {8, 12},  {8, 25},  {1, 5}, {0, 0}, {8, 12} },    // 25
        {  {8, 12},  {9, 26},  {1, 5}, {0, 0}, {8, 12} },    // 26
        {  {8, 13},  {9, 27},  {1, 6}, {0, 0}, {9, 13} },    // 27
        {  {9, 13},  {9, 28},  {1, 6}, {0, 0}, {9, 13} },    // 28
        {  {9, 14},  {10, 29}, {1, 6}, {0, 0}, {9, 14} },    // 29
        {  {9, 14},  {10, 30}, {1, 6}, {0, 0}, {10, 14} },   // 30
        {  {10, 15}, {10, 31}, {1, 6}, {0, 0}, {10, 15} },   // 31
    };

    TShortPoolCfg StorageCpuTable[MaxPreparedCpuCount + 1][5] {
        {  {0, 0},   {0, 0},  {0, 0}, {0, 0}, {0, 0} },     // 0
        {  {1, 1},   {0, 1},  {0, 1}, {0, 0}, {0, 0} },     // 1
        {  {1, 2},   {0, 2},  {0, 1}, {0, 0}, {1, 1} },     // 2
        {  {1, 3},   {0, 3},  {1, 1}, {0, 0}, {1, 1} },     // 3
        {  {1, 4},   {1, 4},  {1, 1}, {0, 0}, {1, 2} },     // 4
        {  {2, 5},   {1, 5},  {1, 1}, {0, 0}, {1, 2} },     // 5
        {  {3, 6},   {1, 6},  {1, 1}, {0, 0}, {1, 2} },     // 6
        {  {4, 7},   {1, 7},  {1, 2}, {0, 0}, {1, 3} },     // 7
        {  {5, 8},   {1, 8},  {1, 2}, {0, 0}, {1, 3} },     // 8
        {  {5, 9},   {1, 9},  {1, 2}, {0, 0}, {2, 4} },     // 9
        {  {6, 10},  {1, 10}, {1, 2}, {0, 0}, {2, 4} },     // 10
        {  {6, 11},  {1, 11}, {2, 3}, {0, 0}, {2, 4} },     // 11
        {  {7, 12},  {1, 12}, {2, 3}, {0, 0}, {2, 5} },     // 12
        {  {8, 13},  {1, 13}, {2, 3}, {0, 0}, {2, 5} },     // 13
        {  {8, 14},  {1, 14}, {2, 3}, {0, 0}, {3, 6} },     // 14
        {  {9, 15},  {1, 15}, {2, 4}, {0, 0}, {3, 6} },     // 15
        {  {10, 16}, {1, 16}, {2, 4}, {0, 0}, {3, 6} },     // 16
        {  {11, 17}, {1, 17}, {2, 4}, {0, 0}, {3, 7} },     // 17
        {  {11, 18}, {1, 18}, {3, 5}, {0, 0}, {3, 7} },     // 18
        {  {11, 19}, {1, 19}, {3, 5}, {0, 0}, {4, 8} },     // 19
        {  {12, 20}, {1, 20}, {3, 5}, {0, 0}, {4, 8} },     // 20
        {  {13, 21}, {1, 21}, {3, 5}, {0, 0}, {4, 8} },     // 21
        {  {14, 22}, {1, 22}, {3, 6}, {0, 0}, {4, 9} },     // 22
        {  {15, 23}, {1, 23}, {3, 6}, {0, 0}, {4, 9} },     // 23
        {  {15, 24}, {1, 24}, {3, 6}, {0, 0}, {5, 10} },    // 24
        {  {16, 25}, {1, 25}, {3, 6}, {0, 0}, {5, 10} },    // 25
        {  {16, 26}, {1, 26}, {4, 7}, {0, 0}, {5, 10} },    // 26
        {  {17, 27}, {1, 27}, {4, 7}, {0, 0}, {5, 11} },    // 27
        {  {18, 28}, {1, 28}, {4, 7}, {0, 0}, {5, 11} },    // 28
        {  {18, 29}, {1, 29}, {4, 7}, {0, 0}, {6, 12} },    // 29
        {  {19, 30}, {1, 30}, {4, 8}, {0, 0}, {6, 12} },    // 30
        {  {20, 31}, {1, 31}, {4, 8}, {0, 0}, {6, 12} },    // 31
    };

    i16 GetIOThreadCount(i16 cpuCount) {
        return (cpuCount - 1) / (MaxPreparedCpuCount * 2) + 1;
    }

    TShortPoolCfg GetShortPoolChg(EPoolKind pool, i16 cpuCount, TShortPoolCfg cpuTable[][5]) {
        i16 k = cpuCount / MaxPreparedCpuCount;
        i16 mod = cpuCount % MaxPreparedCpuCount;
        ui8 poolIdx = static_cast<i8>(pool);
        if (!k) {
            return cpuTable[cpuCount][poolIdx];
        }

        TShortPoolCfg result = cpuTable[MaxPreparedCpuCount][poolIdx];
        result.ThreadCount *= k;
        result.MaxThreadCount *= k;
        TShortPoolCfg additional = cpuTable[mod][poolIdx];
        result.ThreadCount += additional.ThreadCount;
        result.MaxThreadCount += additional.MaxThreadCount;
        return result;
    }

} // anonymous

namespace NKikimr::NAutoConfigInitializer {

    TASPools GetASPools(i16 cpuCount) {
        Y_VERIFY(cpuCount >= 0);
        if (cpuCount == 0) {
            cpuCount = GetCpuCount();
        }
        Y_VERIFY(cpuCount > 0, "Can't read cpu count of this system");
        if (cpuCount >= 4) {
            return TASPools();
        } else if (cpuCount == 3) {
            return TASPools {.SystemPoolId = 0, .UserPoolId = 0, .BatchPoolId = 1, .IOPoolId = 2, .ICPoolId = 3};
        } else {
            return TASPools {.SystemPoolId = 0, .UserPoolId = 0, .BatchPoolId = 0, .IOPoolId = 1, .ICPoolId = 0};
        } 
    }

    TASPools GetASPools(const NKikimrConfig::TActorSystemConfig &config, bool useAutoConfig) {
        if (useAutoConfig) {
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
            i16 cpuCount = (config.HasCpuCount() ? config.GetCpuCount() : 0);
            TASPools pools = GetASPools(cpuCount);
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

    void ApplyAutoConfig(NKikimrConfig::TActorSystemConfig *config) {
        config->SetUseAutoConfig(true);
        config->ClearExecutor();

        i16 cpuCount = config->HasCpuCount() ? config->GetCpuCount() : GetCpuCount();
        Y_VERIFY(cpuCount);
        config->SetCpuCount(cpuCount);

        if (!config->HasScheduler()) {
            auto *scheduler = config->MutableScheduler();
            scheduler->SetResolution(64);
            scheduler->SetSpinThreshold(0);
            scheduler->SetProgressThreshold(10'000);
        }

        TASPools pools = GetASPools(cpuCount);
        ui8 poolCount = pools.GetRealPoolCount();
        std::vector<TString> names = pools.GetRealPoolNames();
        std::vector<ui8> executorIds = pools.GetIndeces();
        std::vector<ui8> priorities = pools.GetPriorities();

        auto *serviceExecutor = config->AddServiceExecutor();
        serviceExecutor->SetServiceName("Interconnect");

        config->SetUserExecutor(pools.SystemPoolId);
        config->SetSysExecutor(pools.UserPoolId);
        config->SetBatchExecutor(pools.BatchPoolId);
        config->SetIoExecutor(pools.IOPoolId);
        serviceExecutor->SetExecutorId(pools.ICPoolId);

        TVector<NKikimrConfig::TActorSystemConfig::TExecutor *> executors;
        for (ui32 poolIdx = 0; poolIdx < poolCount; ++poolIdx) {
            executors.push_back(config->AddExecutor());
        }

        auto &cpuTable = (config->GetNodeType() == NKikimrConfig::TActorSystemConfig::STORAGE ? StorageCpuTable :
                          config->GetNodeType() == NKikimrConfig::TActorSystemConfig::COMPUTE ? ComputeCpuTable :
                          HybridCpuTable );


        for (ui32 poolIdx = 0; poolIdx < poolCount; ++poolIdx) {
            auto *executor = executors[poolIdx];
            if (names[poolIdx] == TASPools::IOPoolName) {
                executor->SetType(NKikimrConfig::TActorSystemConfig::TExecutor::IO);
                executor->SetThreads(GetIOThreadCount(cpuCount));
                executor->SetName(names[poolIdx]);
                continue;
            }
            EPoolKind poolKind = EPoolKind::System;
            if (names[poolIdx] == TASPools::UserPoolName) {
                poolKind = EPoolKind::User;
            } else if (names[poolIdx] == TASPools::BatchPoolName) {
                poolKind = EPoolKind::Batch;
            } else if (names[poolIdx] == TASPools::ICPoolName) {
                poolKind = EPoolKind::IC;
            }
            TShortPoolCfg cfg = GetShortPoolChg(poolKind, cpuCount, cpuTable);
            i16 threadsCount = cfg.ThreadCount;
            if (poolCount == 2) {
                threadsCount = cpuCount;
            }
            executor->SetType(NKikimrConfig::TActorSystemConfig::TExecutor::BASIC);
            executor->SetThreads(threadsCount);
            executor->SetThreads(Max(cfg.MaxThreadCount, threadsCount));
            executor->SetPriority(priorities[poolIdx]);
            executor->SetName(names[poolIdx]);

            if (names[poolIdx] == TASPools::CommonPoolName) {
                executor->SetSpinThreshold(0);
                executor->SetTimePerMailboxMicroSecs(100);
            } else if (names[poolIdx] == TASPools::ICPoolName) {
                executor->SetSpinThreshold(10);
                executor->SetTimePerMailboxMicroSecs(100);
                executor->SetMaxAvgPingDeviation(500);
            } else {
                executor->SetSpinThreshold(1);
            }
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