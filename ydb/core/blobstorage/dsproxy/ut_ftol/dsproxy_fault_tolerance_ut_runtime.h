#pragma once

#include "defs.h"

#include <ydb/core/base/appdata.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/executor_pool_io.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/core/scheme/scheme_type_registry.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/actors/protos/services_common.pb.h>
#include <ydb/core/blobstorage/dsproxy/dsproxy_nodemon.h>
#include <ydb/core/blobstorage/dsproxy/dsproxy.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_iter.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_sets.h>
#include <ydb/core/blobstorage/ut_vdisk/lib/vdisk_mock.h>
#include <util/system/sanitizers.h>

namespace NKikimr {

class TFaultToleranceTestRuntime {
public:
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    TIntrusivePtr<TBlobStorageGroupInfo> GroupInfo;
    TVector<std::pair<TVDiskID, TActorId>> VDisks;
    std::unique_ptr<TAppData> AppData;
    std::unique_ptr<TActorSystem> ActorSystem;
    TProgramShouldContinue KikimrShouldContinue;

    void Setup(TBlobStorageGroupType groupType, ui32 numFailDomains, ui32 numVDisksPerFailDomain, ui32 numRealms) {
        Counters = new ::NMonitoring::TDynamicCounters;

        TIntrusivePtr<NScheme::TTypeRegistry> typeRegistry(new NScheme::TKikimrTypeRegistry());
        auto functionRegistry = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry());
        AppData.reset(new NKikimr::TAppData(0, 1, 2, 3, TMap<TString, ui32>(), typeRegistry.Get(), functionRegistry.Get(), nullptr, &KikimrShouldContinue));
        AppData->Counters = Counters;

        NActors::TActorId loggerActorId = TActorId(1, "logger");
        TIntrusivePtr<NLog::TSettings> logSettings = new NLog::TSettings(loggerActorId, NActorsServices::LOGGER,
                (IsVerbose ? NLog::PRI_NOTICE : NLog::PRI_CRIT),
                NLog::PRI_DEBUG, 0);
        logSettings->Append(
            NActorsServices::EServiceCommon_MIN,
            NActorsServices::EServiceCommon_MAX,
            NActorsServices::EServiceCommon_Name
        );
        logSettings->Append(
            NKikimrServices::EServiceKikimr_MIN,
            NKikimrServices::EServiceKikimr_MAX,
            NKikimrServices::EServiceKikimr_Name
        );
//        TString explanation;
//        logSettings->SetLevel(NLog::PRI_DEBUG, NKikimrServices::BS_PROXY_GET, explanation);
//        logSettings->SetLevel(NLog::PRI_DEBUG, NActorsServices::TEST, explanation);

        GroupInfo.Reset(new TBlobStorageGroupInfo(groupType, numVDisksPerFailDomain, numFailDomains, numRealms));

        for (const auto& vdisk : GroupInfo->GetVDisks()) {
            auto vd = GroupInfo->GetVDiskId(vdisk.OrderNumber);
            VDisks.emplace_back(vd, GroupInfo->GetActorId(vdisk.OrderNumber));
        }

        auto setup = MakeHolder<TActorSystemSetup>();
        constexpr int maxSize = NSan::TSanIsOn() ? 1: 10;
        setup->NodeId = 1;
        setup->ExecutorsCount = 4;
        setup->Executors.Reset(new TAutoPtr<IExecutorPool>[4]);
        setup->Executors[0].Reset(new TBasicExecutorPool(0, Min(8, maxSize), 20));
        setup->Executors[1].Reset(new TBasicExecutorPool(1, 8, 20));
        setup->Executors[2].Reset(new TIOExecutorPool(2, 10));
        setup->Executors[3].Reset(new TBasicExecutorPool(3, 8, 20));
        setup->Scheduler.Reset(new TBasicSchedulerThread(TSchedulerConfig(512, 1000)));

        setup->LocalServices.emplace_back(loggerActorId, TActorSetupCmd(new TLoggerActor(logSettings,
                    CreateStderrBackend(), Counters->GetSubgroup("logger", "counters")), TMailboxType::Simple, 0));

        auto shared = MakeIntrusive<TVDiskMockSharedState>(GroupInfo);
        for (const auto& pair : VDisks) {
            setup->LocalServices.emplace_back(pair.second,
                        TActorSetupCmd(CreateVDiskMockActor(pair.first, shared, GroupInfo->PickTopology()),
                        TMailboxType::Simple, 0));
        }
        TIntrusivePtr<TDsProxyNodeMon> nodeMon(new TDsProxyNodeMon(Counters, true));
        TDsProxyPerPoolCounters perPoolCounters(Counters);
        TIntrusivePtr<TStoragePoolCounters> storagePoolCounters = perPoolCounters.GetPoolCounters("pool_name");
        TControlWrapper enablePutBatching(DefaultEnablePutBatching, false, true);
        TControlWrapper enableVPatch(DefaultEnableVPatch, false, true);
        TControlWrapper slowDiskThreshold(DefaultSlowDiskThreshold * 1000, 1, 1000000);
        TControlWrapper predictedDelayMultiplier(DefaultPredictedDelayMultiplier * 1000, 1, 1000000);
        IActor *dsproxy = CreateBlobStorageGroupProxyConfigured(TIntrusivePtr(GroupInfo), false, nodeMon,
            std::move(storagePoolCounters), TBlobStorageProxyParameters{
                .EnablePutBatching = enablePutBatching,
                .EnableVPatch = enableVPatch,
                .SlowDiskThreshold = slowDiskThreshold,
                .PredictedDelayMultiplier = predictedDelayMultiplier,
            }
        );
        setup->LocalServices.emplace_back(MakeBlobStorageProxyID(GroupInfo->GroupID),
                TActorSetupCmd(dsproxy, TMailboxType::Simple, 0));

        ActorSystem.reset(new TActorSystem(setup, AppData.get(), logSettings));
        LOG_NOTICE(*ActorSystem, NActorsServices::TEST, "Actor system created");
        ActorSystem->Start();
    }

    void Finish() {
        ActorSystem->Stop();
        ActorSystem.reset();
    }
};

} // NKikimr
