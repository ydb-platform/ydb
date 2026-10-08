#pragma once

#include <ydb/core/base/appdata.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_arena.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_ut.h>
#include <ydb/core/blobstorage/vdisk/hulldb/compstrat/hulldb_compstrat_selector.h>
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/scheduler_basic.h>

#include <functional>
#include <type_traits>

namespace NKikimr::NSelectorTest {
using namespace NActors;
using namespace NHullComp;

constexpr ui32 ChunkSize = 128u << 20;
constexpr ui32 LastLevel = 18;

struct TEnvironment {
    TAppData App{0, 0, 0, 0, TMap<TString, ui32>(), nullptr, nullptr, nullptr, nullptr};
    std::unique_ptr<TActorSystem> System;

    explicit TEnvironment(ui32 threads = 1,
            TDuration timePerMailbox = TBasicExecutorPool::DEFAULT_TIME_PER_MAILBOX,
            THolder<ISchedulerThread> scheduler = {}) {
        auto setup = MakeHolder<TActorSystemSetup>();
        setup->NodeId = 1;
        setup->ExecutorsCount = 1;
        setup->Executors.Reset(new TAutoPtr<IExecutorPool>[1]);
        setup->Executors[0].Reset(new TBasicExecutorPool(0, threads, 0, "Batch", nullptr, nullptr, timePerMailbox));
        setup->Scheduler.Reset(scheduler ? scheduler.Release() : new TBasicSchedulerThread);

        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        auto logs = MakeIntrusive<NLog::TSettings>(TActorId(1, "logger"),
            NActorsServices::LOGGER, NLog::PRI_ERROR, NLog::PRI_ERROR, 0u);
        logs->Append(NActorsServices::EServiceCommon_MIN, NActorsServices::EServiceCommon_MAX,
            NActorsServices::EServiceCommon_Name);
        logs->Append(NKikimrServices::EServiceKikimr_MIN, NKikimrServices::EServiceKikimr_MAX,
            NKikimrServices::EServiceKikimr_Name);
        setup->LocalServices.emplace_back(logs->LoggerActorId,
            TActorSetupCmd(new TLoggerActor(logs, CreateStderrBackend(), counters), TMailboxType::Simple, 0));
        App.Counters = counters;
        System = std::make_unique<TActorSystem>(setup, &App, logs);
        System->Start();
    }

    ~TEnvironment() {
        System->Stop();
    }
};

template<class TKey, class TMemRec>
struct TInput {
    using TSst = TLevelSegment<TKey, TMemRec>;
    using TSstPtr = TIntrusivePtr<TSst>;

    TTestContexts Ctx{ChunkSize};
    THullCtxPtr HullCtx = Ctx.GetHullCtx();
    std::shared_ptr<TRopeArena> Arena = std::make_shared<TRopeArena>(&TRopeArenaBackend::Allocate);
    TIntrusivePtr<THullDs> Ds = MakeIntrusive<THullDs>(Ctx.GetHullCtx());
    TIntrusivePtr<TLevelIndex<TKey, TMemRec>> Index;
    std::vector<TSstPtr> Ssts;
    TSelectorParams Params{TBoundariesConstPtr(new TBoundaries(ChunkSize, 8, 8, true)),
        1.0, TInstant::Zero(), {}};
    bool AllowGarbageCollection = true;

    explicit TInput(TActorSystem* system, TDuration ratioCalcBudget = TDuration::Seconds(1))
        : Ctx(ChunkSize, 2u << 20, ratioCalcBudget)
    {
        Ctx.GetVCtx()->ActorSystem = system;
        Ds->LogoBlobs = MakeIntrusive<TLogoBlobsDs>(Ctx.GetLevelIndexSettings(), Arena);
        Ds->Blocks = MakeIntrusive<TBlocksDs>(Ctx.GetLevelIndexSettings(), Arena);
        Ds->Barriers = MakeIntrusive<TBarriersDs>(Ctx.GetLevelIndexSettings(), Arena);
        if constexpr (std::is_same_v<TKey, TKeyLogoBlob>) {
            Index = Ds->LogoBlobs;
        } else if constexpr (std::is_same_v<TKey, TKeyBlock>) {
            Index = Ds->Blocks;
        } else {
            Index = Ds->Barriers;
        }
        for (ui32 i = 0; i < LastLevel; ++i) {
            Index->CurSlice->SortedLevels.emplace_back(TKey::First());
        }
        Ds->LogoBlobs->LoadCompleted();
        Ds->Blocks->LoadCompleted();
        Ds->Barriers->LoadCompleted();
    }

    static TKey MakeKey(ui32 step) {
        if constexpr (std::is_same_v<TKey, TKeyLogoBlob>) {
            return TKey(TLogoBlobID(1, 1, step, 0, 100, 0, 1));
        } else if constexpr (std::is_same_v<TKey, TKeyBlock>) {
            return TKey(step);
        } else {
            return TKey(step, 0, 1, 1, false);
        }
    }

    TSstPtr AddSst(ui32 level, ui32 firstStep, ui32 lastStep, ui32 id, bool huge = false, std::function<TIngress(ui32)> ingress = {}) {
        auto sst = MakeIntrusive<TSst>(Ctx.GetVCtx());
        TTrackableVector<typename TSst::TRec> index(TMemoryConsumer(Ctx.GetVCtx()->SstIndex));
        const ui32 count = lastStep - firstStep + 1;
        index.reserve(count);
        for (ui32 step = firstStep; step <= lastStep; ++step) {
            TMemRec memRec;
            if constexpr (std::is_same_v<TKey, TKeyLogoBlob>) {
                if (ingress) {
                    memRec = TMemRecLogoBlob(ingress(step));
                }
                const TDiskPart part(huge ? 10'000 + id : id, (step - firstStep) * 128, 100);
                if (huge) {
                    memRec.SetHugeBlob(part);
                } else {
                    memRec.SetDiskBlob(part);
                }
            } else if constexpr (std::is_same_v<TKey, TKeyBlock>) {
                memRec = TMemRec(1);
            } else {
                memRec = TMemRec(1, step, TBarrierIngress::CreateFromRaw(Max<ui32>()));
            }
            index.emplace_back(MakeKey(step), memRec);
        }
        sst->LoadedIndex.swap(index);
        sst->AssignedSstId = id;
        sst->AllChunks.push_back(id);
        sst->Info.Chunks = 1;
        sst->Info.Items = count;
        sst->Info.FirstLsn = sst->Info.LastLsn = 1;
        sst->Info.CTime = TInstant::Seconds(1);
        auto ratio = MakeIntrusive<TSstRatio>(TAppData::TimeProvider->Now());
        ratio->IndexItemsTotal = ratio->IndexItemsKeep = count;
        ratio->IndexBytesTotal = ratio->IndexBytesKeep = ui64(count) * (sizeof(TKey) + sizeof(TMemRec));
        if constexpr (std::is_same_v<TKey, TKeyLogoBlob>) {
            if (huge) {
                ratio->HugeDataTotal = ratio->HugeDataKeep = ui64(count) * 100;
            } else {
                ratio->InplacedDataTotal = ratio->InplacedDataKeep = ui64(count) * 100;
                sst->Info.InplaceDataTotalSize = ratio->InplacedDataTotal;
            }
        }
        sst->StorageRatio.Set(ratio, ratio->Time);
        if (level) {
            Index->CurSlice->SortedLevels.at(level - 1).Put(sst);
        } else {
            Index->CurSlice->Level0.Put(sst);
        }
        Ssts.push_back(sst);
        return sst;
    }
};

} // namespace NKikimr::NSelectorTest
