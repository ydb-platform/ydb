#include "queue_backpressure_client.h"
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/executor_pool_io.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/protos/services_common.pb.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_tools.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <ydb/core/blobstorage/vdisk/vdisk_actor.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_config.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <util/folder/tempdir.h>

using namespace NKikimr;
using namespace NActors;

class TFilterActor : public TActorBootstrapped<TFilterActor> {
public:
    using TFilterFunc = std::function<bool (IEventHandle&)>;

private:
    TActorId QueueId;
    TActorId VDiskId;
    TFilterFunc FilterFunc;

public:
    TFilterActor(const TActorId& queueId, const TActorId& vdiskId, TFilterFunc&& filterFunc)
        : QueueId(queueId)
        , VDiskId(vdiskId)
        , FilterFunc(filterFunc)
    {}

    void Bootstrap(const TActorContext& /*ctx*/) {
        Become(&TFilterActor::StateFunc);
    }

    template<typename TPtr>
    void HandleFw(TPtr& ev, const TActorContext& ctx) {
        ctx.ExecutorThread.Send(new IEventHandle(VDiskId, ctx.SelfID, ev->Release().Release(), 0, ev->Cookie));
    }

    template<typename TPtr>
    void HandleBw(TPtr& ev, const TActorContext& ctx) {
        ctx.ExecutorThread.Send(ev->Forward(QueueId));
    }

    template<typename TPtr>
    void HandleForward(TPtr& ev, const TActorContext& ctx) {
        ctx.ExecutorThread.Send(ev->Forward(VDiskId));
    }

    STFUNC(StateFunc) {
        if (FilterFunc(*ev)) {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvBlobStorage::TEvVCheckReadiness, HandleFw);
                HFunc(TEvBlobStorage::TEvVGet, HandleFw);
                HFunc(TEvBlobStorage::TEvVCheckReadinessResult, HandleBw);
                HFunc(TEvBlobStorage::TEvVGetResult, HandleBw);
                HFunc(TEvBlobStorage::TEvVReadyNotify, HandleBw);
                HFunc(TEvBlobStorage::TEvVWindowChange, HandleBw);
                HFunc(TEvBlobStorage::TEvVSyncGuid, HandleForward);
                default: Y_ABORT("unexpected event Type# 0x%08" PRIx32, ev->GetTypeRewrite());
            }
        }
    }
};

class TProxyActor : public TActorBootstrapped<TProxyActor> {
    const TIntrusivePtr<TBlobStorageGroupInfo> Info;
    const TVDiskID VDiskId;
    const TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    const TActorId QueueActorId;

public:
    TProxyActor(TIntrusivePtr<TBlobStorageGroupInfo> info, const TVDiskID& vdiskId,
            TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
            const TActorId& queueActorId)
        : Info(std::move(info))
        , VDiskId(vdiskId)
        , Counters(std::move(counters))
        , QueueActorId(queueActorId)
    {}

    void Bootstrap(const TActorContext& ctx) {
        Become(&TProxyActor::StateFunc);

        TBSProxyContextPtr bsCtx;
        bsCtx = new TBSProxyContext{Counters};
        TIntrusivePtr<NBackpressure::TFlowRecord> flowRecord(new NBackpressure::TFlowRecord);

        TActorId actorId = ctx.Register(CreateVDiskBackpressureClient(Info, VDiskId, NKikimrBlobStorage::PutTabletLog,
                Counters, bsCtx, NBackpressure::TQueueClientId(), "PutTabletLog", 0, false, TDuration::Minutes(1),
                flowRecord, NMonitoring::TCountableBase::EVisibility::Public));

        ctx.ExecutorThread.ActorSystem->RegisterLocalService(QueueActorId, actorId);
    }

    void Handle(TEvProxyQueueState::TPtr& ev, const TActorContext& /*ctx*/) {
        if (ev->Get()->IsConnected) {
            ConnectedEvent.Signal();
        }
    }

    void Handle(TEvBlobStorage::TEvVGetResult::TPtr& /*ev*/, const TActorContext& /*ctx*/) {
        if (!--NumResponsesPending) {
            CompletedEvent.Signal();
        }
    }

    STRICT_STFUNC(StateFunc,
        HFunc(TEvProxyQueueState, Handle)
        HFunc(TEvBlobStorage::TEvVGetResult, Handle)
    )

    TAutoEvent ConnectedEvent;
    TAutoEvent CompletedEvent;
    ui32 NumResponsesPending = 0;
};

class TQueueTestRuntime {
    TTempDir TempDir;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    TIntrusivePtr<TBlobStorageGroupInfo> Info;
    std::unique_ptr<TActorSystem> ActorSystem;
    TString Path;
    ui64 DiskSize;
    ui32 ChunkSize;
    ui64 PDiskGuid;
    NPDisk::TKey PDiskKey;
    NPDisk::TMainKey MainKey;
    TActorId PDiskId;
    std::unique_ptr<TAppData> AppData;
    TVDiskID VDiskId;
    TActorId VDiskActorId;
    TActorId QueueActorId;
    TActorId FilterActorId;
    TActorId ProxyActorId;
    TProxyActor *Proxy;
    TIntrusivePtr<NPDisk::TSectorMap> SectorMap;

public:
    TQueueTestRuntime(TFilterActor::TFilterFunc&& func) {
        Counters.Reset(new ::NMonitoring::TDynamicCounters);

        auto setup = MakeHolder<TActorSystemSetup>();
        setup->NodeId = 1;
        setup->ExecutorsCount = 3;
        setup->Executors.Reset(new TAutoPtr<IExecutorPool>[3]);
        setup->Executors[0].Reset(new TBasicExecutorPool(0, 2, 20));
        setup->Executors[1].Reset(new TBasicExecutorPool(1, 2, 20));
        setup->Executors[2].Reset(new TIOExecutorPool(2, 10));
        setup->Scheduler.Reset(new TBasicSchedulerThread(TSchedulerConfig(512, 100)));

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // PDisk
        SectorMap.Reset(new NPDisk::TSectorMap(128ull << 30));
        Cerr << Path << Endl;
        ChunkSize = 128 << 20;
        DiskSize = SectorMap->DeviceSize;
        PDiskGuid = 1;
        PDiskKey = 1;
        MainKey = NPDisk::TMainKey{ .Keys = { 1 } };
        FormatPDisk(Path, DiskSize, 4096, ChunkSize, PDiskGuid, PDiskKey, PDiskKey, PDiskKey, MainKey.Keys.back(), "queue_test",
                false, false, SectorMap, false);

        PDiskId = MakeBlobStoragePDiskID(1, 1);
        ui64 pDiskCategory = 0;
        TIntrusivePtr<TPDiskConfig> pDiskConfig = new TPDiskConfig(Path, PDiskGuid, 1, pDiskCategory);
        pDiskConfig->GetDriveDataSwitch = NKikimrBlobStorage::TPDiskConfig::DoNotTouch;
        pDiskConfig->WriteCacheSwitch = NKikimrBlobStorage::TPDiskConfig::DoNotTouch;
        pDiskConfig->SectorMap = SectorMap;
        pDiskConfig->EnableSectorEncryption = !pDiskConfig->SectorMap;
        TActorSetupCmd pDiskSetup(CreatePDisk(pDiskConfig.Get(), MainKey, Counters), TMailboxType::Revolving, 0);
        setup->LocalServices.emplace_back(PDiskId, std::move(pDiskSetup));

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // BlobStorage group info
        FilterActorId = TActorId{1, "filter"};
        QueueActorId = TActorId{1, "queue"};
        ProxyActorId = TActorId{1, "proxy"};
        VDiskActorId = TActorId{1, "vdisk"};

        TVector<TActorId> actorIds{
            {FilterActorId} // actor id for our single VDisk that will be used by queue
        };

        VDiskId = TVDiskID{0, 1, 0, 0, 0};
        Info = new TBlobStorageGroupInfo(TBlobStorageGroupType::ErasureNone, 1, 1, 1, &actorIds);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // VDisk
        TVDiskConfig::TBaseInfo baseInfo(
            VDiskId,
            PDiskId,
            PDiskGuid,
            1,
            NPDisk::DEVICE_TYPE_ROT,
            0,
            NKikimrBlobStorage::TVDiskKind::Default,
            2,
            {});
        TIntrusivePtr<TVDiskConfig> vDiskConfig = new TVDiskConfig(baseInfo);

        IActor* vDisk = CreateVDisk(vDiskConfig, Info, Counters);
        TActorSetupCmd vDiskSetup(vDisk, TMailboxType::Revolving, 0);
        setup->LocalServices.emplace_back(VDiskActorId, std::move(vDiskSetup));

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Filter
        IActor *filter = new TFilterActor{QueueActorId, VDiskActorId, std::move(func)};
        setup->LocalServices.emplace_back(FilterActorId, TActorSetupCmd{filter, TMailboxType::Simple, 0});

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Proxy
        Proxy = new TProxyActor(Info, VDiskId, Counters, QueueActorId);
        setup->LocalServices.emplace_back(ProxyActorId, TActorSetupCmd{Proxy, TMailboxType::Simple, 0});

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // LOGGER
        NActors::TActorId loggerActorId{1, "logger"};
        TIntrusivePtr<NActors::NLog::TSettings> logSettings{new NActors::NLog::TSettings{loggerActorId,
            NActorsServices::LOGGER, NActors::NLog::PRI_ERROR, NActors::NLog::PRI_ERROR, 0}};
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

        TString explanation;
        logSettings->SetLevel(NLog::PRI_DEBUG, NKikimrServices::BS_QUEUE, explanation);

        NActors::TLoggerActor *loggerActor = new NActors::TLoggerActor{logSettings, NActors::CreateStderrBackend(),
            Counters};
        NActors::TActorSetupCmd loggerActorCmd{loggerActor, NActors::TMailboxType::Simple, 2};
        setup->LocalServices.emplace_back(loggerActorId, std::move(loggerActorCmd));
        AppData.reset(new TAppData(0, 1, 2, 1, TMap<TString, ui32>(), nullptr, nullptr, nullptr, nullptr));

        ActorSystem.reset(new TActorSystem{setup, AppData.get(), logSettings});
    }

    void Start() {
        ActorSystem->Start();
    }

    void Stop() {
        ActorSystem->Stop();
        ActorSystem.reset();
    }

    void SetNumResponsesPending(ui32 num) {
        Proxy->NumResponsesPending = num;
    }

    void WaitConnected() {
        Proxy->ConnectedEvent.WaitI();
    }

    void WaitCompleted() {
        Proxy->CompletedEvent.WaitI();
    }

    void Send(std::unique_ptr<IEventHandle>&& ev) {
        ActorSystem->Send(ev.release());
    }

    const TActorId& GetProxyActorId() const {
        return ProxyActorId;
    }

    const TActorId& GetQueueActorId() const {
        return QueueActorId;
    }

    const TVDiskID& GetVDiskId() const {
        return VDiskId;
    }
};

Y_UNIT_TEST_SUITE(TBlobStorageQueueTest) {

    Y_UNIT_TEST(TMessageLost) {
        // KIKIMR-9570 this test not is incorrect because we don't allow 'lost messages'
        // without proper interconnect notification; local messages can't be just lost while VDisk is operational
        return; // TODO(alexvru)

        TVector<std::pair<ui64, ui64>> sequence;
        auto filterFunc = [&](IEventHandle& ev) {
            if (ev.GetTypeRewrite() == TEvBlobStorage::TEvVGet::EventType) {
                TEventHandle<TEvBlobStorage::TEvVGet>& evv = reinterpret_cast<TEventHandle<TEvBlobStorage::TEvVGet>&>(ev);
                const auto& record = evv.Get()->Record;
                const auto& msgQoS = record.GetMsgQoS();
                const auto& id = msgQoS.GetMsgId();
                ui64 sequenceId = id.GetSequenceId();
                ui64 msgId = id.GetMsgId();
                sequence.emplace_back(sequenceId, msgId);
                if (sequenceId == 1 && msgId == 1) {
                    return false;
                }
            }
            return true;
        };
        TQueueTestRuntime runtime{std::move(filterFunc)};
        runtime.Start();
        runtime.WaitConnected();
        runtime.SetNumResponsesPending(3);
        for (ui32 i = 0; i < 3; ++i) {
            auto req = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(runtime.GetVDiskId(),
                                                                       TInstant::Max(),
                                                                       NKikimrBlobStorage::EGetHandleClass::AsyncRead,
                                                                       TEvBlobStorage::TEvVGet::EFlags::None,
                                                                       {},
                                                                       {TLogoBlobID(1, 1, 1, 0, 10, i)});
            runtime.Send(std::make_unique<IEventHandle>(runtime.GetQueueActorId(), runtime.GetProxyActorId(), req.release()));
        }
        runtime.WaitCompleted();
        runtime.Stop();
        UNIT_ASSERT_VALUES_EQUAL(sequence, (TVector<std::pair<ui64, ui64>>{{1, 0}, {1, 1}, {1, 2}, {2, 1}, {2, 2}}));
    }

}
