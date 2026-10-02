#pragma once

#include "defs.h"
#include "ydb/core/blobstorage/pdisk/blobstorage_pdisk_tools.h"
#include "ydb/library/actors/testlib/test_runtime.h"

namespace NKikimr {

    class TRuntime : public TTestActorRuntimeBase {
        struct TNodeFactory final: TTestActorRuntimeBase::INodeFactory {
            virtual TIntrusivePtr<TNodeDataBase> CreateNode() override {
                TIntrusivePtr<TNodeDataBase> node = new TNodeDataBase();
                TAppData* appData = new TAppData(0, 0, 0, 0, TMap<TString, ui32>(), nullptr, nullptr, nullptr, nullptr);
                appData->Counters = new NMonitoring::TDynamicCounters;
                node->AppData0.reset(appData);
                return node;
            }
        };

    public:
        TRuntime(ui32 nodeCount) : TTestActorRuntimeBase(nodeCount) {
            NodeFactory.Reset(new TNodeFactory());
        }

        void InitNodeImpl(TNodeDataBase* node, size_t nodeIndex) override {
            node->LogSettings->Append(
                NKikimrServices::EServiceKikimr_MIN,
                NKikimrServices::EServiceKikimr_MAX,
                NKikimrServices::EServiceKikimr_Name
            );
            TTestActorRuntimeBase::InitNodeImpl(node, nodeIndex);
        }

        void* AppData(ui32 nodeId) {
            return GetNodeById(nodeId)->GetAppData();
        }
    };

    class TTestEnv : TNonCopyable {
        std::unique_ptr<TRuntime> Runtime;
        ::NMonitoring::TDynamicCounterPtr Counters;
        TIntrusivePtr<TVDiskConfig> VDiskConfig;
        TIntrusivePtr<TBlobStorageGroupInfo> Info;
        const ui32 GroupId = 0;
        const ui32 NodeId = 1;
        const ui32 PDiskId = 1;
        const ui32 VSlotId = 1;
        const ui64 PDiskGuid = 1;
        const TActorId PDiskServiceId = MakeBlobStoragePDiskID(NodeId, PDiskId);
        const TVDiskID VDiskId{GroupId, 1, 0, 0, 0};
        const TActorId VDiskServiceId = MakeBlobStorageVDiskID(NodeId, PDiskId, VSlotId);
        TIntrusivePtr<TAllVDiskKinds> AllVDiskKinds;
        std::unordered_map<NKikimrBlobStorage::EVDiskQueueId, TActorId> QueueIds;

    public:
        TTestEnv()
            : Runtime(std::make_unique<TRuntime>(1))
            , Counters(new ::NMonitoring::TDynamicCounters)
            , AllVDiskKinds(new TAllVDiskKinds)
        {
            Runtime->Initialize();
            CreatePDiskService();
            CreateVDisk();
            CreateQueues();
        }

        TRuntime *GetRuntime() {
            return Runtime.get();
        }

        NKikimrBlobStorage::TEvVPutResult Put(const TLogoBlobID& id, TString buffer,
                NKikimrBlobStorage::EPutHandleClass prio = NKikimrBlobStorage::EPutHandleClass::TabletLog) {
            return ExecuteQuery<TEvBlobStorage::TEvVPutResult>(std::make_unique<TEvBlobStorage::TEvVPut>(id, TRope(buffer),
                VDiskId, false, nullptr, TInstant::Max(), prio, false), GetQueueId(prio));
        }

        NKikimrBlobStorage::TEvVGetResult Get(const TLogoBlobID& id,
                NKikimrBlobStorage::EGetHandleClass prio = NKikimrBlobStorage::EGetHandleClass::FastRead) {
            auto query = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(VDiskId, TInstant::Max(), prio,
                TEvBlobStorage::TEvVGet::EFlags::None, Nothing(), {id});
            std::unique_ptr<TEvBlobStorage::TEvVGetResult> rp;
            auto r = ExecuteQuery<TEvBlobStorage::TEvVGetResult>(std::unique_ptr<IEventBase>(query.release()),
                GetQueueId(prio), &rp);
            for (size_t i = 0; i < r.ResultSize(); ++i) {
                if (rp->HasBlob(r.GetResult(i))) {
                    r.MutableResult(i)->SetBufferData(rp->GetBlobData(r.GetResult(i)).ConvertToString());
                }
            }
            return r;
        }

        NKikimrBlobStorage::TEvVCollectGarbageResult Collect(ui64 tabletId, ui32 gen, ui32 counter,
                ui8 channel, std::optional<std::pair<ui32, ui32>> collect, bool hard, const TVector<TLogoBlobID>& keep,
                const TVector<TLogoBlobID>& doNotKeep) {
            return ExecuteQuery<TEvBlobStorage::TEvVCollectGarbageResult>(
                std::make_unique<TEvBlobStorage::TEvVCollectGarbage>(tabletId, gen, counter, channel, !!collect,
                collect ? collect->first : 0, collect ? collect->second : 0, hard, keep ? &keep : nullptr,
                doNotKeep ? &doNotKeep : nullptr, VDiskId, TInstant::Max()),
                NKikimrBlobStorage::EVDiskQueueId::PutTabletLog);
        }

        TActorId GetPDiskServiceId() const {
            return PDiskServiceId;
        }

    private:
        template<typename TEvVResult>
        decltype(std::declval<TEvVResult>().Record) ExecuteQuery(std::unique_ptr<IEventBase> query,
                NKikimrBlobStorage::EVDiskQueueId queueId, std::unique_ptr<TEvVResult> *rp = nullptr) {
            const TActorId& edge = Runtime->AllocateEdgeActor(NodeId - 1);
            Runtime->Send(new IEventHandle(QueueIds.at(queueId), edge, query.release()), NodeId - 1);
            auto ev = Runtime->GrabEdgeEvent<TEvVResult>(edge);
            auto *msg = ev->template CastAsLocal<TEvVResult>();
            UNIT_ASSERT(msg);
            if (rp) {
                rp->reset(static_cast<TEvVResult*>(ev->ReleaseBase().Release()));
            }
            return msg->Record;
        }

        TMap<TString, TIntrusivePtr<NPDisk::TSectorMap>> SectorMapByPath;

        std::shared_ptr<NKikimr::NPDisk::TIoContextFactoryOSS> ioContext = std::make_shared<NKikimr::NPDisk::TIoContextFactoryOSS>();

        void CreatePDiskService() {
            NPDisk::TKey mainKey = 123;
            ui64 pDiskGuid = 1;
            ui32 chunkSize = 32 << 20;
            ui64 diskSizeBytes = 32ull << 30ull;
            ui64 pDiskCategory = NKikimr::NPDisk::EDeviceType::DEVICE_TYPE_ROT;
            TString filePath = "/pdisk.dat";
            if (!SectorMapByPath[filePath]) {
                SectorMapByPath[filePath].Reset(new NPDisk::TSectorMap(diskSizeBytes));
                TFormatOptions options;
                options.SectorMap = SectorMapByPath[filePath];
                options.EnableSmallDiskOptimization = false;
                FormatPDisk(filePath, diskSizeBytes, 4 << 10, chunkSize, pDiskGuid,
                        0x123, 0x456, 0x789, mainKey, "", options);
            }

            TIntrusivePtr<TPDiskConfig> pDiskConfig = new TPDiskConfig(filePath, pDiskGuid, 1, pDiskCategory);
            pDiskConfig->GetDriveDataSwitch = NKikimrBlobStorage::TPDiskConfig::DoNotTouch;
            pDiskConfig->WriteCacheSwitch = NKikimrBlobStorage::TPDiskConfig::DoNotTouch;
            pDiskConfig->SectorMap = SectorMapByPath[filePath];
            pDiskConfig->FeatureFlags.SetEnablePDiskDataEncryption(!pDiskConfig->SectorMap);

            NPDisk::TMainKey mainKeys = NPDisk::TMainKey{ .Keys = { mainKey }, .IsInitialized = true };

            TAppData* appData = (TAppData*) Runtime->AppData(1);

            appData->IoContextFactory = ioContext.get();

            IActor* pd = CreatePDisk(pDiskConfig.Get(), mainKeys, appData->Counters);

            Runtime->RegisterService(PDiskServiceId, Runtime->Register(pd, NodeId - 1));
        }

        void CreateVDisk() {
            // prepare group info (erasure=none, single disk)
            TVector<TActorId> vdiskIds(1, VDiskServiceId);
            Info.Reset(new TBlobStorageGroupInfo(TBlobStorageGroupType::ErasureNone, 1, 1, 1, &vdiskIds));

            // create vdisk config
            TVDiskConfig::TBaseInfo baseInfo(VDiskId, PDiskServiceId, PDiskGuid, PDiskId,
                NPDisk::DEVICE_TYPE_SSD, VSlotId, NKikimrBlobStorage::TVDiskKind::Default, 2,
                "static");
            VDiskConfig = AllVDiskKinds->MakeVDiskConfig(baseInfo);
            VDiskConfig->UseCostTracker = false;

            // create and register actor
            std::unique_ptr<IActor> vdisk(NKikimr::CreateVDisk(VDiskConfig, Info, Counters->GetSubgroup("subsystem", "vdisk")));
            Runtime->RegisterService(VDiskServiceId, Runtime->Register(vdisk.release(), NodeId - 1));
        }

        void CreateQueues() {
            using E = NKikimrBlobStorage::EVDiskQueueId;
            for (const auto& queueId : {E::PutTabletLog, E::PutAsyncBlob, E::PutUserData, E::GetAsyncRead, E::GetFastRead,
                    E::GetDiscover, E::GetLowRead}) {
                QueueIds.emplace(queueId, CreateQueue(queueId));
            }
        }

        static NKikimrBlobStorage::EVDiskQueueId GetQueueId(NKikimrBlobStorage::EPutHandleClass prio) {
            switch (prio) {
                case NKikimrBlobStorage::EPutHandleClass::TabletLog:
                    return NKikimrBlobStorage::EVDiskQueueId::PutTabletLog;

                case NKikimrBlobStorage::EPutHandleClass::AsyncBlob:
                    return NKikimrBlobStorage::EVDiskQueueId::PutAsyncBlob;

                case NKikimrBlobStorage::EPutHandleClass::UserData:
                    return NKikimrBlobStorage::EVDiskQueueId::PutUserData;
            }
        }

        static NKikimrBlobStorage::EVDiskQueueId GetQueueId(NKikimrBlobStorage::EGetHandleClass prio) {
            switch (prio) {
                case NKikimrBlobStorage::EGetHandleClass::AsyncRead:
                    return NKikimrBlobStorage::EVDiskQueueId::GetAsyncRead;

                case NKikimrBlobStorage::EGetHandleClass::FastRead:
                    return NKikimrBlobStorage::EVDiskQueueId::GetFastRead;

                case NKikimrBlobStorage::EGetHandleClass::Discover:
                    return NKikimrBlobStorage::EVDiskQueueId::GetDiscover;

                case NKikimrBlobStorage::EGetHandleClass::LowRead:
                    return NKikimrBlobStorage::EVDiskQueueId::GetLowRead;
            }
        }

        TActorId CreateQueue(NKikimrBlobStorage::EVDiskQueueId queueId) {
            const TString& name = NKikimrBlobStorage::EVDiskQueueId_Name(queueId);
            auto counters = Counters->GetSubgroup("queue", name);
            auto bspctx = MakeIntrusive<TBSProxyContext>(counters->GetSubgroup("subsystem", "bsp"));
            auto flowRecord = MakeIntrusive<NBackpressure::TFlowRecord>();
            std::unique_ptr<IActor> actor(CreateVDiskBackpressureClient(Info, VDiskId, queueId,
                counters->GetSubgroup("subsystem", "queue"), bspctx,
                NBackpressure::TQueueClientId(NBackpressure::EQueueClientType::DSProxy, NodeId), name, 0, false,
                TDuration::Seconds(60), flowRecord, NMonitoring::TCountableBase::EVisibility::Private));
            const TActorId& edge = Runtime->AllocateEdgeActor(NodeId - 1);
            const TActorId& actorId = Runtime->Register(actor.release(), NodeId - 1, 0, TMailboxType::EType::HTSwap, 0, edge);
            for (;;) {
                auto ev = Runtime->GrabEdgeEvent<TEvProxyQueueState>(edge);
                auto *msg = ev->CastAsLocal<TEvProxyQueueState>();
                UNIT_ASSERT(msg);
                if (msg->IsConnected) {
                    break;
                }
            }
            return actorId;
        }
    };

} // NKikimr
