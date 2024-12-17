#pragma once

#include "defs.h"

namespace NKikimr {

    class TTestEnv : TNonCopyable {
        std::unique_ptr<TTestActorSystem> Runtime;
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
        TIntrusivePtr<TPDiskMockState> PDiskMockState;
        std::unordered_map<NKikimrBlobStorage::EVDiskQueueId, TActorId> QueueIds;

        class TFakeConfigDispatcher : public TActor<TFakeConfigDispatcher> {
            std::unordered_set<TActorId> Subscribers;
            TActorId EdgeId;
        public:
            TFakeConfigDispatcher()
                : TActor<TFakeConfigDispatcher>(&TFakeConfigDispatcher::StateWork)
            {
            }

            STFUNC(StateWork) {
                switch (ev->GetTypeRewrite()) {
                    hFunc(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest, Handle);
                    hFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, Handle)
                    hFunc(NConsole::TEvConsole::TEvConfigNotificationResponse, Handle)
                    hFunc(NConsole::TEvConfigsDispatcher::TEvRemoveConfigSubscriptionRequest, Handle)
                }
            }

            void Handle(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest::TPtr& ev) {
                auto& items = ev->Get()->ConfigItemKinds;
                if (items.at(0) != NKikimrConsole::TConfigItem::BlobStorageConfigItem) {
                    return;
                }
                Subscribers.emplace(ev->Sender);
            }

            void Handle(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev) {
                EdgeId = ev->Sender;
                for (auto& id : Subscribers) {
                    auto update = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
                    update->Record.CopyFrom(ev->Get()->Record);
                    Send(id, update.Release());
                }
            }

            void Handle(NConsole::TEvConsole::TEvConfigNotificationResponse::TPtr& ev) {
                Forward(ev, EdgeId);
            }

            void Handle(NConsole::TEvConfigsDispatcher::TEvRemoveConfigSubscriptionRequest::TPtr& ev) {
                Send(ev->Sender, MakeHolder<NConsole::TEvConsole::TEvRemoveConfigSubscriptionResponse>().Release());
            }
        };

    public:
        TTestEnv(TIntrusivePtr<TPDiskMockState> state = nullptr)
            : Runtime(std::make_unique<TTestActorSystem>(1))
            , Counters(new ::NMonitoring::TDynamicCounters)
            , AllVDiskKinds(new TAllVDiskKinds)
            , PDiskMockState(state ? state : new TPDiskMockState(NodeId, PDiskId, PDiskGuid, (ui64)10 << 40))
        {
            SetupLogging();
            Runtime->Start();
            CreatePDisk();
            CreateConfigDispatcher();
            CreateVDisk();
            CreateQueues();
        }

        ~TTestEnv() {
            Runtime->Stop();
        }

        TTestActorSystem *GetRuntime() {
            return Runtime.get();
        }

        TIntrusivePtr<TPDiskMockState> GetPDiskMockState() {
            return PDiskMockState;
        }

        NKikimrBlobStorage::TEvVPutResult Put(const TLogoBlobID& id, TString buffer,
                NKikimrBlobStorage::EPutHandleClass prio = NKikimrBlobStorage::EPutHandleClass::TabletLog) {
            return ExecuteQuery<TEvBlobStorage::TEvVPutResult>(std::make_unique<TEvBlobStorage::TEvVPut>(id, TRope(buffer),
                VDiskId, false, nullptr, TInstant::Max(), prio), GetQueueId(prio));
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

        void ChangeMinHugeBlobSize(ui32 minHugeBlobSize) {
            const TActorId& edge = Runtime->AllocateEdgeActor(NodeId);
            auto request = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
            auto perfConfig = NKikimrConfig::TBlobStorageConfig_TVDiskPerformanceConfig();
            perfConfig.SetPDiskType(PDiskTypeToPDiskType(VDiskConfig->BaseInfo.DeviceType));
            perfConfig.SetMinHugeBlobSizeInBytes(minHugeBlobSize);
            
            auto* vdiskTypes = request->Record.MutableConfig()->MutableBlobStorageConfig()->MutableVDiskPerformanceSettings()->MutableVDiskTypes();
            vdiskTypes->Add(std::move(perfConfig));
            
            Runtime->Send(new IEventHandle(NConsole::MakeConfigsDispatcherID(NodeId), edge, request.Release()), NodeId);
            auto ev = Runtime->WaitForEdgeActorEvent({edge});
            Runtime->DestroyActor(edge);
            auto *msg = ev->CastAsLocal<NConsole::TEvConsole::TEvConfigNotificationResponse>();
            UNIT_ASSERT(msg);
        }

    private:
        template<typename TEvVResult>
        decltype(std::declval<TEvVResult>().Record) ExecuteQuery(std::unique_ptr<IEventBase> query,
                NKikimrBlobStorage::EVDiskQueueId queueId, std::unique_ptr<TEvVResult> *rp = nullptr) {
            const TActorId& edge = Runtime->AllocateEdgeActor(NodeId);
            Runtime->Send(new IEventHandle(QueueIds.at(queueId), edge, query.release()), NodeId);
            auto ev = Runtime->WaitForEdgeActorEvent({edge});
            Runtime->DestroyActor(edge);
            auto *msg = ev->CastAsLocal<TEvVResult>();
            UNIT_ASSERT(msg);
            if (rp) {
                rp->reset(static_cast<TEvVResult*>(ev->ReleaseBase().Release()));
            }
            return msg->Record;
        }

        void SetupLogging() {
            Runtime->SetLogPriority(NKikimrServices::BS_PDISK, NLog::PRI_ERROR);
        }

        void CreatePDisk() {
            Runtime->RegisterService(PDiskServiceId, Runtime->Register(CreatePDiskMockActor(PDiskMockState), NodeId));
        }

        void CreateVDisk() {
            // prepare group info (erasure=none, single disk)
            TVector<TActorId> vdiskIds(1, VDiskServiceId);
            Info.Reset(new TBlobStorageGroupInfo(TBlobStorageGroupType::ErasureNone, 1, 1, 1, &vdiskIds));

            // create vdisk config
            TVDiskConfig::TBaseInfo baseInfo(VDiskId, PDiskServiceId, PDiskGuid, PDiskId,
                NPDisk::DEVICE_TYPE_SSD, VSlotId, NKikimrBlobStorage::TVDiskKind::Default, 1,
                "static");
            VDiskConfig = AllVDiskKinds->MakeVDiskConfig(baseInfo);
            VDiskConfig->UseCostTracker = false;

            // create and register actor
            std::unique_ptr<IActor> vdisk(NKikimr::CreateVDisk(VDiskConfig, Info, Counters->GetSubgroup("subsystem", "vdisk")));
            Runtime->RegisterService(VDiskServiceId, Runtime->Register(vdisk.release(), NodeId));
        }

        void CreateQueues() {
            using E = NKikimrBlobStorage::EVDiskQueueId;
            for (const auto& queueId : {E::PutTabletLog, E::PutAsyncBlob, E::PutUserData, E::GetAsyncRead, E::GetFastRead,
                    E::GetDiscover, E::GetLowRead}) {
                QueueIds.emplace(queueId, CreateQueue(queueId));
            }
        }

        void CreateConfigDispatcher() {
            Runtime->RegisterService(NConsole::MakeConfigsDispatcherID(NodeId), Runtime->Register(new TFakeConfigDispatcher(), NodeId));
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
            const TActorId& edge = Runtime->AllocateEdgeActor(NodeId);
            const TActorId& actorId = Runtime->Register(actor.release(), edge, {}, {}, NodeId);
            for (;;) {
                auto ev = Runtime->WaitForEdgeActorEvent({edge});
                auto *msg = ev->CastAsLocal<TEvProxyQueueState>();
                UNIT_ASSERT(msg);
                if (msg->IsConnected) {
                    Runtime->DestroyActor(edge);
                    break;
                }
            }
            return actorId;
        }
    };

} // NKikimr
