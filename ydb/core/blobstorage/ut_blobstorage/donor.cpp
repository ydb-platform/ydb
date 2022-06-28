#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

Y_UNIT_TEST_SUITE(Donor) {

    Y_UNIT_TEST(SlayAfterWiping) {
        TEnvironmentSetup env{{
            .NodeCount = 8,
            .VDiskReplPausedAtStart = true,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        }};
        auto& runtime = env.Runtime;

        env.EnableDonorMode();
        env.CreateBoxAndPool(2, 1);
        env.CommenceReplication();
        env.Sim(TDuration::Seconds(30));

        const ui32 groupId = env.GetGroups().front();

        const TActorId edge = runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        for (ui32 i = 0; i < 100; ++i) {
            const TString buffer = TStringBuilder() << "blob number " << i;
            TLogoBlobID id(1, 1, 1, 0, buffer.size(), 0);
            runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvPut(id, buffer, TInstant::Max()));
            });
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        }

        // wait for sync and stuff
        env.Sim(TDuration::Seconds(3));

        // move slot out from disk
        auto info = env.GetGroupInfo(groupId);
        const TVDiskID& vdiskId = info->GetVDiskId(0);
        const TActorId& vdiskActorId = info->GetActorId(0);
        env.SettlePDisk(vdiskActorId);
        env.Sim(TDuration::Seconds(30));

        // find our donor disk
        auto baseConfig = env.FetchBaseConfig();
        bool found = false;
        std::pair<ui32, ui32> donorPDiskId;
        std::tuple<ui32, ui32, ui32> acceptor;
        for (const auto& slot : baseConfig.GetVSlot()) {
            if (slot.DonorsSize()) {
                UNIT_ASSERT(!found);
                UNIT_ASSERT_VALUES_EQUAL(slot.DonorsSize(), 1);
                const auto& donor = slot.GetDonors(0);
                const auto& id = donor.GetVSlotId();
                UNIT_ASSERT_VALUES_EQUAL(vdiskActorId, MakeBlobStorageVDiskID(id.GetNodeId(), id.GetPDiskId(), id.GetVSlotId()));
                UNIT_ASSERT_VALUES_EQUAL(VDiskIDFromVDiskID(donor.GetVDiskId()), vdiskId);
                donorPDiskId = {id.GetNodeId(), id.GetPDiskId()};
                const auto& acceptorId = slot.GetVSlotId();
                acceptor = {acceptorId.GetNodeId(), acceptorId.GetPDiskId(), acceptorId.GetVSlotId()};
                found = true;
            }
        }
        UNIT_ASSERT(found);

        // restart with formatting
        env.Cleanup();
        const size_t num = env.PDiskMockStates.erase(donorPDiskId);
        UNIT_ASSERT_VALUES_EQUAL(num, 1);
        env.Initialize();

        // wait for initialization
        env.Sim(TDuration::Seconds(30));

        // ensure it has vanished
        baseConfig = env.FetchBaseConfig();
        found = false;
        for (const auto& slot : baseConfig.GetVSlot()) {
            const auto& id = slot.GetVSlotId();
            if (std::make_tuple(id.GetNodeId(), id.GetPDiskId(), id.GetVSlotId()) == acceptor) {
                UNIT_ASSERT(!found);
                UNIT_ASSERT_VALUES_EQUAL(slot.DonorsSize(), 0);
                UNIT_ASSERT_VALUES_EQUAL(slot.GetStatus(), "REPLICATING");
                found = true;
            }
        }
        UNIT_ASSERT(found);
    }

    Y_UNIT_TEST(ConsistentWritesWhenSwitchingToDonorMode) {
        TEnvironmentSetup env{{
            .NodeCount = 9,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        }};
        auto& runtime = *env.Runtime;

        env.EnableDonorMode();
        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Seconds(20));
        auto groups = env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
        const ui32 groupId = groups.front();

        class TWriterActor : public TActorBootstrapped<TWriterActor> {
        public:
            const ui32 GroupId;
            bool *Stopped;
            THashSet<TLogoBlobID> Data;
            std::pair<ui32, ui32> CurrentBarrier;
            ui64 TabletId = 1;
            ui32 CurrentGeneration = 0;
            ui32 CurrentStep = 0;
            ui32 PutsInFlight = 0;

        public:
            TWriterActor(ui32 groupId, bool *stopped)
                : GroupId(groupId)
                , Stopped(stopped)
            {}

            void Bootstrap() {
                Become(&TThis::StateFunc);
                ++CurrentGeneration;
                CurrentStep = 1;
                Data.clear();
                SendToBSProxy(SelfId(), GroupId, new TEvBlobStorage::TEvCollectGarbage(TabletId, CurrentGeneration, 0, 0,
                    true, CurrentGeneration - 1, Max<ui32>(), nullptr, nullptr, TInstant::Max(), false));
            }

            void Handle(TEvBlobStorage::TEvCollectGarbageResult::TPtr ev) {
                UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrProto::OK);
                for (ui32 i = 0; i < 2; ++i) {
                    IssuePut();
                }
            }

            void IssuePut() {
                if (Data.size() == 10000) {
                    if (!PutsInFlight) {
                        *Stopped = true;
                    }
                    return;
                }

                ui32 len = 1 + RandomNumber(100u);
                TString data = TString::Uninitialized(len);
                char *p = data.Detach();
                char *end = p + len;
                TReallyFastRng32 rng(RandomNumber<ui64>());
                while (p + sizeof(ui32) <= end) {
                    *reinterpret_cast<ui32*>(p) = rng();
                    p += sizeof(ui32);
                }
                for (; p != end; ++p) {
                    *p = rng();
                }

                const TLogoBlobID id(TabletId, CurrentGeneration, CurrentStep, 0, len, 0);
                SendToBSProxy(SelfId(), GroupId, new TEvBlobStorage::TEvPut(id, data, TInstant::Max()));
                Cerr << "Put# " << id << Endl;
                Data.emplace(id);

                ++CurrentStep;
                ++PutsInFlight;
            }

            void Handle(TEvBlobStorage::TEvPutResult::TPtr ev) {
                UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, NKikimrProto::OK);
                --PutsInFlight;
                Schedule(TDuration::MicroSeconds(RandomNumber(1000u)), new TEvents::TEvWakeup);
            }

            STRICT_STFUNC(StateFunc,
                cFunc(TEvents::TSystem::Bootstrap, Bootstrap);
                hFunc(TEvBlobStorage::TEvCollectGarbageResult, Handle);
                hFunc(TEvBlobStorage::TEvPutResult, Handle);
                cFunc(TEvents::TSystem::Wakeup, IssuePut);
            )
        };

        bool stopped = false;
        bool resumePending = false;
        TWriterActor *writer = new TWriterActor(groupId, &stopped);
        const TActorId writerId = runtime.Register(writer, 1);
        const TActorId edge = runtime.AllocateEdgeActor(1, __FILE__, __LINE__);

        for (THPTimer timer; TDuration::Seconds(timer.Passed()) <= TDuration::Minutes(3); ) {
            NKikimrBlobStorage::TConfigRequest request;
            request.AddCommand()->MutableQueryBaseConfig();
            auto response = env.Invoke(request);
            UNIT_ASSERT(response.GetSuccess());
            UNIT_ASSERT_VALUES_EQUAL(response.StatusSize(), 1);
            auto& config = response.GetStatus(0).GetBaseConfig();

            bool allReady = true;
            for (const auto& vslot : config.GetVSlot()) {
                if (!vslot.GetReady()) {
                    allReady = false;
                    break;
                }
            }

            if (!allReady) {
                env.Sim(TDuration::Seconds(1));
                continue;
            }

            if (stopped) {
                auto info = env.GetGroupInfo(groupId);
                THashMap<TLogoBlobID, ui32> parts;

                for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
                    TActorId queueId = env.CreateQueueActor(info->GetVDiskId(i), NKikimrBlobStorage::GetFastRead, 1000);
                    for (const auto& id : writer->Data) {
                        auto ev = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(info->GetVDiskId(i), TInstant::Max(),
                            NKikimrBlobStorage::FastRead, {}, {}, {id});
                        runtime.Send(new IEventHandle(queueId, edge, ev.release()), queueId.NodeId());
                        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVGetResult>(edge, false);
                        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Record.GetStatus(), NKikimrProto::OK);
                        for (const auto& item : res->Get()->Record.GetResult()) {
                            if (item.GetStatus() == NKikimrProto::OK) {
                                ++parts[id];
                                break;
                            }
                        }
                    }
                    runtime.Send(new IEventHandle(TEvents::TSystem::Poison, 0, queueId, {}, nullptr, 0), queueId.NodeId());
                }
                for (const auto& id : writer->Data) {
                    UNIT_ASSERT(parts[id] >= 6);
                }

                stopped = false;
                resumePending = true;
            } else {
                if (resumePending) {
                    runtime.Send(new IEventHandle(TEvents::TSystem::Bootstrap, 0, writerId, {}, nullptr, 0), 1);
                    resumePending = false;
                    env.Sim(TDuration::MilliSeconds(RandomNumber(1000u)));
                }

                const ui32 index = RandomNumber(config.VSlotSize());
                const auto& vslot = config.GetVSlot(index);
                Cerr << "Reassign# " << index << " -- " << SingleLineProto(vslot) << Endl;
                NKikimrBlobStorage::TConfigRequest request;
                auto *cmd = request.AddCommand()->MutableReassignGroupDisk();
                cmd->SetGroupId(vslot.GetGroupId());
                cmd->SetGroupGeneration(vslot.GetGroupGeneration());
                cmd->SetFailRealmIdx(vslot.GetFailRealmIdx());
                cmd->SetFailDomainIdx(vslot.GetFailDomainIdx());
                cmd->SetVDiskIdx(vslot.GetVDiskIdx());
                auto response = env.Invoke(request);
                UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
            }
        }
    }

}
