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

    Y_UNIT_TEST(MultipleEvicts) {
        ui32 numDCs = 4;
        ui32 reassignsInFlight = 10;
        ui32 numNodes = numDCs * reassignsInFlight + 3;

        TEnvironmentSetup env{{
            .NodeCount = numNodes,
            .Erasure = TBlobStorageGroupType::ErasureMirror3dc,
            .NumDataCenters = numDCs,
        }};

        env.EnableDonorMode();
        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Seconds(20));

        auto config = env.FetchBaseConfig();

        auto makeVDiskId = [](const NKikimrBlobStorage::TBaseConfig::TVSlot& vslot) {
            return TVDiskIdShort(vslot.GetFailRealmIdx(), vslot.GetFailDomainIdx(), vslot.GetVDiskIdx());
        };

        UNIT_ASSERT_VALUES_UNEQUAL(config.VSlotSize(), 0);
        const auto& evictedVSlot = config.GetVSlot(RandomNumber(config.VSlotSize()));
        const auto& evictedVDiskId = makeVDiskId(evictedVSlot);

        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvDropDonor) {
                env.Runtime->Send(IEventHandle::ForwardOnNondelivery(std::move(ev), TEvents::TEvUndelivered::Disconnected).release(), nodeId);
                return false;
            }
            return true;
        };

        auto printDonorList = [&] (NKikimrBlobStorage::TBaseConfig::TVSlot slot) {
            TStringStream str;
            for (const auto& donor : slot.GetDonors()) {
                const auto& vslot = donor.GetVSlotId();
                str << vslot.GetNodeId() << ':' << vslot.GetPDiskId() << ' ';
            }
            return str.Str();
        };

        for (ui32 i = 0; i < reassignsInFlight; ++i) {
            config = env.FetchBaseConfig();
            for (const auto& slot : config.GetVSlot()) {
                if (makeVDiskId(slot) == evictedVDiskId) {
                    NKikimrBlobStorage::TConfigRequest request;
                    auto *cmd = request.AddCommand()->MutableReassignGroupDisk();
                    cmd->SetGroupId(slot.GetGroupId());
                    cmd->SetGroupGeneration(slot.GetGroupGeneration());
                    cmd->SetFailRealmIdx(slot.GetFailRealmIdx());
                    cmd->SetFailDomainIdx(slot.GetFailDomainIdx());
                    cmd->SetVDiskIdx(slot.GetVDiskIdx());
                    auto response = env.Invoke(request);
                    // UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());

                    std::set<TPDiskId> pdisks;
                    Cerr << slot.DonorsSize() << " donors: " << printDonorList(slot) << Endl;
                    for (const auto& donor : slot.GetDonors()) {
                        const auto& vslotId = donor.GetVSlotId();
                        UNIT_ASSERT_C(pdisks.emplace(vslotId.GetNodeId(), vslotId.GetPDiskId()).second,
                                slot.DonorsSize() << " donors: " << printDonorList(slot));
                    }
                    break;
                }
            }
        }
       // env.Sim(TDuration::Seconds(10));
    }

    void CheckHasDonor(TEnvironmentSetup& env, const TActorId& vdiskActorId, const TVDiskID& vdiskId) {
        auto baseConfig = env.FetchBaseConfig();
        bool found = false;
        for (const auto& slot : baseConfig.GetVSlot()) {
            if (slot.DonorsSize()) {
                UNIT_ASSERT(!found);
                UNIT_ASSERT_VALUES_EQUAL(slot.DonorsSize(), 1);
                const auto& donor = slot.GetDonors(0);
                const auto& id = donor.GetVSlotId();
                UNIT_ASSERT_VALUES_EQUAL(vdiskActorId, MakeBlobStorageVDiskID(id.GetNodeId(), id.GetPDiskId(), id.GetVSlotId()));
                UNIT_ASSERT_VALUES_EQUAL(VDiskIDFromVDiskID(donor.GetVDiskId()), vdiskId);
                found = true;
            }
        }
        UNIT_ASSERT(found);
    }

    TVector<NKikimrBlobStorage::TBaseConfig_TVSlot_TDonorDisk> GetDonors(TEnvironmentSetup& env, const TVDiskID& vdiskId) {
        TVector<NKikimrBlobStorage::TBaseConfig_TVSlot_TDonorDisk> result;
        const auto& baseConfig = env.FetchBaseConfig();
        for (const auto& slot : baseConfig.GetVSlot()) {
            for (size_t donorId = 0; donorId < slot.DonorsSize(); ++donorId) {
                const auto& donor = slot.GetDonors(donorId);
                if (VDiskIDFromVDiskID(donor.GetVDiskId()) == vdiskId) {
                    result.push_back(donor);
                }
            }
        }
        return result;
    }

    Y_UNIT_TEST(CheckOnlineReadRequestToDonor) {
        TEnvironmentSetup env{TEnvironmentSetup::TSettings{
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
        const TString buffer = TString(2_MB, 'b');
        TLogoBlobID logoBlobId(1, 1, 0, 0, buffer.size(), 0);
        TVDiskID vdiskId;
        bool vdiskIdWithBlobSet = false;
        TLogoBlobID vdiskLogoBlobId;

        // Put blob and find vdisk with it and partId = 1
        {
            env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
                if (ev->GetTypeRewrite() == TEvBlobStorage::EvVPut) {
                    Y_UNUSED(nodeId);
                    auto* msg = ev->Get<TEvBlobStorage::TEvVPut>();
                    const auto& blobId = LogoBlobIDFromLogoBlobID(msg->Record.GetBlobID());
                    if (blobId.IsSameBlob(logoBlobId) && blobId.PartId() == 1 && !vdiskIdWithBlobSet) {
                        vdiskId = VDiskIDFromVDiskID(msg->Record.GetVDiskID());
                        vdiskLogoBlobId = blobId;
                        vdiskIdWithBlobSet = true;
                    } else {
                    }
                }
                return true;
            };

            runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvPut(logoBlobId, buffer, TInstant::Max()));
            });
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
            UNIT_ASSERT(vdiskIdWithBlobSet);
        }

        auto info = env.GetGroupInfo(groupId);
        const TActorId& vdiskActorId = info->GetActorId(vdiskId);

        // Move slot out from disk and finf donor
        env.SettlePDisk(vdiskActorId);
        CheckHasDonor(env, vdiskActorId, vdiskId);
        const auto& donors = GetDonors(env, vdiskId);
        UNIT_ASSERT_VALUES_EQUAL(donors.size(), 1);
        const auto& donor = donors.front();

        bool requestVdiskNotYet = false;
        bool fastRequestToDonor = false;
        bool asyncRequestToDonor = false;

        const auto& checkRequestToDonor = [&](std::unique_ptr<IEventHandle>& ev, const NKikimrBlobStorage::EGetHandleClass& handleClass, bool& requestExist) {
            auto* msg = ev->Get<TEvBlobStorage::TEvVGet>();
            if (msg->Record.ExtremeQueriesSize() != 1) {
                return;
            }
            const auto& query = msg->Record.GetExtremeQueries(0);
            const auto& blobId = LogoBlobIDFromLogoBlobID(query.GetId());
            const auto& slotId = donor.GetVSlotId();
            const auto& donorActorId = MakeBlobStorageVDiskID(slotId.GetNodeId(), slotId.GetPDiskId(), slotId.GetVSlotId());

            if (blobId == vdiskLogoBlobId &&
                    ev->Recipient == donorActorId &&
                    msg->Record.GetHandleClass() == handleClass) {
                UNIT_ASSERT(!requestExist);
                requestExist = true;
            }
            return;
        };

        // Check disk answer TEvEnrichNotYet and request FastRead from donor for online read
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            Y_UNUSED(nodeId);
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvEnrichNotYet) {
                UNIT_ASSERT(!requestVdiskNotYet);
                auto msg = ev->Get<TEvBlobStorage::TEvEnrichNotYet>()->Query.Get()->Get();
                UNIT_ASSERT_VALUES_EQUAL(msg->Record.ExtremeQueriesSize(), 1);
                const auto& query = msg->Record.GetExtremeQueries(0);
                const auto& vdid = VDiskIDFromVDiskID(msg->Record.GetVDiskID());
                const auto& blobId = LogoBlobIDFromLogoBlobID(query.GetId());
                UNIT_ASSERT(vdid.SameExceptGeneration(vdiskId));
                UNIT_ASSERT_VALUES_EQUAL(vdid.GroupGeneration, 2);
                UNIT_ASSERT_VALUES_EQUAL(blobId, vdiskLogoBlobId);
                requestVdiskNotYet = true;
            }

            if (ev->GetTypeRewrite() == TEvBlobStorage::EvVGet) {
                checkRequestToDonor(ev, NKikimrBlobStorage::EGetHandleClass::FastRead, fastRequestToDonor);
            }
            return true;
        };

        // Get blob
        {
            auto ev = new TEvBlobStorage::TEvGet(logoBlobId, 0, 0, TInstant::Max(), NKikimrBlobStorage::EGetHandleClass::FastRead);
            runtime->WrapInActorContext(edge, [&] {SendToBSProxy(edge, groupId, ev);});
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
            UNIT_ASSERT(requestVdiskNotYet);
            UNIT_ASSERT(fastRequestToDonor);
        }

        // Check disk request AsyncRead from donor for replication
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            Y_UNUSED(nodeId);
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvVGet) {
                checkRequestToDonor(ev, NKikimrBlobStorage::EGetHandleClass::AsyncRead, asyncRequestToDonor);
            }
            return true;
        };

        // Start replication
        env.CommenceReplication();
        UNIT_ASSERT(asyncRequestToDonor);
    }
}
