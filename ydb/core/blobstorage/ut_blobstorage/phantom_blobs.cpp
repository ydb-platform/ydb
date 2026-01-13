#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/ut_helpers.h>

using namespace NKikimr;

#define Ctest Cerr

Y_UNIT_TEST_SUITE(PhantomBlobs) {

    enum class EOnline {
        Alive = 0,
        Dead,
        Restart,
    };

    struct TNodeState {
        EOnline Online;
        bool PhantomFlagStorageEnabled;
    };

    struct TTestCtx : public TTestCtxBase {
        TTestCtx(TEnvironmentSetup::TSettings settings, ui32 initialBlobs, ui32 unsyncedBlobs,
                std::vector<TNodeState> nodeStates, bool expectPhantoms, ui64 memoryLimit)
            : TTestCtxBase(std::move(settings))
            , InitialBlobCount(initialBlobs)
            , UnsyncedBlobCount(unsyncedBlobs)
            , NodeStates(nodeStates)
            , ExpectPhantoms(expectPhantoms)
            , MemoryLimit(memoryLimit)
        {
            Y_VERIFY(NodeStates.size() == NodeCount);
            SetIcbControls();
        }

        void SetIcbControls() {
            for (ui32 nodeId = 1; nodeId <= NodeCount; ++nodeId) {
                Env->SetIcbControl(nodeId, "VDiskControls.EnablePhantomFlagStorage",
                        NodeStates[nodeId - 1].PhantomFlagStorageEnabled);
            }
        }

        std::vector<TLogoBlobID> WriteInitialData() {
            Ctest << "Write blobs" << Endl;
            std::vector<TLogoBlobID> blobs = WriteCompressedData(TDataProfile{
                .GroupId = GroupId,
                .TotalBlobs = InitialBlobCount,
                .BlobSize = BlobSize,
                .TabletId = TabletId,
                .Channel = Channel,
                .Generation = Generation,
                .Step = Step,
            });

            return blobs;
        }

        void CollectBlobs(TVector<TLogoBlobID>* keepFlags, TVector<TLogoBlobID>* doNotKeepFlags) {
            Env->Runtime->WrapInActorContext(Edge, [&] {
                TString data;
                SendToBSProxy(Edge, GroupId, new TEvBlobStorage::TEvCollectGarbage(
                        TabletId, Generation, ++GenerationCtr, Channel, true, Generation, Step,
                        keepFlags, doNotKeepFlags, TInstant::Max(), true, false));
            });
            Env->WaitForEdgeActorEvent<TEvBlobStorage::TEvCollectGarbageResult>(
                    Edge, false, TInstant::Max());
        }

        void ToggleNodes(bool stop, bool start, bool restart) {
            for (ui32 nodeId = 1; nodeId <= NodeCount; ++nodeId) {
                switch (NodeStates[nodeId - 1].Online) {
                case EOnline::Alive:
                    break;
                case EOnline::Dead:
                    if (stop) {
                        Ctest << "Stop node# " << nodeId << Endl;
                        Env->StopNode(nodeId);
                    } else if (start) {
                        Ctest << "Start node# " << nodeId << Endl;
                        Env->StartNode(nodeId);
                    }
                    break;
                case EOnline::Restart:
                    if (restart) {
                        Ctest << "Restart node# " << nodeId << Endl;
                        Env->StopNode(nodeId);
                        Env->Sim(TDuration::Minutes(1));
                        Env->StartNode(nodeId);
                    }
                    break;
                }
                Env->Sim(TDuration::Minutes(1));
            }
            SetIcbControls();
            AllocateEdgeActor(); // reallocate actor, in case it lived on a restarted or dead node
        }

        void WriteUnsyncedBlobs() {
            for (ui32 i = 0; i < UnsyncedBlobCount; i += UnsyncedBatchSize) {
                Ctest << "Write unsynced blobs batch, blobs written# " << i << Endl;
                Generation += 10;
                std::vector<TLogoBlobID> batch = WriteCompressedData(TDataProfile{
                    .GroupId = GroupId,
                    .TotalBlobs = UnsyncedBatchSize,
                    .BlobSize = BlobSize,
                    .BatchSize = 1000,
                    .TabletId = TabletId,
                    .Channel = Channel,
                    .Generation = Generation,
                    .Step = Step,
                });
                CollectBlobs(nullptr, nullptr);
            }
        }

        void WaitForSync() {
            Ctest << "Wait for sync" << Endl;
            Env->Sim(TDuration::Minutes(30));
        }

        void BaldSyncLog() {
            Ctest << "Force syncLog trim" << Endl;
            const TIntrusivePtr<TBlobStorageGroupInfo> groupInfo = Env->GetGroupInfo(GroupId);
            UNIT_ASSERT(groupInfo);
            for (ui32 orderNumber = 0; orderNumber < groupInfo->Type.BlobSubgroupSize(); ++orderNumber) {
                const TActorId actorId = groupInfo->GetActorId(orderNumber);
                const TVDiskID vdiskId = groupInfo->GetVDiskId(orderNumber);
                const ui32 nodeId = actorId.NodeId();
                if (NodeStates[nodeId - 1].Online == EOnline::Dead) {
                    continue;
                }
                const TActorId edge = Env->Runtime->AllocateEdgeActor(actorId.NodeId());
                Env->Runtime->WrapInActorContext(edge, [&]{
                    TActivationContext::Send(new IEventHandle(
                            actorId, edge, new TEvBlobStorage::TEvVBaldSyncLog(vdiskId, true)));
                });
                Env->WaitForEdgeActorEvent<TEvBlobStorage::TEvVBaldSyncLogResult>(edge, false);
            }
        }

        void CheckStatus() {
            auto status = GetGroupStatus(GroupId);
            Ctest << "Group status# " << status->ToString() << Endl;
        }

        void CheckBlobs(const std::vector<TLogoBlobID>& blobs) {
            Ctest << "Get group configuration" << Endl;
            TIntrusivePtr<TBlobStorageGroupInfo> group = Env->GetGroupInfo(GroupId);

            Ctest << "Check blobs" << Endl;
            for (ui32 orderNumber = 0; orderNumber < Erasure.BlobSubgroupSize(); ++orderNumber) {
                Ctest << "Check orderNumber# " << orderNumber << Endl;
                TVDiskID vdiskId = group->GetVDiskId(orderNumber);
                NKikimrBlobStorage::EVDiskQueueId queue = NKikimrBlobStorage::EVDiskQueueId::GetFastRead;
                Env->WithQueueId(vdiskId, queue, [&](TActorId queueId) {
                    for (const TLogoBlobID& blob : blobs) {
                        for (ui32 partIdx = 1; partIdx <= Erasure.BlobSubgroupSize(); ++partIdx) {
                            auto ev = TEvBlobStorage::TEvVGet::CreateExtremeIndexQuery(vdiskId, TInstant::Max(),
                                    NKikimrBlobStorage::EGetHandleClass::FastRead);
                            ev->AddExtremeQuery(blob, 0, 0);
                            Env->Runtime->Send(new IEventHandle(queueId, Edge, ev.release()), Edge.NodeId());
                            auto res = Env->WaitForEdgeActorEvent<TEvBlobStorage::TEvVGetResult>(Edge, false, TInstant::Max());
                            auto record = res->Get()->Record;
                            UNIT_ASSERT_VALUES_EQUAL_C(record.GetStatus(), NKikimrProto::OK, record.GetErrorReason());
                            UNIT_ASSERT_C(record.ResultSize() == 1, res->ToString());
                            UNIT_ASSERT_C(record.GetResult(0).GetStatus() == NKikimrProto::NODATA, res->ToString());
                            UNIT_ASSERT_C(!record.GetResult(0).HasIngress(), res->ToString());
                        }
                    }
                    TLogoBlobID from(TabletId, 0, 0, Channel, 0, 0, 1);
                    TLogoBlobID to(TabletId, Generation + 100, 9000, Channel, TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie, TLogoBlobID::MaxPartId);
                });
            }
        }

        void CheckMemoryConsumption(ui64 limit) {
            ui64 consumed = Env->AggregateVDiskCounters(Env->StoragePoolName, NodeCount, NodeCount,
                    GroupId, PDiskLayout, "phantom_flag_storage", "StoredFlagsMemoryConsumption");
            UNIT_ASSERT_C(consumed <= limit, "PhantomFlagStorage memory consumption exceeded expected limit, "
                    " Consumed# " << consumed << " Limit# " << limit);
        }

        void RunTest() {
            Initialize();
            const std::vector<TLogoBlobID> blobs = WriteInitialData();
            auto itMiddle = blobs.begin() + blobs.size() / 2;

            Ctest << "Set Keep flags" << Endl;
            CollectBlobs(new TVector<TLogoBlobID>(blobs.begin(), blobs.end()), nullptr);
            WaitForSync();

            Ctest << "Stop dead nodes" << Endl;
            ToggleNodes(true, false, false);
            WaitForSync();

            Ctest << "Set DoNotKeepFlags on first half of blobs" << Endl;
            CollectBlobs(nullptr, new TVector<TLogoBlobID>(blobs.begin(), itMiddle));
            WaitForSync();

            WriteUnsyncedBlobs();

            BaldSyncLog();

            Ctest << "Set DoNotKeepFlags on second half of blobs" << Endl;
            CollectBlobs(nullptr, new TVector<TLogoBlobID>(itMiddle, blobs.end()));
            WaitForSync();

            CheckMemoryConsumption(NodeCount * MemoryLimit * 2);

            Ctest << "Restart nodes" << Endl;
            ToggleNodes(false, false, true);

            Ctest << "Start dead nodes" << Endl;
            ToggleNodes(false, true, false);
            WaitForSync();

            ++Generation;
            Ctest << "Move soft barrier" << Endl;
            CollectBlobs(nullptr, nullptr);
            WaitForSync();

            CheckStatus();
            if (!ExpectPhantoms) {
                CheckBlobs(blobs);
            }
        }

    public:
        const ui64 TabletId = 5000;
        const ui32 Channel = 1;
        ui32 Generation = 1;
        ui32 Step = 1;
        ui32 GenerationCtr = 1;

        const ui64 BlobSize = 10;
        const ui32 UnsyncedBatchSize = 1000;

        const ui32 InitialBlobCount;
        const ui32 UnsyncedBlobCount;
        const std::vector<TNodeState> NodeStates;

        const bool ExpectPhantoms = false;
        const ui64 MemoryLimit = 1000_GB;
    };

    std::vector<TNodeState> GetStates(TBlobStorageGroupType erasure, EOnline online,
            bool phantomFlagStorageEnabled) {
        return std::vector<TNodeState>(erasure.BlobSubgroupSize(), 
                TNodeState{ .Online = online, .PhantomFlagStorageEnabled = phantomFlagStorageEnabled, });
    }

    std::vector<TNodeState> GetStatesAllAlive(TBlobStorageGroupType erasure) {
        return GetStates(erasure, EOnline::Alive, true);
    }

    std::vector<TNodeState> GetStatesOneDead(TBlobStorageGroupType erasure) {
        std::vector<TNodeState> states = GetStates(erasure, EOnline::Alive, true);
        states[0].Online = EOnline::Dead;
        return states;
    }

    std::vector<TNodeState> GetStatesTwoDead(TBlobStorageGroupType erasure) {
        std::vector<TNodeState> states = GetStates(erasure, EOnline::Alive, true);
        states[0].Online = EOnline::Dead;
        states[4].Online = EOnline::Dead;
        return states;
    }

    std::vector<TNodeState> GetStatesOneDeadAllRestart(TBlobStorageGroupType erasure) {
        std::vector<TNodeState> states = GetStates(erasure, EOnline::Restart, true);
        states[0].Online = EOnline::Dead;
        return states;
    }

    std::vector<TNodeState> GetStatesOneDeadActiveOneDeadInactive(TBlobStorageGroupType erasure) {
        std::vector<TNodeState> states = GetStates(erasure, EOnline::Alive, true);
        states[0].Online = EOnline::Dead;
        states[0].PhantomFlagStorageEnabled = false;
        states[4].Online = EOnline::Dead;
        states[0].PhantomFlagStorageEnabled = true;
        return states;
    }

    std::vector<TNodeState> GetStatesTwoDeadInactive(TBlobStorageGroupType erasure) {
        std::vector<TNodeState> states = GetStates(erasure, EOnline::Alive, true);
        states[0].Online = EOnline::Dead;
        states[0].PhantomFlagStorageEnabled = false;
        states[4].Online = EOnline::Dead;
        states[0].PhantomFlagStorageEnabled = false;
        return states;
    }

    std::vector<TNodeState> GetStatesTwoDeadAllAliveInactive(TBlobStorageGroupType erasure) {
        std::vector<TNodeState> states = GetStates(erasure, EOnline::Alive, false);
        states[0].Online = EOnline::Dead;
        states[0].PhantomFlagStorageEnabled = true;
        states[4].Online = EOnline::Dead;
        states[4].PhantomFlagStorageEnabled = false;
        return states;
    }

    std::vector<TNodeState> GetStatesTwoDeadSomeAliveInactive(TBlobStorageGroupType erasure) {
        std::vector<TNodeState> states = GetStates(erasure, EOnline::Alive, false);
        states[0].Online = EOnline::Dead;
        states[0].PhantomFlagStorageEnabled = true;
        states[1].PhantomFlagStorageEnabled = true;
        states[4].Online = EOnline::Dead;
        states[4].PhantomFlagStorageEnabled = false;
        return states;
    }

    void Test(TBlobStorageGroupType erasure, std::vector<TNodeState> nodeStates, bool expectPhantoms,
            ui64 memoryLimit) {
        auto it = std::find_if(nodeStates.begin(), nodeStates.end(),
                [&](const TNodeState& state) { return state.Online != EOnline::Dead; } );
        Y_VERIFY(it != nodeStates.end());
        ui32 controllerNodeId = it - nodeStates.begin() + 1;
        TTestCtx ctx({
            .NodeCount = erasure.BlobSubgroupSize(),
            .Erasure = erasure,
            .ControllerNodeId = controllerNodeId,
            .PDiskChunkSize = 32_MB,
            .EnablePhantomFlagStorage = false,
            .PhantomFlagStorageLimitPerVDiskBytes = memoryLimit,
            .TinySyncLog = true,
        }, 1000, 1000, nodeStates, expectPhantoms, memoryLimit);
        ctx.RunTest();
    }


    #define TEST_PHANTOM_BLOBS(name, erasure, expectPhantoms, memoryLimit)  \
    Y_UNIT_TEST(Test##name##erasure##MemoryLimit##memoryLimit) {            \
        auto e = TBlobStorageGroupType::Erasure##erasure;                   \
        Test(e, GetStates##name(e), expectPhantoms, memoryLimit);           \
    }

    TEST_PHANTOM_BLOBS(OneDead, Mirror3dc, false, 10_MB);
    TEST_PHANTOM_BLOBS(OneDead, Mirror3of4, false, 10_MB);
    TEST_PHANTOM_BLOBS(OneDead, 4Plus2Block, false, 10_MB);


    TEST_PHANTOM_BLOBS(TwoDead, Mirror3dc, false, 10_MB);

    // TODO (serg-belyakov@): persistent phantom flag storage
    // TEST_PHANTOM_BLOBS(OneDeadAllRestart, Mirror3dc, false);
    // TEST_PHANTOM_BLOBS(OneDeadAllRestart, Mirror3of4, false);
    // TEST_PHANTOM_BLOBS(OneDeadAllRestart, 4Plus2Block, false);

    TEST_PHANTOM_BLOBS(TwoDeadInactive, Mirror3dc, false, 10_MB);
    TEST_PHANTOM_BLOBS(OneDeadActiveOneDeadInactive, Mirror3dc, false, 10_MB);
    TEST_PHANTOM_BLOBS(TwoDeadAllAliveInactive, Mirror3dc, true, 10_MB);
    TEST_PHANTOM_BLOBS(TwoDeadSomeAliveInactive, Mirror3dc, false, 10_MB);

    TEST_PHANTOM_BLOBS(OneDead, Mirror3dc, true, 200_B);

    #undef TEST_PHANTOM_BLOBS
}
