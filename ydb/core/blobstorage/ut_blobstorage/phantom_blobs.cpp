#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/ut_helpers.h>

#include <util/random/random.h>

#include <algorithm>
#include <optional>
#include <random>
#include <unordered_set>

using namespace NKikimr;

#define Ctest Cnull

Y_UNIT_TEST_SUITE(PhantomBlobs) {

    enum class EOnline {
        Alive = 0,
        Dead,
        Restart,
    };

    struct TNodeState {
        EOnline Online;
        bool PhantomFlagStorageEnabled;
        ui64 MemoryLimit = 10_MB;
        bool PersistentPhantomFlagStorageEnabled = false;
    };

    struct TTestCtx : public TTestCtxBase {
        TTestCtx(TEnvironmentSetup::TSettings settings, ui32 initialBlobs, ui32 unsyncedBlobs,
                std::vector<TNodeState> nodeStates, bool expectPhantoms)
            : TTestCtxBase(std::move(settings))
            , InitialBlobCount(initialBlobs)
            , UnsyncedBlobCount(unsyncedBlobs)
            , NodeStates(nodeStates)
            , ExpectPhantoms(expectPhantoms)
        {
            Y_VERIFY(NodeStates.size() == NodeCount);
            SetIcbControls();
        }

        void SetIcbControls() {
            for (ui32 nodeId = 1; nodeId <= NodeCount; ++nodeId) {
                Env->SetIcbControl(nodeId, "VDiskControls.EnablePhantomFlagStorage",
                        NodeStates[nodeId - 1].PhantomFlagStorageEnabled);
                Env->SetIcbControl(nodeId, "VDiskControls.PhantomFlagStorageLimitPerVDiskBytes",
                        NodeStates[nodeId - 1].MemoryLimit);
                Env->SetIcbControl(nodeId, "VDiskControls.EnablePersistentPhantomFlagStorage",
                        NodeStates[nodeId - 1].PersistentPhantomFlagStorageEnabled);
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
                        keepFlags, doNotKeepFlags, TInstant::Max(), true, TWriteSource::Unknown, false));
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

        void CheckMemoryConsumption() {
            ui64 limit = 0;
            for (const TNodeState& nodeState : NodeStates) {
                if (nodeState.Online != EOnline::Dead) {
                    limit += nodeState.MemoryLimit;
                }
            }
            limit += 1_KB;  // O(1) overconsumption is OK
            ui64 consumed = Env->AggregateVDiskCounters(Env->StoragePoolName, NodeCount, NodeCount,
                    GroupId, PDiskLayout, "phantomflagstorage", "StoredFlagsMemoryConsumption");
            UNIT_ASSERT_C(consumed <= limit, "PhantomFlagStorage memory consumption exceeded expected limit, "
                    " Consumed# " << consumed << " Limit# " << limit);
            Ctest << "Checking memory consumption: Consumed# " << consumed << " Limit# " << limit << Endl;
        }

        void RunTest() {
            Initialize();
            const std::vector<TLogoBlobID> blobs = WriteInitialData();
            auto itMiddle1 = blobs.begin() + blobs.size() * 1 / 3;
            auto itMiddle2 = blobs.begin() + blobs.size() * 2 / 3;

            Ctest << "Set Keep flags" << Endl;
            CollectBlobs(new TVector<TLogoBlobID>(blobs.begin(), blobs.end()), nullptr);
            WaitForSync();

            Ctest << "Stop dead nodes" << Endl;
            ToggleNodes(true, false, false);
            WaitForSync();

            Ctest << "Set DoNotKeepFlags on first half of blobs" << Endl;
            CollectBlobs(nullptr, new TVector<TLogoBlobID>(blobs.begin(), itMiddle1));
            WaitForSync();

            WriteUnsyncedBlobs();

            BaldSyncLog();

            Ctest << "Set DoNotKeepFlags on second half of blobs" << Endl;
            CollectBlobs(nullptr, new TVector<TLogoBlobID>(itMiddle1, itMiddle2));
            WaitForSync();

            WriteUnsyncedBlobs();
            BaldSyncLog();
            CheckMemoryConsumption();

            CollectBlobs(nullptr, new TVector<TLogoBlobID>(itMiddle2, blobs.end()));

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

        void RunTestWithUpdate(std::vector<TNodeState> nodeStates2) {
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

            CheckMemoryConsumption();

            NodeStates = nodeStates2;
            SetIcbControls();

            CheckMemoryConsumption();

            Ctest << "Set DoNotKeepFlags on second half of blobs" << Endl;
            CollectBlobs(nullptr, new TVector<TLogoBlobID>(itMiddle, blobs.end()));
            WaitForSync();

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

        ui64 RunBlobSizeLimitTest(ui64 blobSizeLimit, ui64 blobSize, ui32 blobCount) {
            Initialize();

            for (ui32 nodeId = 1; nodeId <= NodeCount; ++nodeId) {
                Env->SetIcbControl(nodeId,
                        "VDiskControls.VolatilePhantomFlagStorageBlobSizeLimitBytes", blobSizeLimit);
            }

            Ctest << "Write blobs of size# " << blobSize << " count# " << blobCount << Endl;
            std::vector<TLogoBlobID> blobs = WriteCompressedData(TDataProfile{
                .GroupId = GroupId,
                .TotalBlobs = blobCount,
                .BlobSize = blobSize,
                .TabletId = TabletId,
                .Channel = Channel,
                .Generation = Generation,
                .Step = Step,
            });

            Ctest << "Set Keep flags" << Endl;
            CollectBlobs(new TVector<TLogoBlobID>(blobs.begin(), blobs.end()), nullptr);
            WaitForSync();

            Ctest << "Stop dead nodes" << Endl;
            ToggleNodes(true, false, false);
            WaitForSync();

            Ctest << "Set DoNotKeepFlags on all blobs" << Endl;
            CollectBlobs(nullptr, new TVector<TLogoBlobID>(blobs.begin(), blobs.end()));
            WaitForSync();

            WriteUnsyncedBlobs();
            BaldSyncLog();
            WaitForSync();

            ui64 storedFlags = Env->AggregateVDiskCounters(Env->StoragePoolName, NodeCount, NodeCount,
                    GroupId, PDiskLayout, "phantomflagstorage", "StoredFlagsCount");
            Ctest << "StoredFlagsCount# " << storedFlags << Endl;
            return storedFlags;
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
        std::vector<TNodeState> NodeStates;

        const bool ExpectPhantoms = false;
    };

    std::vector<TNodeState> GetStates(TBlobStorageGroupType erasure, EOnline online,
            bool phantomFlagStorageEnabled, bool isPersistent, ui64 memoryLimit) {
        return std::vector<TNodeState>(erasure.BlobSubgroupSize(), 
                TNodeState{
                    .Online = online,
                    .PhantomFlagStorageEnabled = phantomFlagStorageEnabled,
                    .MemoryLimit = memoryLimit,
                    .PersistentPhantomFlagStorageEnabled = isPersistent,
                });
    }

    std::vector<TNodeState> GetStatesAllAlive(TBlobStorageGroupType erasure, ui64 memoryLimit) {
        return GetStates(erasure, EOnline::Alive, true, false, memoryLimit);
    }

    std::vector<TNodeState> GetStatesOneDead(TBlobStorageGroupType erasure, ui64 memoryLimit) {
        std::vector<TNodeState> states = GetStates(erasure, EOnline::Alive, true, false, memoryLimit);
        states[0].Online = EOnline::Dead;
        return states;
    }

    std::vector<TNodeState> GetStatesTwoDead(TBlobStorageGroupType erasure, ui64 memoryLimit) {
        std::vector<TNodeState> states = GetStates(erasure, EOnline::Alive, true, false, memoryLimit);
        states[0].Online = EOnline::Dead;
        states[4].Online = EOnline::Dead;
        return states;
    }

    std::vector<TNodeState> GetStatesOneDeadAllRestart(TBlobStorageGroupType erasure,
            ui64 memoryLimit) {
        std::vector<TNodeState> states = GetStates(erasure, EOnline::Restart, true, true, memoryLimit);
        states[0].Online = EOnline::Dead;
        return states;
    }

    std::vector<TNodeState> GetStatesOneDeadActiveOneDeadInactive(TBlobStorageGroupType erasure,
            ui64 memoryLimit) {
        std::vector<TNodeState> states = GetStates(erasure, EOnline::Alive, true, false, memoryLimit);
        states[0].Online = EOnline::Dead;
        states[0].PhantomFlagStorageEnabled = false;
        states[4].Online = EOnline::Dead;
        states[0].PhantomFlagStorageEnabled = true;
        return states;
    }

    std::vector<TNodeState> GetStatesTwoDeadInactive(TBlobStorageGroupType erasure,
            ui64 memoryLimit) {
        std::vector<TNodeState> states = GetStates(erasure, EOnline::Alive, true, false, memoryLimit);
        states[0].Online = EOnline::Dead;
        states[0].PhantomFlagStorageEnabled = false;
        states[4].Online = EOnline::Dead;
        states[0].PhantomFlagStorageEnabled = false;
        return states;
    }

    std::vector<TNodeState> GetStatesTwoDeadAllAliveInactive(TBlobStorageGroupType erasure,
            ui64 memoryLimit) {
        std::vector<TNodeState> states = GetStates(erasure, EOnline::Alive, false, false, memoryLimit);
        states[0].Online = EOnline::Dead;
        states[0].PhantomFlagStorageEnabled = true;
        states[4].Online = EOnline::Dead;
        states[4].PhantomFlagStorageEnabled = false;
        return states;
    }

    std::vector<TNodeState> GetStatesTwoDeadSomeAliveInactive(TBlobStorageGroupType erasure,
            ui64 memoryLimit) {
        std::vector<TNodeState> states = GetStates(erasure, EOnline::Alive, false, false, memoryLimit);
        states[0].Online = EOnline::Dead;
        states[0].PhantomFlagStorageEnabled = true;
        states[1].PhantomFlagStorageEnabled = true;
        states[4].Online = EOnline::Dead;
        states[4].PhantomFlagStorageEnabled = false;
        return states;
    }

    void Test(TBlobStorageGroupType erasure, std::vector<TNodeState> nodeStates,
            std::optional<std::vector<TNodeState>> nodeStates2, bool expectPhantoms) {
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
            .TinySyncLog = true,
            .EnablePersistentPhantomFlagStorage = false,
        }, 300, 10000, nodeStates, expectPhantoms);
        if (nodeStates2) {
            ctx.RunTestWithUpdate(*nodeStates2);
        } else {
            ctx.RunTest();
        }
    }


    #define TEST_PHANTOM_BLOBS(name, erasure, expectPhantoms, memoryLimit)  \
    Y_UNIT_TEST(Test##name##erasure##MemoryLimit##memoryLimit) {            \
        auto e = TBlobStorageGroupType::Erasure##erasure;                   \
        Test(e, GetStates##name(e, memoryLimit), {}, expectPhantoms);       \
    }

    TEST_PHANTOM_BLOBS(OneDead, Mirror3dc, false, 10_KB);
    TEST_PHANTOM_BLOBS(OneDead, Mirror3of4, false, 10_KB);
    TEST_PHANTOM_BLOBS(OneDead, 4Plus2Block, false, 10_KB);


    TEST_PHANTOM_BLOBS(TwoDead, Mirror3dc, false, 10_KB);

    TEST_PHANTOM_BLOBS(OneDeadAllRestart, Mirror3dc, false, 10_KB);
    TEST_PHANTOM_BLOBS(OneDeadAllRestart, Mirror3of4, false, 10_KB);
    TEST_PHANTOM_BLOBS(OneDeadAllRestart, 4Plus2Block, false, 10_KB);

    TEST_PHANTOM_BLOBS(TwoDeadInactive, Mirror3dc, false, 10_KB);
    TEST_PHANTOM_BLOBS(OneDeadActiveOneDeadInactive, Mirror3dc, false, 10_KB);
    TEST_PHANTOM_BLOBS(TwoDeadAllAliveInactive, Mirror3dc, true, 10_KB);
    TEST_PHANTOM_BLOBS(TwoDeadSomeAliveInactive, Mirror3dc, false, 10_KB);

    TEST_PHANTOM_BLOBS(OneDead, Mirror3dc, true, 200_B);

    Y_UNIT_TEST(TestDisabling) {
        auto erasure = TBlobStorageGroupType::ErasureMirror3dc;
        auto states1 = GetStates(erasure, EOnline::Alive, true, false, 10_KB);
        states1[0].Online = EOnline::Dead;
        auto states2 = GetStates(erasure, EOnline::Alive, false, false, 10_KB);
        states2[0].Online = EOnline::Dead;
        Test(erasure, states1, states2, true);
    }

    Y_UNIT_TEST(TestEnabling) {
        auto erasure = TBlobStorageGroupType::ErasureMirror3dc;
        auto states1 = GetStates(erasure, EOnline::Alive, false, false, 10_KB);
        states1[0].Online = EOnline::Dead;
        auto states2 = GetStates(erasure, EOnline::Alive, true, false, 10_KB);
        states2[0].Online = EOnline::Dead;
        Test(erasure, states1, states2, true);
    }
    Y_UNIT_TEST(TestLoweringMemoryLimit) {
        auto erasure = TBlobStorageGroupType::ErasureMirror3dc;
        auto states1 = GetStates(erasure, EOnline::Alive, true, false, 10_KB);
        states1[0].Online = EOnline::Dead;
        auto states2 = GetStates(erasure, EOnline::Alive, true, false, 100_B);
        states2[0].Online = EOnline::Dead;
        Test(erasure, states1, states2, true);
    }

    void TestBlobSizeLimit(TBlobStorageGroupType erasure, ui64 blobSize, bool expectStored) {
        std::vector<TNodeState> nodeStates = GetStatesOneDead(erasure, 10_MB);
        auto it = std::find_if(nodeStates.begin(), nodeStates.end(),
                [&](const TNodeState& state) { return state.Online != EOnline::Dead; });
        Y_VERIFY(it != nodeStates.end());
        ui32 controllerNodeId = it - nodeStates.begin() + 1;
        TTestCtx ctx({
            .NodeCount = erasure.BlobSubgroupSize(),
            .Erasure = erasure,
            .ControllerNodeId = controllerNodeId,
            .PDiskChunkSize = 32_MB,
            .EnablePhantomFlagStorage = false,
            .TinySyncLog = true,
            .EnablePersistentPhantomFlagStorage = false,
        }, 0, 10000, nodeStates, false);

        ui64 storedFlags = ctx.RunBlobSizeLimitTest(1_MB, blobSize, 50);
        if (expectStored) {
            UNIT_ASSERT_C(storedFlags > 0,
                    "Expected blobs above the size limit to be stored, but StoredFlagsCount# " << storedFlags);
        } else {
            UNIT_ASSERT_VALUES_EQUAL_C(storedFlags, 0,
                    "Expected blobs below the size limit not to occupy space");
        }
    }

    Y_UNIT_TEST(TestBlobSizeLimitAboveLimit) {
        TestBlobSizeLimit(TBlobStorageGroupType::ErasureMirror3dc, 2_MB, true);
    }

    Y_UNIT_TEST(TestBlobSizeLimitBelowLimit) {
        TestBlobSizeLimit(TBlobStorageGroupType::ErasureMirror3dc, 200, false);
    }

    class TRandomizedPersistentTest : public TTestCtxBase {
        static constexpr ui32 NumStreams = 3;
        static constexpr ui32 BlobsPerStream = 24;
        static constexpr ui32 DeleteBatchSize = 6;
        static constexpr ui32 FillerBlobsPerIteration = 1000;

        struct TBlob {
            TLogoBlobID Id;
            TString Data;
            bool Deleted = false;
        };

        struct TStream {
            ui64 TabletId = 0;
            ui8 Channel = 0;
            ui32 Generation = 1;
            ui32 PerGenerationCounter = 0;
            std::vector<size_t> BlobIndexes;
            std::vector<size_t> DeleteOrder;
            size_t DeleteCursor = 0;
        };

    public:
        explicit TRandomizedPersistentTest(TBlobStorageGroupType erasure)
            : TTestCtxBase(MakeSettings(erasure))
            , Seed(RandomNumber<ui64>())
            , Rng(Seed)
        {}

        void Run(TDuration duration) {
            UNIT_ASSERT_C(duration > TDuration::Zero(), Context("non-positive test duration"));
            Cerr << "PersistentPhantomFlagStorage randomized test seed# " << Seed
                << " erasure# " << Erasure.ToString()
                << " duration# " << duration << Endl;

            Initialize();
            GroupInfo = Env->GetGroupInfo(GroupId);
            UNIT_ASSERT(GroupInfo);

            FindVDiskNodes();
            WriteInitialBlobs();
            SetKeepFlags();
            Env->Sim(TDuration::Minutes(10));

            SelectDeleteOrder();
            SelectLaggingNode();
            Env->StopNode(LaggingNodeId);
            Env->Sim(TDuration::Minutes(10));
            ReallocateEdgeActor();

            const TInstant deadline = TInstant::Now() + duration;
            ui32 iteration = 0;
            do {
                ApplyDeleteBatch();
                WriteFiller(iteration);
                Env->Sim(TDuration::Minutes(5));

                ForceFlagsToPersistentStorage(iteration);
                CommitPersistentState(iteration);

                const ui32 nodeId = ActiveNodeIds[Random(0, ActiveNodeIds.size())];
                RestartNode(nodeId, TStringBuilder() << "iteration# " << iteration);
                ++iteration;
            } while (TInstant::Now() < deadline);

            Cerr << "PersistentPhantomFlagStorage randomized workload iterations# " << iteration
                << " seed# " << Seed << Endl;

            // Restart every surviving VDisk. After this point no volatile copy of a phantom flag
            // from before the partition remains, so the lagging VDisk can only be repaired from
            // state recovered from PersistentPhantomFlagStorage.
            std::vector<ui32> restartOrder = ActiveNodeIds;
            std::shuffle(restartOrder.begin(), restartOrder.end(), Rng);
            for (ui32 nodeId : restartOrder) {
                RestartNode(nodeId, "final restart");
            }

            Env->StartNode(LaggingNodeId);
            Env->Sim(TDuration::Minutes(30));
            ReallocateEdgeActor();
            MoveSoftBarriers();
            Env->Sim(TDuration::Minutes(30));

            auto status = GetGroupStatus(GroupId);
            UNIT_ASSERT_VALUES_EQUAL_C(status->Get()->Status, NKikimrProto::OK, Context("group status"));
            CheckDeletedBlobsOnEveryVDisk();
            CheckLiveBlobs();
        }

    private:
        static TEnvironmentSetup::TSettings MakeSettings(TBlobStorageGroupType erasure) {
            return {
                .NodeCount = erasure.BlobSubgroupSize(),
                .Erasure = erasure,
                .ControllerNodeId = 1,
                .PDiskChunkSize = 32_MB,
                .EnablePhantomFlagStorage = true,
                .TinySyncLog = true,
                .EnablePersistentPhantomFlagStorage = true,
            };
        }

        ui32 Random(ui32 min, ui32 max) {
            UNIT_ASSERT_C(min < max, Context("invalid random range"));
            return std::uniform_int_distribution<ui32>(min, max - 1)(Rng);
        }

        TString Context(const TString& action, std::optional<ui32> iteration = std::nullopt) const {
            TStringBuilder str;
            str << "Seed# " << Seed << " Erasure# " << Erasure.ToString();
            if (iteration) {
                str << " Iteration# " << *iteration;
            }
            str << " Action# " << action;
            return str;
        }

        void FindVDiskNodes() {
            std::unordered_set<ui32> nodes;
            for (ui32 orderNumber = 0; orderNumber < Erasure.BlobSubgroupSize(); ++orderNumber) {
                nodes.insert(GroupInfo->GetActorId(orderNumber).NodeId());
            }
            UNIT_ASSERT_VALUES_EQUAL_C(nodes.size(), Erasure.BlobSubgroupSize(),
                Context("expected one VDisk per node"));
            VDiskNodeIds.assign(nodes.begin(), nodes.end());
            std::sort(VDiskNodeIds.begin(), VDiskNodeIds.end());
        }

        void WriteInitialBlobs() {
            Streams.resize(NumStreams);
            for (ui32 streamIdx = 0; streamIdx < NumStreams; ++streamIdx) {
                TStream& stream = Streams[streamIdx];
                stream.TabletId = 5000 + streamIdx;
                stream.Channel = streamIdx;

                for (ui32 step = 1; step <= BlobsPerStream; ++step) {
                    const ui32 size = Random(64, 1025);
                    const ui32 cookie = streamIdx * BlobsPerStream + step;
                    const TLogoBlobID id(stream.TabletId, 1, step, stream.Channel, size, cookie);
                    TString data(size, 'a' + (streamIdx + step) % 26);
                    Env->PutBlob(GroupId, id, data);
                    stream.BlobIndexes.push_back(Blobs.size());
                    Blobs.push_back({id, std::move(data)});
                }
            }
        }

        void SetKeepFlags() {
            for (TStream& stream : Streams) {
                auto keep = std::make_unique<TVector<TLogoBlobID>>();
                keep->reserve(stream.BlobIndexes.size());
                for (size_t index : stream.BlobIndexes) {
                    keep->push_back(Blobs[index].Id);
                }
                CollectGarbage(stream, std::move(keep), nullptr, 1, BlobsPerStream);
            }
        }

        void SelectDeleteOrder() {
            for (TStream& stream : Streams) {
                stream.DeleteOrder = stream.BlobIndexes;
                std::shuffle(stream.DeleteOrder.begin(), stream.DeleteOrder.end(), Rng);
                stream.DeleteOrder.resize(stream.DeleteOrder.size() * 3 / 4);
                UNIT_ASSERT_C(stream.DeleteOrder.size() >= DeleteBatchSize,
                    Context("delete order is smaller than a batch"));
            }
        }

        void SelectLaggingNode() {
            std::vector<ui32> candidates;
            for (ui32 nodeId : VDiskNodeIds) {
                if (nodeId != Env->Settings.ControllerNodeId) {
                    candidates.push_back(nodeId);
                }
            }
            UNIT_ASSERT_C(!candidates.empty(), Context("no lagging-node candidate"));
            LaggingNodeId = candidates[Random(0, candidates.size())];

            for (ui32 nodeId : VDiskNodeIds) {
                if (nodeId != LaggingNodeId) {
                    ActiveNodeIds.push_back(nodeId);
                }
            }
        }

        void ApplyDeleteBatch() {
            for (TStream& stream : Streams) {
                auto doNotKeep = std::make_unique<TVector<TLogoBlobID>>();
                doNotKeep->reserve(DeleteBatchSize);
                for (ui32 i = 0; i < DeleteBatchSize; ++i) {
                    const size_t blobIndex = stream.DeleteOrder[stream.DeleteCursor % stream.DeleteOrder.size()];
                    ++stream.DeleteCursor;
                    TBlob& blob = Blobs[blobIndex];
                    blob.Deleted = true;
                    doNotKeep->push_back(blob.Id);
                }
                std::sort(doNotKeep->begin(), doNotKeep->end());
                CollectGarbage(stream, nullptr, std::move(doNotKeep), 1, BlobsPerStream);
            }
        }

        void WriteFiller(ui32 iteration) {
            const ui32 generation = 10 + iteration * 2;
            WriteCompressedData(TDataProfile{
                .GroupId = GroupId,
                .TotalBlobs = FillerBlobsPerIteration,
                .BlobSize = Random(8, 65),
                .BatchSize = 250,
                .TabletId = FillerTabletId,
                .Channel = FillerChannel,
                .Generation = generation,
                .Step = 1,
            });
            CollectFiller(generation);
        }

        void ForceFlagsToPersistentStorage(ui32 iteration) {
            std::unordered_set<ui32> committedNodes;
            const std::unordered_set<ui32> expectedNodes(ActiveNodeIds.begin(), ActiveNodeIds.end());
            Env->Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
                if (ev->GetTypeRewrite() == TEvBlobStorage::EvPhantomFlagStorageCommitData &&
                        expectedNodes.contains(ev->Sender.NodeId())) {
                    committedNodes.insert(ev->Sender.NodeId());
                }
                return true;
            };

            BaldActiveSyncLogs(iteration);
            for (ui32 attempt = 0; attempt < 60 && committedNodes.size() != expectedNodes.size(); ++attempt) {
                Env->Sim(TDuration::Seconds(30));
            }
            Env->Runtime->FilterFunction = {};

            UNIT_ASSERT_VALUES_EQUAL_C(committedNodes.size(), expectedNodes.size(),
                Context("not every active VDisk committed persistent phantom flags", iteration));
            for (ui32 nodeId : expectedNodes) {
                UNIT_ASSERT_C(committedNodes.contains(nodeId),
                    Context(TStringBuilder() << "missing persistent commit on node# " << nodeId, iteration));
            }
        }

        void BaldActiveSyncLogs(ui32 iteration) {
            for (ui32 orderNumber = 0; orderNumber < Erasure.BlobSubgroupSize(); ++orderNumber) {
                const TActorId actorId = GroupInfo->GetActorId(orderNumber);
                if (actorId.NodeId() == LaggingNodeId) {
                    continue;
                }

                const TVDiskID vdiskId = GroupInfo->GetVDiskId(orderNumber);
                const TActorId sender = Env->Runtime->AllocateEdgeActor(actorId.NodeId(), __FILE__, __LINE__);
                Env->Runtime->WrapInActorContext(sender, [&] {
                    TActivationContext::Send(new IEventHandle(
                        actorId, sender, new TEvBlobStorage::TEvVBaldSyncLog(vdiskId, true)));
                });
                auto result = Env->WaitForEdgeActorEvent<TEvBlobStorage::TEvVBaldSyncLogResult>(
                    sender, false, TInstant::Max());
                UNIT_ASSERT_C(result, Context("no BaldSyncLog response", iteration));
                UNIT_ASSERT_VALUES_EQUAL_C(result->Get()->Record.GetStatus(), NKikimrProto::OK,
                    Context(TStringBuilder() << "BaldSyncLog failed for orderNumber# " << orderNumber, iteration));
            }
        }

        void CommitPersistentState(ui32 iteration) {
            // A following SyncLog commit records the PersistentPhantomFlagStorage chunk map in
            // the SyncLog entry point before the node is restarted.
            const ui32 generation = 11 + iteration * 2;
            CollectFiller(generation);
            Env->Sim(TDuration::Minutes(5));
        }

        void CollectFiller(ui32 generation) {
            TStream filler{
                .TabletId = FillerTabletId,
                .Channel = FillerChannel,
                .Generation = generation,
                .PerGenerationCounter = 0,
            };
            CollectGarbage(filler, nullptr, nullptr, generation, 1);
        }

        void CollectGarbage(TStream& stream, std::unique_ptr<TVector<TLogoBlobID>> keep,
                std::unique_ptr<TVector<TLogoBlobID>> doNotKeep, ui32 collectGeneration, ui32 collectStep) {
            const ui64 tabletId = stream.TabletId;
            const ui32 generation = stream.Generation;
            const ui32 perGenerationCounter = ++stream.PerGenerationCounter;
            const ui8 channel = stream.Channel;
            Env->Runtime->WrapInActorContext(Edge, [&] {
                SendToBSProxy(Edge, GroupId, new TEvBlobStorage::TEvCollectGarbage(
                    tabletId, generation, perGenerationCounter, channel, true,
                    collectGeneration, collectStep, keep.release(), doNotKeep.release(),
                    TInstant::Max(), true));
            });
            auto result = Env->WaitForEdgeActorEvent<TEvBlobStorage::TEvCollectGarbageResult>(
                Edge, false, TInstant::Max());
            UNIT_ASSERT_C(result, Context("no CollectGarbage response"));
            UNIT_ASSERT_VALUES_EQUAL_C(result->Get()->Status, NKikimrProto::OK,
                Context(TStringBuilder() << "CollectGarbage failed# " << result->Get()->ErrorReason));
        }

        void RestartNode(ui32 nodeId, const TString& action) {
            Env->StopNode(nodeId);
            Env->Sim(TDuration::Minutes(1));
            Env->StartNode(nodeId);
            Env->Sim(TDuration::Minutes(5));
            ReallocateEdgeActor();
            auto status = GetGroupStatus(GroupId);
            UNIT_ASSERT_VALUES_EQUAL_C(status->Get()->Status, NKikimrProto::OK,
                Context(TStringBuilder() << action << " node# " << nodeId));
        }

        void ReallocateEdgeActor() {
            UNIT_ASSERT_C(!ActiveNodeIds.empty() || !VDiskNodeIds.empty(), Context("no node for edge actor"));
            const ui32 nodeId = !ActiveNodeIds.empty() ? ActiveNodeIds.front() : VDiskNodeIds.front();
            AllocateEdgeActorOnSpecificNode(nodeId);
        }

        void MoveSoftBarriers() {
            for (TStream& stream : Streams) {
                ++stream.Generation;
                stream.PerGenerationCounter = 0;
                CollectGarbage(stream, nullptr, nullptr, stream.Generation, Max<ui32>());
            }
        }

        void CheckDeletedBlobsOnEveryVDisk() {
            std::vector<TLogoBlobID> deleted;
            for (const TBlob& blob : Blobs) {
                if (blob.Deleted) {
                    deleted.push_back(blob.Id);
                }
            }
            UNIT_ASSERT_C(!deleted.empty(), Context("randomized workload did not delete blobs"));

            for (ui32 orderNumber = 0; orderNumber < Erasure.BlobSubgroupSize(); ++orderNumber) {
                const TVDiskID vdiskId = GroupInfo->GetVDiskId(orderNumber);
                Env->WithQueueId(vdiskId, NKikimrBlobStorage::EVDiskQueueId::GetFastRead,
                    [&](TActorId queueId) {
                        const TActorId sender = Env->Runtime->AllocateEdgeActor(
                            queueId.NodeId(), __FILE__, __LINE__);
                        auto query = TEvBlobStorage::TEvVGet::CreateExtremeIndexQuery(
                            vdiskId, TInstant::Max(), NKikimrBlobStorage::EGetHandleClass::FastRead);
                        for (const TLogoBlobID& id : deleted) {
                            query->AddExtremeQuery(id, 0, 0);
                        }
                        Env->Runtime->Send(new IEventHandle(queueId, sender, query.release()), sender.NodeId());
                        auto result = Env->WaitForEdgeActorEvent<TEvBlobStorage::TEvVGetResult>(
                            sender, false, TInstant::Max());
                        UNIT_ASSERT_C(result, Context("no VGet response"));
                        const auto& record = result->Get()->Record;
                        UNIT_ASSERT_VALUES_EQUAL_C(record.GetStatus(), NKikimrProto::OK,
                            Context(TStringBuilder() << "VGet failed for orderNumber# " << orderNumber));
                        UNIT_ASSERT_VALUES_EQUAL_C(record.ResultSize(), deleted.size(),
                            Context(TStringBuilder() << "wrong VGet result size for orderNumber# " << orderNumber));
                        for (ui32 i = 0; i < record.ResultSize(); ++i) {
                            UNIT_ASSERT_VALUES_EQUAL_C(record.GetResult(i).GetStatus(), NKikimrProto::NODATA,
                                Context(TStringBuilder() << "phantom blob# " << deleted[i]
                                    << " orderNumber# " << orderNumber));
                            UNIT_ASSERT_C(!record.GetResult(i).HasIngress(),
                                Context(TStringBuilder() << "phantom ingress# " << deleted[i]
                                    << " orderNumber# " << orderNumber));
                        }
                    });
            }
        }

        void CheckLiveBlobs() {
            for (const TBlob& blob : Blobs) {
                if (blob.Deleted) {
                    continue;
                }

                Env->Runtime->WrapInActorContext(Edge, [&] {
                    SendToBSProxy(Edge, GroupId, new TEvBlobStorage::TEvGet(
                        blob.Id, 0, 0, TInstant::Max(), NKikimrBlobStorage::EGetHandleClass::FastRead));
                });
                auto result = Env->WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(
                    Edge, false, TInstant::Max());
                UNIT_ASSERT_C(result, Context("no Get response"));
                UNIT_ASSERT_VALUES_EQUAL_C(result->Get()->Status, NKikimrProto::OK,
                    Context(TStringBuilder() << "Get failed for live blob# " << blob.Id));
                UNIT_ASSERT_VALUES_EQUAL_C(result->Get()->ResponseSz, 1,
                    Context(TStringBuilder() << "wrong Get response size for live blob# " << blob.Id));
                const auto& response = result->Get()->Responses[0];
                UNIT_ASSERT_VALUES_EQUAL_C(response.Status, NKikimrProto::OK,
                    Context(TStringBuilder() << "live blob was collected# " << blob.Id));
                UNIT_ASSERT_VALUES_EQUAL_C(response.Buffer.ConvertToString(), blob.Data,
                    Context(TStringBuilder() << "wrong data for live blob# " << blob.Id));
            }
        }

    private:
        static constexpr ui64 FillerTabletId = 9000;
        static constexpr ui8 FillerChannel = 0;

        const ui64 Seed;
        std::mt19937_64 Rng;
        TIntrusivePtr<TBlobStorageGroupInfo> GroupInfo;
        std::vector<TBlob> Blobs;
        std::vector<TStream> Streams;
        std::vector<ui32> VDiskNodeIds;
        std::vector<ui32> ActiveNodeIds;
        ui32 LaggingNodeId = 0;
    };

    Y_UNIT_TEST(TestRandomizedPersistentMirror3dc) {
        TRandomizedPersistentTest(TBlobStorageGroupType::ErasureMirror3dc).Run(TDuration::Seconds(120));
    }

    Y_UNIT_TEST(TestRandomizedPersistent4Plus2Block) {
        TRandomizedPersistentTest(TBlobStorageGroupType::Erasure4Plus2Block).Run(TDuration::Seconds(120));
    }

    #undef TEST_PHANTOM_BLOBS
}
