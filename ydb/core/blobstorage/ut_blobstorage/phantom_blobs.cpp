#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/ut_helpers.h>

using namespace NKikimr;

#define Ctest Cerr

Y_UNIT_TEST_SUITE(PhantomBlobs) {

    enum class ENodeState {
        Alive = 0,
        Dead,
        Restart,
    };

    struct TTestCtx : public TTestCtxBase {
        TTestCtx(TEnvironmentSetup::TSettings settings, ui32 initialBlobs, ui32 unsyncedBlobs,
                std::vector<ENodeState> NodeStates)
            : TTestCtxBase(std::move(settings))
            , InitialBlobCount(initialBlobs)
            , UnsyncedBlobCount(unsyncedBlobs)
            , NodeStates(NodeStates)
        {
            Y_VERIFY(NodeStates.size() == NodeCount);
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

        void ToggleNodes(bool stopAndRestart) {
            for (ui32 nodeId = 1; nodeId <= NodeCount; ++nodeId) {
                switch (NodeStates[nodeId - 1]) {
                case ENodeState::Alive:
                    break;
                case ENodeState::Dead:
                    if (stopAndRestart) {
                        Ctest << "Stop node# " << nodeId << Endl;
                        Env->StopNode(nodeId);
                    } else {
                        Ctest << "Start node# " << nodeId << Endl;
                        Env->StartNode(nodeId);
                    }
                    break;
                case ENodeState::Restart:
                    if (stopAndRestart) {
                        Ctest << "Restart node# " << nodeId << Endl;
                        Env->StopNode(nodeId);
                        Env->Sim(TDuration::Minutes(1));
                        Env->StartNode(nodeId);
                    }
                    break;
                }
                Env->Sim(TDuration::Minutes(1));
            }
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
                if (NodeStates[nodeId - 1] == ENodeState::Dead) {
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

        void RunTest() {
            Initialize();
            const std::vector<TLogoBlobID> blobs = WriteInitialData();
            auto itMiddle = blobs.begin() + blobs.size() / 2;

            Ctest << "Set Keep flags" << Endl;
            CollectBlobs(new TVector<TLogoBlobID>(blobs.begin(), blobs.end()), nullptr);
            WaitForSync();

            ToggleNodes(true);
            WaitForSync();

            Ctest << "Set DoNotKeepFlags on first half of blobs" << Endl;
            CollectBlobs(nullptr, new TVector<TLogoBlobID>(blobs.begin(), itMiddle));
            WaitForSync();

            WriteUnsyncedBlobs();

            BaldSyncLog();

            Ctest << "Set DoNotKeepFlags on second half of blobs" << Endl;
            CollectBlobs(nullptr, new TVector<TLogoBlobID>(itMiddle, blobs.end()));
            WaitForSync();

            ToggleNodes(false);
            WaitForSync();

            ++Generation;
            Ctest << "Move soft barrier" << Endl;
            CollectBlobs(nullptr, nullptr);
            WaitForSync();

            CheckStatus();
            CheckBlobs(blobs);
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
        const std::vector<ENodeState> NodeStates;
    };

    std::vector<ENodeState> GetStatesAllAlive(TBlobStorageGroupType erasure) {
        return std::vector<ENodeState>(erasure.BlobSubgroupSize(), ENodeState::Alive);
    }

    std::vector<ENodeState> GetStatesOneDead(TBlobStorageGroupType erasure) {
        std::vector<ENodeState> states(erasure.BlobSubgroupSize(), ENodeState::Alive);
        states[0] = ENodeState::Dead;
        return states;
    }

    std::vector<ENodeState> GetStatesTwoDead(TBlobStorageGroupType erasure) {
        std::vector<ENodeState> states(erasure.BlobSubgroupSize(), ENodeState::Alive);
        states[0] = ENodeState::Dead;
        states[4] = ENodeState::Dead;
        return states;
    }

    std::vector<ENodeState> GetStatesOneDeadAllRestart(TBlobStorageGroupType erasure) {
        std::vector<ENodeState> states(erasure.BlobSubgroupSize(), ENodeState::Restart);
        states[0] = ENodeState::Dead;
        return states;
    }

    void Test(TBlobStorageGroupType erasure, std::vector<ENodeState> NodeStates) {
        auto it = std::find_if(NodeStates.begin(), NodeStates.end(),
                [&](const ENodeState& state) { return state != ENodeState::Dead; } );
        Y_VERIFY(it != NodeStates.end());
        ui32 controllerNodeId = it - NodeStates.begin() + 1;
        TTestCtx ctx({
            .NodeCount = erasure.BlobSubgroupSize(),
            .Erasure = erasure,
            .ControllerNodeId = controllerNodeId,
            .PDiskChunkSize = 32_MB,
            .EnablePhantomFlagStorage = true,
            .TinySyncLog = true,
        }, 1000, 1000, NodeStates);
        ctx.RunTest();
    }


    #define TEST_PHANTOM_BLOBS(name, erasure)                   \
    Y_UNIT_TEST(Test##name##erasure) {                          \
        auto e = TBlobStorageGroupType::Erasure##erasure;       \
        Test(e, GetStates##name(e));                            \
    }

    // TEST_PHANTOM_BLOBS(AllAlive, Mirror3dc);
    // TEST_PHANTOM_BLOBS(AllAlive, Mirror3of4);
    // TEST_PHANTOM_BLOBS(AllAlive, 4Plus2Block);

    TEST_PHANTOM_BLOBS(OneDead, Mirror3dc);
    // TEST_PHANTOM_BLOBS(OneDead, Mirror3of4);
    // TEST_PHANTOM_BLOBS(OneDead, 4Plus2Block);


    TEST_PHANTOM_BLOBS(TwoDead, Mirror3dc);

    // TEST_PHANTOM_BLOBS(OneDeadAllRestart, Mirror3dc);
    // TEST_PHANTOM_BLOBS(OneDeadAllRestart, Mirror3of4);
    // TEST_PHANTOM_BLOBS(OneDeadAllRestart, 4Plus2Block);

}
