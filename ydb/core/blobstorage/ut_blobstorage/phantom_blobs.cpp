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
        TTestCtx(TEnvironmentSetup::TSettings settings)
            : TTestCtxBase(std::move(settings)) {
        }

        void RunTest(ui32 initialBlobs, ui32 unsyncedBlobs, std::vector<ENodeState> nodeStates) {
            Y_VERIFY(nodeStates.size() == NodeCount);
            const ui64 blobSize = 10;
            const ui32 unsyncedBatchSize = 10000;
            Initialize();

            ui64 tabletId = 5000;
            ui32 channel = 1;
            ui32 generation = 1;
            ui32 step = 1;
            ui32 perGenCtr = 1;

            Ctest << "Write blobs" << Endl;
            std::vector<TLogoBlobID> blobs = WriteCompressedData(TDataProfile{
                .GroupId = GroupId,
                .TotalBlobs = initialBlobs,
                .BlobSize = blobSize,
                .TabletId = tabletId,
                .Channel = channel,
                .Generation = generation,
                .Step = step,
            });

            auto collectEverything = [&](TVector<TLogoBlobID>* keepFlags, TVector<TLogoBlobID>* doNotKeepFlags) {
                Env->Runtime->WrapInActorContext(Edge, [&] {
                    TString data;
                    SendToBSProxy(Edge, GroupId, new TEvBlobStorage::TEvCollectGarbage(
                            tabletId, generation, ++perGenCtr, channel, true, generation, step,
                            keepFlags, doNotKeepFlags, TInstant::Max(), true, false));
                });
                Env->WaitForEdgeActorEvent<TEvBlobStorage::TEvCollectGarbageResult>(
                        Edge, false, TInstant::Max());
            };

            Ctest << "Set Keep flags" << Endl;
            collectEverything(new TVector<TLogoBlobID>(blobs.begin(), blobs.end()), nullptr);

            Ctest << "Wait for sync" << Endl;
            Env->Sim(TDuration::Minutes(30));

            Ctest << "Shutdown and restart nodes" << Endl;
            for (ui32 nodeId = 1; nodeId <= NodeCount; ++nodeId) {
                switch (nodeStates[nodeId - 1]) {
                case ENodeState::Alive:
                    break;
                case ENodeState::Dead:
                    Env->StopNode(nodeId);
                    break;
                case ENodeState::Restart:
                    Env->StopNode(nodeId);
                    Env->Sim(TDuration::Minutes(1));
                    Env->StartNode(nodeId);
                    break;
                }
                Env->Sim(TDuration::Minutes(1));
            }

            Ctest << "Wait for sync" << Endl;
            Env->Sim(TDuration::Minutes(30));

            AllocateEdgeActor(); // reallocate actor, in case it lived on a restarted or dead node

            Ctest << "Set DoNotKeepFlags" << Endl;
            collectEverything(nullptr, new TVector<TLogoBlobID>(blobs.begin(), blobs.end()));

            for (ui32 i = 0; i < unsyncedBlobs; i += unsyncedBatchSize) {
                Ctest << "Write batch, blobs written# " << i << Endl;
                generation += 10;
                std::vector<TLogoBlobID> batch = WriteCompressedData(TDataProfile{
                    .GroupId = GroupId,
                    .TotalBlobs = unsyncedBatchSize,
                    .BlobSize = blobSize,
                    .BatchSize = 1000,
                    .TabletId = tabletId,
                    .Channel = channel,
                    .Generation = generation,
                    .Step = step,
                });
                // collectEverything(new TVector<TLogoBlobID>(batch.begin(), batch.end()), nullptr);
                // collectEverything(nullptr, new TVector<TLogoBlobID>(batch.begin(), batch.end()));
                collectEverything(nullptr, nullptr);
            }

            Ctest << "Wait for sync" << Endl;
            Env->Sim(TDuration::Minutes(30));

            Ctest << "Enable nodes" << Endl;
            for (ui32 nodeId = 1; nodeId <= NodeCount; ++nodeId) {
                if (nodeStates[nodeId - 1] == ENodeState::Dead) {
                    Env->StartNode(nodeId);
                }
            }

            Ctest << "Wait for sync" << Endl;
            Env->Sim(TDuration::Minutes(30));

            ++generation;
            Ctest << "Move soft barrier" << Endl;
            collectEverything(nullptr, nullptr);

            Env->Sim(TDuration::Minutes(30));

            // Env->Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            //     Cerr << " --- " << ev->Sender << "->" << ev->Recipient << ev->ToString() << Endl;
            //     return true;
            // };

            auto status = GetGroupStatus(GroupId);
            Ctest << "Group status# " << status->ToString() << Endl;

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
                    TLogoBlobID from(tabletId, 0, 0, channel, 0, 0, 1);
                    TLogoBlobID to(tabletId, generation + 100, 9000, channel, TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie, TLogoBlobID::MaxPartId);
                });
            }
        }
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

    void Test(TBlobStorageGroupType erasure, std::vector<ENodeState> nodeStates) {
        auto it = std::find_if(nodeStates.begin(), nodeStates.end(),
                [&](const ENodeState& state) { return state != ENodeState::Dead; } );
        Y_VERIFY(it != nodeStates.end());
        ui32 controllerNodeId = it - nodeStates.begin() + 1;
        TTestCtx ctx({
            .NodeCount = erasure.BlobSubgroupSize(),
            .Erasure = erasure,
            .ControllerNodeId = controllerNodeId,
            .PDiskChunkSize = 32_MB,
        });
        ctx.RunTest(1000, 3'000'000, nodeStates);
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
