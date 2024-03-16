#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

#include <library/cpp/iterator/enumerate.h>

#include <util/random/entropy.h>


using TPartsLocations = TVector<TVector<ui8>>;

TString ToString(const TPartsLocations& partsLocations) {
    TStringBuilder b;
    b << "[";
    for (const auto& parts: partsLocations) {
        if (parts.size() == 0) {
            b << ". ";
        } else {
            for (auto x: parts) {
                b << ToString(x);
            }
            b << " ";
        }
    }
    b << "]";
    return b;
}


struct TTestEnv {
    TTestEnv(ui32 nodeCount, TBlobStorageGroupType erasure)
    : Env({
        .NodeCount = nodeCount,
        .VDiskReplPausedAtStart = false,
        .Erasure = erasure,
        .FeatureFlags = MakeFeatureFlags(),
    })
    {
        Env.CreateBoxAndPool(1, 1);
        Env.Sim(TDuration::Minutes(1));

        auto groups = Env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
        GroupInfo = Env.GetGroupInfo(groups.front());

        for (ui32 i = 0; i < Env.Settings.NodeCount; ++i) {
            RunningNodes.insert(i);
        }
    }

    static TFeatureFlags MakeFeatureFlags() {
        TFeatureFlags res;
        res.SetUseVDisksBalancing(true);
        return res;
    }

    static TString PrepareData(const ui32 dataLen, const ui32 start) {
        TString data(Reserve(dataLen));
        for (ui32 i = 0; i < dataLen; ++i) {
            data.push_back('a' + (start + i) % 26);
        }
        return data;
    };

    void SendPut(ui32 step, const TString& data, NKikimrProto::EReplyStatus expectedStatus) {
        const TLogoBlobID id(1, 1, step, 0, data.size(), 0);
        Cerr << "SEND TEvPut with key " << id.ToString() << Endl;
        const TActorId sender = Env.Runtime->AllocateEdgeActor(GroupInfo->GetActorId(*RunningNodes.begin()).NodeId(), __FILE__, __LINE__);
        auto ev = std::make_unique<TEvBlobStorage::TEvPut>(id, data, TInstant::Max());
        Env.Runtime->WrapInActorContext(sender, [&] {
            SendToBSProxy(sender, GroupInfo->GroupID, ev.release());
        });
        auto res = Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(sender, false);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, expectedStatus);
        Cerr << "TEvPutResult: " << res->Get()->ToString() << Endl;
    };

    auto SendGet(ui32 step, ui32 dataSize, bool mustRestoreFirst=false) {
        const TLogoBlobID blobId(1, 1, step, 0, dataSize, 0);
        Cerr << "SEND TEvGet with key " << blobId.ToString() << Endl;
        const TActorId sender = Env.Runtime->AllocateEdgeActor(GroupInfo->GetActorId(*RunningNodes.begin()).NodeId(), __FILE__, __LINE__);
        auto ev = std::make_unique<TEvBlobStorage::TEvGet>(
            blobId,
            /* shift */ 0,
            /* size */ dataSize,
            TInstant::Max(),
            NKikimrBlobStorage::EGetHandleClass::FastRead,
            mustRestoreFirst
        );
        Env.Runtime->WrapInActorContext(sender, [&] () {
            SendToBSProxy(sender, GroupInfo->GroupID, ev.release());
        });
        TInstant getDeadline = Env.Now() + TDuration::Seconds(30);
        auto res = Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(sender, /* termOnCapture */ false, getDeadline);
        Cerr << "TEvGetResult: " << res->Get()->ToString() << Endl;
        return res;
    };

    TActorId GetQueue(const TVDiskID& vDiskId) {
        if (!Queues.contains(vDiskId)) {
            Queues[vDiskId] = Env.CreateQueueActor(vDiskId, NKikimrBlobStorage::EVDiskQueueId::GetFastRead, 1000);
        }
        return Queues[vDiskId];
    }

    TVector<ui32> GetParts(ui32 position, const TLogoBlobID& blobId) {
        if (!RunningNodes.contains(position)) {
            return {};
        }
        auto vDiskId = GroupInfo->GetVDiskId(position);
        auto ev = TEvBlobStorage::TEvVGet::CreateExtremeIndexQuery(
            vDiskId, TInstant::Max(), NKikimrBlobStorage::EGetHandleClass::AsyncRead,
            TEvBlobStorage::TEvVGet::EFlags::None, 0,
            {{blobId, 0, 0}}
        );
        const TActorId sender = Env.Runtime->AllocateEdgeActor(GroupInfo->GetActorId(*RunningNodes.begin()).NodeId(), __FILE__, __LINE__);
        TVector<ui32> partsRes;

        Cerr << "Get request for vdisk " << position << Endl;
        auto queueId = GetQueue(vDiskId);
        Env.Runtime->WrapInActorContext(sender, [&] {
            Env.Runtime->Send(new IEventHandle(queueId, sender, ev.release()));
        });
        auto res = Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVGetResult>(sender, false);
        auto parts = res->Get()->Record.GetResult().at(0).GetParts();
        partsRes = TVector<ui32>(parts.begin(), parts.end());
        return partsRes;
    }

    TPartsLocations GetExpectedPartsLocations(const TLogoBlobID& blobId) {
        TPartsLocations result(GroupInfo->GetTopology().GType.BlobSubgroupSize());
        TBlobStorageGroupInfo::TOrderNums orderNums;
        GroupInfo->GetTopology().PickSubgroup(blobId.Hash(), orderNums);
        for (ui32 i = 0; i < GroupInfo->GetTopology().GType.TotalPartCount(); ++i) {
            result[orderNums[i]].push_back(i + 1);
        }
        return result;
    }

    TPartsLocations GetActualPartsLocations(const TLogoBlobID& blobId) {
        TPartsLocations result(GroupInfo->GetTopology().GType.BlobSubgroupSize());
        for (ui32 i = 0; i < result.size(); ++i) {
            for (ui32 part: GetParts(i, blobId)) {
                result[i].push_back(part);
            }
            Sort(result[i].begin(), result[i].end());
        }
        return result;
    }

    bool CheckPartsLocations(const TLogoBlobID& blobId) {
        auto expectedParts = GetExpectedPartsLocations(blobId);
        auto actualParts = GetActualPartsLocations(blobId);
        TString errMsg = ToString(expectedParts) + " != " + ToString(actualParts);
        UNIT_ASSERT_VALUES_EQUAL_C(expectedParts.size(), actualParts.size(), errMsg);

        for (ui32 i = 0; i < expectedParts.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL_C(expectedParts[i].size(), actualParts[i].size(), errMsg);
            for (ui32 j = 0; j < expectedParts[i].size(); ++j) {
                UNIT_ASSERT_VALUES_EQUAL_C(expectedParts[i][j], actualParts[i][j], errMsg);
            }
        }

        return true;
    }

    void StopNode(ui32 position) {
        if (!RunningNodes.contains(position)) {
            return;
        }
        Env.StopNode(GroupInfo->GetActorId(position).NodeId());
        RunningNodes.erase(position);
    }

    void StartNode(ui32 position) {
        if (RunningNodes.contains(position)) {
            return;
        }
        Env.StartNode(GroupInfo->GetActorId(position).NodeId());
        RunningNodes.insert(position);
        for (auto [_, queueId]: Queues) {
            Env.Runtime->Send(new IEventHandle(TEvents::TSystem::Poison, 0, queueId, {}, nullptr, 0), queueId.NodeId());
        }
        Queues.clear();
    }

    TEnvironmentSetup* operator->() {
        return &Env;
    }

    TEnvironmentSetup Env;
    TIntrusivePtr<TBlobStorageGroupInfo> GroupInfo;
    THashSet<ui32> RunningNodes;
    THashMap<TVDiskID, TActorId> Queues;
};

TLogoBlobID MakeLogoBlobId(ui32 step, ui32 dataSize) {
    return TLogoBlobID(1, 1, step, 0, dataSize, 0);
}


TString GenData(ui32 len) {
    TString res = TString::Uninitialized(len);
    EntropyPool().Read(res.Detach(), res.size());
    return res;
}


struct TStopOneNodeTest {
    TTestEnv Env;
    TString data;

    void RunTest() {
        ui32 step = 0;

        { // Check just a normal put works
            Env.SendPut(++step, data, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(Env.SendGet(step, data.size())->Get()->Responses[0].Buffer.ConvertToString(), data);
            Env.CheckPartsLocations(MakeLogoBlobId(step, data.size()));
        }


        { // Stop one node that should have a part, make put, start it and check that blob would be moved from handoff on main
            auto blobId = MakeLogoBlobId(++step, data.size());
            auto locations = Env.GetExpectedPartsLocations(blobId);
            ui32 nodeIdWithBlob = 0;
            while (locations[nodeIdWithBlob].size() == 0) ++nodeIdWithBlob;

            Env.StopNode(nodeIdWithBlob);
            Env.SendPut(step, data, NKikimrProto::OK);
            Env->Sim(TDuration::Seconds(10));
            Env.StartNode(nodeIdWithBlob);
            Env->Sim(TDuration::Seconds(10));

            Cerr << "Start compaction 1" << Endl;
            for (ui32 pos = 0; pos < Env->Settings.NodeCount; ++pos) {
                Env->CompactVDisk(Env.GroupInfo->GetActorId(pos));
            }
            Env->Sim(TDuration::Seconds(10));
            Cerr << "Finish compaction 1" << Endl;

            Cerr << "Start compaction 2" << Endl;
            for (ui32 pos = 0; pos < Env->Settings.NodeCount; ++pos) {
                Env->CompactVDisk(Env.GroupInfo->GetActorId(pos));
            }
            Env->Sim(TDuration::Seconds(10));
            Cerr << "Finish compaction 2" << Endl;

            Cerr << "Start compaction 3" << Endl;
            for (ui32 pos = 0; pos < Env->Settings.NodeCount; ++pos) {
                Env->CompactVDisk(Env.GroupInfo->GetActorId(pos));
            }
            Env->Sim(TDuration::Seconds(10));
            Cerr << "Finish compaction 3" << Endl;

            Env.CheckPartsLocations(MakeLogoBlobId(step, data.size()));
            UNIT_ASSERT_VALUES_EQUAL(Env.SendGet(step, data.size())->Get()->Responses[0].Buffer.ConvertToString(), data);
        }
    }
};

struct TRandomTest {
    TTestEnv Env;
    ui32 NumIters;

    void RunTest() {
        TVector<TString> data(Reserve(NumIters));

        for (ui32 step = 0; step < NumIters; ++step) {
            Cerr << step << Endl;
            data.push_back(GenData(16 + random() % 4096));
            auto blobId = MakeLogoBlobId(step, data.back().size());
            auto locations = Env.GetExpectedPartsLocations(blobId);

            if (random() % 10 == 1 && Env.RunningNodes.size() + 2 > Env->Settings.NodeCount) {
                ui32 nodeId = random() % Env->Settings.NodeCount;
                Cerr << "Stop node " << nodeId << Endl;
                Env.StopNode(nodeId);
                Env->Sim(TDuration::Seconds(10));
            }

            Env.SendPut(step, data.back(), NKikimrProto::OK);

            if (random() % 10 == 1) {
                for (ui32 pos = 0; pos < Env->Settings.NodeCount; ++pos) {
                    if (!Env.RunningNodes.contains(pos)) {
                        Cerr << "Start node " << pos << Endl;
                        Env.StartNode(pos);
                        Env->Sim(TDuration::Seconds(10));
                        break;
                    }
                }
            }

            if (random() % 50 == 1) {
                ui32 pos = random() % Env->Settings.NodeCount;
                if (Env.RunningNodes.contains(pos)) {
                    Env->CompactVDisk(Env.GroupInfo->GetActorId(pos));
                    Env->Sim(TDuration::Seconds(10));
                }
            }

            // Wipe random node
            if (random() % 100 == 1) {
                ui32 pos = random() % Env->Settings.NodeCount;
                if (Env.RunningNodes.contains(pos)) {
                    auto baseConfig = Env->FetchBaseConfig();
                    const auto& someVSlot = baseConfig.GetVSlot(pos);
                    const auto& loc = someVSlot.GetVSlotId();
                    Env->Wipe(loc.GetNodeId(), loc.GetPDiskId(), loc.GetVSlotId(),
                        TVDiskID(someVSlot.GetGroupId(), someVSlot.GetGroupGeneration(), someVSlot.GetFailRealmIdx(),
                        someVSlot.GetFailDomainIdx(), someVSlot.GetVDiskIdx()));
                    Env->Sim(TDuration::Seconds(10));
                }
            }
        }

        for (ui32 pos = 0; pos < Env->Settings.NodeCount; ++pos) {
            Env.StartNode(pos);
        }

        Env->Sim(TDuration::Seconds(300));
        Cerr << "Start checking" << Endl;
        for (ui32 step = 0; step < NumIters; ++step) {
            Cerr << step << Endl;
            Env.CheckPartsLocations(MakeLogoBlobId(step, data[step].size()));
            UNIT_ASSERT_VALUES_EQUAL(Env.SendGet(step, data[step].size())->Get()->Responses[0].Buffer.ConvertToString(), data[step]);
        }
    }
};



Y_UNIT_TEST_SUITE(VDiskBalancing) {

    Y_UNIT_TEST(TestStopOneNode_Block42) {
        TStopOneNodeTest{TTestEnv(8, TBlobStorageGroupType::Erasure4Plus2Block), GenData(100)}.RunTest();
    }
    Y_UNIT_TEST(TestStopOneNode_Mirror3dc) {
        TStopOneNodeTest{TTestEnv(9, TBlobStorageGroupType::ErasureMirror3dc), GenData(100)}.RunTest();
    }
    Y_UNIT_TEST(TestStopOneNode_Block42_HugeBlob) {
        TStopOneNodeTest{TTestEnv(8, TBlobStorageGroupType::Erasure4Plus2Block), GenData(521_KB)}.RunTest();
    }
    Y_UNIT_TEST(TestStopOneNode_Mirror3dc_HugeBlob) {
        TStopOneNodeTest{TTestEnv(9, TBlobStorageGroupType::ErasureMirror3dc), GenData(521_KB)}.RunTest();
    }

    Y_UNIT_TEST(TestRandom_Block42) {
        TRandomTest{TTestEnv(8, TBlobStorageGroupType::Erasure4Plus2Block), 1000}.RunTest();
    }
    Y_UNIT_TEST(TestRandom_Mirror3dc) {
        TRandomTest{TTestEnv(9, TBlobStorageGroupType::ErasureMirror3dc), 1000}.RunTest();
    }

}
