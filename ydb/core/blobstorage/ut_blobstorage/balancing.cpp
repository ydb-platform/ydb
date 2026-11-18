#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/vdisk/repl/blobstorage_replbroker.h>
#include <ydb/core/util/random.h>

#include <library/cpp/iterator/enumerate.h>



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
        .ConfigPreprocessor = [](ui32, TNodeWardenConfig& conf) {
            auto* balancingConf = conf.BlobStorageConfig->MutableVDiskBalancingConfig();
            balancingConf->SetEnableSend(true);
            balancingConf->SetEnableDelete(true);
            balancingConf->SetBalanceOnlyHugeBlobs(false);
            balancingConf->SetSecondsToSleepIfNothingToDo(1);
        },
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

    static TString PrepareData(const ui32 dataLen, const ui32 start) {
        TString data(Reserve(dataLen));
        for (ui32 i = 0; i < dataLen; ++i) {
            data.push_back('a' + (start + i) % 26);
        }
        return data;
    };

    NKikimrProto::EReplyStatus SendPut(ui32 step, const TString& data) {
        const TLogoBlobID id(1, 1, step, 0, data.size(), 0);
        Cerr << "SEND TEvPut with key " << id.ToString() << Endl;
        const TActorId sender = Env.Runtime->AllocateEdgeActor(GroupInfo->GetActorId(*RunningNodes.begin()).NodeId(), __FILE__, __LINE__);
        auto ev = std::make_unique<TEvBlobStorage::TEvPut>(id, data, TInstant::Max());
        Env.Runtime->WrapInActorContext(sender, [&] {
            SendToBSProxy(sender, GroupInfo->GroupID, ev.release());
        });
        auto res = Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(sender, false);
        Cerr << "TEvPutResult: " << res->Get()->ToString() << Endl;
        return res->Get()->Status;
    };

    void SendPut(ui32 step, const TString& data, NKikimrProto::EReplyStatus expectedStatus) {
        UNIT_ASSERT_VALUES_EQUAL(SendPut(step, data), expectedStatus);
    };

    auto SendGet(ui32 step, ui32 dataSize, bool mustRestoreFirst=false) {
        const TLogoBlobID blobId(1, 1, step, 0, dataSize, 0);
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
        TString errMsg = ToString(expectedParts) + " != " + ToString(actualParts) + " Key: " + blobId.ToString();
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

    void SetVDiskReadOnly(ui32 position, bool value) {
        const TVDiskID& someVDisk = GroupInfo->GetVDiskId(position);
        auto baseConfig = Env.FetchBaseConfig();

        const auto& somePDisk = baseConfig.GetPDisk(position);
        const auto& someVSlot = baseConfig.GetVSlot(position);
        Cerr << "Setting VDisk read-only to " << value << " for position " << position << Endl;
        if (!value) {
            Env.PDiskMockStates[{somePDisk.GetNodeId(), somePDisk.GetPDiskId()}]->SetReadOnly(someVDisk, value);
        }
        Env.SetVDiskReadOnly(somePDisk.GetNodeId(), somePDisk.GetPDiskId(), someVSlot.GetVSlotId().GetVSlotId(), someVDisk, value);
        Env.Sim(TDuration::Seconds(30));
        if (value) {
            Env.PDiskMockStates[{somePDisk.GetNodeId(), somePDisk.GetPDiskId()}]->SetReadOnly(someVDisk, value);
        }
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
            Env->Sim(TDuration::Seconds(60));
            Cerr << "Start compaction" << Endl;
            for (ui32 pos = 0; pos < Env->Settings.NodeCount; ++pos) {
                Env->CompactVDisk(Env.GroupInfo->GetActorId(pos));
            }
            Env->Sim(TDuration::Seconds(30));
            Cerr << "Finish compaction" << Endl;

            Env.CheckPartsLocations(MakeLogoBlobId(step, data.size()));
            UNIT_ASSERT_VALUES_EQUAL(Env.SendGet(step, data.size())->Get()->Responses[0].Buffer.ConvertToString(), data);
        }
    }
};

struct TDontSendToReadOnlyTest {
    TTestEnv Env;
    TString data;

    void RunTest() {
        ui32 step = 0;

        { // Check just a normal put works
            Env.SendPut(++step, data, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(Env.SendGet(step, data.size())->Get()->Responses[0].Buffer.ConvertToString(), data);
            Env.CheckPartsLocations(MakeLogoBlobId(step, data.size()));
        }


        {
            auto blobId = MakeLogoBlobId(++step, data.size());
            auto locations = Env.GetExpectedPartsLocations(blobId);
            ui32 nodeIdWithBlob = 0;
            while (locations[nodeIdWithBlob].size() == 0) ++nodeIdWithBlob;

            Env.SetVDiskReadOnly(nodeIdWithBlob, true);
            Env.SendPut(step, data, NKikimrProto::OK);

            // Check that balancing does not send anything when vdisk in readonly mode
            Env.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
                if (ev->GetTypeRewrite() == TEvBlobStorage::EvVPut || ev->GetTypeRewrite() == TEvBlobStorage::EvVMultiPut) {
                    UNIT_ASSERT(false);
                }
                return true;
            };
            Env->Sim(TDuration::Seconds(60));
            Env.Env.Runtime->FilterFunction = {};

            Env.SetVDiskReadOnly(nodeIdWithBlob, false);

            Env->Sim(TDuration::Seconds(60));

            Cerr << "Start compaction" << Endl;
            for (ui32 pos = 0; pos < Env->Settings.NodeCount; ++pos) {
                Env->CompactVDisk(Env.GroupInfo->GetActorId(pos));
            }
            Env->Sim(TDuration::Seconds(30));
            Cerr << "Finish compaction" << Endl;

            Env.CheckPartsLocations(MakeLogoBlobId(step, data.size()));
            UNIT_ASSERT_VALUES_EQUAL(Env.SendGet(step, data.size())->Get()->Responses[0].Buffer.ConvertToString(), data);
        }
    }
};

struct TRandomTest {
    TTestEnv Env;
    ui32 NumIters;
    ui32 MaxBlobSize;

    void RunTest() {
        srand(123456);
        TVector<TString> data(Reserve(NumIters));

        TVector<ui32> successfulSteps;

        for (ui32 step = 0; step < NumIters; ++step) {
            Cerr << "Step = " << step << Endl;
            data.push_back(GenRandomBuffer(16 + random() % MaxBlobSize));

            if (Env.SendPut(step, data.back()) == NKikimrProto::OK) {
                successfulSteps.push_back(step);
            }

            if (random() % 10 == 1 && Env.RunningNodes.size() + 2 > Env->Settings.NodeCount) {
                ui32 pos = random() % Env->Settings.NodeCount;
                Cerr << "Stop node " << pos << Endl;
                Env.StopNode(pos);
                Env->Sim(TDuration::Seconds(10));
            }

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
                    Cerr << "Compact vdisk " << pos << Endl;
                    Env->CompactVDisk(Env.GroupInfo->GetActorId(pos));
                    Env->Sim(TDuration::Seconds(10));
                }
            }

            // Wipe random node
            if (random() % 100 == 1) {
                ui32 pos = random() % Env->Settings.NodeCount;
                if (Env.RunningNodes.contains(pos) && Env.RunningNodes.contains(0)) {
                    Cerr << "Wipe node " << pos << Endl;
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

        UNIT_ASSERT(successfulSteps.size() > NumIters / 2);

        Cerr << "Starting nodes" << Endl;
        for (ui32 pos = 0; pos < Env->Settings.NodeCount; ++pos) {
            Env.StartNode(pos);
        }
        Env->Sim(TDuration::Seconds(60));

        Cerr << "Start compaction 1" << Endl;
        for (ui32 pos = 0; pos < Env->Settings.NodeCount; ++pos) {
            Env->CompactVDisk(Env.GroupInfo->GetActorId(pos));
        }
        Env->Sim(TDuration::Seconds(60));

        Cerr << "Start checking" << Endl;
        for (ui32 step: successfulSteps) {
            Env.CheckPartsLocations(MakeLogoBlobId(step, data[step].size()));
            UNIT_ASSERT_VALUES_EQUAL_C(Env.SendGet(step, data[step].size())->Get()->Responses[0].Buffer.ConvertToString(), data[step], MakeLogoBlobId(step, data[step].size()).ToString());
        }
    }
};


struct TTwoPartsOnOneNodeTest {
    TTestEnv Env;
    TString data;

    void RunTest() {
        ui32 step = 0;

        auto printActualBlobLocations = [&](const TLogoBlobID& blobId) {
            auto actualLocations = Env.GetActualPartsLocations(blobId);
            for (const auto& [pos, parts]: Enumerate(actualLocations)) {
                Cerr << "Node " << pos << ": ";
                for (ui32 part: parts) {
                    Cerr << part << " ";
                }
                Cerr << Endl;
            }
        };


        auto blobId = MakeLogoBlobId(++step, data.size());
        auto expectedLocations = Env.GetExpectedPartsLocations(blobId);
        TVector<ui32> partIdxToNodeId(6);
        TVector<ui32> handoffNodeIds;
        for (ui32 nodeId = 0; nodeId < expectedLocations.size(); ++nodeId) {
            if (expectedLocations[nodeId].empty()) {
                handoffNodeIds.push_back(nodeId);
            }

            for (ui32 partId: expectedLocations[nodeId]) {
                partIdxToNodeId[partId - 1] = nodeId;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(handoffNodeIds.size(), 2);

        // stop one main node and one handoff node, and send put, so that we have 1 part on handoff
        Env.StopNode(partIdxToNodeId[0]);
        Env.StopNode(handoffNodeIds[1]);
        Env.SendPut(step, data, NKikimrProto::OK);
        printActualBlobLocations(blobId);

        // start main node we stopped before, and stop another main node
        // so after get with restore we should have 2 parts on handoff
        Env.StartNode(partIdxToNodeId[0]);
        Env.StopNode(partIdxToNodeId[1]);
        Env->Sim(TDuration::Seconds(10));
        auto res = Env.SendGet(step, data.size(), true);
        printActualBlobLocations(blobId);

        UNIT_ASSERT_VALUES_EQUAL(Env.SendGet(step, data.size())->Get()->Responses[0].Buffer.ConvertToString(), data);
        auto actualLocations = Env.GetActualPartsLocations(blobId);
        // check that we really have 2 parts on handoff
        UNIT_ASSERT_VALUES_EQUAL(actualLocations[handoffNodeIds[0]].size(), 2);

        // start all stopped nodes
        Env.StartNode(partIdxToNodeId[1]);
        Env.StartNode(handoffNodeIds[1]);
        Env->Sim(TDuration::Seconds(30));

        // run compactions
        Cerr << "Start compaction 1" << Endl;
        for (ui32 pos = 0; pos < Env->Settings.NodeCount; ++pos) {
            Env->CompactVDisk(Env.GroupInfo->GetActorId(pos));
        }
        Env->Sim(TDuration::Seconds(30));
        Cerr << "Finish compaction 1" << Endl;

        Env.CheckPartsLocations(MakeLogoBlobId(step, data.size()));
        UNIT_ASSERT_VALUES_EQUAL(Env.SendGet(step, data.size())->Get()->Responses[0].Buffer.ConvertToString(), data);
    }
};



Y_UNIT_TEST_SUITE(VDiskBalancing) {

    Y_UNIT_TEST(WaitsForDurabilityReplication) {
        TEnvironmentSetup env({
            .NodeCount = 9,
            .VDiskReplPausedAtStart = false,
            .Erasure = TBlobStorageGroupType::ErasureMirror3dc,
            .ConfigPreprocessor = [](ui32, TNodeWardenConfig& config) {
                config.BlobStorageConfig->MutableVDiskBalancingConfig()->SetEnableSend(true);
                config.BlobStorageConfig->MutableVDiskBalancingConfig()->SetSecondsToSleepIfNothingToDo(3600);
            },
        });
        bool durabilityReplicated = false;
        THashSet<TActorId> balancingActors;
        THashSet<TActorId> statusRequesters;
        ui32 statusResponses = 0;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvBlobStorage::EvVStatus) {
                statusRequesters.insert(event->Sender);
            } else if (event->GetTypeRewrite() == TEvBlobStorage::EvVStatusResult
                    && statusRequesters.contains(event->Recipient)) {
                NKikimrBlobStorage::TEvVStatusResult& record = event->Get<TEvBlobStorage::TEvVStatusResult>()->Record;
                record.SetReplicated(true);
                record.SetDurabilityReplicated(durabilityReplicated);
                balancingActors.insert(event->Recipient);
                ++statusResponses;
            }
            return true;
        };
        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Seconds(60));
        UNIT_ASSERT(statusResponses);
        // Failed readiness checks retry after ten seconds; an empty successful pass sleeps for an hour.
        UNIT_ASSERT_C(balancingActors.size() > 9, balancingActors.size());
        durabilityReplicated = true;
        env.Sim(TDuration::Seconds(30));
        const ui32 completedActors = balancingActors.size();
        env.Sim(TDuration::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL(balancingActors.size(), completedActors);
        env.Runtime->FilterFunction = {};
    }

    Y_UNIT_TEST(SenderSkipsUnreadableHandoffPart) {
        // PDisk answers a read with bad sectors as OK with gaps. The balancing sender must not use such data:
        // TBufferWithGaps::ToString() aborts the process when the buffer has gaps.
        std::optional<TDiskPart> unreadable;
        ui32 unreadableNodeId = 0;
        ui32 unreadableReadsServed = 0;
        THashSet<TActorId> statusRequesters;

        TEnvironmentSetup env({
            .NodeCount = 9,
            .VDiskReplPausedAtStart = true, // only balancing may move the handoff part to its main replica
            .Erasure = TBlobStorageGroupType::ErasureMirror3dc,
            .ConfigPreprocessor = [](ui32, TNodeWardenConfig& config) {
                auto* balancing = config.BlobStorageConfig->MutableVDiskBalancingConfig();
                balancing->SetEnableSend(true);
                balancing->SetSecondsToSleepIfNothingToDo(1);
            },
        });
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvBlobStorage::EvVStatus) {
                statusRequesters.insert(event->Sender);
            } else if (event->GetTypeRewrite() == TEvBlobStorage::EvVStatusResult
                    && statusRequesters.contains(event->Recipient)) {
                // repl is paused, so report every VDisk as replicated to let balancing run
                NKikimrBlobStorage::TEvVStatusResult& record = event->Get<TEvBlobStorage::TEvVStatusResult>()->Record;
                record.SetReplicated(true);
                record.SetDurabilityReplicated(true);
            } else if (event->GetTypeRewrite() == NPDisk::TEvChunkReadResult::EventType && unreadable
                    && event->Recipient.NodeId() == unreadableNodeId) {
                auto *msg = event->Get<NPDisk::TEvChunkReadResult>();
                if (msg->Status == NKikimrProto::OK && msg->ChunkIdx == unreadable->ChunkIdx
                        && msg->Offset == unreadable->Offset) {
                    msg->Data.AddGap(msg->Offset, msg->Offset + msg->Data.Size());
                    ++unreadableReadsServed;
                }
            }
            return true;
        };

        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Minutes(1));
        const auto groups = env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
        const auto info = env.GetGroupInfo(groups.front());

        // A huge blob whose first main replica is not on the node that writes it.
        const TString data = GenRandomBuffer(521_KB);
        TLogoBlobID id;
        TBlobStorageGroupInfo::TOrderNums orderNums;
        for (ui32 step = 1; !id; ++step) {
            const TLogoBlobID candidate(1, 1, step, 0, data.size(), 0);
            orderNums.clear();
            info->GetTopology().PickSubgroup(candidate.Hash(), orderNums);
            if (info->GetActorId(orderNums[0]).NodeId() != env.Settings.ControllerNodeId) {
                id = candidate;
            }
        }

        // Write the blob while the node of its first main replica is down, so that this part lands on a handoff.
        const ui32 mainNodeId = info->GetActorId(orderNums[0]).NodeId();
        env.StopNode(mainNodeId);
        env.PutBlob(info->GroupID.GetRawId(), id, data);
        env.Sim(TDuration::Seconds(10));

        for (ui32 i = info->Type.TotalPartCount(); i < orderNums.size(); ++i) {
            const TActorId actorId = info->GetActorId(orderNums[i]);
            if (actorId.NodeId() == mainNodeId) {
                continue;
            }
            const auto layout = env.SyncQuery<TEvBlobStorage::TEvCaptureVDiskLayoutResult,
                TEvBlobStorage::TEvCaptureVDiskLayout>(actorId);
            for (const auto& item : layout->Layout) {
                if (item.BlobId == id
                        && item.RecordType == TEvBlobStorage::TEvCaptureVDiskLayoutResult::ERecordType::HugeBlob) {
                    UNIT_ASSERT(!unreadable);
                    unreadable = item.Location;
                    unreadableNodeId = actorId.NodeId();
                }
            }
        }
        UNIT_ASSERT_C(unreadable, "the part was not written to a handoff");

        // Bring the main replica back; balancing on the handoff reads the part to send it there.
        env.StartNode(mainNodeId);
        env.Sim(TDuration::Minutes(2));
        UNIT_ASSERT_C(unreadableReadsServed, "balancing never read the handoff part");
        env.Runtime->FilterFunction = {};
    }

    Y_UNIT_TEST(TestStopOneNode_Block42) {
        TStopOneNodeTest{TTestEnv(8, TBlobStorageGroupType::Erasure4Plus2Block), GenRandomBuffer(100)}.RunTest();
    }
    Y_UNIT_TEST(TestStopOneNode_Mirror3dc) {
        TStopOneNodeTest{TTestEnv(9, TBlobStorageGroupType::ErasureMirror3dc), GenRandomBuffer(100)}.RunTest();
    }
    Y_UNIT_TEST(TestStopOneNode_Block42_HugeBlob) {
        TStopOneNodeTest{TTestEnv(8, TBlobStorageGroupType::Erasure4Plus2Block), GenRandomBuffer(521_KB * 6)}.RunTest();
    }
    Y_UNIT_TEST(TestStopOneNode_Mirror3dc_HugeBlob) {
        TStopOneNodeTest{TTestEnv(9, TBlobStorageGroupType::ErasureMirror3dc), GenRandomBuffer(521_KB)}.RunTest();
    }

    Y_UNIT_TEST(TestRandom_Block42) {
        TRandomTest{TTestEnv(8, TBlobStorageGroupType::Erasure4Plus2Block), 1000, 521_KB * 6}.RunTest();
    }
    Y_UNIT_TEST(TestRandom_Mirror3dc) {
        TRandomTest{TTestEnv(9, TBlobStorageGroupType::ErasureMirror3dc), 1000, 521_KB}.RunTest();
    }

    Y_UNIT_TEST(TwoPartsOnOneNodeTest_Block42) {
        TTwoPartsOnOneNodeTest{TTestEnv(8, TBlobStorageGroupType::Erasure4Plus2Block), GenRandomBuffer(100)}.RunTest();
    }
    Y_UNIT_TEST(TwoPartsOnOneNodeTest_Block42_HugeBlob) {
        TTwoPartsOnOneNodeTest{TTestEnv(8, TBlobStorageGroupType::Erasure4Plus2Block), GenRandomBuffer(521_KB * 6)}.RunTest();
    }

    Y_UNIT_TEST(TestDontSendToReadOnlyTest_Block42) {
        TDontSendToReadOnlyTest{TTestEnv(8, TBlobStorageGroupType::Erasure4Plus2Block), GenRandomBuffer(100)}.RunTest();
    }

    // Balancing holds the replication token of its PDisk for a batch or waits for it in the broker queue. A VDisk
    // stopped at that moment must return the token or withdraw the request, otherwise replication and balancing of all
    // VDisks on that PDisk wait for the token forever.
    void TestReplTokenReturnedWhenVDiskStops(bool requestPending) {
        std::optional<TDiskPart> handoffPart;
        ui32 handoffNodeId = 0;
        bool holdReads = true;
        ui32 heldReads = 0;
        ui32 tokenRequestsOnHandoffNode = 0;
        ui32 tokensGrantedOnHandoffNode = 0;
        TActorId probe;
        ui32 probeGrants = 0;
        THashSet<TActorId> statusRequesters;

        TEnvironmentSetup env({
            .NodeCount = 9,
            .VDiskReplPausedAtStart = true, // only balancing asks for replication tokens
            .Erasure = TBlobStorageGroupType::ErasureMirror3dc,
            .ConfigPreprocessor = [](ui32, TNodeWardenConfig& config) {
                auto* balancing = config.BlobStorageConfig->MutableVDiskBalancingConfig();
                balancing->SetEnableSend(true);
                balancing->SetSecondsToSleepIfNothingToDo(1);
                // the batch, and with it the token, lasts as long as its read is held below
                balancing->SetReadBatchTimeoutMs(TDuration::Hours(1).MilliSeconds());
            },
        });
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvBlobStorage::EvVStatus) {
                statusRequesters.insert(event->Sender);
            } else if (event->GetTypeRewrite() == TEvBlobStorage::EvVStatusResult
                    && statusRequesters.contains(event->Recipient)) {
                // repl is paused, so report every VDisk as replicated to let balancing run
                NKikimrBlobStorage::TEvVStatusResult& record = event->Get<TEvBlobStorage::TEvVStatusResult>()->Record;
                record.SetReplicated(true);
                record.SetDurabilityReplicated(true);
            } else if (event->GetTypeRewrite() == TEvBlobStorage::EvQueryReplToken) {
                if (event->Sender != probe && event->Sender.NodeId() == handoffNodeId) {
                    ++tokenRequestsOnHandoffNode;
                }
            } else if (event->GetTypeRewrite() == TEvBlobStorage::EvReplToken) {
                if (event->Recipient == probe) {
                    ++probeGrants;
                    return false; // the probe is an edge actor that does not wait for events
                } else if (event->Recipient.NodeId() == handoffNodeId) {
                    ++tokensGrantedOnHandoffNode;
                }
            } else if (event->GetTypeRewrite() == NPDisk::TEvChunkReadResult::EventType && handoffPart && holdReads
                    && event->Recipient.NodeId() == handoffNodeId) {
                auto *msg = event->Get<NPDisk::TEvChunkReadResult>();
                if (msg->ChunkIdx == handoffPart->ChunkIdx && msg->Offset == handoffPart->Offset) {
                    ++heldReads;
                    return false; // the balancing sender waits for this read, so its batch keeps the token
                }
            }
            return true;
        };

        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Minutes(1));
        const auto groups = env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
        const auto info = env.GetGroupInfo(groups.front());

        // A huge blob whose first main replica is not on the node that writes it.
        const TString data = GenRandomBuffer(521_KB);
        TLogoBlobID id;
        TBlobStorageGroupInfo::TOrderNums orderNums;
        for (ui32 step = 1; !id; ++step) {
            const TLogoBlobID candidate(1, 1, step, 0, data.size(), 0);
            orderNums.clear();
            info->GetTopology().PickSubgroup(candidate.Hash(), orderNums);
            if (info->GetActorId(orderNums[0]).NodeId() != env.Settings.ControllerNodeId) {
                id = candidate;
            }
        }

        // Write the blob while the node of its first main replica is down, so that this part lands on a handoff.
        const ui32 mainNodeId = info->GetActorId(orderNums[0]).NodeId();
        env.StopNode(mainNodeId);
        env.PutBlob(info->GroupID.GetRawId(), id, data);
        env.Sim(TDuration::Seconds(10));

        TActorId handoffActorId;
        TVDiskID handoffVDiskId;
        for (ui32 i = info->Type.TotalPartCount(); i < orderNums.size(); ++i) {
            const TActorId actorId = info->GetActorId(orderNums[i]);
            if (actorId.NodeId() == mainNodeId) {
                continue;
            }
            const auto layout = env.SyncQuery<TEvBlobStorage::TEvCaptureVDiskLayoutResult,
                TEvBlobStorage::TEvCaptureVDiskLayout>(actorId);
            for (const auto& item : layout->Layout) {
                if (item.BlobId == id
                        && item.RecordType == TEvBlobStorage::TEvCaptureVDiskLayoutResult::ERecordType::HugeBlob) {
                    UNIT_ASSERT(!handoffPart);
                    handoffPart = item.Location;
                    handoffActorId = actorId;
                    handoffVDiskId = info->GetVDiskId(orderNums[i]);
                }
            }
        }
        UNIT_ASSERT_C(handoffPart, "the part was not written to a handoff");
        const auto [nodeId, pdiskId, vslotId] = DecomposeVDiskServiceId(handoffActorId);
        handoffNodeId = nodeId;

        // The probe takes and returns the token of this PDisk the way a VDisk does.
        probe = env.Runtime->AllocateEdgeActor(nodeId, __FILE__, __LINE__);
        const auto sendToBroker = [&](IEventBase *ev) {
            env.Runtime->Send(new IEventHandle(MakeBlobStorageReplBrokerID(), probe, ev), handoffNodeId);
        };
        if (requestPending) {
            // hold the token, so that the request of balancing waits in the queue
            sendToBroker(new TEvQueryReplToken(pdiskId));
            env.Sim(TDuration::Seconds(1));
            UNIT_ASSERT_VALUES_EQUAL(probeGrants, 1);
        }

        // Bring the main replica back; balancing on the handoff asks for the token to send the part there and, once
        // the token is granted, starts a batch whose read is held.
        env.StartNode(mainNodeId);
        const auto balancingWaits = [&] {
            return requestPending ? tokenRequestsOnHandoffNode > 0 : heldReads && tokensGrantedOnHandoffNode;
        };
        for (ui32 i = 0; i < 120 && !balancingWaits(); ++i) {
            env.Sim(TDuration::Seconds(1));
        }
        UNIT_ASSERT_C(balancingWaits(), "balancing of the handoff part did not start"
            << " tokenRequestsOnHandoffNode# " << tokenRequestsOnHandoffNode
            << " tokensGrantedOnHandoffNode# " << tokensGrantedOnHandoffNode << " heldReads# " << heldReads);

        // Stop the handoff VDisk while its balancing holds the token or waits for it. The held read is never delivered,
        // so the batch is still running; reads of the new incarnation are not held.
        holdReads = false;
        env.Wipe(nodeId, pdiskId, vslotId, handoffVDiskId);
        env.Sim(TDuration::Minutes(1));
        if (requestPending) {
            sendToBroker(new TEvReleaseReplToken);
        }

        // The token of this PDisk must be available again.
        const ui32 grantsBefore = probeGrants;
        sendToBroker(new TEvQueryReplToken(pdiskId));
        env.Sim(TDuration::Minutes(2));
        UNIT_ASSERT_C(probeGrants > grantsBefore, "the replication token of PDisk " << pdiskId << " was not returned");
        sendToBroker(new TEvReleaseReplToken);
        env.Runtime->FilterFunction = {};
    }

    Y_UNIT_TEST(ReplTokenReturnedWhenVDiskStopsDuringBatch) {
        TestReplTokenReturnedWhenVDiskStops(false);
    }

    Y_UNIT_TEST(ReplTokenRequestWithdrawnWhenVDiskStops) {
        TestReplTokenReturnedWhenVDiskStops(true);
    }
}
