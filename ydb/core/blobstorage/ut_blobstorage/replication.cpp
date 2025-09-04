#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_barrier.h>
#include <util/system/info.h>

#define SINGLE_THREAD 1

enum class EState {
    OK,
    FORMAT,
    OFFLINE,
};

TString DoTestCase(TBlobStorageGroupType::EErasureSpecies erasure, const std::vector<EState>& states,
        bool detainReplication = false) {
    TStringStream s;
    IOutputStream& log = SINGLE_THREAD ? Cerr : s;

    log << "*** SETUP: " << TBlobStorageGroupType::ErasureSpeciesName(erasure);
    for (EState state : states) {
        log << " ";
        switch (state) {
            case EState::OK: log << "OK"; break;
            case EState::FORMAT: log << "FORMAT"; break;
            case EState::OFFLINE: log << "OFFLINE"; break;
        }
    }
    log << Endl;

    std::function<bool(ui32, std::unique_ptr<IEventHandle>&)> filterFunction;
    auto prepareRuntime = [&](TTestActorSystem& runtime) {
        runtime.FilterFunction = filterFunction;
        runtime.LogStream = &log;
    };

    ui32 cleanNodeId;
    ui32 nodeCount = states.size();
    for (cleanNodeId = 1; cleanNodeId <= nodeCount; ++cleanNodeId) {
        if (states[cleanNodeId - 1] != EState::OFFLINE) {
            break;
        }
    }
    TEnvironmentSetup env(TEnvironmentSetup::TSettings{
        .NodeCount = nodeCount,
        .Erasure = erasure,
        .PrepareRuntime = prepareRuntime,
        .ControllerNodeId = cleanNodeId,
    });
    env.CreateBoxAndPool(1, 1);
    env.Sim(TDuration::Minutes(1));

    auto baseConfig = env.FetchBaseConfig();
    Y_ABORT_UNLESS(baseConfig.GroupSize() == 1);
    ui32 groupId = baseConfig.GetGroup(0).GetGroupId();

    auto groupInfo = env.GetGroupInfo(groupId);
    const auto& topology = groupInfo->GetTopology();
    std::vector<ui32> pdiskLayout = MakePDiskLayout(baseConfig, topology, groupId);
    std::vector<TActorId> queues;
    for (ui32 i = 0; i < groupInfo->GetTotalVDisksNum(); ++i) {
        queues.push_back(env.CreateQueueActor(groupInfo->GetVDiskId(i), NKikimrBlobStorage::EVDiskQueueId::GetFastRead, 0));
    }

    TString data = "hello";
    TLogoBlobID id(1, 1, 1, 0, data.size(), 0);

    {
        TActorId edge = env.Runtime->AllocateEdgeActor(1);
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvPut(id, data, TInstant::Max(),
                NKikimrBlobStorage::TabletLog, TEvBlobStorage::TEvPut::TacticMaxThroughput));
        });
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge);
        Y_ABORT_UNLESS(res->Get()->Status == NKikimrProto::OK);
    }

    ui32 numDisksWithBlob = 0;
    ui32 numDisksNotOk = 0;

    std::set<TActorId> edges;
    for (ui32 i = 0; i < groupInfo->GetTotalVDisksNum(); ++i) {
        auto ev = TEvBlobStorage::TEvVGet::CreateExtremeIndexQuery(groupInfo->GetVDiskId(i), TInstant::Max(),
            NKikimrBlobStorage::EGetHandleClass::FastRead, TEvBlobStorage::TEvVGet::EFlags::None,
            groupInfo->GetActorId(i).NodeId());
        ev->AddExtremeQuery(id, 0, 0);
        const TActorId& queueId = queues[i];
        const TActorId& edge = env.Runtime->AllocateEdgeActor(queueId.NodeId());
        env.Runtime->Send(new IEventHandle(queueId, edge, ev.release()), queueId.NodeId());
        const bool inserted = edges.insert(edge).second;
        Y_ABORT_UNLESS(inserted);
    }
    while (!edges.empty()) {
        auto res = env.Runtime->WaitForEdgeActorEvent(edges);
        const size_t numErased = edges.erase(res->Recipient);
        Y_ABORT_UNLESS(numErased);
        env.Runtime->DestroyActor(res->Recipient);
        auto *msg = res->CastAsLocal<TEvBlobStorage::TEvVGetResult>();
        Y_ABORT_UNLESS(msg);
        const auto& record = msg->Record;
        Y_ABORT_UNLESS(record.GetStatus() == NKikimrProto::OK);
        Y_ABORT_UNLESS(record.ResultSize() == 1);
        const auto& result = record.GetResult(0);
        const ui32 nodeId = record.GetCookie();
        Y_ABORT_UNLESS(nodeId);
        Cerr << nodeId << " -> " << NKikimrProto::EReplyStatus_Name(result.GetStatus()) << Endl;
        if (result.GetStatus() == NKikimrProto::OK) {
            ++numDisksWithBlob;
            if (states[nodeId - 1] != EState::OK) {
                ++numDisksNotOk;
            }
        } else {
            Y_ABORT_UNLESS(result.GetStatus() == NKikimrProto::NODATA);
            if (states[nodeId - 1] == EState::FORMAT) {
                log << "early abort -- formatted disk did not contain any parts" << Endl;
                return s.Str();
            }
        }
    }

    log << "numDisksWithBlob# " << numDisksWithBlob << " numDisksNotOk# " << numDisksNotOk << Endl;

    {
        TActorId edge = env.Runtime->AllocateEdgeActor(1);
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvCollectGarbage(id.TabletID(), 1, 0, id.Channel(),
                true, id.Generation(), Max<ui32>(), new TVector<TLogoBlobID>(1, id), nullptr, TInstant::Max(), false));
        });
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvCollectGarbageResult>(edge);
        Y_ABORT_UNLESS(res->Get()->Status == NKikimrProto::OK);
    }

    auto checkBlob = [&] {
        TActorId edge = env.Runtime->AllocateEdgeActor(cleanNodeId);
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(),
                NKikimrBlobStorage::EGetHandleClass::FastRead));
        });
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge);
        auto *msg = res->Get();
        Y_ABORT_UNLESS(msg->ResponseSz == 1);
        return msg->Responses[0].Status;
    };

    Y_ABORT_UNLESS(checkBlob() == NKikimrProto::OK);

    // wait some time for sync data to spread
    TInstant syncStartWall = Now();
    TInstant syncStartClock = env.Runtime->GetClock();
    for (;;) {
        std::set<ui32> ingress;
        ui32 num = 0;
        std::set<TActorId> edges;
        for (ui32 i = 0; i < groupInfo->GetTotalVDisksNum(); ++i) {
            const TActorId queueId = queues[i];
            auto query = std::make_unique<TEvBlobStorage::TEvVGetBarrier>(groupInfo->GetVDiskId(i), TKeyBarrier::First(),
                TKeyBarrier::Inf(), nullptr, true);
            const TActorId& edge = env.Runtime->AllocateEdgeActor(queueId.NodeId());
            env.Runtime->Send(new IEventHandle(queueId, edge, query.release()), edge.NodeId());
            const bool inserted = edges.insert(edge).second;
            Y_ABORT_UNLESS(inserted);
        }
        while (!edges.empty()) {
            auto res = env.Runtime->WaitForEdgeActorEvent(edges);
            const size_t numErased = edges.erase(res->Recipient);
            Y_ABORT_UNLESS(numErased);
            env.Runtime->DestroyActor(res->Recipient);
            auto *msg = res->CastAsLocal<TEvBlobStorage::TEvVGetBarrierResult>();
            Y_ABORT_UNLESS(msg);

            //log << "Result# " << msg->ToString() << Endl;
            const auto& record = msg->Record;
            Y_ABORT_UNLESS(record.GetStatus() == NKikimrProto::OK);
            if (record.KeysSize() == 0 && record.ValuesSize() == 0) {
                continue;
            }
            Y_ABORT_UNLESS(record.KeysSize() == 1);
            Y_ABORT_UNLESS(record.ValuesSize() == 1);
            auto& key = record.GetKeys(0);
            Y_ABORT_UNLESS(key.GetTabletId() == id.TabletID());
            Y_ABORT_UNLESS(key.GetChannel() == id.Channel());
            auto& value = record.GetValues(0);
            Y_ABORT_UNLESS(value.GetCollectGen() == id.Generation());
            Y_ABORT_UNLESS(value.GetCollectStep() == Max<ui32>());
            ingress.insert(value.GetIngress());
            ++num;
        }
        if (num == groupInfo->GetTotalVDisksNum() && ingress.size() == 1 && *ingress.begin()) {
            break;
        }
        env.Sim(TDuration::Seconds(5));
    }
    log << "syncTime wall# " << (Now() - syncStartWall) << " actor# " << (env.Runtime->GetClock() - syncStartClock) << Endl;

    env.Cleanup();

    TBlobStorageGroupInfo::TGroupVDisks err(&groupInfo->GetTopology());
    for (auto& [key, state] : env.PDiskMockStates) {
        switch (states[key.first - 1]) {
            case EState::FORMAT:
                log << "formatted pdisk " << key.first << ":" << key.second << Endl;
                state.Reset();
                [[fallthrough]];
            case EState::OFFLINE:
                for (ui32 i = 0; i < groupInfo->GetTotalVDisksNum(); ++i) {
                    if (groupInfo->GetActorId(i).NodeId() == key.first) {
                        err |= {&groupInfo->GetTopology(), groupInfo->GetVDiskId(i)};
                    }
                }
                break;

            case EState::OK:
                break;
        }
    }

    std::vector<std::pair<ui32, std::unique_ptr<IEventHandle>>> detainedMsgs;

    filterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
        if (ev->Type == TEvBlobStorage::EvVGet && states[ev->Recipient.NodeId() - 1] == EState::OFFLINE) {
            env.Runtime->Send(IEventHandle::ForwardOnNondelivery(std::move(ev), TEvents::TEvUndelivered::Disconnected).release(), nodeId);
            return false;
        }
        if (ev->Type == TEvBlobStorage::EvReplFinished && detainReplication) {
            detainedMsgs.emplace_back(nodeId, std::move(ev));
            return false;
        }
        return true;
    };

    env.Initialize();
    env.Sim(TDuration::Minutes(360));

    const NKikimrProto::EReplyStatus status = checkBlob();
    log << "checkBlob status# " << NKikimrProto::EReplyStatus_Name(status) << Endl;
    if (groupInfo->GetQuorumChecker().CheckFailModelForGroup(err)) {
        Y_ABORT_UNLESS(status == NKikimrProto::OK);
    } else {
        Y_ABORT_UNLESS(status == NKikimrProto::ERROR || status == NKikimrProto::OK);
    }

    if (detainReplication) {
        ui64 vdisksWithStuckRepl = env.AggregateVDiskCounters(env.StoragePoolName, nodeCount, nodeCount,
                groupId, pdiskLayout, "repl", "ReplMadeNoProgress", {}, false);
        UNIT_ASSERT_VALUES_UNEQUAL(vdisksWithStuckRepl, 0);
        env.Runtime->FilterFunction = {};
        for (auto& [nodeId, ev] : detainedMsgs) {
            env.Runtime->Send(ev.release(), nodeId);
        }
        checkBlob();
        env.Sim(TDuration::Minutes(360));
        vdisksWithStuckRepl = env.AggregateVDiskCounters(env.StoragePoolName, nodeCount, nodeCount,
                groupId, pdiskLayout, "repl", "ReplMadeNoProgress", {}, false);
        UNIT_ASSERT_VALUES_EQUAL(vdisksWithStuckRepl, 0);
    }

    return s.Str();
}

void DoTest(TBlobStorageGroupType::EErasureSpecies erasure) {
    TMutex mutex, logMutex;
    std::vector<std::pair<TBlobStorageGroupType::EErasureSpecies, std::vector<EState>>> queue;
    size_t queueIndex = 0;
    std::deque<TString> logQueue;
    ui32 testCasesProcessed = 0, totalCases = 0;

    auto threadFunc = [&] {
        for (;;) {
            size_t index;
            with_lock (mutex) {
                if (queueIndex == queue.size()) {
                    break;
                }
                index = queueIndex++;
            }

            // run test case
            TString log = DoTestCase(queue[index].first, queue[index].second);

            with_lock (logMutex) {
                ++testCasesProcessed;
                logQueue.push_back(TStringBuilder() << testCasesProcessed << "/" << totalCases << " test case(s) processed so far" << Endl << Endl << log << Endl);
            }
        }
    };

    const TBlobStorageGroupType type(erasure);
//    for (ui32 numFmt = 1; numFmt < type.BlobSubgroupSize(); ++numFmt) { // number of disks to format
//        for (ui32 numBad = 0; numBad + numFmt < type.BlobSubgroupSize(); ++numBad) { // number of partitioned nodes
    for (ui32 numFmt : {1}) {
        for (ui32 numBad : {2}) {
            std::vector<EState> states;
            for (ui32 i = 0; i < numFmt; ++i) {
                states.push_back(EState::FORMAT);
            }
            for (ui32 i = 0; i < numBad; ++i) {
                states.push_back(EState::OFFLINE);
            }
            while (states.size() < type.BlobSubgroupSize()) {
                states.push_back(EState::OK);
            }
            Y_ABORT_UNLESS(states.size() == type.BlobSubgroupSize());
            std::sort(states.begin(), states.end());
            do {
#if SINGLE_THREAD
                DoTestCase(erasure, states);
#else
                queue.emplace_back(erasure, states);
#endif
                ++totalCases;
            } while (std::next_permutation(states.begin(), states.end()));
        }
    }

    std::list<TThread> pool;
    for (ui32 i = 0; i < NSystemInfo::NumberOfCpus(); ++i) {
        pool.emplace_back(threadFunc);
    }
    for (auto& thread : pool) {
        thread.Start();
    }
    for (ui32 n = totalCases; !SINGLE_THREAD && n; ) {
        std::deque<TString> items;
        with_lock (logMutex) {
            items.swap(logQueue);
        }
        if (logQueue.empty()) {
            Sleep(TDuration::MilliSeconds(100));
        }
        for (; !items.empty(); items.pop_front()) {
            Cerr << items.front();
            --n;
        }
    }
    for (auto& thread : pool) {
        thread.Join();
    }
}

Y_UNIT_TEST_SUITE(Replication) {
//    Y_UNIT_TEST(Phantoms_mirror3dc) { DoTest(TBlobStorageGroupType::ErasureMirror3dc); }
//    Y_UNIT_TEST(Phantoms_block4_2) { DoTest(TBlobStorageGroupType::Erasure4Plus2Block); }
//    Y_UNIT_TEST(Phantoms_mirror3of4) { DoTest(TBlobStorageGroupType::ErasureMirror3of4); }

    using E = EState;
    Y_UNIT_TEST(Phantoms_mirror3dc_special) {
        DoTestCase(TBlobStorageGroupType::ErasureMirror3dc, {E::OK, E::FORMAT, E::OK, E::OK, E::OFFLINE, E::OK, E::OK, E::OFFLINE, E::OK});
    }

    Y_UNIT_TEST(ReplStuck_mirror3dc) {
        DoTestCase(TBlobStorageGroupType::ErasureMirror3dc, {E::OK, E::FORMAT, E::OK, E::OK, E::OFFLINE, E::OK, E::OK, E::OFFLINE, E::OK}, true);
    }
}
