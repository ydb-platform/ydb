#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/dsproxy/dsproxy.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_partlayout.h>
#include <util/system/condvar.h>
#include <util/system/info.h>

#define SINGLE_THREAD 0

namespace {

bool DoTestCase(TBlobStorageGroupType::EErasureSpecies erasure, const std::set<std::pair<ui32, ui32>>& orderNumToPart,
        bool doNotNeedToRestore, const TLogoBlobID& id, const TString& data, IOutputStream *stream) {
    const TBlobStorageGroupType type(erasure);
    const ui32 numDisks = type.BlobSubgroupSize();

    TEnvironmentSetup env(TEnvironmentSetup::TSettings{
        .NodeCount = numDisks,
        .Erasure = erasure,
        .PrepareRuntime = [&](TTestActorSystem& runtime) {
            runtime.LogStream = stream;
        },
    });

    env.CreateBoxAndPool(1, 1);

    const auto& groups = env.GetGroups();
    Y_ABORT_UNLESS(groups.size() == 1);
    const auto& info = env.GetGroupInfo(groups.front());

    TBlobStorageGroupInfo::TOrderNums orderNums;
    info->GetTopology().PickSubgroup(id.Hash(), orderNums);

    TString temp = data;
    char *buffer = temp.Detach();
    Encrypt(buffer, buffer, 0, temp.size(), id, *info);
    TDataPartSet parts;
    info->Type.SplitData((TBlobStorageGroupType::ECrcMode)id.CrcMode(), temp, parts);

    TString error;

    // put parts to disks
    for (const auto& [orderNum, partIdx] : orderNumToPart) {
        const TActorId& queueId = env.CreateQueueActor(info->GetVDiskId(orderNum), NKikimrBlobStorage::PutTabletLog, 0);
        const TActorId& sender = env.Runtime->AllocateEdgeActor(queueId.NodeId());
        const TLogoBlobID blobId(id, partIdx + 1);
        TRope buffer = type.PartSize(blobId) ? parts.Parts[partIdx].OwnedString : TRope(TString());
        env.Runtime->Send(new IEventHandle(queueId, sender, new TEvBlobStorage::TEvVPut(blobId, buffer,
            info->GetVDiskId(orderNum), false, nullptr, TInstant::Max(), NKikimrBlobStorage::TabletLog)),
            sender.NodeId());
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVPutResult>(sender);
        Y_ABORT_UNLESS(res->Get()->Record.GetStatus() == NKikimrProto::OK);
    }

    // sync
    env.Sim(TDuration::Seconds(10));

    // obtain config
    NKikimrBlobStorage::TConfigRequest request;
    request.AddCommand()->MutableQueryBaseConfig();
    NKikimrBlobStorage::TConfigResponse response = env.Invoke(request);
    Y_ABORT_UNLESS(response.GetSuccess());
    for (const auto& vslot : response.GetStatus(0).GetBaseConfig().GetVSlot()) {
        const TVDiskID vdiskId(vslot.GetGroupId(), vslot.GetGroupGeneration(), vslot.GetFailRealmIdx(),
            vslot.GetFailDomainIdx(), vslot.GetVDiskIdx());
        const auto& v = vslot.GetVSlotId();
        env.Wipe(v.GetNodeId(), v.GetPDiskId(), v.GetVSlotId(), vdiskId);
        env.Sim(TDuration::Seconds(30));
    }

    // collect blob info from cluster
    TSubgroupPartLayout layout;
    std::vector<std::tuple<TVDiskID, TLogoBlobID, NKikimrProto::EReplyStatus>> v;
    for (ui32 i = 0; i < numDisks; ++i) {
        const TActorId& queueId = env.CreateQueueActor(info->GetVDiskId(i), NKikimrBlobStorage::GetFastRead, 0);
        const TActorId& sender = env.Runtime->AllocateEdgeActor(queueId.NodeId());
        auto query = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(info->GetVDiskId(i), TInstant::Max(),
            NKikimrBlobStorage::FastRead);
        query->AddExtremeQuery(id, 0, 0);
        env.Runtime->Send(new IEventHandle(queueId, sender, query.release()), sender.NodeId());
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVGetResult>(sender);
        const auto& record = res->Get()->Record;
        Y_ABORT_UNLESS(record.GetStatus() == NKikimrProto::OK);
        const TVDiskID& vdiskId = VDiskIDFromVDiskID(record.GetVDiskID());
        for (const auto& blob : record.GetResult()) {
            const auto& id = LogoBlobIDFromLogoBlobID(blob.GetBlobID());
            v.emplace_back(vdiskId, id, blob.GetStatus());
            if (blob.GetStatus() == NKikimrProto::OK || blob.GetStatus() == NKikimrProto::NOT_YET) {
                layout.AddItem(info->GetIdxInSubgroup(vdiskId, id.FullID().Hash()), id.PartId() - 1, info->Type);
            }
        }
    }

    std::sort(v.begin(), v.end());
    for (const auto& item : v) {
        const TVDiskID& vdiskId = std::get<0>(item);
        const TLogoBlobID& blobId = std::get<1>(item);
        const NKikimrProto::EReplyStatus status = std::get<2>(item);
        *stream << "VDiskId# " << vdiskId << " BlobId# " << blobId << " Status# " << NKikimrProto::EReplyStatus_Name(status) << Endl;
        if (doNotNeedToRestore && (status == NKikimrProto::OK || status == NKikimrProto::NOT_YET)) {
            const ui32 orderNum = info->GetOrderNumber(vdiskId);
            if (!orderNumToPart.count(std::make_pair(orderNum, blobId.PartId() - 1))) {
                error = "Excessive part written to disk";
            }
        }
    }

    if (error || doNotNeedToRestore) {
        // skip checking
    } else if (info->GetQuorumChecker().GetBlobState(layout, {&info->GetTopology()}) != TBlobStorageGroupInfo::EBS_FULL) {
        error = "Blob is not in EBS_FULL state";
    } else {
        switch (info->Type.GetErasure()) {
            case TBlobStorageGroupType::ErasureMirror3dc:
            case TBlobStorageGroupType::ErasureMirror3of4:
                break;

            default: {
                ui32 numDistinctParts = 0;
                for (ui32 i = 0; i < info->Type.TotalPartCount(); ++i) {
                    if (layout.GetDisksWithPart(i)) {
                        ++numDistinctParts;
                    }
                }
                if (numDistinctParts != info->Type.TotalPartCount()) {
                    error = "Not enough parts written";
                }
                break;
            }
        }
    }

    *stream << "[";
    bool first = true;
    for (const auto& [orderNum, part] : orderNumToPart) {
        const auto& vdiskId = info->GetVDiskId(orderNum);
        *stream << (std::exchange(first, false) ? "" : " ") << "[" << (int)vdiskId.FailRealm << ":"
            << (int)vdiskId.FailDomain << "]:" << part;
    }
    *stream << "] " << (error ? error : "Valid") << Endl;

    return !error;
}

template<typename F, typename G>
void RunInThreads(size_t numTasks, F&& func, G&& postprocess) {
    using TResult = std::invoke_result_t<F, size_t>;

    std::deque<TResult> output;
    TMutex outputMutex;
    TCondVar outputCV;
    size_t numRunning = 0;

    size_t index = 0;
    TMutex indexMutex;
    auto threadFunc = [&] {
        for (;;) {
            size_t taskIndex;
            with_lock (indexMutex) {
                if (index == numTasks) {
                    break;
                }
                taskIndex = index++;
            }

            TResult result = std::invoke(func, taskIndex);

            with_lock (outputMutex) {
                output.push_back(std::move(result));
            }
            outputCV.Signal();
        }
        with_lock (outputMutex) {
            if (!--numRunning) {
                outputCV.BroadCast();
            }
        }
    };

    std::list<TThread> threads;
    while (threads.size() < NSystemInfo::NumberOfCpus()) {
        threads.emplace_back(threadFunc);
        ++numRunning;
    }
    for (auto& thread : threads) {
        thread.Start();
    }
    for (;;) {
        std::deque<TResult> results;
        with_lock (outputMutex) {
            results.swap(output);
            if (results.empty()) {
                if (numRunning) {
                    outputCV.Wait(outputMutex);
                    continue;
                } else {
                    break;
                }
            }
        }
        for (TResult& item : results) {
            std::invoke(postprocess, item);
        }
    }
    for (auto& thread : threads) {
        thread.Join();
    }
}

void DoTest(TBlobStorageGroupType::EErasureSpecies erasure) {
    const TBlobStorageGroupType type(erasure);
    const ui32 numDisks = type.BlobSubgroupSize();

    const ui32 numRealms = erasure == TBlobStorageGroupType::ErasureMirror3dc ? 3 : 1;
    const ui32 numDomains = erasure == TBlobStorageGroupType::ErasureMirror3dc ? 3 : type.BlobSubgroupSize();
    TBlobStorageGroupInfo tempInfo(type, 1, numDomains, numRealms); // group with the same layout as ours
    const auto& topology = tempInfo.GetTopology();

    TString data = "Hello, World!\n";
    TLogoBlobID id(1, 1, 1, 0, data.size(), 0);

    TBlobStorageGroupInfo::TOrderNums orderNums;
    topology.PickSubgroup(id.Hash(), orderNums);

    std::vector<std::vector<ui32>> suitablePartsByOrderNum(numDisks);
    for (ui32 i = 0; i < type.BlobSubgroupSize(); ++i) {
        const ui32 orderNum = orderNums[i];
        auto& v = suitablePartsByOrderNum[orderNum];
        v.push_back(0); // no parts
        switch (erasure) {
            case TBlobStorageGroupType::ErasureMirror3dc:
                v.push_back(1 << (i % 3)); // per-dc part indexing
                break;

            case TBlobStorageGroupType::ErasureMirror3of4:
                v.push_back(1 << 2); // meta
                v.push_back(1 << (i & 1)); // data
                break;

            default:
                if (i < type.TotalPartCount()) {
                    v.push_back(1 << i); // main
                } else {
                    for (ui32 j = 1; j < (1 << type.TotalPartCount()); ++j) {
                        if (PopCount(j) <= 1) {
                            v.push_back(j); // handoff
                        }
                    }
                }
                break;
        }
    }

    std::vector<std::tuple<std::set<std::pair<ui32, ui32>>, bool>> tasks;

    std::vector<size_t> indexes(numDisks, 0);
    for (;;) {
        TSubgroupPartLayout layout;
        std::set<std::pair<ui32, ui32>> orderNumToPart;
        for (ui32 i = 0; i < numDisks; ++i) {
            ui32 mask = suitablePartsByOrderNum[i][indexes[i]];
            while (mask) {
                const ui32 part = CountTrailingZeroBits(mask);
                orderNumToPart.emplace(i, part);
                layout.AddItem(topology.GetIdxInSubgroup(topology.GetVDiskId(i), id.Hash()), part, type);
                mask &= ~(1 << part);
            }
        }
        bool doNotNeedToRestore;
        switch (erasure) {
            case TBlobStorageGroupType::ErasureMirror3dc:
                doNotNeedToRestore = topology.GetQuorumChecker().CheckQuorumForSubgroup(layout.GetInvolvedDisks(&topology)) ||
                    (!layout.GetDisksWithPart(0) && !layout.GetDisksWithPart(1) && !layout.GetDisksWithPart(2));
                break;

            case TBlobStorageGroupType::ErasureMirror3of4: {
                auto [data, any] = layout.GetMirror3of4State();
                doNotNeedToRestore = !data || (data >= 3 && any >= 5);
                break;
            }

            default: {
                ui32 numDistinctParts = 0;
                for (ui32 i = 0; i < type.TotalPartCount(); ++i) {
                    if (layout.GetDisksWithPart(i)) {
                        ++numDistinctParts;
                    }
                }
                doNotNeedToRestore = numDistinctParts < type.MinimalRestorablePartCount() || (
                    numDistinctParts == type.TotalPartCount() &&
                    layout.GetInvolvedDisks(&topology).GetNumSetItems() >= type.DataParts() + type.ParityParts() &&
                    layout.CountEffectiveReplicas(type) == numDistinctParts
                );
                break;
            }
        }

        tasks.emplace_back(std::move(orderNumToPart), doNotNeedToRestore);

        bool carry = true;
        for (ui32 i = 0; carry && i < numDisks; ++i) {
            carry = ++indexes[i] == suitablePartsByOrderNum[i].size();
            if (carry) {
                indexes[i] = 0;
            }
        }
        if (carry) {
            break;
        }
    }

    Cerr << "total# " << tasks.size() << Endl;

    ui32 badCases = 0;

#if SINGLE_THREAD
    for (const auto& [orderNumToPart, doNotNeedToRestore] : tasks) {
        badCases += !DoTestCase(erasure, orderNumToPart, doNotNeedToRestore, id, data, &Cerr);
    }
#else
    ui32 done = 0;
    auto func = [&](size_t index) {
        const auto& [orderNumToPart, doNotNeedToRestore] = tasks[index];
        TStringStream stream;
        const bool res = !DoTestCase(erasure, orderNumToPart, doNotNeedToRestore, id, data, &stream);
        return std::make_tuple(stream.Str(), res);
    };
    auto post = [&](const std::tuple<TString, bool>& value) {
        const auto& [log, res] = value;
        Cerr << log;
        badCases += res;
        Cerr << "done " << ++done << " of " << tasks.size() << Endl;
    };
    RunInThreads(tasks.size(), func, post);
#endif

    UNIT_ASSERT_VALUES_EQUAL(badCases, 0);
}

}

Y_UNIT_TEST_SUITE(Osiris) {

    Y_UNIT_TEST(mirror3dc) { DoTest(TBlobStorageGroupType::ErasureMirror3dc); }
    Y_UNIT_TEST(mirror3of4) { DoTest(TBlobStorageGroupType::ErasureMirror3of4); }
    Y_UNIT_TEST(block42) { DoTest(TBlobStorageGroupType::Erasure4Plus2Block); }

}
