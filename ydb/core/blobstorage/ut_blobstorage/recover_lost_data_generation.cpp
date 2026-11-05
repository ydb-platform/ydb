#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>

Y_UNIT_TEST_SUITE(RecoverLostDataGeneration) {

    // A VDisk that lost its data runs one full sync proxy per peer. Hold the first requests of these proxies until
    // the group generation changes, so that no peer is synced in the old generation, then let them go. Every retry
    // after that must be made in the new generation, otherwise the peers reject it and recovery never completes.
    Y_UNIT_TEST(CompletesAfterGenerationChange) {
        constexpr ui32 nodeCount = 9;
        TEnvironmentSetup env(TEnvironmentSetup::TSettings{
            .NodeCount = nodeCount,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        });
        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Minutes(1));
        const ui32 groupId = env.GetGroups().front();
        auto info = env.GetGroupInfo(groupId);
        const ui32 recoveringNode = info->GetActorId(0).NodeId();
        const TString data(1024, 'S');
        const TLogoBlobID blob(1, 1, 1, 0, data.size(), 0);
        env.PutBlob(groupId, blob, data);
        env.WaitForSync(info, blob);

        env.Cleanup();
        for (auto& [key, state] : env.PDiskMockStates) {
            if (key.first == recoveringNode) {
                state.Reset();
            }
        }
        env.Initialize();

        const ui32 initialGeneration = info->GroupGeneration;
        bool hold = true;
        std::vector<std::unique_ptr<IEventHandle>> held;
        ui32 generationSeen = 0;
        bool recovered = false;
        ui32 failedReplies = 0;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvBlobStorage::EvVSyncFull:
                    if (hold && nodeId != recoveringNode && ev->Sender.NodeId() == recoveringNode) {
                        held.push_back(std::move(ev));
                        return false;
                    }
                    break;

                case TEvBlobStorage::EvVSyncFullResult:
                    if (nodeId == recoveringNode && !hold) {
                        const auto status = ev->Get<TEvBlobStorage::TEvVSyncFullResult>()->Record.GetStatus();
                        failedReplies += status != NKikimrProto::OK && status != NKikimrProto::NODATA;
                    }
                    break;

                case TEvBlobStorage::EvVGenerationChange:
                    if (nodeId == recoveringNode) {
                        generationSeen = Max(generationSeen, ev->Get<TEvVGenerationChange>()->NewInfo->GroupGeneration);
                    }
                    break;

                case TEvBlobStorage::EvSyncerLostDataRecovered:
                    if (nodeId == recoveringNode) {
                        recovered = true;
                    }
                    break;
            }
            return true;
        };
        auto waitUntil = [&](auto predicate, const char* phase) {
            for (ui32 i = 0; i != 600 && !predicate(); ++i) {
                env.Sim(TDuration::MilliSeconds(100));
            }
            UNIT_ASSERT_C(predicate(), phase);
        };

        // every peer outside the own fail domain gets a full sync proxy
        const ui32 peers = info->GetTotalVDisksNum() - 1;
        waitUntil([&] { return held.size() >= peers; }, "Lost data recovery must start full sync with every peer");
        UNIT_ASSERT(!recovered);

        // move another member of the group to the spare node to advance the generation
        std::set<ui32> groupNodes;
        for (ui32 i = 0; i != info->GetTotalVDisksNum(); ++i) {
            groupNodes.insert(info->GetActorId(i).NodeId());
        }
        ui32 spare = 1;
        while (groupNodes.contains(spare)) {
            ++spare;
        }
        UNIT_ASSERT(spare <= nodeCount);
        std::optional<ui32> sparePDiskId;
        const auto baseConfig = env.FetchBaseConfig();
        for (const auto& pdisk : baseConfig.GetPDisk()) {
            if (pdisk.GetNodeId() == spare) {
                sparePDiskId = pdisk.GetPDiskId();
                break;
            }
        }
        UNIT_ASSERT_C(sparePDiskId, "The spare node must have a registered PDisk");
        const auto disk = info->GetVDiskId(1);
        NKikimrBlobStorage::TConfigRequest request;
        // the controller may not yet see the restarted disks as ready; only the generation change matters here
        request.SetIgnoreGroupFailModelChecks(true);
        request.SetIgnoreDegradedGroupsChecks(true);
        request.SetIgnoreDisintegratedGroupsChecks(true);
        auto* cmd = request.AddCommand()->MutableReassignGroupDisk();
        cmd->SetGroupId(groupId);
        cmd->SetGroupGeneration(initialGeneration);
        cmd->SetFailRealmIdx(disk.FailRealm);
        cmd->SetFailDomainIdx(disk.FailDomain);
        cmd->SetVDiskIdx(disk.VDisk);
        cmd->MutableTargetPDiskId()->SetNodeId(spare);
        cmd->MutableTargetPDiskId()->SetPDiskId(*sparePDiskId);
        const auto result = env.Invoke(request);
        UNIT_ASSERT_C(result.GetSuccess(), result.GetErrorDescription());
        UNIT_ASSERT_C(result.GetStatus(0).GetSuccess(), result.GetStatus(0).GetErrorDescription());
        waitUntil([&] { return generationSeen > initialGeneration; },
            "The recovering VDisk must receive the new generation");
        env.Sim(TDuration::Seconds(1));
        UNIT_ASSERT(!recovered);

        // let the requests of the old generation go; peers reject them, and the proxies retry
        hold = false;
        for (auto& ev : held) {
            const ui32 nodeId = ev->GetRecipientRewrite().NodeId();
            env.Runtime->Send(ev.release(), nodeId);
        }
        held.clear();

        for (ui32 i = 0; i != 600 && !recovered; ++i) {
            env.Sim(TDuration::MilliSeconds(100));
        }
        Cerr << "recovered# " << recovered << " failedReplies# " << failedReplies << Endl;
        UNIT_ASSERT_C(recovered, "Lost data recovery must complete in the new generation");

        env.Runtime->FilterFunction = {};
        info = env.GetGroupInfo(groupId);
        for (ui32 i = 0; i != info->GetTotalVDisksNum(); ++i) {
            env.WaitForVDiskRepl(info->GetActorId(i), info->GetVDiskId(i));
        }
        const TActorId edge = env.Runtime->AllocateEdgeActor(recoveringNode);
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvGet(blob, 0, 0, TInstant::Max(),
                NKikimrBlobStorage::FastRead));
        });
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->ResponseSz, 1);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Responses[0].Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Responses[0].Buffer.ConvertToString(), data);
    }

}
