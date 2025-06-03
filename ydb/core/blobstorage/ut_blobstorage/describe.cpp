#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_impl.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

Y_UNIT_TEST_SUITE(BSCDescribe) {

    NKikimrBlobStorage::TEvControllerDescribeResponse Describe(TEnvironmentSetup& env, TVDiskID& vdId) {
        NKikimrBlobStorage::TEvControllerDescribeRequest request;
        auto* vdiskId = request.AddTargets()->MutableVDiskId();
        VDiskIDFromVDiskID(vdId, vdiskId);

        const TActorId self = env.Runtime->AllocateEdgeActor(env.Settings.ControllerNodeId, __FILE__, __LINE__);
        auto ev = std::make_unique<TEvBlobStorage::TEvControllerDescribeRequest>();
        ev->Record.CopyFrom(request);
        env.Runtime->SendToPipe(env.TabletId, self, ev.release(), 0, TTestActorSystem::GetPipeConfigWithRetries());
        auto response = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvControllerDescribeResponse>(self);
        return response->Get()->Record;
    }

    std::vector<TVDiskID> GetGroupVDisks(TEnvironmentSetup& env) {
        std::vector<TVDiskID> vdisks;

        auto config = env.FetchBaseConfig();

        auto& group = config.get_idx_group(0);
        
        for (auto& vslot : config.GetVSlot()) {
            if (group.GetGroupId() == vslot.GetGroupId()) {
                auto vdiskId = TVDiskID(group.GetGroupId(), group.GetGroupGeneration(), vslot.GetFailRealmIdx(), vslot.GetFailDomainIdx(), vslot.GetVDiskIdx());
                vdisks.push_back(vdiskId);
            }
        }

        return vdisks;
    }

    TString ResponseText(NKikimrBlobStorage::TEvControllerDescribeResponse& resp) {
        TStringStream result;
        for (auto& res : resp.GetResults()) {
            switch (res.GetResponseCase()) {
                case NKikimrBlobStorage::TEvControllerDescribeResponse_TResponse::kVDiskResponse:
                    result << res.GetVDiskResponse().GetResult() << Endl;
                    break;
                case NKikimrBlobStorage::TEvControllerDescribeResponse_TResponse::kPDiskResponse:
                    result << res.GetPDiskResponse().GetResult() << Endl;
                    break;
                case NKikimrBlobStorage::TEvControllerDescribeResponse_TResponse::kGroupResponse:
                    result << res.GetGroupResponse().GetResult() << Endl;
                    break;
                default:
                    break;
            }
        } 

        return result.Str();
    }

    Y_UNIT_TEST(Test1) {
        TEnvironmentSetup env({
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block
        });

        env.UpdateSettings(false, false);
        env.CreateBoxAndPool(2, 1);
        env.Sim(TDuration::Seconds(30));

        auto vdisks = GetGroupVDisks(env);

        // Making all vdisks bad, group is disintegrated
        const TActorId sender = env.Runtime->AllocateEdgeActor(env.Settings.ControllerNodeId, __FILE__, __LINE__);
        for (auto& pdisk : env.PDiskActors) {
            env.Runtime->WrapInActorContext(sender, [&] () {
                env.Runtime->Send(new IEventHandle(EvBecomeError, 0, pdisk, sender, nullptr, 0));
            });
        }

        env.Sim(TDuration::Minutes(1));

        NKikimrBlobStorage::TEvControllerDescribeResponse resp = Describe(env, vdisks[0]);
        
        UNIT_ASSERT_STRING_CONTAINS(ResponseText(resp), "Disk is not ready, but is not eligible for self-heal, because PDisk has Status# ACTIVE");
    }

    Y_UNIT_TEST(Test2) {
        TEnvironmentSetup env({
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block
        });

        env.UpdateSettings(false, false);
        env.CreateBoxAndPool(2, 1);
        env.Sim(TDuration::Seconds(30));

        auto vdisks = GetGroupVDisks(env);

        for (auto& pdisk : env.PDiskMockStates) {
            auto& pdiskId = pdisk.first;
            if (pdiskId.second == 1000) {
                // Only set INACTIVE to first pdisk on each node.
                env.UpdateDriveStatus(pdiskId.first, pdiskId.second, NKikimrBlobStorage::EDriveStatus::INACTIVE, {}, true);
            }
        }

        env.Sim(TDuration::Minutes(1));
        
        NKikimrBlobStorage::TEvControllerDescribeResponse resp = Describe(env, vdisks[0]);

        UNIT_ASSERT_STRING_CONTAINS(ResponseText(resp), "Disk is ready and does not require self-heal");
    }

    Y_UNIT_TEST(Test3) {
        TEnvironmentSetup env({
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block
        });

        env.UpdateSettings(false, false);
        env.CreateBoxAndPool(2, 1);
        env.Sim(TDuration::Seconds(30));

        auto vdisks = GetGroupVDisks(env);

        const TActorId sender = env.Runtime->AllocateEdgeActor(env.Settings.ControllerNodeId, __FILE__, __LINE__);
        for (auto& pdisk : env.PDiskActors) {
            env.Runtime->WrapInActorContext(sender, [&] () {
                env.Runtime->Send(new IEventHandle(EvBecomeError, 0, pdisk, sender, nullptr, 0));
            });
            break;
        }

        for (auto& [nodeId, pdiskId] : {std::pair{2, 1000}, {3, 1000}}) {
            env.UpdateDriveStatus(nodeId, pdiskId, NKikimrBlobStorage::EDriveStatus::INACTIVE, {}, true);
        }

        env.UpdateDriveStatus(1, 1000, NKikimrBlobStorage::EDriveStatus::FAULTY, {}, true);

        env.Sim(TDuration::Minutes(1));

        NKikimrBlobStorage::TEvControllerDescribeResponse resp = Describe(env, vdisks[0]);
        
        UNIT_ASSERT_STRING_CONTAINS(ResponseText(resp), "Self-Heal unavailable, it may break the group");
    }
}
