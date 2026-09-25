#include <ydb/core/base/hive.h>
#include <ydb/core/base/tablet.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/blob_depot/blob_depot_tablet.h>

#include "blob_depot_event_managers.h"
#include "blob_depot_test_env.h"

using namespace NKikimr;

namespace {

ui64 GetBlobDepotTabletId(TEnvironmentSetup& env, ui32 groupId) {
    const auto baseConfig = env.FetchBaseConfig();
    for (const auto& group : baseConfig.GetGroup()) {
        if (group.GetGroupId() == groupId) {
            const ui64 tabletId = group.GetVirtualGroupInfo().GetBlobDepotId();
            UNIT_ASSERT_C(tabletId, "virtual group has no BlobDepot tablet");
            return tabletId;
        }
    }
    UNIT_FAIL("virtual group not found in base config");
    return 0;
}

TIntrusivePtr<TTabletStorageInfo> GetTabletStorageInfo(TEnvironmentSetup& env, ui64 tabletId) {
    auto& runtime = *env.Runtime;
    const TActorId sender = runtime.AllocateEdgeActor(1);
    runtime.SendToPipe(runtime.GetDomainsInfo()->GetHive(), sender,
        new TEvHive::TEvGetTabletStorageInfo(tabletId), 0, TTestActorSystem::GetPipeConfigWithRetries());

    auto response = env.WaitForEdgeActorEvent<TEvHive::TEvGetTabletStorageInfoResult>(sender);
    UNIT_ASSERT(response);
    UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Record.GetStatus(), NKikimrProto::OK,
        response->Get()->Record.GetStatusMessage());
    return TabletStorageInfoFromProto(response->Get()->Record.GetInfo());
}

struct TReassignment {
    TVector<ui32> OldGroups;
    TVector<ui32> NewGroups;
};

struct TMoveDataTest {
    TBlobDepotTestEnvironment TEnv;
    TEnvironmentSetup& Env;
    const ui32 VirtualGroup;
    const ui64 BlobDepotTabletId;
    TReassignment Reassignment;
    TBSState State;
    const TActorId Edge;
    std::vector<TBlobInfo> Blobs;

    TMoveDataTest()
        : TEnv(1, 12)
        , Env(*TEnv.Env)
        , VirtualGroup(TEnv.BlobDepot)
        , BlobDepotTabletId(GetBlobDepotTabletId(Env, VirtualGroup))
        , Edge(Env.Runtime->AllocateEdgeActor(1))
    {
        Blobs.reserve(64);

        auto& runtime = *Env.Runtime;
        runtime.SetLogPriority(NKikimrServices::BLOB_DEPOT, NLog::PRI_DEBUG);
    }

    TBlobInfo& AddBlob(ui32 cookie, ui32 gen = 1, ui32 step = 1, ui32 channel = 0, ui32 size = 1024) {
        constexpr ui64 tabletId = 100;
        State[tabletId];
        Blobs.emplace_back(TEnv.DataGen(size), tabletId, cookie, gen, step, channel);
        return Blobs.back();
    }

    void Put(TBlobInfo& blob) {
        VerifiedPut(Env, 1, VirtualGroup, blob, State, false);
    }

    void Get(TBlobInfo& blob) {
        VerifiedGet(Env, 1, VirtualGroup, blob, false, false, std::nullopt, State, false);
    }

    void Collect(TBlobInfo& blob) {
        ui32 recordGeneration = 2;
        ui32 perGenerationCounter = 1;

        VerifiedCollectGarbage(Env, 1, VirtualGroup,
            blob.Id.TabletID(), recordGeneration, perGenerationCounter, blob.Id.Channel(),
            true, blob.Id.Generation(),
            blob.Id.Step(), nullptr, nullptr,
            false, true, Blobs, State, false);
    }

    void ReassignAllChannels() {
        auto& runtime = *Env.Runtime;
        const auto oldInfo = GetTabletStorageInfo(Env, BlobDepotTabletId);

        THashSet<ui32> groups;
        for (const auto& channel : oldInfo->Channels) {
            groups.insert(channel.LatestEntry()->GroupID);
        }

        TVector<ui32> availableGroups;
        for (const ui32 groupId : TEnv.RegularGroups) {
            if (!groups.contains(groupId)) {
                availableGroups.push_back(groupId);
            }
        }
        UNIT_ASSERT_C(availableGroups.size() >= oldInfo->Channels.size(),
            "not enough groups to reassign every BlobDepot channel");

        TVector<ui32> channels;
        TVector<ui32> oldGroups, newGroups;
        for (ui32 channel = 0; channel < oldInfo->Channels.size(); ++channel) {
            channels.push_back(channel);
            oldGroups.push_back(oldInfo->Channels[channel].LatestEntry()->GroupID);
            newGroups.push_back(availableGroups[channel]);
        }

        auto sender = runtime.AllocateEdgeActor(1);
        runtime.SendToPipe(runtime.GetDomainsInfo()->GetHive(), sender,
            new TEvHive::TEvReassignTablet(BlobDepotTabletId, channels, newGroups), 0,
            TTestActorSystem::GetPipeConfigWithRetries());
        runtime.DestroyActor(sender);

        for (ui32 attempt = 0; attempt < 60; ++attempt) {
            const auto info = GetTabletStorageInfo(Env, BlobDepotTabletId);
            bool reassigned = (info->Channels.size() == newGroups.size());
            for (ui32 channel = 0; reassigned && channel < info->Channels.size(); ++channel) {
                reassigned = reassigned && (info->Channels[channel].LatestEntry()->GroupID == newGroups[channel]);
            }
            if (reassigned) {
                Reassignment = {
                    std::move(oldGroups),
                    std::move(newGroups),
                };
                return;
            }
            Env.Sim(TDuration::Seconds(1));
        }

        UNIT_FAIL("BlobDepot channels were not reassigned");
    }

    void RestartTablet() {
        auto& runtime = *Env.Runtime;
        auto sender = runtime.AllocateEdgeActor(1);
        runtime.WrapInActorContext(sender, [&] {
            TActivationContext::Register(CreateTabletKiller(BlobDepotTabletId));
        });
        runtime.DestroyActor(sender);
    }

    void SendMoveData(const TVector<ui32>& groups) {
        auto& runtime = *Env.Runtime;
        runtime.SendToPipe(BlobDepotTabletId, Edge, new TEvTablet::TEvMoveData(groups), 0,
            TTestActorSystem::GetPipeConfigWithRetries());
        Env.WaitForEdgeActorEvent<TEvTabletPipe::TEvClientConnected>(Edge, false);
        Env.WaitForEdgeActorEvent<TEvTabletPipe::TEvClientDestroyed>(Edge, false);
    }

    void SendMoveData() {
        SendMoveData(Reassignment.OldGroups);
    }

    void WaitMoveData(NKikimrTabletBase::TEvMoveDataResponse::EStatus status = NKikimrTabletBase::TEvMoveDataResponse::Success) {
        auto response = Env.WaitForEdgeActorEvent<TEvTablet::TEvMoveDataResponse>(Edge);
        UNIT_ASSERT_EQUAL(response->Get()->Record.GetStatus(), status);
        Cerr << response->Get()->Record.GetErrorReason() << Endl;
    }

    void MoveData() {
        SendMoveData();
        WaitMoveData();
    }
};

} // anonymous namespace

Y_UNIT_TEST_SUITE(BlobDepotMoveData) {

    Y_UNIT_TEST(OneBlob) {
        TMoveDataTest test;
        auto& blob = test.AddBlob(1);
        test.Put(blob);

        test.ReassignAllChannels();
        test.RestartTablet();

        test.MoveData();

        test.Get(blob);
    }

    Y_UNIT_TEST(OldAndNewBlob) {
        TMoveDataTest test;
        auto& oldBlob = test.AddBlob(1);
        test.Put(oldBlob);

        test.ReassignAllChannels();
        test.RestartTablet();

        auto& newBlob = test.AddBlob(2);
        test.Put(newBlob);

        test.MoveData();

        test.Get(oldBlob);
        test.Get(newBlob);
    }

    Y_UNIT_TEST(ManyBlobs) {
        TMoveDataTest test;
        for (ui32 cookie = 1; cookie <= 32; ++cookie) {
            test.Put(test.AddBlob(cookie, 128 + cookie * 31));
        }

        test.ReassignAllChannels();
        test.RestartTablet();

        test.MoveData();

        for (auto& blob : test.Blobs) {
            test.Get(blob);
        }
    }

    Y_UNIT_TEST(BlobDeletedBeforeMove) {
        TMoveDataTest test;

        std::unique_ptr<IEventHandle> eventCopyBlob;
        bool stop = false;
        test.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& event) -> bool {
            if (!stop && event->GetTypeRewrite() == NBlobDepot::TBlobDepot::TEvMoveDataContinue::EventType) {
                eventCopyBlob = std::move(event);
                stop = true;
                return false;
            }
            return true;
        };

        auto& blob = test.AddBlob(1);
        test.Put(blob);

        test.ReassignAllChannels();
        test.RestartTablet();

        test.SendMoveData();

        test.Env.Runtime->Sim([&] {
            return stop;
        });

        test.Collect(blob);

        UNIT_ASSERT(eventCopyBlob);
        auto nodeId = eventCopyBlob->Sender.NodeId();
        test.Env.Runtime->Send(std::move(eventCopyBlob), nodeId);

        test.WaitMoveData();
    }

    Y_UNIT_TEST(BlobDeletedAfterMove) {
        TMoveDataTest test;

        std::unique_ptr<IEventHandle> eventBlobCopied;
        bool stop = false;
        test.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& event) -> bool {
            if (!stop && event->GetTypeRewrite() == NBlobDepot::TBlobDepot::TEvMoveDataBlobCopied::EventType) {
                eventBlobCopied = std::move(event);
                stop = true;
                return false;
            }
            return true;
        };

        auto& blob = test.AddBlob(1);
        test.Put(blob);

        test.ReassignAllChannels();
        test.RestartTablet();

        test.SendMoveData();

        test.Env.Runtime->Sim([&] {
            return stop;
        });

        test.Collect(blob);

        UNIT_ASSERT(eventBlobCopied);
        auto nodeId = eventBlobCopied->Sender.NodeId();
        test.Env.Runtime->Send(std::move(eventBlobCopied), nodeId);

        test.WaitMoveData();
    }

    Y_UNIT_TEST(MoveDataGroupIdMismatch) {
        TMoveDataTest test;
        auto& blob = test.AddBlob(1);
        test.Put(blob);

        test.ReassignAllChannels();
        test.RestartTablet();

        test.SendMoveData({test.Reassignment.NewGroups[0]});
        test.WaitMoveData(NKikimrTabletBase::TEvMoveDataResponse::ErrorGroupIdMismatch);

        test.Get(blob);
    }
}
