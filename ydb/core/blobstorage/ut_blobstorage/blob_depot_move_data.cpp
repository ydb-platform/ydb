#include <ydb/core/base/hive.h>
#include <ydb/core/base/tablet.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>
#include <ydb/core/testlib/tablet_helpers.h>

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

TReassignment ReassignAllChannels(TBlobDepotTestEnvironment& tenv, ui64 tabletId) {
    auto& env = *tenv.Env;
    auto& runtime = *env.Runtime;
    const auto oldInfo = GetTabletStorageInfo(env, tabletId);

    THashSet<ui32> groups;
    for (const auto& channel : oldInfo->Channels) {
        groups.insert(channel.LatestEntry()->GroupID);
    }

    TVector<ui32> availableGroups;
    for (const ui32 groupId : tenv.RegularGroups) {
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

    const TActorId sender = runtime.AllocateEdgeActor(1);
    runtime.SendToPipe(runtime.GetDomainsInfo()->GetHive(), sender,
        new TEvHive::TEvReassignTablet(tabletId, channels, newGroups), 0,
        TTestActorSystem::GetPipeConfigWithRetries());
    runtime.DestroyActor(sender);

    for (ui32 attempt = 0; attempt < 60; ++attempt) {
        const auto info = GetTabletStorageInfo(env, tabletId);
        bool reassigned = (info->Channels.size() == newGroups.size());
        for (ui32 channel = 0; reassigned && channel < info->Channels.size(); ++channel) {
            reassigned = reassigned && (info->Channels[channel].LatestEntry()->GroupID == newGroups[channel]);
        }
        if (reassigned) {
            return {
                std::move(oldGroups),
                std::move(newGroups),
            };
        }
        env.Sim(TDuration::Seconds(1));
    }

    UNIT_FAIL("BlobDepot channels were not reassigned");
    return {};
}

void RestartTablet(TBlobDepotTestEnvironment& tenv, ui64 tabletId) {
    auto& env = *tenv.Env;
    auto& runtime = *env.Runtime;
    auto edge = runtime.AllocateEdgeActor(1);
    runtime.WrapInActorContext(edge, [&] {
        TActivationContext::Register(CreateTabletKiller(tabletId));
    });
    runtime.DestroyActor(edge);
}

NKikimrTabletBase::TEvMoveDataResponse::EStatus MoveData(
        TEnvironmentSetup& env, ui64 tabletId, const TVector<ui32>& groups)
{
    auto& runtime = *env.Runtime;
    const TActorId sender = runtime.AllocateEdgeActor(1);
    runtime.SendToPipe(tabletId, sender, new TEvTablet::TEvMoveData(groups), 0,
        TTestActorSystem::GetPipeConfigWithRetries());
    auto response = env.WaitForEdgeActorEvent<TEvTablet::TEvMoveDataResponse>(sender);
    return response->Get()->Record.GetStatus();
}

struct TMoveDataTest {
    TBlobDepotTestEnvironment TEnv;
    TEnvironmentSetup& Env;
    const ui32 VirtualGroup;
    const ui64 BlobDepotTabletId;
    TBSState State;
    std::vector<TBlobInfo> Blobs;

    TMoveDataTest()
        : TEnv(1, 12)
        , Env(*TEnv.Env)
        , VirtualGroup(TEnv.BlobDepot)
        , BlobDepotTabletId(GetBlobDepotTabletId(Env, VirtualGroup))
    {
        Blobs.reserve(64);

        auto& runtime = *Env.Runtime;
        runtime.SetLogPriority(NKikimrServices::BLOB_DEPOT, NLog::PRI_DEBUG);
    }

    TBlobInfo& AddBlob(ui32 cookie, ui32 size = 1024) {
        constexpr ui64 tabletId = 100;
        State[tabletId];
        Blobs.emplace_back(TEnv.DataGen(size), tabletId, cookie);
        return Blobs.back();
    }

    void Put(TBlobInfo& blob) {
        VerifiedPut(Env, 1, VirtualGroup, blob, State, false);
    }

    void Get(TBlobInfo& blob) {
        VerifiedGet(Env, 1, VirtualGroup, blob, false, false, std::nullopt, State, false);
    }
};

} // anonymous namespace

Y_UNIT_TEST_SUITE(BlobDepotMoveData) {

    Y_UNIT_TEST(OneBlob) {
        TMoveDataTest test;
        auto& blob = test.AddBlob(1);
        test.Put(blob);

        const auto reassignment = ReassignAllChannels(test.TEnv, test.BlobDepotTabletId);
        RestartTablet(test.TEnv, test.BlobDepotTabletId);

        const auto status = MoveData(test.Env, test.BlobDepotTabletId, reassignment.OldGroups);
        UNIT_ASSERT(status == NKikimrTabletBase::TEvMoveDataResponse::Success);

        test.Get(blob);
    }

    Y_UNIT_TEST(OldAndNewBlob) {
        TMoveDataTest test;
        auto& oldBlob = test.AddBlob(1);
        test.Put(oldBlob);

        const auto reassignment = ReassignAllChannels(test.TEnv, test.BlobDepotTabletId);
        RestartTablet(test.TEnv, test.BlobDepotTabletId);

        auto& newBlob = test.AddBlob(2);
        test.Put(newBlob);

        const auto status = MoveData(test.Env, test.BlobDepotTabletId, reassignment.OldGroups);
        UNIT_ASSERT(status == NKikimrTabletBase::TEvMoveDataResponse::Success);

        test.Get(oldBlob);
        test.Get(newBlob);
    }

    Y_UNIT_TEST(ManyBlobs) {
        TMoveDataTest test;
        for (ui32 cookie = 1; cookie <= 32; ++cookie) {
            test.Put(test.AddBlob(cookie, 128 + cookie * 31));
        }

        const auto reassignment = ReassignAllChannels(test.TEnv, test.BlobDepotTabletId);
        RestartTablet(test.TEnv, test.BlobDepotTabletId);

        const auto status = MoveData(test.Env, test.BlobDepotTabletId, reassignment.OldGroups);
        UNIT_ASSERT(status == NKikimrTabletBase::TEvMoveDataResponse::Success);

        for (auto& blob : test.Blobs) {
            test.Get(blob);
        }
    }
}
