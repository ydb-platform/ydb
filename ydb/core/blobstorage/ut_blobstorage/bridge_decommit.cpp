#include "bridge_common.h"

#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/protos/subdomains.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <algorithm>
#include <iterator>

using namespace NBridgeTest;

namespace {

    // nodes of the natively bridged cluster: 8 nodes per pile
    TNodeLocation GetBridgedLocation(ui32 nodeId) {
        NActorsInterconnect::TNodeLocation location;
        location.SetBridgePileName(TStringBuilder() << "pile_" << (nodeId - 1) / 8 + 1);
        location.SetDataCenter("my_dc");
        location.SetRack(TStringBuilder() << "rack_" << nodeId);
        location.SetUnit(TStringBuilder() << "unit_" << nodeId);
        return TNodeLocation(location);
    }

    // nodes of the cluster being converted have no pile names in their locations
    TNodeLocation GetUnbridgedLocation(ui32 nodeId) {
        NActorsInterconnect::TNodeLocation location;
        location.SetDataCenter("my_dc");
        location.SetRack(TStringBuilder() << "rack_" << nodeId);
        location.SetUnit(TStringBuilder() << "unit_" << nodeId);
        return TNodeLocation(location);
    }

    // test environment has no SchemeCache, while BS_CONTROLLER resolves database of the decommitted group through it;
    // this stub describes the root domain containing provided storage pools
    class TSchemeCacheStub : public TActor<TSchemeCacheStub> {
        const NKikimrSubDomains::TDomainDescription Description;

    public:
        TSchemeCacheStub(NKikimrSubDomains::TDomainDescription description)
            : TActor(&TThis::StateFunc)
            , Description(std::move(description))
        {}

        void Handle(TEvTxProxySchemeCache::TEvNavigateKeySet::TPtr ev) {
            TAutoPtr<NSchemeCache::TSchemeCacheNavigate> request = ev->Get()->Request.Release();
            for (auto& entry : request->ResultSet) {
                entry.Status = NSchemeCache::TSchemeCacheNavigate::EStatus::Ok;
                entry.DomainInfo = MakeIntrusive<NSchemeCache::TDomainInfo>(Description);
                auto domain = MakeIntrusive<NSchemeCache::TSchemeCacheNavigate::TDomainDescription>();
                domain->Description = Description;
                entry.DomainDescription = domain;
            }
            Send(ev->Sender, new TEvTxProxySchemeCache::TEvNavigateKeySetResult(request), 0, ev->Cookie);
        }

        STRICT_STFUNC(StateFunc,
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySet, Handle)
        )
    };

    void SetupSchemeCacheStub(TEnvironmentSetup& env) {
        NKikimrSubDomains::TDomainDescription description;
        auto *key = description.MutableDomainKey();
        key->SetSchemeShard(env.Runtime->GetDomainsInfo()->GetDomain()->SchemeRoot);
        key->SetPathId(1);
        auto *pool = description.AddStoragePools();
        pool->SetName(env.StoragePoolName);
        pool->SetKind(env.StoragePoolName);

        for (ui32 nodeId : env.Runtime->GetNodes()) {
            env.Runtime->RegisterService(MakeSchemeCacheID(), env.Runtime->Register(new TSchemeCacheStub(description),
                nodeId));
        }
    }

    NKikimrBlobStorage::TConfigResponse DecommitGroup(TEnvironmentSetup& env, ui32 groupId) {
        NKikimrBlobStorage::TConfigRequest request;
        auto *cmd = request.AddCommand()->MutableDecommitGroups();
        cmd->AddGroupIds(groupId);
        cmd->SetDatabase(TStringBuilder() << '/' << env.Runtime->GetDomainsInfo()->GetDomain()->Name);
        return env.Invoke(request); // channel profiles are generated from the group's own pool
    }

    const NKikimrBlobStorage::TBaseConfig::TGroup& FindGroup(const NKikimrBlobStorage::TBaseConfig& baseConfig,
            ui32 groupId) {
        for (const auto& group : baseConfig.GetGroup()) {
            if (group.GetGroupId() == groupId) {
                return group;
            }
        }
        UNIT_FAIL("group not found GroupId# " << groupId);
        Y_UNREACHABLE();
    }

    void WaitForDecommitStatus(TEnvironmentSetup& env, ui32 groupId, NKikimrBlobStorage::TGroupDecommitStatus::E expected) {
        for (ui32 i = 0; i < 60; ++i) {
            const auto baseConfig = env.FetchBaseConfig();
            if (FindGroup(baseConfig, groupId).GetVirtualGroupInfo().GetDecommitStatus() == expected) {
                return;
            }
            env.Sim(TDuration::Seconds(10));
        }
        UNIT_FAIL("decommit status " << NKikimrBlobStorage::TGroupDecommitStatus::E_Name(expected) << " not reached");
    }

    // keeps decommission in IN_PROGRESS state by holding the puts copying assimilated data into BlobDepot channels; blocks
    // and barriers get assimilated meanwhile, so the BlobDepot is ready for agent queries, while the data still resides
    // in the group being decommitted
    class TCopyingHold {
        TEnvironmentSetup& Env;
        bool Hold = true;
        std::vector<std::pair<ui32, std::unique_ptr<IEventHandle>>> Held;

    public:
        TCopyingHold(TEnvironmentSetup& env)
            : Env(env)
        {
            Env.Runtime->FilterFunction = [this](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
                if (Hold && ev->GetTypeRewrite() == TEvBlobStorage::EvPut &&
                        ev->Get<TEvBlobStorage::TEvPut>()->WriteSource == TWriteSource::BlobDepotPut) {
                    Held.emplace_back(nodeId, std::move(ev));
                    return false;
                }
                return true;
            };
        }

        ~TCopyingHold() {
            Env.Runtime->FilterFunction = {};
        }

        bool HasHeld() const {
            return !Held.empty();
        }

        void Release() {
            Hold = false;
            Env.Runtime->FilterFunction = {};
            for (auto& [nodeId, ev] : std::exchange(Held, {})) {
                Env.Runtime->Send(ev.release(), nodeId);
            }
        }
    };

    struct TPoolSnapshot {
        std::set<ui32> Groups;
        THashMap<ui32, ui32> NumVSlots; // group id -> number of VDisks
    };

    TPoolSnapshot MakePoolSnapshot(TEnvironmentSetup& env) {
        TPoolSnapshot res;
        const auto baseConfig = env.FetchBaseConfig();
        for (const auto *group : GetPoolGroups(baseConfig, 1)) {
            res.Groups.insert(group->GetGroupId());
        }
        for (const auto& vslot : baseConfig.GetVSlot()) {
            ++res.NumVSlots[vslot.GetGroupId()];
        }
        return res;
    }

    // decommit the whole bridged group, checking data through restarted nodes while decommission is in progress; returns
    // the groups that vanished during decommission
    std::set<ui32> RunDecommission(TEnvironmentSetup& env, TDataChecker& checker, ui32 groupId,
            const std::vector<ui32>& nodesToRestart) {
        SetupSchemeCacheStub(env);

        const TPoolSnapshot before = MakePoolSnapshot(env);
        UNIT_ASSERT(before.Groups.contains(groupId));

        TCopyingHold hold(env);
        const auto response = DecommitGroup(env, groupId);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        WaitForDecommitStatus(env, groupId, NKikimrBlobStorage::TGroupDecommitStatus::IN_PROGRESS);
        for (ui32 i = 0; i < 120 && !hold.HasHeld(); ++i) {
            env.Sim(TDuration::Seconds(1));
        }
        UNIT_ASSERT(hold.HasHeld());

        // the data is not copied yet, so it is read from the piles through the BlobDepot agent; restarted nodes start
        // proxies of the group being decommitted from scratch
        checker.CheckAll();
        for (ui32 nodeId : nodesToRestart) {
            Cerr << "*** restarting node " << nodeId << " during decommission" << Endl;
            env.RestartNode(nodeId);
            env.Sim(TDuration::Seconds(30));
            checker.CheckAll(nodeId);
        }

        hold.Release();
        WaitForDecommitStatus(env, groupId, NKikimrBlobStorage::TGroupDecommitStatus::DONE);
        env.Sim(TDuration::Seconds(10));

        const TPoolSnapshot after = MakePoolSnapshot(env);
        UNIT_ASSERT(after.Groups.contains(groupId));
        UNIT_ASSERT(!after.NumVSlots.contains(groupId));
        std::set<ui32> vanished;
        std::ranges::set_difference(before.Groups, after.Groups, std::inserter(vanished, vanished.end()));
        UNIT_ASSERT(std::ranges::includes(before.Groups, after.Groups));
        for (ui32 id : vanished) {
            UNIT_ASSERT(!after.NumVSlots.contains(id)); // VDisks of vanished piles are released too
        }
        for (ui32 id : after.Groups) {
            if (id != groupId) {
                UNIT_ASSERT_VALUES_EQUAL(before.NumVSlots.Value(id, 0), after.NumVSlots.Value(id, 0));
            }
        }

        const auto baseConfig = env.FetchBaseConfig();
        const auto& group = FindGroup(baseConfig, groupId);
        UNIT_ASSERT(!group.GetIsProxyGroup());
        UNIT_ASSERT_VALUES_EQUAL(group.VSlotIdSize(), 0);

        // data is served by the BlobDepot now, including the nodes that were restarted
        checker.CheckAll();
        checker.Write(groupId);
        for (ui32 nodeId : nodesToRestart) {
            checker.Write(groupId, nodeId);
            checker.CheckAll(nodeId);
        }

        // controller has to load the decommitted group correctly
        Cerr << "*** restarting controller node" << Endl;
        env.RestartNode(env.Settings.ControllerNodeId);
        env.Sim(TDuration::Seconds(30));
        UNIT_ASSERT(FindGroup(env.FetchBaseConfig(), groupId).GetVirtualGroupInfo().GetDecommitStatus() ==
            NKikimrBlobStorage::TGroupDecommitStatus::DONE);
        checker.Write(groupId);
        checker.CheckAll();

        return vanished;
    }

}

Y_UNIT_TEST_SUITE(BridgeDecommit) {

    // bridge proxy group with its own pile groups is decommitted as a whole, pile groups vanish along with their VDisks
    Y_UNIT_TEST(DecommitProxyGroup) {
        TEnvironmentSetup env{{
            .NodeCount = 16,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetBridgedLocation,
            .SetupHive = true,
            .SelfManagementConfig = true,
            .NumPiles = 2,
            .AutomaticBootstrap = true,
        }};
        env.CreatePool(2);
        env.Sim(TDuration::Seconds(30));

        const auto groups = env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 2);
        const ui32 groupId = groups.front();

        TDataChecker checker(env);
        for (ui32 i = 0; i < 10; ++i) {
            checker.Write(groupId);
        }
        checker.CheckAll();

        // single pile of the bridged group can't be decommitted
        const auto baseConfig = env.FetchBaseConfig();
        std::optional<ui32> pileGroupId;
        for (const auto *group : GetPoolGroups(baseConfig, 1)) {
            if (std::ranges::find(groups, group->GetGroupId()) == groups.end()) {
                pileGroupId = group->GetGroupId();
                break;
            }
        }
        UNIT_ASSERT(pileGroupId);
        const auto response = DecommitGroup(env, *pileGroupId);
        UNIT_ASSERT(!response.GetSuccess());
        UNIT_ASSERT_STRING_CONTAINS(response.GetErrorDescription(), "pile");

        const auto vanished = RunDecommission(env, checker, groupId, {4, 12});
        UNIT_ASSERT_VALUES_EQUAL(vanished.size(), 2);
    }

    // group converted into an inline pile keeps its id, so it stays in place as the virtual group, but loses its VDisks
    Y_UNIT_TEST(DecommitInlinePile) {
        TEnvironmentSetup env{{
            .NodeCount = 9,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetUnbridgedLocation,
            .SetupHive = true,
            .SelfManagementConfig = true,
            .AutomaticBootstrap = true,
        }};
        env.CreatePool(2);
        env.Sim(TDuration::Seconds(30));

        const auto groups = env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 2);
        const ui32 groupId = groups.front();

        TDataChecker checker(env);
        checker.Write(groupId);
        RollingRestartWithSinglePileBridgeConfig(env, checker, groupId);

        const auto res = ConvertToBridge(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Minutes(1));
        UNIT_ASSERT(!QueryStorageConfig(env).GetClusterStateDetails().GetBscConversionPending());

        for (ui32 i = 0; i < 10; ++i) {
            checker.Write(groupId);
        }
        checker.CheckAll();

        const auto vanished = RunDecommission(env, checker, groupId, {5});
        UNIT_ASSERT(vanished.empty());
    }

}
