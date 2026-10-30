#include "bridge_common.h"

#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blob_depot/blob_depot_tablet.h>
#include <ydb/core/protos/subdomains.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <util/system/type_name.h>

#include <algorithm>
#include <deque>
#include <iterator>

using namespace NBridgeTest;

namespace {

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

    NKikimrBlobStorage::TConfigResponse RecommissionGroup(TEnvironmentSetup& env, ui32 groupId) {
        NKikimrBlobStorage::TConfigRequest request;
        request.AddCommand()->MutableRecommissionGroups()->AddGroupIds(groupId);
        return env.Invoke(request);
    }

    NKikimrBlobStorage::TGroupDecommitStatus::E GetDecommitStatus(TEnvironmentSetup& env, ui32 groupId) {
        return FindGroup(env.FetchBaseConfig(), groupId).GetVirtualGroupInfo().GetDecommitStatus();
    }

    void WaitForDecommitStatus(TEnvironmentSetup& env, ui32 groupId, NKikimrBlobStorage::TGroupDecommitStatus::E expected) {
        WaitUntil(env, [&] { return GetDecommitStatus(env, groupId) == expected; },
            TStringBuilder() << "decommit status " << NKikimrBlobStorage::TGroupDecommitStatus::E_Name(expected));
    }

    // keeps events matching the predicate until released
    class TEventHold {
        TEnvironmentSetup& Env;
        const std::function<bool(IEventHandle&)> Predicate;
        bool Hold = true;
        std::vector<std::pair<ui32, std::unique_ptr<IEventHandle>>> Held;

    public:
        TEventHold(TEnvironmentSetup& env, std::function<bool(IEventHandle&)> predicate)
            : Env(env)
            , Predicate(std::move(predicate))
        {}

        bool Filter(ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (Hold && Predicate(*ev)) {
                Held.emplace_back(nodeId, std::move(ev));
                return false;
            }
            return true;
        }

        bool HasHeld() const {
            return !Held.empty();
        }

        void WaitForHeld() {
            for (ui32 i = 0; i < 120 && Held.empty(); ++i) {
                Env.Sim(TDuration::Seconds(1));
            }
            UNIT_ASSERT(!Held.empty());
        }

        void Release() {
            Hold = false;
            for (auto& [nodeId, ev] : std::exchange(Held, {})) {
                // runtime resolves service id of the recipient at send, and the actor it was resolved to may be gone
                // already, so the event is sent anew
                const TActorId forwardOnNondelivery = ev->GetForwardOnNondeliveryRecipient();
                const TActorId *forward = forwardOnNondelivery ? &forwardOnNondelivery : nullptr;
                Env.Runtime->Send(ev->HasEvent()
                    ? new IEventHandle(ev->Recipient, ev->Sender, ev->ReleaseBase().Release(), ev->Flags, ev->Cookie, forward)
                    // events coming from other nodes are kept serialized
                    : new IEventHandle(ev->Type, ev->Flags, ev->Recipient, ev->Sender, ev->ReleaseChainBuffer(), ev->Cookie,
                        forward), nodeId);
            }
        }
    };

    // runtime has the only filter function, this one combines several filters
    class TFilterChain {
        using TFilter = std::function<bool(ui32, std::unique_ptr<IEventHandle>&)>;

        TEnvironmentSetup& Env;
        std::deque<TFilter> Filters;
        std::deque<TEventHold> Holds;

    public:
        TFilterChain(TEnvironmentSetup& env)
            : Env(env)
        {
            Env.Runtime->FilterFunction = [this](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
                for (auto& filter : Filters) {
                    if (!filter(nodeId, ev)) {
                        return false;
                    }
                }
                return true;
            };
        }

        ~TFilterChain() {
            Env.Runtime->FilterFunction = {};
        }

        void Add(TFilter filter) {
            Filters.push_back(std::move(filter));
        }

        TEventHold& AddHold(std::function<bool(IEventHandle&)> predicate) {
            TEventHold& hold = Holds.emplace_back(Env, std::move(predicate));
            Add([&hold](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) { return hold.Filter(nodeId, ev); });
            return hold;
        }
    };

    // BlobDepot agent forwards assimilation requests to the proxy serving the physical part of the decommitted group,
    // which is the bridge proxy in case of bridge proxy group; remember it, as it is not reachable through service id
    void CaptureBridgeProxyBehindAgent(TFilterChain& chain, std::optional<TActorId>& res) {
        chain.Add([&res](ui32, std::unique_ptr<IEventHandle>& ev) {
            // the agent forwards to the actor id; requests sent through the service id before the agent was started may
            // come to node warden as undelivered ones (runtime resolves service ids at send, so original recipient is used)
            const TActorId recipient = ev->Recipient;
            if (!res && ev->GetTypeRewrite() == TEvBlobStorage::EvAssimilate && !recipient.IsService() &&
                    recipient != MakeBlobStorageNodeWardenID(recipient.NodeId())) {
                res.emplace(recipient);
            }
            return true;
        });
    }

    std::vector<ui32> GetPileGroupIds(TEnvironmentSetup& env, ui32 proxyGroupId) {
        // piles of bridge proxy group get subsequent group ids
        const auto baseConfig = env.FetchBaseConfig();
        UNIT_ASSERT(FindGroup(baseConfig, proxyGroupId).GetIsProxyGroup());
        std::vector<ui32> res;
        for (ui32 i = 0; i < env.Settings.NumPiles; ++i) {
            const auto& group = FindGroup(baseConfig, proxyGroupId + 1 + i);
            UNIT_ASSERT(!group.GetIsProxyGroup());
            UNIT_ASSERT(group.VSlotIdSize());
            res.push_back(group.GetGroupId());
        }
        return res;
    }

    TEnvironmentSetup::TSettings MakeBridgedSettings() {
        return {
            .NodeCount = 16,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetBridgedLocation,
            .SetupHive = true,
            .SelfManagementConfig = true,
            .NumPiles = 2,
            .AutomaticBootstrap = true,
        };
    }

    // create two bridge proxy groups with their piles and write some data to the first one, which is returned
    ui32 PrepareBridgedCluster(TEnvironmentSetup& env, TDataChecker& checker) {
        env.CreatePool(2);
        env.Sim(TDuration::Seconds(30));

        const auto groups = env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 2);
        const ui32 groupId = groups.front();

        for (ui32 i = 0; i < 10; ++i) {
            checker.Write(groupId);
        }
        checker.CheckAll();
        return groupId;
    }

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

    struct TDecommitHooks {
        std::function<void()> InProgress; // called when decommission is in progress, while data still resides in the group
        std::function<void()> Done; // called right after decommission is over
    };

    // decommit the whole bridged group, checking data through restarted nodes while decommission is in progress; returns
    // the groups that vanished during decommission
    std::set<ui32> RunDecommission(TEnvironmentSetup& env, TFilterChain& chain, TDataChecker& checker, ui32 groupId,
            const std::vector<ui32>& nodesToRestart, const TDecommitHooks& hooks = {}) {
        SetupSchemeCacheStub(env);

        const TPoolSnapshot before = MakePoolSnapshot(env);
        UNIT_ASSERT(before.Groups.contains(groupId));

        // keep decommission in IN_PROGRESS state by holding the puts copying assimilated data into BlobDepot channels;
        // blocks and barriers get assimilated meanwhile, so the BlobDepot is ready for agent queries, while the data still
        // resides in the group being decommitted
        TEventHold& hold = chain.AddHold([](IEventHandle& ev) {
            return ev.GetTypeRewrite() == TEvBlobStorage::EvPut &&
                ev.Get<TEvBlobStorage::TEvPut>()->WriteSource == TWriteSource::BlobDepotPut;
        });
        // remember the latest group info reported to nodes
        auto reportedGroupInfo = std::make_shared<TGroupInfoTracker>();
        chain.Add([reportedGroupInfo](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            return reportedGroupInfo->Filter(nodeId, ev);
        });

        const auto response = DecommitGroup(env, groupId);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        WaitForDecommitStatus(env, groupId, NKikimrBlobStorage::TGroupDecommitStatus::IN_PROGRESS);
        hold.WaitForHeld();

        // the data is not copied yet, so it is read from the piles through the BlobDepot agent; restarted nodes start
        // proxies of the group being decommitted from scratch
        checker.CheckAll();
        for (ui32 nodeId : nodesToRestart) {
            Cerr << "*** restarting node " << nodeId << " during decommission" << Endl;
            env.RestartNode(nodeId);
            env.Sim(TDuration::Seconds(30));
            checker.CheckAll(nodeId);
        }
        if (hooks.InProgress) {
            hooks.InProgress();
        }

        hold.Release();
        WaitForDecommitStatus(env, groupId, NKikimrBlobStorage::TGroupDecommitStatus::DONE);
        if (hooks.Done) {
            hooks.Done();
        }
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

        // nodes see it as an ordinary decommitted group, without any bridge part (inline pile loses it too)
        const auto& info = reportedGroupInfo->Get(groupId);
        UNIT_ASSERT_C(info.GetDecommitStatus() == NKikimrBlobStorage::TGroupDecommitStatus::DONE, info.ShortDebugString());
        UNIT_ASSERT_C(!info.HasBridgeGroupState() && !info.HasBridgeProxyGroupId() && !info.HasBridgePileId(),
            info.ShortDebugString());

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
        UNIT_ASSERT(GetDecommitStatus(env, groupId) == NKikimrBlobStorage::TGroupDecommitStatus::DONE);
        checker.Write(groupId);
        checker.CheckAll();

        return vanished;
    }

}

Y_UNIT_TEST_SUITE(BridgeDecommit) {

    // bridge proxy group with its own pile groups is decommitted as a whole, pile groups vanish along with their VDisks
    Y_UNIT_TEST(DecommitProxyGroup) {
        TEnvironmentSetup env(MakeBridgedSettings());
        TDataChecker checker(env);
        const ui32 groupId = PrepareBridgedCluster(env, checker);

        // single pile of the bridged group can't be decommitted
        const auto response = DecommitGroup(env, GetPileGroupIds(env, groupId).front());
        UNIT_ASSERT(!response.GetSuccess());
        UNIT_ASSERT_STRING_CONTAINS(response.GetErrorDescription(), "pile");

        TFilterChain chain(env);
        const auto vanished = RunDecommission(env, chain, checker, groupId, {4, 12});
        UNIT_ASSERT_VALUES_EQUAL(vanished.size(), 2);
    }

    // recommission is not supported in bridge mode: the group in progress keeps its VDisks in piles, and the decommitted
    // one would get them outside of any pile
    Y_UNIT_TEST(RecommissionRefused) {
        TEnvironmentSetup env(MakeBridgedSettings());
        TDataChecker checker(env);
        const ui32 groupId = PrepareBridgedCluster(env, checker);

        auto expectRefused = [&] {
            const auto response = RecommissionGroup(env, groupId);
            UNIT_ASSERT(!response.GetSuccess());
            UNIT_ASSERT_STRING_CONTAINS(response.GetErrorDescription(), "bridge mode");
        };
        TFilterChain chain(env);
        RunDecommission(env, chain, checker, groupId, {}, {.InProgress = expectRefused, .Done = expectRefused});
    }

    // group converted into an inline pile keeps its id, so it stays in place as the virtual group, but loses its VDisks
    Y_UNIT_TEST(DecommitInlinePile) {
        TEnvironmentSetup env(MakeUnbridgedSettings(true));
        TDataChecker checker(env);
        const ui32 groupId = ConvertCluster(env, checker, 2);
        for (ui32 i = 0; i < 10; ++i) {
            checker.Write(groupId);
        }
        checker.CheckAll();

        TFilterChain chain(env);
        const auto vanished = RunDecommission(env, chain, checker, groupId, {5});
        UNIT_ASSERT(vanished.empty());
    }

    // service id of the inline pile being decommitted points to the BlobDepot agent, while requests with forced
    // generation (like the ones of the syncer of another pile) are addressed to the physical group; the agent must not
    // serve them by itself
    Y_UNIT_TEST(ForcedGenerationReachesInlinePile) {
        TEnvironmentSetup env(MakeUnbridgedSettings(true));
        TDataChecker checker(env);
        const ui32 groupId = ConvertCluster(env, checker, 2);
        for (ui32 i = 0; i < 10; ++i) {
            checker.Write(groupId);
        }
        checker.CheckAll();

        // the blob kept by the tablet; Keep flag is known to the physical group only
        const auto [writtenGroupId, id, data] = checker.GetWritten().back();
        UNIT_ASSERT_VALUES_EQUAL(writtenGroupId, groupId);
        {
            const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
            env.Runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvCollectGarbage(id.TabletID(), id.Generation(), 0,
                    id.Channel(), false, 0, 0, new TVector<TLogoBlobID>{id}, nullptr, TInstant::Max(), false));
            });
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvCollectGarbageResult>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL_C(res->Get()->Status, NKikimrProto::OK, res->Get()->ToString());
        }

        TFilterChain chain(env);
        RunDecommission(env, chain, checker, groupId, {}, {.InProgress = [&] {
            auto res = QueryPileGroup<TEvBlobStorage::TEvGetResult>(env, groupId, 1,
                new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(), NKikimrBlobStorage::FastRead));
            UNIT_ASSERT_VALUES_EQUAL_C(res->Status, NKikimrProto::OK, res->ToString());
            UNIT_ASSERT_VALUES_EQUAL(res->ResponseSz, 1);
            UNIT_ASSERT_VALUES_EQUAL_C(res->Responses[0].Status, NKikimrProto::OK, res->ToString());
            UNIT_ASSERT_VALUES_EQUAL(res->Responses[0].Buffer.ConvertToString(), data);
            UNIT_ASSERT_C(res->Responses[0].Keep, res->ToString()); // the agent knows nothing of keep flags

            // the generation fence of the physical group works as well
            const ui32 generation = GetNodeGroupGeneration(env, groupId, 1);
            const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
            env.Runtime->WrapInActorContext(edge, [&] {
                auto *request = new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(), NKikimrBlobStorage::FastRead);
                request->ForceGroupGeneration = generation - 1;
                SendToBSProxy(edge, groupId, request);
            });
            auto stale = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL_C(stale->Get()->Status, NKikimrProto::RACE, stale->Get()->ToString());
        }});
    }

    // piles don't know that their bridge proxy group is being decommitted, so writes not coming from the BlobDepot are
    // blocked by the bridge proxy
    Y_UNIT_TEST(BridgeProxyBlocksWrites) {
        TEnvironmentSetup env(MakeBridgedSettings());
        TDataChecker checker(env);
        const ui32 groupId = PrepareBridgedCluster(env, checker);

        TFilterChain chain(env);
        std::optional<TActorId> bridgeProxyId;
        CaptureBridgeProxyBehindAgent(chain, bridgeProxyId);

        RunDecommission(env, chain, checker, groupId, {}, {.InProgress = [&] {
            UNIT_ASSERT(bridgeProxyId);
            const TActorId edge = env.Runtime->AllocateEdgeActor(bridgeProxyId->NodeId(), __FILE__, __LINE__);
            auto send = [&](IEventBase *ev) {
                env.Runtime->Send(new IEventHandle(*bridgeProxyId, edge, ev), edge.NodeId());
            };
            auto checkBlocked = [&](const auto& res) {
                UNIT_ASSERT_VALUES_EQUAL_C(res->Get()->Status, NKikimrProto::ERROR, res->Get()->ToString());
                UNIT_ASSERT_STRING_CONTAINS(res->Get()->ErrorReason, "decommitted");
            };

            const TString data = "write behind the agent";
            send(new TEvBlobStorage::TEvPut(TLogoBlobID(100501, 1, 1, 0, data.size(), 0), data, TInstant::Max()));
            checkBlocked(env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false));

            send(new TEvBlobStorage::TEvBlock(100501, 2, TInstant::Max()));
            checkBlocked(env.WaitForEdgeActorEvent<TEvBlobStorage::TEvBlockResult>(edge, false));

            send(new TEvBlobStorage::TEvCollectGarbage(100501, 1, 0, 0, true, 1, 1, nullptr, nullptr, TInstant::Max(),
                false));
            checkBlocked(env.WaitForEdgeActorEvent<TEvBlobStorage::TEvCollectGarbageResult>(edge, false));

            // reads are served as usual, the agent needs them
            const auto& [_, id, expected] = checker.GetWritten().front();
            send(new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(), NKikimrBlobStorage::FastRead));
            CheckGetResult(*env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false)->Get(), expected);
        }});
    }

    // request of the agent racing with the pile proxy, which hasn't got the new configuration yet, is restarted by the
    // bridge proxy itself rather than through the service id of the group, which belongs to the agent
    Y_UNIT_TEST(RaceRestartedInBridgeProxy) {
        TEnvironmentSetup env(MakeBridgedSettings());
        TDataChecker checker(env);
        const ui32 groupId = PrepareBridgedCluster(env, checker);
        const auto pileGroupIds = GetPileGroupIds(env, groupId);

        TFilterChain chain(env);
        std::optional<TActorId> bridgeProxyId;
        CaptureBridgeProxyBehindAgent(chain, bridgeProxyId);

        RunDecommission(env, chain, checker, groupId, {}, {.InProgress = [&] {
            UNIT_ASSERT(bridgeProxyId);
            const TActorId bridgeProxy = *bridgeProxyId;
            const ui32 nodeId = bridgeProxy.NodeId();
            const TActorId edge = env.Runtime->AllocateEdgeActor(nodeId, __FILE__, __LINE__);
            const TLogoBlobID id = std::get<1>(checker.GetWritten().back());
            const TString expected = std::get<2>(checker.GetWritten().back());

            // the filter outlives this function, so its state is shared
            struct TRaceState {
                bool GetSent = false;
                bool RaceSeen = false;
                std::vector<std::unique_ptr<IEventHandle>> HeldConfigs;
            };
            auto state = std::make_shared<TRaceState>();
            chain.Add([&env, state, pileGroupIds, bridgeProxy, nodeId, edge, id](ui32 eventNodeId,
                    std::unique_ptr<IEventHandle>& ev) {
                if (state->RaceSeen) {
                    return true;
                } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvConfigureProxy) {
                    const auto& info = ev->Get<TEvBlobStorage::TEvConfigureProxy>()->Info;
                    if (eventNodeId == nodeId && info && std::ranges::count(pileGroupIds, info->GroupID.GetRawId())) {
                        state->HeldConfigs.push_back(std::move(ev)); // pile proxies lag behind
                        return false;
                    } else if (ev->GetRecipientRewrite() == bridgeProxy && info && !state->GetSent) {
                        // bridge proxy gets new configuration, so the request issued right after it races
                        state->GetSent = true;
                        env.Runtime->Send(new IEventHandle(bridgeProxy, edge, new TEvBlobStorage::TEvGet(id, 0, 0,
                            TInstant::Max(), NKikimrBlobStorage::FastRead)), nodeId);
                    }
                } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvGetResult && state->GetSent &&
                        ev->GetRecipientRewrite() == bridgeProxy && ev->HasEvent()) {
                    const auto& msg = *ev->Get<TEvBlobStorage::TEvGetResult>();
                    if (msg.Status == NKikimrProto::RACE && msg.ResponseSz == 1 && msg.Responses[0].Id == id) {
                        state->RaceSeen = true;
                        for (auto& config : std::exchange(state->HeldConfigs, {})) {
                            env.Runtime->Send(config.release(), nodeId);
                        }
                    }
                }
                return true;
            });

            // pile state change spins generation of the whole bridged group
            SwitchPileState(env, 1, NKikimrBridge::TClusterState::DISCONNECTED);

            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false);
            UNIT_ASSERT(state->GetSent);
            UNIT_ASSERT(state->RaceSeen);
            CheckGetResult(*res->Get(), expected);
            // the agent would answer itself, if it got the restarted request
            UNIT_ASSERT_VALUES_EQUAL(res->Sender, bridgeProxy);
        }});
    }

    // pile of the group being decommitted gets synced as usual: piles don't block writes, so syncers proceed
    Y_UNIT_TEST(PileSyncDuringDecommit) {
        TEnvironmentSetup env(MakeBridgedSettings());
        TDataChecker checker(env);
        const ui32 groupId = PrepareBridgedCluster(env, checker);

        // let the second pile miss some data
        SwitchPileState(env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        env.Sim(TDuration::Seconds(10));
        for (ui32 i = 0; i < 10; ++i) {
            checker.Write(groupId);
        }

        TFilterChain chain(env);
        RunDecommission(env, chain, checker, groupId, {}, {.InProgress = [&] {
            SwitchPileState(env, 1, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
            WaitForPileState(env, 1, NKikimrBridge::TClusterState::SYNCHRONIZED);
            UNIT_ASSERT(GetDecommitStatus(env, groupId) == NKikimrBlobStorage::TGroupDecommitStatus::IN_PROGRESS);
            checker.CheckAll(9);
        }});
    }

    // decommission is over while the pile is still being synced; the pile becomes synchronized, as there is nothing to sync
    // for the decommitted group anymore
    Y_UNIT_TEST(DoneWhilePileUnsynced) {
        TEnvironmentSetup env(MakeBridgedSettings());
        TDataChecker checker(env);
        const ui32 groupId = PrepareBridgedCluster(env, checker);
        const auto pileGroupIds = GetPileGroupIds(env, groupId);

        SwitchPileState(env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        env.Sim(TDuration::Seconds(10));
        for (ui32 i = 0; i < 10; ++i) {
            checker.Write(groupId);
        }

        TFilterChain chain(env);
        // stop syncing piles of the group being decommitted, while the other group gets synced
        TEventHold& syncerHold = chain.AddHold([&](IEventHandle& ev) {
            if (ev.GetTypeRewrite() != TEvBlobStorage::EvAssimilate || !ev.Get<TEvBlobStorage::TEvAssimilate>()->Reverse) {
                return false;
            }
            // recipient rewrite is resolved already, so the original recipient is checked
            return std::ranges::any_of(pileGroupIds, [&](ui32 id) { return ev.Recipient == MakeBlobStorageProxyID(id); });
        });

        RunDecommission(env, chain, checker, groupId, {}, {
            .InProgress = [&] {
                SwitchPileState(env, 1, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
                syncerHold.WaitForHeld();
                env.Sim(TDuration::Minutes(1)); // the other group gets synced meanwhile
                UNIT_ASSERT(GetPileState(env, 1) != NKikimrBridge::TClusterState::SYNCHRONIZED);
            },
            .Done = [&] {
                syncerHold.Release();
                WaitForPileState(env, 1, NKikimrBridge::TClusterState::SYNCHRONIZED);
            },
        });
        checker.CheckAll(9);
    }

    // ordinary group of not bridged cluster goes the same way through decommission
    Y_UNIT_TEST(DecommitUnbridgedGroup) {
        TEnvironmentSetup env(MakeUnbridgedSettings(true));
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

        TFilterChain chain(env);
        const auto vanished = RunDecommission(env, chain, checker, groupId, {5});
        UNIT_ASSERT(vanished.empty());
    }

    // requests the bridge proxy holds when the group loses its piles are answered with error: the agent stops the proxy
    // right after the group is decommitted, and a request either still waits for pile response or waits for the next
    // generation, when the pile proxy has got newer configuration than the bridge proxy
    Y_UNIT_TEST(RequestsAnsweredWhenDone) {
        TEnvironmentSetup env(MakeBridgedSettings());
        TDataChecker checker(env);
        const ui32 groupId = PrepareBridgedCluster(env, checker);

        TFilterChain chain(env);
        std::optional<TActorId> bridgeProxyId;
        CaptureBridgeProxyBehindAgent(chain, bridgeProxyId);

        const TLogoBlobID inFlightId = std::get<1>(checker.GetWritten()[0]);
        const TLogoBlobID pendingId = std::get<1>(checker.GetWritten()[1]);
        std::optional<TActorId> edge; // both requests are sent from it, it has to be waiting for any response it gets
        TEventHold *raceHold = nullptr;
        TEventHold *doneHold = nullptr;

        RunDecommission(env, chain, checker, groupId, {}, {
            .InProgress = [&] {
                UNIT_ASSERT(bridgeProxyId);
                const TActorId bridgeProxy = *bridgeProxyId;
                const ui32 nodeId = bridgeProxy.NodeId();
                edge.emplace(env.Runtime->AllocateEdgeActor(nodeId, __FILE__, __LINE__));

                auto send = [&](const TLogoBlobID& id) {
                    env.Runtime->Send(new IEventHandle(bridgeProxy, *edge, new TEvBlobStorage::TEvGet(id, 0, 0,
                        TInstant::Max(), NKikimrBlobStorage::FastRead)), nodeId);
                };
                auto isResultFor = [bridgeProxy](IEventHandle& ev, const TLogoBlobID& id) {
                    // results from pile proxies of the same node are local events
                    if (ev.GetTypeRewrite() != TEvBlobStorage::EvGetResult || ev.GetRecipientRewrite() != bridgeProxy ||
                            !ev.HasEvent()) {
                        return false;
                    }
                    const auto& msg = *ev.Get<TEvBlobStorage::TEvGetResult>();
                    return msg.ResponseSz == 1 && msg.Responses[0].Id == id;
                };

                // the first request never gets responses from piles
                TEventHold& pileResultHold = chain.AddHold([=](IEventHandle& ev) { return isResultFor(ev, inFlightId); });
                send(inFlightId);
                pileResultHold.WaitForHeld();

                // the second one races with the pile proxy, which gets newer configuration (spun by pile state change)
                // than the bridge proxy has, so it is going to wait for the next generation; this expires in 2 seconds,
                // so the race result is held until the bridge proxy is about to be stopped
                chain.AddHold([=](IEventHandle& ev) {
                    return ev.GetTypeRewrite() == TEvBlobStorage::EvConfigureProxy && ev.GetRecipientRewrite() == bridgeProxy;
                });
                raceHold = &chain.AddHold([=](IEventHandle& ev) {
                    return isResultFor(ev, pendingId) && ev.Get<TEvBlobStorage::TEvGetResult>()->Status == NKikimrProto::RACE;
                });
                SwitchPileState(env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
                env.Sim(TDuration::Seconds(5));
                send(pendingId);
                raceHold->WaitForHeld();

                // final configuration of the agent, which makes it stop the bridge proxy, is held too
                doneHold = &chain.AddHold([=](IEventHandle& ev) {
                    if (ev.GetTypeRewrite() != TEvBlobStorage::EvConfigureProxy || ev.GetRecipientRewrite() == bridgeProxy ||
                            !ev.HasEvent()) {
                        return false;
                    }
                    const auto& info = ev.Get<TEvBlobStorage::TEvConfigureProxy>()->Info;
                    return info && info->GroupID.GetRawId() == groupId &&
                        info->DecommitStatus == NKikimrBlobStorage::TGroupDecommitStatus::DONE;
                });
            },
            .Done = [&] {
                doneHold->WaitForHeld();

                // now the second request waits for the next generation, and the bridge proxy gets stopped right away
                raceHold->Release();
                doneHold->Release();

                std::set<TLogoBlobID> answered;
                for (ui32 i = 0; i < 2; ++i) {
                    auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(*edge, false,
                        env.Runtime->GetClock() + TDuration::Minutes(1));
                    UNIT_ASSERT_C(res, "no response from stopped bridge proxy");
                    const auto& msg = *res->Get();
                    UNIT_ASSERT_VALUES_EQUAL_C(msg.Status, NKikimrProto::ERROR, msg.ToString());
                    UNIT_ASSERT_STRING_CONTAINS(msg.ErrorReason, "stopped");
                    UNIT_ASSERT_VALUES_EQUAL(msg.ResponseSz, 1);
                    answered.insert(msg.Responses[0].Id);
                }
                UNIT_ASSERT(answered == std::set<TLogoBlobID>({inFlightId, pendingId}));
            },
        });
    }

    // BlobDepot retries notification of decommission finish with a delay when BS_CONTROLLER fails to process it
    Y_UNIT_TEST(DecommittedNotifyRetried) {
        TEnvironmentSetup env(MakeBridgedSettings());
        TDataChecker checker(env);
        const ui32 groupId = PrepareBridgedCluster(env, checker);

        TFilterChain chain(env);
        constexpr ui32 numErrors = 3;
        struct TNotifyState {
            std::vector<TInstant> NotifyTimestamps;
            ui32 NumErrorsInjected = 0;
        };
        auto state = std::make_shared<TNotifyState>();
        chain.Add([&env, state](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvControllerGroupDecommittedNotify) {
                state->NotifyTimestamps.push_back(env.Runtime->GetClock());
            } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvControllerGroupDecommittedResponse &&
                    state->NumErrorsInjected < numErrors) {
                ev->Get<TEvBlobStorage::TEvControllerGroupDecommittedResponse>()->Record.SetStatus(NKikimrProto::ERROR);
                ++state->NumErrorsInjected;
            }
            return true;
        });

        // BS_CONTROLLER finishes decommission on the first notification, while BlobDepot keeps retrying; wait for the retries
        // to finish before the controller restart
        RunDecommission(env, chain, checker, groupId, {}, {.Done = [&] {
            WaitUntil(env, [&] { return state->NotifyTimestamps.size() > numErrors; }, "notification retries");
            env.Sim(TDuration::Seconds(10));
        }});
        UNIT_ASSERT_VALUES_EQUAL(state->NumErrorsInjected, numErrors);
        const auto& timestamps = state->NotifyTimestamps;
        UNIT_ASSERT_VALUES_EQUAL(timestamps.size(), numErrors + 1);
        for (size_t i = 1; i < timestamps.size(); ++i) {
            UNIT_ASSERT_GE_C(timestamps[i] - timestamps[i - 1], TDuration::Seconds(5), "notify# " << i);
        }
    }

    // the copy of the decommitted group's blob into BlobDepot fails; it never gets bound to the key, and the retry writes
    // another blob, so the failed one must not hold BlobDepot garbage collection back
    Y_UNIT_TEST(FailedCopyReleasesGarbageCollection) {
        TEnvironmentSetup env(MakeUnbridgedSettings(true));
        env.CreatePool(2);
        env.Sim(TDuration::Seconds(30));
        const ui32 groupId = env.GetGroups().front();

        TDataChecker checker(env);
        for (ui32 i = 0; i < 10; ++i) {
            checker.Write(groupId);
        }

        TFilterChain chain(env);
        bool failed = false;
        TActorId blobDepotId;
        chain.Add([&](ui32, std::unique_ptr<IEventHandle>& ev) {
            IActor *actor = env.Runtime->GetActor(ev->GetRecipientRewrite());
            if (!actor) {
                return true;
            } else if (TypeName(*actor).EndsWith("::TBlobDepot")) {
                blobDepotId = ev->GetRecipientRewrite();
            } else if (!failed && ev->GetTypeRewrite() == TEvBlobStorage::EvPutResult && ev->HasEvent() &&
                    TypeName(*actor).EndsWith("::TGroupAssimilator")) {
                ev->Get<TEvBlobStorage::TEvPutResult>()->Status = NKikimrProto::ERROR;
                failed = true;
            }
            return true;
        });
        RunDecommission(env, chain, checker, groupId, {});
        UNIT_ASSERT(failed);
        UNIT_ASSERT(blobDepotId);

        size_t pinned = 0;
        UNIT_ASSERT(env.Runtime->WrapInActorContext(blobDepotId, [&](IActor *actor) {
            auto *blobDepot = dynamic_cast<NBlobDepot::TBlobDepot*>(actor);
            UNIT_ASSERT(blobDepot);
            for (const auto& channel : blobDepot->Channels) {
                pinned += channel.AssimilatedBlobsInFlight.size();
            }
        }));
        UNIT_ASSERT_VALUES_EQUAL(pinned, 0);
    }

    // BlobDepot restarts while blobs of the decommitted group are being assimilated; it goes on from where it was, rather
    // than from the beginning of the group, and it knows which blobs are indexed already, so a read of the key indexed as
    // absent doesn't go to the group
    Y_UNIT_TEST(AssimilationResumedAfterRestart) {
        TEnvironmentSetup env(MakeUnbridgedSettings(true));
        env.CreatePool(2);
        env.Sim(TDuration::Seconds(30));
        const ui32 groupId = env.GetGroups().front();
        SetupSchemeCacheStub(env);

        const TString data = "blob";
        for (ui32 step : {1, 2, 4, 5}) {
            const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
            env.Runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvPut(TLogoBlobID(100500, 1, step, 0, data.size(), 0),
                    data, TInstant::Max()));
            });
            UNIT_ASSERT_VALUES_EQUAL(env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false)->Get()->Status,
                NKikimrProto::OK);
        }

        // the assimilator gets the page with the blobs, and nothing more, so the blobs never get finished
        bool blobsAssimilated = false;
        bool restarted = false;
        std::optional<std::optional<TLogoBlobID>> resumedFrom; // SkipBlobsUpTo of the first request after restart
        ui32 sourceReads = 0;
        TActorId blobDepotId;
        TFilterChain chain(env);
        chain.Add([&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (!ev->HasEvent()) {
                return true;
            }
            const auto typeOf = [&](const TActorId& actorId) {
                IActor *actor = env.Runtime->GetActor(actorId);
                return actor ? TypeName(*actor) : TString();
            };
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvAssimilateResult &&
                    typeOf(ev->GetRecipientRewrite()).EndsWith("::TGroupAssimilator")) {
                if (blobsAssimilated) {
                    return false;
                }
                blobsAssimilated = !ev->Get<TEvBlobStorage::TEvAssimilateResult>()->Blobs.empty();
            } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvAssimilate && typeOf(ev->Sender).EndsWith("::TGroupAssimilator")) {
                if (restarted && !resumedFrom) {
                    resumedFrom.emplace(ev->Get<TEvBlobStorage::TEvAssimilate>()->SkipBlobsUpTo);
                }
            } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvGet && typeOf(ev->Sender).Contains("TResolveDecommitActor")) {
                ++sourceReads;
            } else if (typeOf(ev->GetRecipientRewrite()).EndsWith("::TBlobDepot")) {
                blobDepotId = ev->GetRecipientRewrite();
            }
            return true;
        });

        const auto response = DecommitGroup(env, groupId);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        WaitUntil(env, [&] { return blobsAssimilated; }, "blobs assimilated", TDuration::Seconds(1), 120);
        env.Sim(TDuration::Seconds(5)); // let the assimilation state be stored
        UNIT_ASSERT(blobDepotId);

        const ui32 blobDepotNodeId = blobDepotId.NodeId();
        restarted = true;
        env.RestartNode(blobDepotNodeId);
        WaitUntil(env, [&] { return resumedFrom.has_value(); }, "assimilation resumed", TDuration::Seconds(1), 120);
        UNIT_ASSERT(*resumedFrom);
        UNIT_ASSERT_VALUES_EQUAL(**resumedFrom, TLogoBlobID(100500, 1, 5, 0, data.size(), 0));

        const ui32 readerNodeId = blobDepotNodeId == 1 ? 2 : 1;
        const TActorId edge = env.Runtime->AllocateEdgeActor(readerNodeId, __FILE__, __LINE__);
        sourceReads = 0;
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvGet(TLogoBlobID(100500, 1, 3, 0, data.size(), 0), 0, 0,
                TInstant::Max(), NKikimrBlobStorage::FastRead));
        });
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false);
        UNIT_ASSERT_VALUES_EQUAL_C(res->Get()->Status, NKikimrProto::OK, res->Get()->ToString());
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Responses[0].Status, NKikimrProto::NODATA);
        UNIT_ASSERT_VALUES_EQUAL(sourceReads, 0);
    }

    // reads of the group being decommitted fail for a while; the assimilator retries them with a backoff instead of
    // rescanning its data in a loop, and copies the blobs once reads succeed again
    Y_UNIT_TEST(FailedReadsRetriedWithBackoff) {
        TEnvironmentSetup env(MakeUnbridgedSettings(true));
        env.CreatePool(2);
        env.Sim(TDuration::Seconds(30));
        const ui32 groupId = env.GetGroups().front();
        SetupSchemeCacheStub(env);

        TDataChecker checker(env);
        for (ui32 i = 0; i < 10; ++i) {
            checker.Write(groupId);
        }

        bool failReads = true;
        ui32 numReads = 0;
        TFilterChain chain(env);
        chain.Add([&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (!ev->HasEvent()) {
                return true;
            }
            const auto typeOf = [&](const TActorId& actorId) {
                IActor *actor = env.Runtime->GetActor(actorId);
                return actor ? TypeName(*actor) : TString();
            };
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvGet && typeOf(ev->Sender).EndsWith("::TGroupAssimilator")) {
                if (failReads && ++numReads == 1000) {
                    failReads = false; // no backoff, don't spin the test forever
                }
            } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvGetResult && failReads &&
                    typeOf(ev->GetRecipientRewrite()).EndsWith("::TGroupAssimilator")) {
                auto *msg = ev->Get<TEvBlobStorage::TEvGetResult>();
                for (ui32 i = 0; i < msg->ResponseSz; ++i) {
                    msg->Responses[i].Status = NKikimrProto::ERROR;
                }
            }
            return true;
        });

        const auto response = DecommitGroup(env, groupId);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        WaitUntil(env, [&] { return numReads != 0; }, "assimilator reads", TDuration::Seconds(1), 120);
        env.Sim(TDuration::Seconds(10));
        UNIT_ASSERT(failReads);
        UNIT_ASSERT_LE_C(numReads, 100, numReads);
        UNIT_ASSERT(GetDecommitStatus(env, groupId) == NKikimrBlobStorage::TGroupDecommitStatus::IN_PROGRESS);

        failReads = false;
        WaitForDecommitStatus(env, groupId, NKikimrBlobStorage::TGroupDecommitStatus::DONE);
        checker.CheckAll();
    }
}
