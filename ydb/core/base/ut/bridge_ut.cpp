#include <ydb/core/base/bridge.h>
#include <ydb/core/protos/config.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/random/fast.h>
#include <util/string/builder.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(BridgePileRegistry) {

    NKikimrBridge::TClusterState MakeClusterState(size_t numPiles) {
        NKikimrBridge::TClusterState state;
        for (size_t i = 0; i < numPiles; ++i) {
            state.AddPerPileState(NKikimrBridge::TClusterState::SYNCHRONIZED);
        }
        return state;
    }

    NKikimrConfig::TBridgeConfig MakeBridgeConfig(const TVector<TString>& names) {
        NKikimrConfig::TBridgeConfig config;
        for (const TString& name : names) {
            config.AddPiles()->SetName(name);
        }
        return config;
    }

    TString Slots(const NKikimrBridge::TPileRegistry& registry) {
        TStringBuilder res;
        for (const auto& pile : registry.GetPiles()) {
            res << (res.empty() ? "" : " ") << (!pile.HasName() ? "-" : pile.GetName().empty() ? "''" : pile.GetName());
        }
        return res;
    }

    Y_UNIT_TEST(ImpliedRegistry) {
        const auto single = NBridge::MakeImpliedPileRegistry(MakeBridgeConfig({"a"}));
        UNIT_ASSERT_VALUES_EQUAL(single.PilesSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(single.GetPiles(0).GetName(), "a");
        UNIT_ASSERT_EQUAL(TBridgePileId::FromProto(&single, &NKikimrBridge::TPileRegistry::GetImplicitPileId),
            TBridgePileId::FromPileIndex(0));
        UNIT_ASSERT_EQUAL(NBridge::ResolvePileId(single, std::nullopt), TBridgePileId::FromPileIndex(0));
        UNIT_ASSERT_EQUAL(NBridge::ResolvePileId(single, "a"), TBridgePileId::FromPileIndex(0));
        UNIT_ASSERT(!NBridge::ResolvePileId(single, "b"));

        const auto multi = NBridge::MakeImpliedPileRegistry(MakeBridgeConfig({"a", "b", "c"}));
        UNIT_ASSERT_VALUES_EQUAL(multi.PilesSize(), 3);
        UNIT_ASSERT(!multi.GetImplicitPileId());
        UNIT_ASSERT(!NBridge::ResolvePileId(multi, std::nullopt));
        UNIT_ASSERT_EQUAL(NBridge::ResolvePileId(multi, "a"), TBridgePileId::FromPileIndex(0));
        UNIT_ASSERT_EQUAL(NBridge::ResolvePileId(multi, "c"), TBridgePileId::FromPileIndex(2));

        // implied registry resolves piles exactly as positional resolution against bridge config does
        const auto config = MakeBridgeConfig({"a", "b", "c"});
        for (const std::optional<TString>& name : {std::optional<TString>(), std::optional<TString>("b"),
                std::optional<TString>("x")}) {
            const auto index = NBridge::ResolvePileIndex(config, name);
            const auto id = NBridge::ResolvePileId(multi, name);
            UNIT_ASSERT_VALUES_EQUAL(index.has_value(), static_cast<bool>(id));
            if (index) {
                UNIT_ASSERT_EQUAL(TBridgePileId::FromPileIndex(*index), id);
            }
        }
    }

    Y_UNIT_TEST(Validation) {
        NKikimrBridge::TPileRegistry registry;
        registry.AddPiles()->SetName("a");
        registry.AddPiles()->SetName("b");
        UNIT_ASSERT(!NBridge::ValidatePileRegistry(registry, MakeClusterState(2)));
        UNIT_ASSERT(NBridge::ValidatePileRegistry(registry, MakeClusterState(1)));

        auto duplicate = registry;
        duplicate.MutablePiles(1)->SetName("a");
        UNIT_ASSERT(NBridge::ValidatePileRegistry(duplicate, MakeClusterState(2)));

        // free slot can't be the last one, the primary, promoted or implicit pile, and it is INACTIVE
        auto withFree = registry;
        withFree.MutablePiles(1)->ClearName();
        auto state = MakeClusterState(2);
        state.SetPerPileState(1, NKikimrBridge::TClusterState::INACTIVE);
        UNIT_ASSERT(NBridge::ValidatePileRegistry(withFree, state));
        withFree.AddPiles()->SetName("c");
        state.AddPerPileState(NKikimrBridge::TClusterState::SYNCHRONIZED);
        UNIT_ASSERT(!NBridge::ValidatePileRegistry(withFree, state));
        UNIT_ASSERT(!NBridge::ResolvePileId(withFree, ""));
        for (const auto pileState : {NKikimrBridge::TClusterState::SYNCHRONIZED,
                NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1, NKikimrBridge::TClusterState::SUSPENDED,
                NKikimrBridge::TClusterState::DISCONNECTED}) {
            auto wrong = state;
            wrong.SetPerPileState(1, pileState);
            UNIT_ASSERT(NBridge::ValidatePileRegistry(withFree, wrong));
        }
        auto promoted = state;
        TBridgePileId::FromPileIndex(1).CopyToProto(&promoted, &NKikimrBridge::TClusterState::SetPromotedPile);
        UNIT_ASSERT(NBridge::ValidatePileRegistry(withFree, promoted));
        auto implicitFree = withFree;
        TBridgePileId::FromPileIndex(1).CopyToProto(&implicitFree, &NKikimrBridge::TPileRegistry::SetImplicitPileId);
        UNIT_ASSERT(NBridge::ValidatePileRegistry(implicitFree, state));
        auto duplicateWithFree = withFree;
        duplicateWithFree.MutablePiles(2)->SetName("a");
        UNIT_ASSERT(NBridge::ValidatePileRegistry(duplicateWithFree, state));

        auto implicit = registry;
        TBridgePileId::FromPileIndex(1).CopyToProto(&implicit, &NKikimrBridge::TPileRegistry::SetImplicitPileId);
        UNIT_ASSERT(!NBridge::ValidatePileRegistry(implicit, MakeClusterState(2)));
        UNIT_ASSERT_EQUAL(NBridge::ResolvePileId(implicit, std::nullopt), TBridgePileId::FromPileIndex(1));
        TBridgePileId::FromPileIndex(2).CopyToProto(&implicit, &NKikimrBridge::TPileRegistry::SetImplicitPileId);
        UNIT_ASSERT(NBridge::ValidatePileRegistry(implicit, MakeClusterState(2)));

        // nodes of the implicit pile must be able to connect
        TBridgePileId::FromPileIndex(1).CopyToProto(&implicit, &NKikimrBridge::TPileRegistry::SetImplicitPileId);
        auto inactive = MakeClusterState(2);
        inactive.SetPerPileState(1, NKikimrBridge::TClusterState::INACTIVE);
        UNIT_ASSERT(NBridge::ValidatePileRegistry(implicit, inactive));
    }

    TString States(const NKikimrBridge::TClusterState& state) {
        TStringBuilder res;
        for (const auto pileState : state.GetPerPileState()) {
            res << (res.empty() ? "" : " ") << NKikimrBridge::TClusterState::EPileState_Name(pileState);
        }
        return res;
    }

    Y_UNIT_TEST(ApplyPileRegistryChange) {
        const auto registry = NBridge::MakeImpliedPileRegistry(MakeBridgeConfig({"a", "b"}));
        auto apply = [&](const NKikimrBridge::TPileRegistry& current, NKikimrBridge::TClusterState *state,
                const TVector<TString>& names) {
            auto next = NBridge::AssignPileIds(current, MakeBridgeConfig(names));
            auto error = NBridge::ApplyPileRegistryChange(current, next, state);
            if (!error) {
                UNIT_ASSERT(!NBridge::ValidatePileRegistry(next, *state));
            }
            return std::make_pair(next, error);
        };

        // new piles are INACTIVE, piles keep their states wherever they are listed
        auto state = MakeClusterState(2);
        state.SetPerPileState(1, NKikimrBridge::TClusterState::DISCONNECTED);
        auto [added, error] = apply(registry, &state, {"c", "b", "a", "d"});
        UNIT_ASSERT_C(!error, *error);
        UNIT_ASSERT_VALUES_EQUAL(Slots(added), "a b c d");
        UNIT_ASSERT_VALUES_EQUAL(States(state), "SYNCHRONIZED DISCONNECTED INACTIVE INACTIVE");

        // only INACTIVE piles can be dropped, the one in other state can't be, even being disconnected
        for (const TVector<TString>& names : {TVector<TString>{"a", "c", "d"}, TVector<TString>{"b", "c", "d"},
                TVector<TString>{"a", "x", "c", "d"}}) {
            auto copy = state;
            const auto [next, error] = apply(added, &copy, names);
            UNIT_ASSERT(error);
            UNIT_ASSERT_STRING_CONTAINS(*error, "can't be removed from BridgeConfig, only INACTIVE piles can");
        }

        // dropped INACTIVE pile leaves INACTIVE free slot, which the next pile takes being INACTIVE too
        auto [dropped, error1] = apply(added, &state, {"a", "b", "d"});
        UNIT_ASSERT_C(!error1, *error1);
        UNIT_ASSERT_VALUES_EQUAL(Slots(dropped), "a b - d");
        UNIT_ASSERT_VALUES_EQUAL(States(state), "SYNCHRONIZED DISCONNECTED INACTIVE INACTIVE");
        auto [reused, error2] = apply(dropped, &state, {"a", "b", "d", "e"});
        UNIT_ASSERT_C(!error2, *error2);
        UNIT_ASSERT_VALUES_EQUAL(Slots(reused), "a b e d");
        UNIT_ASSERT_VALUES_EQUAL(States(state), "SYNCHRONIZED DISCONNECTED INACTIVE INACTIVE");

        // free slots at the end are trimmed along with their states
        auto [trimmed, error3] = apply(reused, &state, {"b", "a"});
        UNIT_ASSERT_C(!error3, *error3);
        UNIT_ASSERT_VALUES_EQUAL(Slots(trimmed), "a b");
        UNIT_ASSERT_VALUES_EQUAL(States(state), "SYNCHRONIZED DISCONNECTED");

        // registry and cluster state must match
        auto wrong = MakeClusterState(3);
        UNIT_ASSERT(NBridge::ApplyPileRegistryChange(registry, registry, &wrong));
    }

    // the same pile added and dropped many times keeps the registry and cluster state of the same size
    Y_UNIT_TEST(ApplyPileRegistryChangeCycles) {
        auto registry = NBridge::MakeImpliedPileRegistry(MakeBridgeConfig({"a", "b"}));
        auto state = MakeClusterState(2);
        for (ui32 i = 0; i < 100; ++i) {
            const TString name = TStringBuilder() << "p" << i % 3;
            for (const TVector<TString>& names : {TVector<TString>{"a", name, "b"}, TVector<TString>{"b", "a"}}) {
                auto next = NBridge::AssignPileIds(registry, MakeBridgeConfig(names));
                const auto error = NBridge::ApplyPileRegistryChange(registry, next, &state);
                UNIT_ASSERT_C(!error, *error);
                registry = std::move(next);
                UNIT_ASSERT(!NBridge::ValidatePileRegistry(registry, state));
                UNIT_ASSERT_LE(state.PerPileStateSize(), 3);
            }
            UNIT_ASSERT_VALUES_EQUAL(States(state), "SYNCHRONIZED SYNCHRONIZED");
        }
    }

    Y_UNIT_TEST(InactivePileTraits) {
        const auto traits = NBridge::PileStateTraits(NKikimrBridge::TClusterState::INACTIVE);
        UNIT_ASSERT(!traits.RequiresDataQuorum);
        UNIT_ASSERT(!traits.RequiresConfigQuorum);
        UNIT_ASSERT(!traits.AllowsConnection);
    }

    // bridged group has no part in absent piles and in the piles beyond the end of its list
    Y_UNIT_TEST(GroupPiles) {
        NKikimrBridge::TGroupState state;
        for (ui32 groupId : {10u, 0u, 12u}) {
            auto *pile = state.AddPile();
            pile->SetGroupId(groupId);
        }
        state.MutablePile(1)->SetAbsent(true);

        UNIT_ASSERT(!NBridge::GetGroupPile(state, TBridgePileId()));
        UNIT_ASSERT_VALUES_EQUAL(NBridge::GetGroupPile(state, TBridgePileId::FromPileIndex(0))->GetGroupId(), 10);
        UNIT_ASSERT(!NBridge::GetGroupPile(state, TBridgePileId::FromPileIndex(1)));
        UNIT_ASSERT_VALUES_EQUAL(NBridge::GetGroupPile(state, TBridgePileId::FromPileIndex(2))->GetGroupId(), 12);
        UNIT_ASSERT(!NBridge::GetGroupPile(state, TBridgePileId::FromPileIndex(3)));
        NBridge::GetGroupPile(&state, TBridgePileId::FromPileIndex(2))->SetGroupGeneration(5);
        UNIT_ASSERT_VALUES_EQUAL(state.GetPile(2).GetGroupGeneration(), 5);

        TStringBuilder visited;
        NBridge::ForEachGroupPile(state, [&](TBridgePileId bridgePileId,
                const NKikimrBridge::TGroupState::TPile& pile) {
            visited << bridgePileId.GetPileIndex() << ':' << pile.GetGroupId() << ' ';
        });
        UNIT_ASSERT_VALUES_EQUAL(visited, "0:10 2:12 ");
        NBridge::ForEachGroupPile(&state, [](TBridgePileId, NKikimrBridge::TGroupState::TPile& pile) {
            pile.SetGroupGeneration(7);
        });
        UNIT_ASSERT_VALUES_EQUAL(state.GetPile(0).GetGroupGeneration(), 7);
        UNIT_ASSERT_VALUES_EQUAL(state.GetPile(1).GetGroupGeneration(), 0);
        UNIT_ASSERT_VALUES_EQUAL(state.GetPile(2).GetGroupGeneration(), 7);
    }

    // removed part leaves a hole, absent parts at the end are trimmed; adding and removing parts keeps the list bounded
    Y_UNIT_TEST(RemoveGroupPile) {
        NKikimrBridge::TGroupState state;
        for (ui32 groupId : {10u, 11u, 12u}) {
            state.AddPile()->SetGroupId(groupId);
        }
        auto piles = [&] {
            TStringBuilder res;
            for (const auto& pile : state.GetPile()) {
                res << (pile.GetAbsent() ? TString("-") : ToString(pile.GetGroupId())) << ' ';
            }
            return res;
        };

        NBridge::RemoveGroupPile(&state, TBridgePileId::FromPileIndex(1));
        UNIT_ASSERT_VALUES_EQUAL(piles(), "10 - 12 ");
        NBridge::RemoveGroupPile(&state, TBridgePileId::FromPileIndex(2));
        UNIT_ASSERT_VALUES_EQUAL(piles(), "10 ");
        for (ui32 i = 0; i < 3; ++i) {
            NBridge::AddGroupPile(&state, TBridgePileId::FromPileIndex(2))->SetGroupId(20 + i);
            UNIT_ASSERT_VALUES_EQUAL(piles(), TStringBuilder() << "10 - " << 20 + i << ' ');
            NBridge::RemoveGroupPile(&state, TBridgePileId::FromPileIndex(2));
            UNIT_ASSERT_VALUES_EQUAL(piles(), "10 ");
        }
        NBridge::RemoveGroupPile(&state, TBridgePileId::FromPileIndex(0));
        UNIT_ASSERT_VALUES_EQUAL(state.PileSize(), 0);
    }

    Y_UNIT_TEST(AssignPileIds) {
        const auto registry = NBridge::MakeImpliedPileRegistry(MakeBridgeConfig({"a", "b", "c"}));
        auto assign = [](const NKikimrBridge::TPileRegistry& registry, std::initializer_list<TString> names) {
            return NBridge::AssignPileIds(registry, MakeBridgeConfig(names));
        };

        // piles keep their ids wherever they are listed in the config
        UNIT_ASSERT_VALUES_EQUAL(Slots(assign(registry, {"c", "a", "b"})), "a b c");

        // new pile is appended when there are no free slots
        UNIT_ASSERT_VALUES_EQUAL(Slots(assign(registry, {"d", "a", "b", "c"})), "a b c d");

        // dropped pile frees its slot, free slots at the end are trimmed
        const auto dropped = assign(registry, {"a", "c"});
        UNIT_ASSERT_VALUES_EQUAL(Slots(dropped), "a - c");
        UNIT_ASSERT(!NBridge::ResolvePileId(dropped, "b"));
        UNIT_ASSERT(!NBridge::ResolvePileId(dropped, ""));
        UNIT_ASSERT_EQUAL(NBridge::ResolvePileId(dropped, "c"), TBridgePileId::FromPileIndex(2));
        UNIT_ASSERT_VALUES_EQUAL(Slots(assign(dropped, {"a"})), "a");
        UNIT_ASSERT_VALUES_EQUAL(Slots(assign(dropped, {"c"})), "- - c");

        // new piles take free slots in ascending order first, including the ones freed by the same change
        const auto holes = assign(assign(registry, {"a", "b", "c", "d", "e"}), {"a", "c", "e"});
        UNIT_ASSERT_VALUES_EQUAL(Slots(holes), "a - c - e");
        UNIT_ASSERT_VALUES_EQUAL(Slots(assign(holes, {"z", "y", "a", "c", "x", "e"})), "a z c y e x");
        UNIT_ASSERT_VALUES_EQUAL(Slots(assign(holes, {"x", "e"})), "x - - - e");
        UNIT_ASSERT_VALUES_EQUAL(Slots(assign(registry, {"a", "x", "c"})), "a x c");

        // implicit pile setting is kept while its pile stays in the config
        const auto single = NBridge::MakeImpliedPileRegistry(MakeBridgeConfig({"a"}));
        const auto two = assign(single, {"b", "a"});
        UNIT_ASSERT_VALUES_EQUAL(Slots(two), "a b");
        UNIT_ASSERT_EQUAL(NBridge::ResolvePileId(two, std::nullopt), TBridgePileId::FromPileIndex(0));
        const auto replaced = assign(two, {"c", "b"});
        UNIT_ASSERT_VALUES_EQUAL(Slots(replaced), "c b");
        UNIT_ASSERT(!replaced.GetImplicitPileId());
        UNIT_ASSERT(!assign(two, {"b"}).GetImplicitPileId());
    }

    Y_UNIT_TEST(AssignPileIdsCycles) {
        const auto initial = NBridge::MakeImpliedPileRegistry(MakeBridgeConfig({"a", "b"}));

        // the same pile added and removed many times returns to its slot
        auto registry = initial;
        for (ui32 i = 0; i < 100; ++i) {
            registry = NBridge::AssignPileIds(registry, MakeBridgeConfig({"a", "c", "b"}));
            UNIT_ASSERT_VALUES_EQUAL(Slots(registry), "a b c");
            registry = NBridge::AssignPileIds(registry, MakeBridgeConfig({"a", "b"}));
            UNIT_ASSERT_VALUES_EQUAL(Slots(registry), "a b");
        }

        // so does a series of piles with distinct names, and the original piles keep their ids
        for (ui32 i = 0; i < 100; ++i) {
            const TString name = TStringBuilder() << "p" << i;
            registry = NBridge::AssignPileIds(registry, MakeBridgeConfig({name, "b", "a"}));
            UNIT_ASSERT_VALUES_EQUAL(Slots(registry), "a b " + name);
            registry = NBridge::AssignPileIds(registry, MakeBridgeConfig({"b"}));
            UNIT_ASSERT_VALUES_EQUAL(Slots(registry), "- b");
            registry = NBridge::AssignPileIds(registry, MakeBridgeConfig({"b", "a"}));
            UNIT_ASSERT_VALUES_EQUAL(Slots(registry), "a b");
        }

        // random changes: registry is never longer than the maximum number of piles configured at once, piles keep
        // their ids while they stay in the config
        TReallyFastRng32 rng(1);
        registry = initial;
        TVector<TString> configured{"a", "b"};
        TVector<TString> dropped;
        size_t maxConfigured = configured.size();
        ui32 nextName = 0;
        for (ui32 i = 0; i < 10000; ++i) {
            if (configured.size() > 1 && (configured.size() == 5 || rng() % 2)) {
                const size_t index = rng() % configured.size();
                dropped.push_back(configured[index]);
                configured.erase(configured.begin() + index);
            } else {
                TString name;
                if (!dropped.empty() && rng() % 2) {
                    const size_t index = rng() % dropped.size();
                    name = dropped[index];
                    dropped.erase(dropped.begin() + index);
                } else {
                    name = TStringBuilder() << "p" << nextName++;
                }
                configured.insert(configured.begin() + rng() % (configured.size() + 1), name);
            }
            maxConfigured = Max(maxConfigured, configured.size());

            const auto next = NBridge::AssignPileIds(registry, MakeBridgeConfig(configured));
            UNIT_ASSERT_LE(next.PilesSize(), maxConfigured);
            UNIT_ASSERT(next.GetPiles(next.PilesSize() - 1).HasName());
            size_t numOccupied = 0;
            for (const auto& pile : next.GetPiles()) {
                numOccupied += pile.HasName();
            }
            UNIT_ASSERT_VALUES_EQUAL(numOccupied, configured.size());
            for (const TString& name : configured) {
                const auto id = NBridge::ResolvePileId(next, name);
                UNIT_ASSERT(id);
                if (const auto prev = NBridge::ResolvePileId(registry, name)) {
                    UNIT_ASSERT_EQUAL(id, prev);
                }
            }
            registry = next;
        }
    }

    // empty pile name is accepted by bridge config, so it is a name like any other one, and not a free slot
    Y_UNIT_TEST(EmptyPileName) {
        const auto config = MakeBridgeConfig({"", "b"});
        const auto registry = NBridge::MakeImpliedPileRegistry(config);
        UNIT_ASSERT_VALUES_EQUAL(Slots(registry), "'' b");
        UNIT_ASSERT(!NBridge::ValidatePileRegistry(registry, MakeClusterState(2)));
        UNIT_ASSERT_EQUAL(NBridge::ResolvePileId(registry, ""), TBridgePileId::FromPileIndex(0));
        UNIT_ASSERT_EQUAL(NBridge::ResolvePileId(registry, "b"), TBridgePileId::FromPileIndex(1));
        UNIT_ASSERT(!NBridge::ResolvePileId(registry, std::nullopt));
        UNIT_ASSERT(NBridge::ResolvePileIndex(config, "") == std::optional<size_t>(0));

        auto duplicate = registry;
        duplicate.MutablePiles(1)->SetName("");
        UNIT_ASSERT(NBridge::ValidatePileRegistry(duplicate, MakeClusterState(2)));

        // the pile keeps its slot, and it takes a free one when added
        UNIT_ASSERT_VALUES_EQUAL(Slots(NBridge::AssignPileIds(registry, MakeBridgeConfig({"b", ""}))), "'' b");
        const auto dropped = NBridge::AssignPileIds(registry, MakeBridgeConfig({"b"}));
        UNIT_ASSERT_VALUES_EQUAL(Slots(dropped), "- b");
        UNIT_ASSERT(!NBridge::ResolvePileId(dropped, ""));
        UNIT_ASSERT_VALUES_EQUAL(Slots(NBridge::AssignPileIds(dropped, MakeBridgeConfig({"b", ""}))), "'' b");

        // the only pile of the cluster converted from not bridged one is the implicit pile
        const auto single = NBridge::MakeImpliedPileRegistry(MakeBridgeConfig({""}));
        UNIT_ASSERT(!NBridge::ValidatePileRegistry(single, MakeClusterState(1)));
        UNIT_ASSERT_EQUAL(NBridge::ResolvePileId(single, std::nullopt), TBridgePileId::FromPileIndex(0));
        UNIT_ASSERT_EQUAL(NBridge::ResolvePileId(single, ""), TBridgePileId::FromPileIndex(0));
        const auto two = NBridge::AssignPileIds(single, MakeBridgeConfig({"", "b"}));
        UNIT_ASSERT_VALUES_EQUAL(Slots(two), "'' b");
        UNIT_ASSERT_EQUAL(NBridge::ResolvePileId(two, std::nullopt), TBridgePileId::FromPileIndex(0));
    }

    Y_UNIT_TEST(BridgeInfoResolvePile) {
        TBridgeInfo info;
        info.Piles.push_back(TBridgeInfo::TPile{.BridgePileId = TBridgePileId::FromPileIndex(0), .Name = "a"});
        info.Piles.push_back(TBridgeInfo::TPile{.BridgePileId = TBridgePileId::FromPileIndex(1), .Name = "b"});
        UNIT_ASSERT_EQUAL(info.ResolvePile("b"), &info.Piles[1]);
        UNIT_ASSERT(!info.ResolvePile("c"));
        UNIT_ASSERT(!info.ResolvePile(std::nullopt));
        info.Piles.push_back(TBridgeInfo::TPile{.BridgePileId = TBridgePileId::FromPileIndex(2), .IsFreeSlot = true});
        UNIT_ASSERT(!info.ResolvePile(""));
        info.Piles.push_back(TBridgeInfo::TPile{.BridgePileId = TBridgePileId::FromPileIndex(3), .Name = ""});
        UNIT_ASSERT_EQUAL(info.ResolvePile(""), &info.Piles[3]);
        UNIT_ASSERT(!NBridge::GetImplicitPileId(&info));
        info.ImplicitPile = &info.Piles[0];
        UNIT_ASSERT_EQUAL(info.ResolvePile(std::nullopt), &info.Piles[0]);
        UNIT_ASSERT_EQUAL(NBridge::GetImplicitPileId(&info), TBridgePileId::FromPileIndex(0));
        UNIT_ASSERT_EQUAL(NBridge::GetPlacementPileId(TBridgePileId(), &info), TBridgePileId::FromPileIndex(0));
        UNIT_ASSERT_EQUAL(NBridge::GetPlacementPileId(TBridgePileId::FromPileIndex(1), &info),
            TBridgePileId::FromPileIndex(1));
    }

}

} // NKikimr
