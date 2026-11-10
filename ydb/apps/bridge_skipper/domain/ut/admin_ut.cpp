#include <ydb/apps/bridge_skipper/domain/mutation.h>
#include <ydb/apps/bridge_skipper/domain/view.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NBridgeSkipper {
namespace {

TClusterView View(ui64 generation = 7, ui64 sentAt = 95) {
    return {.Generation = generation,
        .Piles = {{"a", EPileState::Primary}, {"b", EPileState::Synchronized}, {"c", EPileState::Promoted}},
        .Endpoint = "a:2135", .SentAt = TMonotonic::Seconds(sentAt)};
}

TExactUpdate Failover() {
    return {{"a", EPileState::Disconnected}, {"b", EPileState::Primary}, {"c", EPileState::Synchronized}};
}

TEndpointState Endpoint(const TString& endpoint, const TString& pile) {
    return {.Endpoint = endpoint, .Pile = pile, .Identity = EEndpointIdentity::Verified, .Current = true};
}

TConnectivityReply Sees(const TString& vantage, const std::set<TString>& withQuorum, const TString& endpoint = {}) {
    TConnectivity report{.VantagePile = vantage, .Generation = 7};
    for (const auto& pile : withQuorum) {
        report.Piles[pile] = {.HasNodeQuorum = true, .InConfigQuorum = true};
    }
    return {endpoint.empty() ? vantage : endpoint, report};
}

} // namespace

Y_UNIT_TEST_SUITE(AdminView) {
    Y_UNIT_TEST(NewestSingleNodeGenerationWins) {
        auto newer = View(8);
        newer.Endpoint = "b:2135";
        const auto result = SelectView({View(), newer}, 7, TMonotonic::Seconds(100), TDuration::Seconds(30));
        UNIT_ASSERT(result.View);
        UNIT_ASSERT_VALUES_EQUAL(result.View->Generation, 8);
        UNIT_ASSERT_VALUES_EQUAL(result.View->Endpoint, "b:2135");
    }

    Y_UNIT_TEST(OnlyMaximumGenerationConflictsMatter) {
        auto conflict = View();
        conflict.Piles["c"] = EPileState::Synchronized;
        auto result = SelectView({View(), conflict}, 0, TMonotonic::Seconds(100), TDuration::Seconds(30));
        UNIT_ASSERT(result.Blocker == EBlockerReason::ViewConflict);
        result = SelectView({View(), conflict, View(8)}, 0, TMonotonic::Seconds(100), TDuration::Seconds(30));
        UNIT_ASSERT(result.View);
        UNIT_ASSERT_VALUES_EQUAL(result.View->Generation, 8);
    }

    Y_UNIT_TEST(NewestPendingAndConversionMetadataDoesNotConflict) {
        auto newer = View(7, 96);
        newer.Pending = TPendingOperation{"REMOVE", TString("retired"), "retrying"};
        newer.Piles["retired"] = EPileState::Disconnected;
        auto older = newer;
        older.SentAt = TMonotonic::Seconds(95);
        older.Pending.reset();
        newer.ConversionInProgress = true;
        const auto result = SelectView({newer, older}, 0, TMonotonic::Seconds(100), TDuration::Seconds(30));
        UNIT_ASSERT(result.View);
        UNIT_ASSERT(result.View->Pending == newer.Pending);
        UNIT_ASSERT(result.View->ConversionInProgress);
    }

    Y_UNIT_TEST(OnlyInactiveRegistryDifferencesAreTolerated) {
        auto older = View();
        older.Piles["retired"] = EPileState::Inactive;
        auto newer = View(7, 96);
        const auto result = SelectView({older, newer}, 0, TMonotonic::Seconds(100), TDuration::Seconds(30));
        UNIT_ASSERT(result.View);
        UNIT_ASSERT(!result.View->Piles.contains("retired"));
        older.Piles["retired"] = EPileState::Disconnected;
        UNIT_ASSERT(SelectView({older, newer}, 0, TMonotonic::Seconds(100),
            TDuration::Seconds(30)).Blocker == EBlockerReason::ViewConflict);
        older.Piles["retired"] = EPileState::Synchronized;
        UNIT_ASSERT(SelectView({older, newer}, 0, TMonotonic::Seconds(100),
            TDuration::Seconds(30)).Blocker == EBlockerReason::ViewConflict);
    }

    Y_UNIT_TEST(EvidenceExpiresAtThePublishedTtlBoundary) {
        const auto result = SelectView({View(7, 70)}, 7, TMonotonic::Seconds(100), TDuration::Seconds(30));
        UNIT_ASSERT(!result.View);
        UNIT_ASSERT(result.Blocker == EBlockerReason::NoView);
    }

    Y_UNIT_TEST(FreshnessUsesSendTimeAndHighWaterIsEnforced) {
        const auto now = TMonotonic::Seconds(100);
        const auto ttl = TDuration::Seconds(30);
        UNIT_ASSERT(SelectView({View(9, 69)}, 0, now, ttl).Blocker == EBlockerReason::NoView);
        UNIT_ASSERT(SelectView({View(7, 71)}, 8, now, ttl).Blocker == EBlockerReason::StaleHistory);
        UNIT_ASSERT(SelectView({View(7, 71)}, 7, now, ttl).View);
        UNIT_ASSERT(SelectView({}, 0, now, ttl).Blocker == EBlockerReason::NoView);
    }

    Y_UNIT_TEST(CurrentEndpointsAreFreshRepliesAtTheSelectedGeneration) {
        auto stale = View(7, 69);
        stale.Endpoint = "stale";
        auto older = View(6, 96);
        older.Endpoint = "older";
        auto peer = View(7, 90);
        peer.Endpoint = "peer";
        const auto result = SelectView({View(), stale, older, peer}, 0, TMonotonic::Seconds(100), TDuration::Seconds(30));
        UNIT_ASSERT(result.View);
        UNIT_ASSERT(result.CurrentEndpoints == std::set<TString>({"a:2135", "peer"}));
        auto conflict = peer;
        conflict.Piles["c"] = EPileState::Synchronized;
        UNIT_ASSERT(SelectView({View(), conflict}, 0, TMonotonic::Seconds(100), TDuration::Seconds(30)).CurrentEndpoints.empty());
    }

    // Decode is the single validator and excludes these replies (runtime
    // DecodeRejectsDuplicatesUnknownEnumsAndInvalidViewsBeforeMaps).
    Y_UNIT_TEST(InvalidRolesAndStatesAreRejectedByValidation) {
        auto roles = View();
        roles.Piles["b"] = EPileState::Primary;
        UNIT_ASSERT_VALUES_EQUAL(ValidateClusterView(roles), "INVALID_PUBLIC_ROLES");
        auto unknown = View();
        unknown.Piles["b"] = static_cast<EPileState>(99);
        UNIT_ASSERT_VALUES_EQUAL(ValidateClusterView(unknown), "UNSUPPORTED_PUBLIC_STATE");
        auto empty = View();
        empty.Piles.clear();
        UNIT_ASSERT_VALUES_EQUAL(ValidateClusterView(empty), "INVALID_PUBLIC_ROLES");
    }

    Y_UNIT_TEST(TransitionsAreExplainedByOwnUpdatesAndFollowUps) {
        constexpr auto P = EPileState::Primary;
        constexpr auto S = EPileState::Synchronized;
        constexpr auto N = EPileState::NotSynchronized;
        constexpr auto D = EPileState::Disconnected;
        auto previous = View();
        previous.Piles["d"] = N;
        previous.Piles["e"] = EPileState::Suspended;
        previous.Piles["f"] = D;
        const TPendingMutation pending{7, Failover()};
        struct TCase {
            TNamedPileStates Changes;
            bool WithPending;
            ui64 Generation;
            bool Explained;
        };
        const std::vector<TCase> cases = {
            {{}, false, 8, true},
            {{}, false, 10, true},
            // Same generation: only absent-versus-INACTIVE differences are explained.
            {{}, false, 7, true},
            {{{"retired", EPileState::Inactive}}, false, 7, true},
            {{{"b", D}}, false, 7, false},
            {{{"a", D}, {"b", P}, {"c", S}}, true, 7, false},
            {{}, false, 6, false},
            {{{"a", S}, {"c", P}}, false, 8, true},
            {{{"c", P}}, false, 8, false},
            {{{"d", S}}, false, 8, true},
            {{{"e", D}}, false, 8, true},
            {{{"a", S}, {"c", P}, {"d", S}, {"e", D}}, false, 8, true},
            {{{"b", D}}, false, 8, false},
            {{{"f", N}}, false, 8, false},
            {{{"retired", EPileState::Inactive}}, false, 8, true},
            {{{"a", D}, {"b", P}, {"c", S}}, false, 8, false},
            {{{"a", D}, {"b", P}, {"c", S}}, true, 8, true},
            {{{"a", D}, {"b", P}, {"c", S}, {"d", S}}, true, 9, true},
            {{{"a", D}, {"b", P}}, true, 8, false},
            {{{"a", D}, {"b", P}, {"c", S}, {"d", D}}, true, 8, false},
        };
        for (size_t i = 0; i < cases.size(); ++i) {
            const auto& item = cases[i];
            auto next = previous;
            next.Generation = item.Generation;
            for (const auto& [name, state] : item.Changes) {
                next.Piles[name] = state;
            }
            const auto result = ExplainTransition(previous, next,
                item.WithPending ? std::optional<TPendingMutation>(pending) : std::nullopt);
            UNIT_ASSERT_VALUES_EQUAL_C(result == ETransitionExplanation::Explained, item.Explained, "case " << i);
        }
        // An update made for an older generation (a stale last success) does
        // not explain an operator repeating the same overlay.
        auto repeat = previous;
        repeat.Generation = 8;
        for (const auto& [name, state] : Failover()) {
            repeat.Piles[name] = state;
        }
        UNIT_ASSERT(ExplainTransition(previous, repeat, pending) == ETransitionExplanation::Explained);
        const TPendingMutation stale{previous.Generation - 1, Failover()};
        UNIT_ASSERT(ExplainTransition(previous, repeat, stale) == ETransitionExplanation::Unexplained);
    }

    // S11a (P1): binding connectivity reports count for SeesSurvivors and endpoint selection.
    Y_UNIT_TEST(BindingReportsCountForSeesSurvivors) {
        const auto view = View();
        const auto endpoints = std::vector<TEndpointState>{Endpoint("b", "b")};
        auto report = Sees("b", {"a", "b", "c"});
        report.Report.BindingInProgress = true;
        UNIT_ASSERT(SeesSurvivors({report}, "b", "b", {"b", "c"}));
        UNIT_ASSERT_VALUES_EQUAL(*SelectMutationEndpoint(view, endpoints, Failover(), {report}), "b");
    }

    // A rejection of this invocation cannot resolve earlier ambiguous attempts.
    Y_UNIT_TEST(RejectedOutcomePreservesThePendingLock) {
        const TPersistentState state{"cluster", 7, TPendingMutation{7, Failover()}};
        const auto result = ApplyOutcome(state, EMutationOutcome::Rejected);
        UNIT_ASSERT(result == state);
    }
}

Y_UNIT_TEST_SUITE(AdminUpdateValidation) {
    Y_UNIT_TEST(NewPrimaryMustExplicitlyClearOtherPromoted) {
        UNIT_ASSERT(!IsFailoverShape(View(), {{"a", EPileState::Disconnected}, {"b", EPileState::Primary}}));
        UNIT_ASSERT(IsFailoverShape(View(), Failover()));
        UNIT_ASSERT(IsFailoverShape(View(), {{"a", EPileState::Disconnected}, {"c", EPileState::Primary}}));
        auto suspended = View();
        suspended.Piles["b"] = EPileState::Suspended;
        UNIT_ASSERT(!IsFailoverShape(suspended, {{"b", EPileState::Disconnected}}));
    }

    Y_UNIT_TEST(EmptyNameAndUnsupportedStateRemainDistinct) {
        auto view = View();
        view.Piles.erase("a");
        view.Piles[""] = EPileState::Primary;
        UNIT_ASSERT(IsFailoverShape(view, {{"", EPileState::Disconnected}, {"b", EPileState::Primary},
            {"c", EPileState::Synchronized}}));
        view.Piles["b"] = static_cast<EPileState>(99);
        UNIT_ASSERT_VALUES_EQUAL(ValidateClusterView(view), "UNSUPPORTED_PUBLIC_STATE");
        UNIT_ASSERT_VALUES_EQUAL(ToString(static_cast<EPileState>(99)), "UNKNOWN(99)");
    }

    Y_UNIT_TEST(ConversionAndDisjointPendingOperationPermitFailover) {
        auto view = View();
        view.ConversionInProgress = true;
        view.Piles["retired"] = EPileState::Disconnected;
        view.Pending = TPendingOperation{"REMOVE", TString("retired"), "retrying"};
        UNIT_ASSERT(IsFailoverShape(view, Failover()));
    }
}

Y_UNIT_TEST_SUITE(AdminMutation) {
    Y_UNIT_TEST(SamePileEndpointsCannotBorrowSurvivorVisibility) {
        for (const TString& capable : {TString("a1"), TString("a2")}) {
            auto view = View();
            view.Piles.erase("c");
            const auto isolated = capable == "a1" ? TString("a2") : TString("a1");
            const std::vector<TEndpointState> endpoints{Endpoint(isolated, "a"), Endpoint(capable, "a")};
            const auto reports = std::vector<TConnectivityReply>{
                Sees("a", {}, isolated), Sees("a", {"a"}, capable)};
            const TExactUpdate update{{"b", EPileState::Disconnected}};
            UNIT_ASSERT_VALUES_EQUAL(*SelectMutationEndpoint(view, endpoints, update, reports), capable);
            UNIT_ASSERT(!SelectMutationEndpoint(view, {Endpoint(isolated, "a")}, update, reports));
        }
    }

    Y_UNIT_TEST(EnumNamesAreStable) {
        UNIT_ASSERT_VALUES_EQUAL(ToString(EPileState::NotSynchronized), "NOT_SYNCHRONIZED");
        UNIT_ASSERT_VALUES_EQUAL(ToString(EPileVerdict::Replicating), "REPLICATING");
        UNIT_ASSERT_VALUES_EQUAL(ToString(EBlockerReason::NoMutationEndpoint), "NO_MUTATION_ENDPOINT");
        UNIT_ASSERT_VALUES_EQUAL(ToString(EDecisionKind::Ready), "READY");
        UNIT_ASSERT_VALUES_EQUAL(ToString(EMutationOutcome::NotAppliedHere), "NOT_APPLIED_HERE");
        UNIT_ASSERT_VALUES_EQUAL(ToString(EEndpointIdentity::Mismatch), "MISMATCH");
    }

    Y_UNIT_TEST(ResendsFollowTheLastOutcome) {
        const auto now = TMonotonic::Seconds(100);
        const auto backoff = TDuration::Seconds(30);
        UNIT_ASSERT(MayResend({}, {}, now, backoff, 0));
        UNIT_ASSERT(!MayResend(TMonotonic::Seconds(80), EMutationOutcome::Ambiguous, now, backoff, 0));
        UNIT_ASSERT(MayResend(TMonotonic::Seconds(70), EMutationOutcome::Ambiguous, now, backoff, 0));
        UNIT_ASSERT(MayResend(TMonotonic::Seconds(99), EMutationOutcome::NotAppliedHere, now, backoff, 0));
        for (auto outcome : {EMutationOutcome::Success, EMutationOutcome::GenerationMoved, EMutationOutcome::Rejected}) {
            UNIT_ASSERT(!MayResend(TMonotonic::Seconds(1), outcome, now, backoff, 0));
        }
        UNIT_ASSERT(!MayResend(now, {}, now, backoff, 0));
        UNIT_ASSERT(!MayResend(TMonotonic::Seconds(50), EMutationOutcome::Ambiguous, now, backoff, 1));
        UNIT_ASSERT(MayResend(TMonotonic::Seconds(40), EMutationOutcome::Ambiguous, now, backoff, 1));
        const auto late = TMonotonic::Seconds(1000);
        UNIT_ASSERT(!MayResend(late - TDuration::Seconds(299), EMutationOutcome::Ambiguous, late, backoff, 20));
        UNIT_ASSERT(MayResend(late - TDuration::Seconds(300), EMutationOutcome::Ambiguous, late, backoff, 20));
        UNIT_ASSERT(MayResend(late - TDuration::Seconds(300), EMutationOutcome::Ambiguous, late, backoff, 60));
    }

    Y_UNIT_TEST(NewerViewsAndTerminalOutcomesResolveTheRecord) {
        const TPersistentState state{"cluster", 7, TPendingMutation{7, Failover()}};
        UNIT_ASSERT(!ResolvedBy(*state.PendingMutation, View(7)));
        UNIT_ASSERT(ResolvedBy(*state.PendingMutation, View(8)));
        for (auto outcome : {EMutationOutcome::Success, EMutationOutcome::GenerationMoved}) {
            const auto result = ApplyOutcome(state, outcome);
            UNIT_ASSERT_VALUES_EQUAL(result.GenerationHighWater, 8);
            UNIT_ASSERT(!result.PendingMutation);
            auto ahead = state;
            ahead.GenerationHighWater = 11;
            UNIT_ASSERT_VALUES_EQUAL(ApplyOutcome(ahead, outcome).GenerationHighWater, 11);
        }
        for (auto outcome : {EMutationOutcome::NotAppliedHere, EMutationOutcome::Ambiguous}) {
            UNIT_ASSERT(ApplyOutcome(state, outcome) == state);
        }
        auto noPending = state;
        noPending.PendingMutation.reset();
        UNIT_ASSERT(ApplyOutcome(noPending, EMutationOutcome::Success) == noPending);
    }

    Y_UNIT_TEST(EndpointSelectionRequiresIdentityCurrencyAndSurvivingQuorum) {
        const auto view = View();
        auto endpoints = std::vector<TEndpointState>{Endpoint("a", "a"), Endpoint("c", "c"), Endpoint("b", "b")};
        const std::vector<TConnectivityReply> reports{Sees("a", {"a", "b", "c"}), Sees("b", {"a", "b", "c"}),
            Sees("c", {"a", "b", "c"})};
        UNIT_ASSERT_VALUES_EQUAL(*SelectMutationEndpoint(view, endpoints, Failover(), reports), "b");
        UNIT_ASSERT_VALUES_EQUAL(*SelectMutationEndpoint(view, endpoints, Failover(), reports, {"b"}), "c");
        UNIT_ASSERT(!SelectMutationEndpoint(view, endpoints, Failover(), reports, {"b", "c"}));
        endpoints[1].Identity = EEndpointIdentity::Mismatch;
        endpoints[2].Current = false;
        UNIT_ASSERT(!SelectMutationEndpoint(view, endpoints, Failover(), reports));
        endpoints[2].Current = true;
        endpoints[2].Identity = EEndpointIdentity::Unknown;
        UNIT_ASSERT(!SelectMutationEndpoint(view, endpoints, Failover(), reports));
    }

    Y_UNIT_TEST(EndpointMustSeeEverySurvivorFromItsOwnPile) {
        const auto view = View();
        const auto endpoints = std::vector<TEndpointState>{Endpoint("b", "b"), Endpoint("c", "c")};
        // b's component cannot see c, although c sees itself with quorum.
        const std::vector<TConnectivityReply> reports{Sees("b", {"b"}), Sees("c", {"b", "c"})};
        UNIT_ASSERT(!SeesSurvivors(reports, "b", "b", {"b", "c"}));
        UNIT_ASSERT(SeesSurvivors(reports, "c", "c", {"b", "c"}));
        UNIT_ASSERT(!SeesSurvivors(reports, "a", "a", {}));
        UNIT_ASSERT_VALUES_EQUAL(*SelectMutationEndpoint(view, endpoints, Failover(), reports), "c");
        UNIT_ASSERT(!SelectMutationEndpoint(view, endpoints, Failover(), reports, {"c"}));
    }

    Y_UNIT_TEST(SynchronizedEndpointIsPreferredOverSuspended) {
        auto view = View();
        view.Piles["d"] = EPileState::Suspended;
        const TExactUpdate update{{"c", EPileState::Disconnected}};
        const auto endpoints = std::vector<TEndpointState>{Endpoint("1:2135", "d"), Endpoint("2:2135", "b")};
        const std::vector<TConnectivityReply> reports{Sees("b", {"a", "b", "d"}, "2:2135"), Sees("d", {"a", "b", "d"}, "1:2135")};
        UNIT_ASSERT_VALUES_EQUAL(*SelectMutationEndpoint(view, endpoints, update, reports), "2:2135");
        UNIT_ASSERT_VALUES_EQUAL(*SelectMutationEndpoint(view, endpoints, update, reports, {"2:2135"}), "1:2135");
    }

    Y_UNIT_TEST(StageTwoEndpointsRequireConnectivityAndExplicitResyncRemovesParticipation) {
        auto view = View();
        view.Piles["d"] = EPileState::NotSynchronized;
        const auto endpoints = std::vector<TEndpointState>{Endpoint("d", "d")};
        const TExactUpdate update{{"b", EPileState::Disconnected}};
        UNIT_ASSERT(!SelectMutationEndpoint(view, endpoints, update, {}));
        auto connectivity = Sees("d", {"a", "c", "d"});
        UNIT_ASSERT_VALUES_EQUAL(*SelectMutationEndpoint(view, endpoints, update, {connectivity}), "d");
        auto resync = update;
        resync["d"] = EPileState::NotSynchronized;
        UNIT_ASSERT(!SelectMutationEndpoint(view, endpoints, resync, {connectivity}));
        connectivity.Report.Piles["d"].InConfigQuorum = false;
        UNIT_ASSERT(!SelectMutationEndpoint(view, endpoints, update, {connectivity}));
    }

    Y_UNIT_TEST(BscProvenStageTwoSurvivorStillRequiresNodeQuorum) {
        auto view = View();
        view.Piles["c"] = EPileState::NotSynchronized;
        const auto endpoints = std::vector<TEndpointState>{Endpoint("a", "a")};
        const TExactUpdate update{{"b", EPileState::Disconnected}};
        auto connectivity = Sees("a", {"a", "b"});
        connectivity.Report.Piles["c"] = {.HasNodeQuorum = false, .InConfigQuorum = false};
        const TPileParticipation participation{{"c", {true, TMonotonic::Seconds(100)}}};
        UNIT_ASSERT(!SelectMutationEndpoint(view, endpoints, update, {connectivity}, {}, participation));
        connectivity.Report.Piles["c"].HasNodeQuorum = true;
        UNIT_ASSERT_VALUES_EQUAL(*SelectMutationEndpoint(view, endpoints, update, {connectivity}, {}, participation), "a");
    }
}

} // namespace NBridgeSkipper
