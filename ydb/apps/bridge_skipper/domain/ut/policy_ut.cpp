#include <ydb/apps/bridge_skipper/domain/policy.h>
#include <ydb/public/api/protos/ydb_monitoring.pb.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_value.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NBridgeSkipper {
namespace {

TMonotonic At(ui64 seconds) {
    return TMonotonic::Seconds(seconds);
}

TPileVerdict Verdict(EPileVerdict verdict, ui64 sentAt = 100) {
    TPileVerdict result;
    result.Verdict = verdict;
    result.NewestEvidenceSentAt = At(sentAt);
    return result;
}

TClusterView ViewAt(
    ui64 generation,
    const TNamedPileStates& piles = {
        {"a", EPileState::Primary}, {"b", EPileState::Synchronized}, {"c", EPileState::Synchronized}})
{
    TClusterView view;
    view.Generation = generation;
    view.Piles = piles;
    return view;
}

struct TPolicyCase {
    TClusterView View;
    THealthEvaluation Health;
    TConfirmations Confirmations;
    std::optional<TPendingMutation> Pending;
    std::vector<TEndpointState> Endpoints;

    TPolicyCase() {
        View.Generation = 7;
        Add("a", EPileState::Primary);
        Add("b", EPileState::Synchronized);
        Add("c", EPileState::Synchronized);
        Health.BscAvailable = true;
    }

    void Add(const TString& name, EPileState state) {
        View.Piles[name] = state;
        if (state == EPileState::Primary || state == EPileState::Promoted
            || state == EPileState::Synchronized || state == EPileState::NotSynchronized)
        {
            Health.Participating.insert(name);
            Health.Piles[name] = Verdict(EPileVerdict::Healthy);
        }
        // Every vantage sees every pile with quorum.
        for (auto& reply : Health.Connectivity) {
            auto& report = reply.Report;
            report.Piles[name] = {3, 3, true, true, true};
        }
        TConnectivity report;
        report.Generation = View.Generation;
        report.VantagePile = name;
        report.SentAt = At(101);
        for (const auto& [pile, pileState] : View.Piles) {
            report.Piles[pile] = {3, 3, true, true, true};
        }
        Health.Connectivity.push_back({Endpoint(name).Endpoint, report});
    }

    TEndpointState Endpoint(const TString& pile) const {
        TEndpointState endpoint;
        endpoint.Endpoint = TString("grpc://") + pile + ":2135";
        endpoint.Pile = pile;
        endpoint.Identity = EEndpointIdentity::Verified;
        endpoint.Current = true;
        return endpoint;
    }

    void Down(const TString& pile, EPileVerdict verdict = EPileVerdict::Failed) {
        Health.Piles[pile] = Verdict(verdict);
    }

    void Confirm() {
        Confirmations.Update(View, Health.Piles, At(100));
        for (auto& [pile, verdict] : Health.Piles) {
            verdict.NewestEvidenceSentAt = At(120);
        }
        Confirmations.Update(View, Health.Piles, At(120));
    }

    TDecision DecideAt(const std::set<TString>& excluded = {}) {
        if (Endpoints.empty()) {
            for (const auto& [name, state] : View.Piles) {
                Endpoints.push_back(Endpoint(name));
            }
        }
        TViewSelection selection;
        selection.View = View;
        return Decide(selection, Health, Confirmations, Pending, Endpoints,
            TDuration::Seconds(20), excluded);
    }
};

void AssertBlocked(const TDecision& decision, EBlockerReason reason) {
    UNIT_ASSERT(decision.Kind == EDecisionKind::Blocked);
    UNIT_ASSERT(decision.Blocker == reason);
    UNIT_ASSERT(!decision.Plan);
    UNIT_ASSERT(decision.CliCommand.empty());
}

void AssertUpdate(const TDecision& decision, const TExactUpdate& update) {
    UNIT_ASSERT(decision.Kind == EDecisionKind::Ready);
    UNIT_ASSERT(decision.Plan);
    UNIT_ASSERT(decision.Plan->Update == update);
    UNIT_ASSERT_VALUES_EQUAL(decision.Plan->ExpectedGeneration, 7);
    UNIT_ASSERT_VALUES_EQUAL(decision.CliCommand, RenderCliCommand(*decision.Plan));
}

// A BSC-mode or fallback SelfCheck reply with one RED group part per listed pile
// and one GREY "no data" group part per grey pile.
TSelfCheckClassification ClassifiedSelfCheck(
    bool bsc,
    const std::vector<TString>& redPiles,
    const std::vector<TString>& greyPiles = {})
{
    using TStatus = Ydb::Monitoring::StatusFlag;
    Ydb::Monitoring::SelfCheckResult result;
    result.set_self_check_result(redPiles.empty() && bsc
        ? Ydb::Monitoring::SelfCheck::GOOD
        : Ydb::Monitoring::SelfCheck::EMERGENCY);
    if (!bsc) {
        auto* marker = result.add_issue_log();
        marker->set_id("bsc");
        marker->set_type("STORAGE");
        marker->set_status(TStatus::RED);
        marker->set_message("System tablet BSC didn't provide information");
    }
    for (const auto& pile : redPiles) {
        auto* group = result.add_issue_log();
        group->set_id("group-" + pile);
        group->set_type(bsc ? "BRIDGE_GROUP" : "STORAGE_GROUP");
        group->set_status(TStatus::RED);
        group->set_message("Group failed");
        group->mutable_location()->mutable_storage()->mutable_pool()->mutable_group()->mutable_pile()->set_name(pile);
        group->add_reason("disk-" + pile);
        auto* disk = result.add_issue_log();
        disk->set_id("disk-" + pile);
        disk->set_type("VDISK");
        disk->set_status(TStatus::RED);
        disk->set_message("VDisk is not available");
    }
    for (const auto& pile : greyPiles) {
        auto* group = result.add_issue_log();
        group->set_id("grey-group-" + pile);
        group->set_type("STORAGE_GROUP");
        group->set_status(TStatus::GREY);
        group->set_message("Group has disks with no data");
        group->mutable_location()->mutable_storage()->mutable_pool()->mutable_group()->mutable_pile()->set_name(pile);
        group->add_reason("grey-disk-" + pile);
        auto* disk = result.add_issue_log();
        disk->set_id("grey-disk-" + pile);
        disk->set_type("VDISK");
        disk->set_status(TStatus::GREY);
        disk->set_message("VDisk has no data");
    }
    return ClassifySelfCheck(result);
}

// Raw replies through ClassifySelfCheck, EvaluatePiles and Decide.
struct TComposite {
    TClusterView View;
    THealthReplies Replies;
    THealthParams Params;
    THealthState State;
    TConfirmations Confirmations;
    std::set<TString> Silent;

    explicit TComposite(const TNamedPileStates& piles) {
        View.Generation = 7;
        View.Piles = piles;
        Params.BscWarmup = TDuration::Zero();
        for (const auto& [pile, state] : piles) {
            TEndpointState endpoint;
            endpoint.Endpoint = pile + ":2135";
            endpoint.Pile = pile;
            endpoint.Identity = EEndpointIdentity::Verified;
            endpoint.Current = true;
            Replies.Endpoints.push_back(endpoint);
        }
    }

    void AddSelfCheck(
        const TString& pile,
        bool bsc,
        const std::vector<TString>& redPiles,
        const std::vector<TString>& greyPiles = {})
    {
        TSelfCheckReply reply;
        reply.Endpoint = pile + ":2135";
        reply.ResponderPile = pile;
        reply.SentAt = At(60);
        reply.Classification = ClassifiedSelfCheck(bsc, redPiles, greyPiles);
        Replies.SelfChecks.push_back(std::move(reply));
    }

    // One side of a partition: its SelfCheck reply, and a connectivity report
    // showing only its own pile with node quorum.
    void AddSide(
        const TString& pile,
        bool bsc,
        const std::vector<TString>& redPiles,
        const std::vector<TString>& greyPiles = {})
    {
        AddSelfCheck(pile, bsc, redPiles, greyPiles);
        TConnectivity report;
        report.VantagePile = pile;
        report.Generation = View.Generation;
        report.SentAt = At(60);
        for (const auto& [name, state] : View.Piles) {
            report.Piles[name] = {3, name == pile ? 3u : 0u, name == pile, true, true};
        }
        Replies.Connectivity.push_back({pile + ":2135", report});
    }

    // The side's last replies, sent at 60, are older than the TTL from 100 on.
    void Silence(const TString& pile) {
        Silent.insert(pile + ":2135");
        for (auto& endpoint : Replies.Endpoints) {
            if (endpoint.Pile == pile) {
                endpoint.Current = false;
            }
        }
    }

    // Evaluates at 100 and 120 with fresh replies and decides at 120.
    TDecision Run() {
        THealthEvaluation health;
        for (const ui64 seconds : {100, 120}) {
            for (auto& reply : Replies.SelfChecks) {
                if (!Silent.contains(reply.Endpoint)) {
                    reply.SentAt = At(seconds);
                }
            }
            for (auto& reply : Replies.Connectivity) {
                if (!Silent.contains(reply.Endpoint)) {
                    reply.Report.SentAt = At(seconds);
                }
            }
            health = EvaluatePiles(View, Replies, At(seconds), Params, State);
            Confirmations.Update(View, health.Piles, At(seconds));
        }
        TViewSelection selection;
        selection.View = View;
        return Decide(selection, health, Confirmations, {}, Replies.Endpoints, TDuration::Seconds(20));
    }
};

} // namespace

Y_UNIT_TEST_SUITE(BridgeSkipperPolicy) {
    Y_UNIT_TEST(ConfirmationNeedsAFreshlySentEvidenceSpan) {
        TConfirmations confirmations;
        std::map<TString, TPileVerdict> verdicts = {{"a", Verdict(EPileVerdict::Failed, 100)}};
        confirmations.Update(ViewAt(7), verdicts, At(100));
        UNIT_ASSERT(!confirmations.IsConfirmed("a", TDuration::Seconds(20)));
        verdicts["a"].NewestEvidenceSentAt = At(119);
        confirmations.Update(ViewAt(7), verdicts, At(119));
        UNIT_ASSERT(!confirmations.IsConfirmed("a", TDuration::Seconds(20)));
        verdicts["a"].NewestEvidenceSentAt = At(120);
        confirmations.Update(ViewAt(7), verdicts, At(120));
        UNIT_ASSERT(confirmations.IsConfirmed("a", TDuration::Seconds(20)));
        verdicts["a"].NewestEvidenceSentAt = At(99);
        confirmations.Update(ViewAt(7), verdicts, At(121));
        UNIT_ASSERT(!confirmations.IsConfirmed("a", TDuration::Seconds(20)));
        verdicts["a"].NewestEvidenceSentAt = At(121);
        confirmations.Update(ViewAt(7), verdicts, At(122));
        UNIT_ASSERT(confirmations.IsConfirmed("a", TDuration::Seconds(20)));
        UNIT_ASSERT(!confirmations.IsConfirmed("missing", TDuration::Seconds(20)));
    }

    // RED replies sent at 100 and 110, then only failed attempts: the cached
    // replies span 10 s of evidence and must not confirm a 20 s window.
    Y_UNIT_TEST(CachedEvidenceAloneNeverConfirms) {
        TConfirmations confirmations;
        std::map<TString, TPileVerdict> verdicts = {{"a", Verdict(EPileVerdict::Failed, 100)}};
        confirmations.Update(ViewAt(7), verdicts, At(100));
        verdicts["a"].NewestEvidenceSentAt = At(110);
        for (ui64 now = 110; now <= 125; ++now) {
            confirmations.Update(ViewAt(7), verdicts, At(now));
            UNIT_ASSERT(!confirmations.IsConfirmed("a", TDuration::Seconds(20)));
        }
        verdicts["a"].NewestEvidenceSentAt = At(120);
        confirmations.Update(ViewAt(7), verdicts, At(126));
        UNIT_ASSERT(confirmations.IsConfirmed("a", TDuration::Seconds(20)));
    }

    Y_UNIT_TEST(FailedToLostKeepsWindowButNonDownAndGenerationChangesReset) {
        TConfirmations confirmations;
        std::map<TString, TPileVerdict> verdicts = {{"a", Verdict(EPileVerdict::Failed)}};
        confirmations.Update(ViewAt(7), verdicts, At(100));
        verdicts["a"] = Verdict(EPileVerdict::Lost, 120);
        confirmations.Update(ViewAt(7), verdicts, At(110));
        UNIT_ASSERT(confirmations.IsConfirmed("a", TDuration::Seconds(20)));
        ui64 now = 120;
        for (const auto nonDown : {EPileVerdict::Unknown, EPileVerdict::Healthy, EPileVerdict::Replicating}) {
            verdicts["a"].Verdict = nonDown;
            confirmations.Update(ViewAt(7), verdicts, At(now));
            UNIT_ASSERT(!confirmations.IsConfirmed("a", TDuration::Seconds(20)));
            verdicts["a"] = Verdict(EPileVerdict::Failed, now + 10);
            confirmations.Update(ViewAt(7), verdicts, At(now + 10));
            UNIT_ASSERT(!confirmations.IsConfirmed("a", TDuration::Seconds(20)));
            now += 30;
        }
        confirmations.Update(ViewAt(8, {{"a", EPileState::Synchronized}, {"b", EPileState::Primary}}), verdicts, At(now));
        UNIT_ASSERT(!confirmations.IsConfirmed("a", TDuration::Seconds(20)));
        confirmations.Update(ViewAt(8, {{"a", EPileState::Synchronized}, {"b", EPileState::Primary}}), {}, At(now + 1));
        UNIT_ASSERT(confirmations.Piles.empty());
    }

    Y_UNIT_TEST(ViewBlockersHavePriority) {
        TPolicyCase input;
        input.Add("suspended", EPileState::Suspended);
        input.Down("suspended", EPileVerdict::Lost);
        for (const auto blocker : {EBlockerReason::NoView, EBlockerReason::ViewConflict, EBlockerReason::StaleHistory}) {
            TViewSelection selection;
            selection.Blocker = blocker;
            AssertBlocked(Decide(selection, input.Health, input.Confirmations, {}, {},
                TDuration::Seconds(20)), blocker);
        }
        AssertBlocked(Decide({}, {}, {}, {}, {}, TDuration::Seconds(20)), EBlockerReason::NoView);
    }

    Y_UNIT_TEST(SoleGlobalSurvivorDisconnectsAllFailedInOneVector) {
        TPolicyCase input;
        input.Down("b");
        input.Down("c");
        input.Confirm();
        AssertUpdate(input.DecideAt(), {{"b", EPileState::Disconnected}, {"c", EPileState::Disconnected}});
    }

    Y_UNIT_TEST(SeveralFailedIncludingPrimaryUseOneRequest) {
        TPolicyCase input;
        input.Down("a");
        input.Down("c");
        input.Confirm();
        const auto decision = input.DecideAt();
        AssertUpdate(decision, {{"a", EPileState::Disconnected}, {"b", EPileState::Primary}, {"c", EPileState::Disconnected}});
        UNIT_ASSERT_VALUES_EQUAL(decision.Plan->Endpoint, "grpc://b:2135");
    }

    Y_UNIT_TEST(ExactBatchPreservesOmittedAndDemotesAbandonedPromotion) {
        TPolicyCase input;
        input.View.Piles["c"] = EPileState::Promoted;
        input.Health.Piles["c"] = Verdict(EPileVerdict::Unknown);
        input.Add("untouched", EPileState::NotSynchronized);
        input.Health.Participating.erase("untouched");
        input.Health.Piles.erase("untouched");
        input.Down("a");
        input.Confirm();
        AssertUpdate(input.DecideAt(), {{"a", EPileState::Disconnected}, {"b", EPileState::Primary}, {"c", EPileState::Synchronized}});
    }

    Y_UNIT_TEST(HealthyPromotedIsPreferredOverNameOrderedSynchronizedPiles) {
        TPolicyCase input;
        input.View.Piles["c"] = EPileState::Promoted;
        input.Down("a");
        input.Confirm();
        AssertUpdate(input.DecideAt(), {{"a", EPileState::Disconnected}, {"c", EPileState::Primary}});
    }

    Y_UNIT_TEST(SynchronizedCandidatesAreChosenInNameOrder) {
        TPolicyCase input;
        input.Down("a");
        input.Confirm();
        AssertUpdate(input.DecideAt(), {{"a", EPileState::Disconnected}, {"b", EPileState::Primary}});
    }

    Y_UNIT_TEST(LostPromotedIsDisconnectedWhateverTheStoredPrimarysVerdict) {
        TPolicyCase input;
        input.View.Piles["c"] = EPileState::Promoted;
        input.Down("c", EPileVerdict::Lost);
        input.Health.BscAvailable = false;
        input.Health.Piles["a"] = Verdict(EPileVerdict::Unknown);
        input.Health.Piles["a"].SelfQuorum = true;
        input.Confirm();
        AssertUpdate(input.DecideAt(), {{"c", EPileState::Disconnected}});
        input.Health.Piles["a"].FallbackRed = true;
        AssertUpdate(input.DecideAt(), {{"c", EPileState::Disconnected}});
    }

    Y_UNIT_TEST(LostPrimaryFallbackRequiresSelfQuorumAndNoRedAndNoBsc) {
        TPolicyCase input;
        input.Down("a", EPileVerdict::Lost);
        input.Health.Piles["b"] = Verdict(EPileVerdict::Unknown);
        input.Health.Piles["c"] = Verdict(EPileVerdict::Unknown);
        input.Health.BscAvailable = false;
        input.Confirm();
        AssertBlocked(input.DecideAt(), EBlockerReason::NoCandidate);
        input.Health.Piles["b"].SelfQuorum = true;
        AssertUpdate(input.DecideAt(), {{"a", EPileState::Disconnected}, {"b", EPileState::Primary}});
        input.Health.Piles["b"].FallbackRed = true;
        AssertBlocked(input.DecideAt(), EBlockerReason::NoCandidate);
        input.Health.Piles["b"].FallbackRed = false;
        input.Health.BscAvailable = true;
        AssertBlocked(input.DecideAt(), EBlockerReason::NoCandidate);
    }

    Y_UNIT_TEST(ParticipatingStageTwoFailureIsBatchedWithPrimary) {
        TPolicyCase input;
        input.View.Piles["c"] = EPileState::NotSynchronized;
        input.Down("a");
        input.Down("c", EPileVerdict::Lost);
        input.Confirm();
        AssertUpdate(input.DecideAt(), {{"a", EPileState::Disconnected}, {"b", EPileState::Primary}, {"c", EPileState::Disconnected}});
    }

    Y_UNIT_TEST(NonparticipatingPilesAreNotTargetsOrCandidates) {
        TPolicyCase input;
        input.Add("stage1", EPileState::NotSynchronized);
        input.Add("inactive", EPileState::Inactive);
        input.Add("disconnected", EPileState::Disconnected);
        input.Health.Participating.erase("stage1");
        input.Down("stage1");
        input.Down("inactive");
        input.Down("disconnected");
        UNIT_ASSERT(input.DecideAt().Kind == EDecisionKind::Stable);
        input.Down("a");
        input.Health.Piles["b"] = Verdict(EPileVerdict::Unknown);
        input.Health.Piles["c"] = Verdict(EPileVerdict::Unknown);
        input.Confirm();
        AssertBlocked(input.DecideAt(), EBlockerReason::NoCandidate);
    }

    Y_UNIT_TEST(SuspendedLostIsImmediateBlocker) {
        TPolicyCase input;
        input.Add("s", EPileState::Suspended);
        input.Down("s", EPileVerdict::Lost);
        AssertBlocked(input.DecideAt(), EBlockerReason::SuspendedPileLost);
    }

    Y_UNIT_TEST(UnknownPrimaryIsVisibleAndStaysPrimaryWhenANonPrimaryIsDisconnected) {
        TPolicyCase input;
        input.Health.Piles["a"] = Verdict(EPileVerdict::Unknown);
        auto decision = input.DecideAt();
        UNIT_ASSERT(decision.Kind == EDecisionKind::Unknown);
        UNIT_ASSERT(decision.Reason.Contains("a"));
        UNIT_ASSERT(!decision.Plan);
        input.Down("b");
        input.Confirm();
        AssertUpdate(input.DecideAt(), {{"b", EPileState::Disconnected}});
    }

    Y_UNIT_TEST(HealthyAndReplicatingAreStableButReplicationDoesNotQualifySurvivor) {
        TPolicyCase input;
        input.Health.Piles["a"] = Verdict(EPileVerdict::Replicating);
        UNIT_ASSERT(input.DecideAt().Kind == EDecisionKind::Stable);
        input.Down("b");
        input.Confirm();
        AssertUpdate(input.DecideAt(), {{"b", EPileState::Disconnected}});
        input.Health.Piles["a"] = Verdict(EPileVerdict::Failed);
        input.Health.Piles["b"] = Verdict(EPileVerdict::Replicating);
        input.Health.Piles["c"] = Verdict(EPileVerdict::Replicating);
        input.Confirm();
        AssertBlocked(input.DecideAt(), EBlockerReason::NoCandidate);
    }

    Y_UNIT_TEST(MissingParticipatingVerdictIsUnknown) {
        TPolicyCase input;
        input.Health.Piles.erase("a");
        UNIT_ASSERT(input.DecideAt().Kind == EDecisionKind::Unknown);
    }

    Y_UNIT_TEST(AllDownPilesMustConfirmBeforeOneBatch) {
        TPolicyCase input;
        input.Down("b");
        input.Confirm();
        input.Down("c");
        input.Confirmations.Update(input.View, input.Health.Piles, At(120));
        auto decision = input.DecideAt();
        UNIT_ASSERT(decision.Kind == EDecisionKind::Confirming);
        UNIT_ASSERT(decision.Reason.Contains("b"));
        UNIT_ASSERT(decision.Reason.Contains("c"));
        UNIT_ASSERT(!decision.Plan);
        input.Health.Piles["c"].NewestEvidenceSentAt = At(121);
        input.Confirmations.Update(input.View, input.Health.Piles, At(140));
        AssertUpdate(input.DecideAt(), {{"b", EPileState::Disconnected}, {"c", EPileState::Disconnected}});
    }

    Y_UNIT_TEST(ConfirmationFromAnotherGenerationCannotAuthorizePlan) {
        TPolicyCase input;
        input.Down("b");
        input.Confirm();
        input.View.Generation = 8;
        UNIT_ASSERT(input.DecideAt().Kind == EDecisionKind::Confirming);
    }

    Y_UNIT_TEST(SurvivorQuorumIncludesSuspendedAndParticipatingStageTwo) {
        for (const auto state : {EPileState::Synchronized, EPileState::Suspended, EPileState::NotSynchronized}) {
            TPolicyCase input;
            input.Add("s", state);
            for (auto& reply : input.Health.Connectivity) {
                auto& report = reply.Report;
                if (report.VantagePile != "s") {
                    report.Piles["s"].HasNodeQuorum = false;
                }
            }
            input.Down("b");
            input.Confirm();
            // Only s claims its own quorum, and s cannot receive the update.
            input.Endpoints = {input.Endpoint("a"), input.Endpoint("c")};
            AssertBlocked(input.DecideAt(), EBlockerReason::NoMutationEndpoint);
            input.Health.Connectivity.front().Report.Piles["s"].HasNodeQuorum = true;
            AssertUpdate(input.DecideAt(), {{"b", EPileState::Disconnected}});
        }
    }

    Y_UNIT_TEST(SurvivorQuorumMustBeSeenByTheReceivingSide) {
        TPolicyCase input;
        input.Down("c");
        input.Confirm();
        // b's own report shows its quorum; a, the only endpoint, sees b without it.
        input.Health.Connectivity.front().Report.Piles["b"].HasNodeQuorum = false;
        input.Endpoints = {input.Endpoint("a")};
        AssertBlocked(input.DecideAt(), EBlockerReason::NoMutationEndpoint);
        input.Endpoints.push_back(input.Endpoint("b"));
        const auto decision = input.DecideAt();
        AssertUpdate(decision, {{"c", EPileState::Disconnected}});
        UNIT_ASSERT_VALUES_EQUAL(decision.Plan->Endpoint, "grpc://b:2135");
    }

    // D3: fallback RED vetoes a candidate only without fresh BSC-mode replies.
    Y_UNIT_TEST(FallbackRedDoesNotVetoABscHealthyCandidate) {
        TPolicyCase input;
        input.Down("a", EPileVerdict::Lost);
        input.Health.Piles["b"].FallbackRed = true;
        input.Confirm();
        AssertUpdate(input.DecideAt(), {{"a", EPileState::Disconnected}, {"b", EPileState::Primary}});
        input.Health.Piles["a"].Verdict = EPileVerdict::Failed;
        AssertUpdate(input.DecideAt(), {{"a", EPileState::Disconnected}, {"b", EPileState::Primary}});
        input.Health.Piles["b"] = Verdict(EPileVerdict::Unknown);
        input.Health.Piles["c"] = Verdict(EPileVerdict::Unknown);
        AssertBlocked(input.DecideAt(), EBlockerReason::NoCandidate);
    }

    Y_UNIT_TEST(PartitionWithBscOnPrimarySideDisconnectsTheOtherSide) {
        TComposite input({{"a", EPileState::Primary}, {"b", EPileState::Synchronized}});
        input.AddSide("a", true, {"b"});
        input.AddSide("b", false, {"a"});
        const auto decision = input.Run();
        AssertUpdate(decision, {{"b", EPileState::Disconnected}});
        UNIT_ASSERT_VALUES_EQUAL(decision.Plan->Endpoint, "a:2135");
    }

    Y_UNIT_TEST(PartitionWithBscOnPromotedSidePromotesIt) {
        TComposite input({{"a", EPileState::Primary}, {"b", EPileState::Promoted}});
        input.AddSide("a", false, {"b"});
        input.AddSide("b", true, {"a"});
        const auto decision = input.Run();
        AssertUpdate(decision, {{"a", EPileState::Disconnected}, {"b", EPileState::Primary}});
        UNIT_ASSERT_VALUES_EQUAL(decision.Plan->Endpoint, "b:2135");
    }

    Y_UNIT_TEST(PartitionWithFallbackOnBothSidesIsUnknown) {
        TComposite input({{"a", EPileState::Primary}, {"b", EPileState::Synchronized}});
        input.AddSide("a", false, {"b"});
        input.AddSide("b", false, {"a"});
        const auto decision = input.Run();
        UNIT_ASSERT(decision.Kind == EDecisionKind::Unknown);
        UNIT_ASSERT(!decision.Plan);
    }

    Y_UNIT_TEST(UnobservableBscHostSideIsLostAndReplacedThroughFallback) {
        TComposite input({{"a", EPileState::Primary}, {"b", EPileState::Synchronized}});
        input.AddSide("a", true, {});
        input.AddSide("b", false, {"a"});
        input.Silence("a");
        const auto decision = input.Run();
        AssertUpdate(decision, {{"a", EPileState::Disconnected}, {"b", EPileState::Primary}});
        UNIT_ASSERT_VALUES_EQUAL(decision.Plan->Endpoint, "b:2135");
    }

    Y_UNIT_TEST(StageTwoPileWithoutValidReportIsUnknownNotStable) {
        TComposite input({{"a", EPileState::Primary}, {"b", EPileState::Synchronized}, {"c", EPileState::NotSynchronized}});
        input.AddSelfCheck("a", true, {});
        const auto decision = input.Run();
        UNIT_ASSERT(decision.Kind == EDecisionKind::Unknown);
        UNIT_ASSERT_VALUES_EQUAL(decision.Reason, "UNKNOWN: 'c'");
    }

    Y_UNIT_TEST(NoEligibleEndpointAndPendingMutationAreDistinctBlockers) {
        TPolicyCase input;
        input.Down("b");
        input.Confirm();
        AssertBlocked(input.DecideAt({"grpc://a:2135", "grpc://c:2135"}), EBlockerReason::NoMutationEndpoint);
        input.Pending = TPendingMutation{7, {{"c", EPileState::Disconnected}}};
        AssertBlocked(input.DecideAt(), EBlockerReason::PendingMutation);
        input.Pending->Update = {{"b", EPileState::Disconnected}};
        AssertUpdate(input.DecideAt(), input.Pending->Update);
        input.Pending->ExpectedGeneration = 6;
        input.Pending->Update = {{"c", EPileState::Disconnected}};
        AssertUpdate(input.DecideAt(), {{"b", EPileState::Disconnected}});
    }

    Y_UNIT_TEST(ConversionAndUnrelatedPendingOperationDoNotBlockFailover) {
        TPolicyCase input;
        input.Add("inactive", EPileState::Inactive);
        input.View.Pending = TPendingOperation{"ADD", TString("inactive"), "retrying"};
        input.View.ConversionInProgress = true;
        input.Down("b");
        input.Confirm();
        AssertUpdate(input.DecideAt(), {{"b", EPileState::Disconnected}});
    }

    Y_UNIT_TEST(EmptyPileNameCanBePrimaryAndFailoverTarget) {
        TPolicyCase input;
        input.View.Piles.erase("a");
        input.Health.Piles.erase("a");
        input.Health.Participating.erase("a");
        input.Add("", EPileState::Primary);
        input.Down("");
        input.Confirm();
        AssertUpdate(input.DecideAt(), {{"", EPileState::Disconnected}, {"b", EPileState::Primary}});
    }

    Y_UNIT_TEST(CliSetFormQuotesEndpointPileAndGeneration) {
        TPlan plan{7, {{"b'\n$(touch pwned)", EPileState::Disconnected}}, "grpc://host'\n$(touch pwned):2135"};
        UNIT_ASSERT_VALUES_EQUAL(RenderCliCommand(plan),
            "ydb -e 'grpc://host'\\''\n$(touch pwned):2135' admin cluster bridge update"
            " --set 'b'\\''\n$(touch pwned):DISCONNECTED' --expected-generation '7'");
    }

    Y_UNIT_TEST(CliFileFormUsesActualParserSchemaAndQuotesJson) {
        TPlan plan{7, {{"", EPileState::Disconnected}, {"b:\"\n'", EPileState::Primary}}, "grpc://b:2135"};
        const auto command = RenderCliCommand(plan);
        UNIT_ASSERT(command.StartsWith("printf '%s' '"));
        UNIT_ASSERT(command.Contains(" | ydb -e 'grpc://b:2135' admin cluster bridge update --file '/dev/stdin' --expected-generation '7'"));
        // Decode the shell's single-quoted word, including escaped apostrophes.
        const auto begin = TString("printf '%s' ").size();
        const auto end = command.find(" | ydb -e ");
        TString json;
        bool quoted = false;
        for (size_t i = begin; i < end; ++i) {
            if (command[i] == '\'') {
                quoted = !quoted;
            } else if (!quoted && command[i] == '\\') {
                json += command[++i];
            } else {
                json += command[i];
            }
        }
        NJson::TJsonValue value;
        UNIT_ASSERT(NJson::ReadJsonTree(json, &value));
        UNIT_ASSERT(value.IsArray());
        UNIT_ASSERT_VALUES_EQUAL(value.GetArray().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(value[0]["pile_name"].GetString(), "");
        UNIT_ASSERT_VALUES_EQUAL(value[0]["state"].GetString(), "DISCONNECTED");
        UNIT_ASSERT_VALUES_EQUAL(value[1]["pile_name"].GetString(), "b:\"\n'");
        UNIT_ASSERT_VALUES_EQUAL(value[1]["state"].GetString(), "PRIMARY");
        plan.Update = {{"colon:name", EPileState::Disconnected}};
        UNIT_ASSERT(RenderCliCommand(plan).Contains("--file '/dev/stdin'"));
    }

    // S1 (B2): a same-primary superset plan replaces the pending record.
    Y_UNIT_TEST(SupersetReplacementAtSameGenerationIsAllowed) {
        TPolicyCase input;
        input.Down("b");
        input.Down("c");
        input.Confirm();
        input.Pending = TPendingMutation{7, {{"b", EPileState::Disconnected}}};
        AssertUpdate(input.DecideAt(), {{"b", EPileState::Disconnected}, {"c", EPileState::Disconnected}});
    }

    // S2 (B2): a plan that disagrees with the pending record on the primary stays blocked.
    Y_UNIT_TEST(OppositeSidePlanAtSameGenerationStaysBlocked) {
        TPolicyCase input;
        input.Down("a");
        input.Confirm();
        // The pending plan keeps a as primary; the new plan makes b primary.
        input.Pending = TPendingMutation{7, {{"b", EPileState::Disconnected}}};
        AssertBlocked(input.DecideAt(), EBlockerReason::PendingMutation);
        // The reverse: the pending plan moved the primary, the new plan keeps it.
        TPolicyCase reverse;
        reverse.Down("b");
        reverse.Confirm();
        reverse.Pending = TPendingMutation{7, {{"a", EPileState::Disconnected}, {"b", EPileState::Primary}}};
        AssertBlocked(reverse.DecideAt(), EBlockerReason::PendingMutation);
    }

    // S3 (B3/B4): a Lost stored primary fails over to the PROMOTED pile through fallback eligibility.
    Y_UNIT_TEST(LostStoredPrimaryWithPromotedPileFailsOverWithoutBsc) {
        TPolicyCase input;
        input.View.Piles["c"] = EPileState::Promoted;
        input.Down("a", EPileVerdict::Lost);
        input.Health.BscAvailable = false;
        input.Health.Piles["b"] = Verdict(EPileVerdict::Unknown);
        input.Health.Piles["c"] = Verdict(EPileVerdict::Unknown);
        input.Health.Piles["c"].SelfQuorum = true;
        input.Confirm();
        AssertUpdate(input.DecideAt(), {{"a", EPileState::Disconnected}, {"c", EPileState::Primary}});
        input.Health.Piles["c"].FallbackRed = true;
        AssertBlocked(input.DecideAt(), EBlockerReason::NoCandidate);
    }

    // S4 (B3/B4): a Replicating primary does not block disconnecting a confirmed failed pile.
    Y_UNIT_TEST(ReplicatingPrimaryDoesNotBlockDisconnectingAConfirmedFailedPile) {
        TPolicyCase input;
        input.Health.Piles["a"] = Verdict(EPileVerdict::Replicating);
        input.Down("b");
        input.Confirm();
        const auto decision = input.DecideAt();
        AssertUpdate(decision, {{"b", EPileState::Disconnected}});
        UNIT_ASSERT_VALUES_EQUAL(decision.Plan->Endpoint, "grpc://a:2135");
    }

    // S5 (B3/B4): a primary in its BSC warm-up does not block disconnecting a confirmed failed pile.
    Y_UNIT_TEST(WarmingUpPrimaryDoesNotBlockDisconnectingAConfirmedFailedPile) {
        TComposite input({{"a", EPileState::Primary}, {"b", EPileState::Synchronized}});
        input.Params.BscWarmup = TDuration::Seconds(30);
        input.AddSide("a", true, {"b"});
        const auto decision = input.Run();
        AssertUpdate(decision, {{"b", EPileState::Disconnected}});
        UNIT_ASSERT_VALUES_EQUAL(decision.Plan->Endpoint, "a:2135");
    }

    // S6 (B9): confirmation survives a generation bump with unchanged states and roles.
    Y_UNIT_TEST(ConfirmationSurvivesAGenerationBumpWithUnchangedStateAndRole) {
        TPolicyCase input;
        input.Down("b");
        input.Confirmations.Update(input.View, input.Health.Piles, At(100));
        input.Health.Piles["b"].NewestEvidenceSentAt = At(115);
        input.Confirmations.Update(input.View, input.Health.Piles, At(115));
        // A no-op bump: the same states and roles at the next generation.
        input.View.Generation = 8;
        input.Confirmations.Update(input.View, input.Health.Piles, At(116));
        input.Health.Piles["b"].NewestEvidenceSentAt = At(120);
        input.Confirmations.Update(input.View, input.Health.Piles, At(120));
        const auto decision = input.DecideAt();
        UNIT_ASSERT(decision.Kind == EDecisionKind::Ready);
        UNIT_ASSERT(decision.Plan);
        UNIT_ASSERT_VALUES_EQUAL(decision.Plan->ExpectedGeneration, 8);
        UNIT_ASSERT(decision.Plan->Update == TExactUpdate({{"b", EPileState::Disconnected}}));
    }

    // S7 (B9): a state or role change on the bumped generation restarts confirmation.
    Y_UNIT_TEST(RoleChangeOnTheBumpedGenerationRestartsConfirmation) {
        for (const bool roleChange : {false, true}) {
            TPolicyCase input;
            input.Down("b");
            input.Confirmations.Update(input.View, input.Health.Piles, At(100));
            input.Health.Piles["b"].NewestEvidenceSentAt = At(115);
            input.Confirmations.Update(input.View, input.Health.Piles, At(115));
            input.View.Generation = 8;
            if (roleChange) {
                input.View.Piles["c"] = EPileState::Promoted;
            } else {
                input.View.Piles["b"] = EPileState::NotSynchronized;
            }
            input.Confirmations.Update(input.View, input.Health.Piles, At(116));
            input.Health.Piles["b"].NewestEvidenceSentAt = At(120);
            input.Confirmations.Update(input.View, input.Health.Piles, At(120));
            UNIT_ASSERT(input.DecideAt().Kind == EDecisionKind::Confirming);
        }
    }

    // B9: a bump that changes an unrelated pile keeps the confirmed pile's window.
    Y_UNIT_TEST(UnrelatedStateChangeOnTheBumpKeepsConfirmation) {
        TPolicyCase input;
        input.Add("d", EPileState::NotSynchronized);
        input.Down("b");
        input.Confirmations.Update(input.View, input.Health.Piles, At(100));
        input.Health.Piles["b"].NewestEvidenceSentAt = At(115);
        input.Confirmations.Update(input.View, input.Health.Piles, At(115));
        input.View.Generation = 8;
        input.View.Piles["d"] = EPileState::Synchronized;
        input.Health.Piles["d"] = Verdict(EPileVerdict::Healthy);
        input.Confirmations.Update(input.View, input.Health.Piles, At(116));
        input.Health.Piles["b"].NewestEvidenceSentAt = At(120);
        input.Confirmations.Update(input.View, input.Health.Piles, At(120));
        const auto decision = input.DecideAt();
        UNIT_ASSERT(decision.Kind == EDecisionKind::Ready);
        UNIT_ASSERT_VALUES_EQUAL(decision.Plan->ExpectedGeneration, 8);
    }

    // S11b (P1): binding connectivity reports count for the survivor quorum at dispatch.
    Y_UNIT_TEST(BindingReportsDoNotBlockDispatch) {
        TComposite input({{"a", EPileState::Primary}, {"b", EPileState::Synchronized}});
        input.AddSide("a", true, {"b"});
        input.Replies.Connectivity.front().Report.BindingInProgress = true;
        const auto decision = input.Run();
        AssertUpdate(decision, {{"b", EPileState::Disconnected}});
        UNIT_ASSERT_VALUES_EQUAL(decision.Plan->Endpoint, "a:2135");
    }

    // S13 (D4): GREY on a candidate's own parts vetoes its promotion.
    Y_UNIT_TEST(GreyOnTheCandidatesOwnPartsVetoesPromotion) {
        // Without GREY on its own parts, b's self quorum promotes it after a is Lost.
        TComposite promoted({{"a", EPileState::Primary}, {"b", EPileState::Synchronized}});
        promoted.AddSide("b", false, {"a"});
        AssertUpdate(promoted.Run(), {{"a", EPileState::Disconnected}, {"b", EPileState::Primary}});
        // A fresh reply with GREY "no data" parts attributed to b vetoes the promotion.
        TComposite vetoed({{"a", EPileState::Primary}, {"b", EPileState::Synchronized}});
        vetoed.AddSide("b", false, {"a"}, {"b"});
        AssertBlocked(vetoed.Run(), EBlockerReason::NoCandidate);
    }
}

Y_UNIT_TEST_SUITE(TLifecyclePlanning) {
    Y_UNIT_TEST(PublicStateActionMatrixAndHealthGates) {
        for (const auto state : {EPileState::Synchronized, EPileState::Promoted, EPileState::NotSynchronized,
                EPileState::Suspended, EPileState::Disconnected, EPileState::Inactive, static_cast<EPileState>(999)})
        {
            TPolicyCase c;
            c.Add("b", state);
            c.Endpoints = {c.Endpoint("a"), c.Endpoint("b")};
            TViewSelection selection;
            selection.View = c.View;
            const auto stable = c.DecideAt();
            for (const auto action : {EOperationKind::Switchover, EOperationKind::TakeOffline, EOperationKind::Rejoin}) {
                const auto result = PlanLifecycle(action, "b", selection, c.Health, c.Confirmations, stable, {}, c.Endpoints);
                const bool expected = state == EPileState::Synchronized && action != EOperationKind::Rejoin
                    || state == EPileState::Disconnected && action == EOperationKind::Rejoin;
                UNIT_ASSERT_VALUES_EQUAL_C(result.Plan.has_value(), expected, ToString(state) << " / " << ToString(action));
                UNIT_ASSERT(!PlanLifecycle(action, "a", selection, c.Health, c.Confirmations, stable, {}, c.Endpoints).Plan);
            }
        }
        TPolicyCase c;
        c.Endpoints = {c.Endpoint("a"), c.Endpoint("b")};
        c.Health.Piles["b"].Verdict = EPileVerdict::Replicating;
        TViewSelection selection;
        selection.View = c.View;
        UNIT_ASSERT(!PlanLifecycle(EOperationKind::Switchover, "b", selection, c.Health, c.Confirmations, c.DecideAt(), {}, c.Endpoints).Plan);
        UNIT_ASSERT(PlanLifecycle(EOperationKind::TakeOffline, "b", selection, c.Health, c.Confirmations, c.DecideAt(), {}, c.Endpoints).Plan);
        c.Add("d", EPileState::NotSynchronized);
        for (auto& report : c.Health.Connectivity) {
            report.Report.Piles.erase("d");
        }
        selection.View = c.View;
        UNIT_ASSERT(!PlanLifecycle(EOperationKind::TakeOffline, "b", selection, c.Health, c.Confirmations, c.DecideAt(), {}, c.Endpoints).Plan);
    }

    Y_UNIT_TEST(TakeOfflineAllowsReplicatingPrimaryAndReplicatingTarget) {
        // Pin both directions of the documented Healthy/Replicating gate:
        // a Replicating primary may remain while Healthy b is taken offline,
        // and Replicating b may leave while the primary remains Healthy.
        for (const TString replicatingPile : {"a", "b"}) {
            TPolicyCase c;
            c.View.Piles.clear();
            c.Health = {};
            c.Health.BscAvailable = true;
            c.Add("a", EPileState::Primary);
            c.Add("b", EPileState::Synchronized);
            c.Health.Piles[replicatingPile] = Verdict(EPileVerdict::Replicating);
            c.Endpoints = {c.Endpoint("a"), c.Endpoint("b")};
            TViewSelection selection;
            selection.View = c.View;
            const auto stable = c.DecideAt();
            UNIT_ASSERT(stable.Kind == EDecisionKind::Stable);
            const auto offline = PlanLifecycle(EOperationKind::TakeOffline, "b", selection, c.Health,
                c.Confirmations, stable, {}, c.Endpoints);
            UNIT_ASSERT_C(offline.Plan, "Replicating pile: " << replicatingPile);
            AssertUpdate(offline, {{"b", EPileState::Suspended}});
            UNIT_ASSERT_VALUES_EQUAL(offline.Plan->Endpoint, c.Endpoint("a").Endpoint);
        }
    }

    Y_UNIT_TEST(ExactRolesAndOfflineDispatch) {
        TPolicyCase c;
        c.View.Piles["c"] = EPileState::Promoted;
        c.Endpoints = {c.Endpoint("b"), c.Endpoint("c")};
        TViewSelection selection;
        selection.View = c.View;
        const auto failover = c.DecideAt();
        auto plan = PlanLifecycle(EOperationKind::Switchover, "b", selection, c.Health,
            c.Confirmations, failover, {}, c.Endpoints);
        UNIT_ASSERT(plan.Plan);
        UNIT_ASSERT(plan.Plan->Update == (TExactUpdate{{"b", EPileState::Promoted}, {"c", EPileState::Synchronized}}));
        plan = PlanLifecycle(EOperationKind::TakeOffline, "b", selection, c.Health,
            c.Confirmations, failover, {}, c.Endpoints);
        UNIT_ASSERT(plan.Plan);
        UNIT_ASSERT_VALUES_EQUAL(plan.Plan->Endpoint, c.Endpoint("c").Endpoint);
        UNIT_ASSERT(plan.Plan->Update == (TExactUpdate{{"b", EPileState::Suspended}}));
    }

    Y_UNIT_TEST(BscProvenStageTwoUnknownBlocksEveryLifecycleAction) {
        TPolicyCase c;
        c.Add("c", EPileState::NotSynchronized);
        c.Add("d", EPileState::Disconnected);
        for (auto& report : c.Health.Connectivity) {
            report.Report.Piles["c"].InConfigQuorum = false;
        }
        c.Health.Participation["c"] = {true, At(100)}; // fresh BSC part overrides connectivity
        c.Health.Piles["c"] = Verdict(EPileVerdict::Unknown);
        TViewSelection selection;
        selection.View = c.View;
        const auto failover = c.DecideAt();
        UNIT_ASSERT(failover.Kind == EDecisionKind::Unknown);
        for (const auto kind : {EOperationKind::Switchover, EOperationKind::TakeOffline, EOperationKind::Rejoin}) {
            const TString target = kind == EOperationKind::Rejoin ? "d" : "b";
            UNIT_ASSERT(!PlanLifecycle(kind, target, selection, c.Health, c.Confirmations,
                failover, {}, c.Endpoints).Plan);
        }
        c.Health.Piles["c"].Verdict = EPileVerdict::Replicating;
        UNIT_ASSERT(PlanLifecycle(EOperationKind::TakeOffline, "b", selection, c.Health,
            c.Confirmations, c.DecideAt(), {}, c.Endpoints).Plan);
        c.Health.Participation["c"].Value.reset();
        c.Health.Piles["c"].Verdict = EPileVerdict::Unknown;
        UNIT_ASSERT(!PlanLifecycle(EOperationKind::TakeOffline, "b", selection, c.Health,
            c.Confirmations, c.DecideAt(), {}, c.Endpoints).Plan);
        c.Health.Participation["c"] = {false, At(100)};
        c.Health.Participating.erase("c");
        c.Health.Piles.erase("c");
        UNIT_ASSERT(PlanLifecycle(EOperationKind::TakeOffline, "b", selection, c.Health,
            c.Confirmations, c.DecideAt(), {}, c.Endpoints).Plan);
    }

    Y_UNIT_TEST(UnsafeMaintenanceAndLifecyclePendingNeverAdmitFailoverSupersets) {
        TPolicyCase c;
        TViewSelection selection;
        selection.View = c.View;
        c.Health.Piles["a"].Verdict = EPileVerdict::Unknown;
        UNIT_ASSERT(!PlanLifecycle(EOperationKind::TakeOffline, "b", selection, c.Health,
            c.Confirmations, c.DecideAt(), {}, c.Endpoints).Plan);
        c.Down("b");
        c.Confirm();
        c.Pending = TPendingMutation{7, {{"b", EPileState::Disconnected}}, EOperationKind::Rejoin, EDispatchOrigin::Manual};
        UNIT_ASSERT(!c.DecideAt().Plan);
    }

    Y_UNIT_TEST(RejoinNeverRefreshesSynchronizingPileAndPendingTargetBlocks) {
        TPolicyCase c;
        c.Add("d", EPileState::Disconnected);
        c.Endpoints = {c.Endpoint("a")};
        TViewSelection selection;
        selection.View = c.View;
        const auto stable = c.DecideAt();
        auto plan = PlanLifecycle(EOperationKind::Rejoin, "d", selection, c.Health,
            c.Confirmations, stable, {}, c.Endpoints);
        UNIT_ASSERT(plan.Plan);
        UNIT_ASSERT(plan.Plan->Update == (TExactUpdate{{"d", EPileState::NotSynchronized}}));
        selection.View->Pending = TPendingOperation{"ADD", TString("d"), ""};
        UNIT_ASSERT(!PlanLifecycle(EOperationKind::Rejoin, "d", selection, c.Health,
            c.Confirmations, stable, {}, c.Endpoints).Plan);
        selection.View->Pending.reset();
        selection.View->Piles["d"] = EPileState::NotSynchronized;
        UNIT_ASSERT(!PlanLifecycle(EOperationKind::Rejoin, "d", selection, c.Health,
            c.Confirmations, stable, {}, c.Endpoints).Plan);
    }
}

} // namespace NBridgeSkipper
