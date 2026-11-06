#include <ydb/apps/bridge_skipper/domain/health.h>
#include <ydb/apps/bridge_skipper/domain/mutation.h>
#include <ydb/public/api/protos/ydb_monitoring.pb.h>

#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>
#include <google/protobuf/text_format.h>
#include <util/stream/file.h>

namespace NBridgeSkipper {
namespace {

TClusterView View() {
    TClusterView view;
    view.Generation = 7;
    view.Piles = {{"a", EPileState::Primary}, {"b", EPileState::Synchronized}};
    view.SentAt = TMonotonic::Seconds(100);
    return view;
}

Ydb::Monitoring::SelfCheckResult Fixture(const TString& name) {
    TFileInput input(ArcadiaSourceRoot() + "/ydb/apps/bridge_skipper/domain/ut/fixtures/" + name + ".pbtxt");
    Ydb::Monitoring::SelfCheckResult result;
    UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(input.ReadAll(), &result));
    return result;
}

TSelfCheckClassification Classify(const TString& name) {
    return ClassifySelfCheck(Fixture(name));
}

struct TScenario {
    TClusterView Cluster = View();
    THealthReplies Replies;
    THealthParams Params;
    THealthState State;
    TMonotonic Now = TMonotonic::Seconds(100);

    TScenario() {
        Params.BscWarmup = TDuration::Zero();
        AddSelfCheck("a", {});
    }

    void AddSelfCheck(const TString& pile, TSelfCheckClassification classification) {
        TEndpointState endpoint;
        endpoint.Endpoint = pile + ":2135";
        endpoint.Pile = pile;
        endpoint.Identity = EEndpointIdentity::Verified;
        endpoint.Current = true;
        Replies.Endpoints.push_back(endpoint);
        TSelfCheckReply reply;
        reply.Endpoint = endpoint.Endpoint;
        reply.ResponderPile = pile;
        reply.SentAt = Now;
        reply.Classification = std::move(classification);
        Replies.SelfChecks.push_back(std::move(reply));
    }

    void AddConnectivity(const TString& pile, bool aQuorum, bool bQuorum, bool bConfig = true) {
        TConnectivity report;
        report.VantagePile = pile;
        report.Generation = Cluster.Generation;
        report.SentAt = Now;
        report.Piles["a"] = {3, aQuorum ? 3u : 0u, aQuorum, true, true};
        report.Piles["b"] = {3, bQuorum ? 3u : 0u, bQuorum, bConfig, bConfig};
        Replies.Connectivity.push_back({pile + ":2135", report});
    }

    THealthEvaluation Evaluate() {
        return EvaluatePiles(Cluster, Replies, Now, Params, State);
    }

    void Refresh(ui64 seconds) {
        Now = TMonotonic::Seconds(seconds);
        for (auto& reply : Replies.SelfChecks) {
            reply.SentAt = Now;
        }
        for (auto& reply : Replies.Connectivity) {
            reply.Report.SentAt = Now;
        }
    }
};

void AssertVerdict(const THealthEvaluation& evaluation, const TString& pile, EPileVerdict expected) {
    UNIT_ASSERT(evaluation.Piles.contains(pile));
    UNIT_ASSERT(evaluation.Piles.at(pile).Verdict == expected);
    UNIT_ASSERT(!evaluation.Piles.at(pile).Reasons.empty());
}

} // namespace

Y_UNIT_TEST_SUITE(BridgeSkipperHealth) {
    Y_UNIT_TEST(ValidatedConnectivityKeepsQueriedEndpointForRouting) {
        for (const unsigned invalid : {0u, 1u, 2u, 3u}) {
            TScenario s;
            s.Replies.Endpoints.front().Endpoint = "a1";
            s.Replies.SelfChecks.front().Endpoint = "a1";
            s.AddSelfCheck("a", {});
            s.Replies.Endpoints.back().Endpoint = "a2";
            s.Replies.SelfChecks.back().Endpoint = "a2";
            s.AddConnectivity("a", false, true);
            s.Replies.Connectivity.back().Endpoint = "a1";
            s.AddConnectivity("a", true, true);
            s.Replies.Connectivity.back().Endpoint = "a2";
            // A forwarded reporter id is not the queried receiving endpoint.
            s.Replies.Connectivity.back().Report.ReporterNodeId = 999;
            if (invalid == 1) {
                s.Replies.Connectivity.back().Report.SentAt = TMonotonic::Seconds(69);
            } else if (invalid == 2) {
                s.Replies.Endpoints.back().Identity = EEndpointIdentity::Mismatch;
            } else if (invalid == 3) {
                --s.Replies.Connectivity.back().Report.Generation;
            }
            const auto health = s.Evaluate();
            const auto endpoint = SelectMutationEndpoint(s.Cluster, s.Replies.Endpoints,
                {{"b", EPileState::Disconnected}}, health.Connectivity);
            if (invalid) {
                UNIT_ASSERT(!endpoint);
            } else {
                UNIT_ASSERT(endpoint);
                UNIT_ASSERT_VALUES_EQUAL(*endpoint, "a2");
            }
        }
    }

    Y_UNIT_TEST(FallbackGreyAndFallbackRedAreUnknown) {
        for (const TString name : {"fallback_grey", "fallback_red"}) {
            TScenario s;
            s.Replies.SelfChecks.front().Classification = Classify(name);
            const auto evaluation = s.Evaluate();
            UNIT_ASSERT(!evaluation.BscAvailable);
            AssertVerdict(evaluation, "a", EPileVerdict::Unknown);
            AssertVerdict(evaluation, "b", EPileVerdict::Unknown);
            UNIT_ASSERT(evaluation.Piles.at("b").FallbackRed);
            UNIT_ASSERT(!evaluation.Piles.at("a").FallbackRed);
        }
    }

    Y_UNIT_TEST(RedPartAndInheritedParentAreOnePilePart) {
        auto raw = Fixture("bsc_red");
        auto classification = ClassifySelfCheck(raw);
        UNIT_ASSERT_VALUES_EQUAL(classification.Parts.size(), 1);
        UNIT_ASSERT(classification.Parts.at("b") == EPart::RedPart);
        raw.mutable_issue_log(1)->set_status(Ydb::Monitoring::StatusFlag::RED);
        classification = ClassifySelfCheck(raw);
        UNIT_ASSERT_VALUES_EQUAL(classification.Parts.size(), 1);
        TScenario s;
        s.Replies.SelfChecks.front().Classification = classification;
        auto evaluation = s.Evaluate();
        AssertVerdict(evaluation, "b", EPileVerdict::Failed);
        AssertVerdict(evaluation, "a", EPileVerdict::Healthy);
        UNIT_ASSERT_VALUES_EQUAL(evaluation.Piles.at("b").NewestEvidenceSentAt, s.Now);
    }

    // D4: a GREY "no data" group with a pile name is an unknown part; without one it is nothing.
    Y_UNIT_TEST(GreyNoDataGroupIsAnIncompletePartOnlyWithAPileName) {
        auto raw = Fixture("fallback_grey");
        auto classification = ClassifySelfCheck(raw);
        UNIT_ASSERT_VALUES_EQUAL(classification.Parts.size(), 1);
        UNIT_ASSERT(classification.Parts.at("b") == EPart::Incomplete);
        UNIT_ASSERT_VALUES_EQUAL(classification.UnnamedRed, 0);
        raw.mutable_issue_log(1)->clear_location();
        classification = ClassifySelfCheck(raw);
        UNIT_ASSERT(classification.Parts.empty());
        UNIT_ASSERT_VALUES_EQUAL(classification.UnnamedRed, 0);
    }

    Y_UNIT_TEST(UnattributedAndObservationIssuesDoNotBlamePiles) {
        auto raw = Fixture("unattributed");
        for (const TString type : {"PILE_STATE_STORAGE", "PILE_SCHEME_BOARD", "PILE_BOARD", "BRIDGE_PILE"}) {
            auto* issue = raw.add_issue_log();
            issue->set_type(type);
            issue->set_status(Ydb::Monitoring::StatusFlag::RED);
            issue->mutable_location()->mutable_compute()->mutable_pile()->set_name("b");
        }
        const auto classification = ClassifySelfCheck(raw);
        UNIT_ASSERT_VALUES_EQUAL(classification.UnnamedRed, 1);
        UNIT_ASSERT_VALUES_EQUAL(classification.Parts.size(), 1);
        UNIT_ASSERT(classification.Parts.at("17") == EPart::RedPart);
        UNIT_ASSERT_VALUES_EQUAL(classification.Notes.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(classification.Notes.back(), "BRIDGE_PILE RED pile=b: ");
        UNIT_ASSERT_VALUES_EQUAL(classification.SelfCheckResult, "EMERGENCY");
        TScenario s;
        s.Replies.SelfChecks.front().Classification = classification;
        const auto evaluation = s.Evaluate();
        AssertVerdict(evaluation, "b", EPileVerdict::Healthy);
    }

    Y_UNIT_TEST(CleanBscReplyWithPileIssuesIsCleanAndNoted) {
        const auto classification = Classify("bsc_clean_with_pile_issues");
        UNIT_ASSERT(classification.Mode == ESelfCheckMode::Bsc);
        UNIT_ASSERT(classification.Parts.empty());
        UNIT_ASSERT_VALUES_EQUAL(classification.Notes.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(classification.Notes.front(),
            "PILE_STATE_STORAGE RED pile=b: There is not enough functional rings");
        UNIT_ASSERT_VALUES_EQUAL(classification.OmittedNotes, 0);
        TScenario s;
        s.Replies.SelfChecks.front().Classification = classification;
        const auto evaluation = s.Evaluate();
        AssertVerdict(evaluation, "a", EPileVerdict::Healthy);
        AssertVerdict(evaluation, "b", EPileVerdict::Healthy);
    }

    Y_UNIT_TEST(NotesAreBoundedAndCounted) {
        auto raw = Fixture("bsc_clean_with_pile_issues");
        for (size_t i = 0; i < MaxSelfCheckNotes + 3; ++i) {
            *raw.add_issue_log() = raw.issue_log(0);
        }
        const auto classification = ClassifySelfCheck(raw);
        UNIT_ASSERT_VALUES_EQUAL(classification.Notes.size(), MaxSelfCheckNotes);
        UNIT_ASSERT_VALUES_EQUAL(classification.OmittedNotes, 4);
    }

    Y_UNIT_TEST(ReplicationRequiresOnlyExactBlueVDiskReasons) {
        auto raw = Fixture("blue_only");
        UNIT_ASSERT(ClassifySelfCheck(raw).Parts.at("b") == EPart::ReplicatingPart);
        TScenario s;
        s.Replies.SelfChecks.front().Classification = ClassifySelfCheck(raw);
        AssertVerdict(s.Evaluate(), "b", EPileVerdict::Replicating);
        raw.mutable_issue_log(3)->set_status(Ydb::Monitoring::StatusFlag::RED);
        UNIT_ASSERT(ClassifySelfCheck(raw).Parts.at("b") == EPart::RedPart);
        s.Replies.SelfChecks.front().Classification = ClassifySelfCheck(raw);
        AssertVerdict(s.Evaluate(), "b", EPileVerdict::Failed);
        raw.mutable_issue_log(3)->set_status(Ydb::Monitoring::StatusFlag::BLUE);
        raw.mutable_issue_log(3)->set_message("Replication in progress ");
        UNIT_ASSERT(ClassifySelfCheck(raw).Parts.at("b") == EPart::RedPart);
        raw.mutable_issue_log(3)->set_message("Replication in progress");
        raw.mutable_issue_log(1)->add_reason("missing");
        UNIT_ASSERT(ClassifySelfCheck(raw).Parts.at("b") == EPart::Incomplete);
        s.Replies.SelfChecks.front().Classification = ClassifySelfCheck(raw);
        AssertVerdict(s.Evaluate(), "b", EPileVerdict::Unknown);
    }

    Y_UNIT_TEST(ThrottledReasonIsNeutralForReplication) {
        auto raw = Fixture("blue_with_orange");
        UNIT_ASSERT(ClassifySelfCheck(raw).Parts.at("b") == EPart::ReplicatingPart);
        TScenario s;
        s.Replies.SelfChecks.front().Classification = ClassifySelfCheck(raw);
        AssertVerdict(s.Evaluate(), "b", EPileVerdict::Replicating);
        raw.mutable_issue_log(5)->set_status(Ydb::Monitoring::StatusFlag::YELLOW);
        UNIT_ASSERT(ClassifySelfCheck(raw).Parts.at("b") == EPart::ReplicatingPart);
        // At least one replication reason is still required.
        for (int i = 2; i < 5; ++i) {
            raw.mutable_issue_log(i)->set_status(Ydb::Monitoring::StatusFlag::GREEN);
        }
        UNIT_ASSERT(ClassifySelfCheck(raw).Parts.at("b") == EPart::RedPart);
        raw = Fixture("blue_with_orange");
        for (const auto status : {Ydb::Monitoring::StatusFlag::RED, Ydb::Monitoring::StatusFlag::GREY}) {
            raw.mutable_issue_log(5)->set_status(status);
            UNIT_ASSERT(ClassifySelfCheck(raw).Parts.at("b") == EPart::RedPart);
        }
    }

    Y_UNIT_TEST(PartPrecedencePreservesAnyRedGroup) {
        auto raw = Fixture("blue_only");
        *raw.add_issue_log() = Fixture("bsc_red").issue_log(2);
        raw.mutable_issue_log(raw.issue_log_size() - 1)->set_id("other");
        raw.mutable_issue_log(raw.issue_log_size() - 1)->clear_reason();
        UNIT_ASSERT(ClassifySelfCheck(raw).Parts.at("b") == EPart::RedPart);
        raw = Fixture("fallback_grey");
        *raw.add_issue_log() = Fixture("fallback_red").issue_log(1);
        UNIT_ASSERT(ClassifySelfCheck(raw).Parts.at("b") == EPart::FallbackRed);
        raw = Fixture("blue_only");
        *raw.add_issue_log() = Fixture("blue_only").issue_log(1);
        raw.mutable_issue_log(raw.issue_log_size() - 1)->set_id("other");
        raw.mutable_issue_log(raw.issue_log_size() - 1)->add_reason("missing");
        UNIT_ASSERT(ClassifySelfCheck(raw).Parts.at("b") == EPart::Incomplete);
        *raw.add_issue_log() = Fixture("bsc_red").issue_log(2);
        raw.mutable_issue_log(raw.issue_log_size() - 1)->set_id("red");
        raw.mutable_issue_log(raw.issue_log_size() - 1)->clear_reason();
        UNIT_ASSERT(ClassifySelfCheck(raw).Parts.at("b") == EPart::RedPart);
    }

    Y_UNIT_TEST(TruncationBlocksHealthAndReplicationButNotFailureAndYellowOnlyWarns) {
        auto raw = Fixture("truncated");
        TScenario s;
        s.Cluster.Piles["c"] = EPileState::Synchronized;
        s.Replies.SelfChecks.front().Classification = ClassifySelfCheck(raw);
        UNIT_ASSERT(s.Replies.SelfChecks.front().Classification.Truncated);
        UNIT_ASSERT(s.Replies.SelfChecks.front().Classification.Parts.at("b") == EPart::Incomplete);
        UNIT_ASSERT(s.Replies.SelfChecks.front().Classification.Parts.at("c") == EPart::RedPart);
        auto evaluation = s.Evaluate();
        AssertVerdict(evaluation, "a", EPileVerdict::Unknown);
        AssertVerdict(evaluation, "b", EPileVerdict::Unknown);
        AssertVerdict(evaluation, "c", EPileVerdict::Failed);
        for (const auto status : {Ydb::Monitoring::StatusFlag::YELLOW, Ydb::Monitoring::StatusFlag::ORANGE}) {
            raw.mutable_issue_log(raw.issue_log_size() - 1)->set_status(status);
            auto classification = ClassifySelfCheck(raw);
            UNIT_ASSERT(!classification.Truncated);
            UNIT_ASSERT(classification.SizeWarning);
            s.Replies.SelfChecks.front().Classification = classification;
            AssertVerdict(s.Evaluate(), "a", EPileVerdict::Healthy);
        }
    }

    Y_UNIT_TEST(DuplicateReasonIdIsIncompleteNotRed) {
        auto raw = Fixture("bsc_red");
        UNIT_ASSERT(ClassifySelfCheck(raw).Parts.at("b") == EPart::RedPart);
        // A second issue with the VDisk's id makes the reason ambiguous.
        auto* twin = raw.add_issue_log();
        twin->set_id("disk");
        twin->set_type("VDISK");
        twin->set_status(Ydb::Monitoring::StatusFlag::GREEN);
        UNIT_ASSERT(ClassifySelfCheck(raw).Parts.at("b") == EPart::Incomplete);
        TScenario s;
        s.Replies.SelfChecks.front().Classification = ClassifySelfCheck(raw);
        AssertVerdict(s.Evaluate(), "b", EPileVerdict::Unknown);
    }

    Y_UNIT_TEST(FallbackMarkerIsExact) {
        auto raw = Fixture("fallback_grey");
        UNIT_ASSERT(ClassifySelfCheck(raw).Mode == ESelfCheckMode::Fallback);
        UNIT_ASSERT(ClassifySelfCheck(raw).Notes.empty());
        for (const TString text : {"System tablet BSC didn't provide information ", "System tablet BSC didn't provide storage configuration"}) {
            raw.mutable_issue_log(0)->set_message(text);
            UNIT_ASSERT(ClassifySelfCheck(raw).Mode == ESelfCheckMode::Bsc);
        }
        raw = Fixture("fallback_grey");
        raw.mutable_issue_log(0)->set_status(Ydb::Monitoring::StatusFlag::ORANGE);
        UNIT_ASSERT(ClassifySelfCheck(raw).Mode == ESelfCheckMode::Bsc);
        raw = Fixture("fallback_grey");
        raw.mutable_issue_log(0)->set_type("STORAGE_GROUP");
        UNIT_ASSERT(ClassifySelfCheck(raw).Mode == ESelfCheckMode::Bsc);
    }

    Y_UNIT_TEST(Stage2ParticipatesAndStage1DoesNot) {
        TScenario s;
        s.Cluster.Piles["b"] = EPileState::NotSynchronized;
        s.Replies.SelfChecks.front().Classification = Classify("stage2");
        s.AddConnectivity("a", true, false);
        auto evaluation = s.Evaluate();
        UNIT_ASSERT(evaluation.Participating.contains("b"));
        AssertVerdict(evaluation, "b", EPileVerdict::Failed);
        s.Replies.SelfChecks.front().Classification = Classify("fallback_grey");
        AssertVerdict(s.Evaluate(), "b", EPileVerdict::Lost);
        s.Replies.Connectivity.front().Report.Piles["b"].InConfigQuorum = false;
        s.Replies.Connectivity.front().Report.Piles["b"].InDataQuorum = false;
        UNIT_ASSERT(!s.Evaluate().Piles.contains("b"));
        UNIT_ASSERT(!s.Evaluate().Participating.contains("b"));
        for (const auto state : {EPileState::Disconnected, EPileState::Inactive}) {
            s.Cluster.Piles["b"] = state;
            UNIT_ASSERT(!s.Evaluate().Piles.contains("b"));
        }
    }

    Y_UNIT_TEST(Stage2ParticipationNeedsAReportAndABindingReportCounts) {
        TScenario s;
        s.Cluster.Piles["b"] = EPileState::NotSynchronized;
        s.Replies.SelfChecks.front().Classification = Classify("stage2");
        for (const bool binding : {false, true}) {
            if (binding) {
                s.AddConnectivity("a", true, false);
                s.Replies.Connectivity.front().Report.BindingInProgress = true;
            }
            const auto evaluation = s.Evaluate();
            UNIT_ASSERT(evaluation.Participating.contains("b"));
            if (binding) {
                AssertVerdict(evaluation, "b", EPileVerdict::Failed);
                UNIT_ASSERT_VALUES_EQUAL(evaluation.Connectivity.size(), 1);
            } else {
                AssertVerdict(evaluation, "b", EPileVerdict::Failed);
            }
            UNIT_ASSERT(!s.State.CleanSince.contains("b"));
        }
    }

    Y_UNIT_TEST(LostNeedsUnanimousOutsideReportsAndNoInsideQuorum) {
        TScenario s;
        s.Replies.SelfChecks.front().Classification = Classify("fallback_grey");
        s.AddConnectivity("a", true, false);
        AssertVerdict(s.Evaluate(), "b", EPileVerdict::Lost);
        s.AddSelfCheck("b", Classify("fallback_grey"));
        s.AddConnectivity("b", false, true);
        const auto partition = s.Evaluate();
        AssertVerdict(partition, "a", EPileVerdict::Unknown);
        AssertVerdict(partition, "b", EPileVerdict::Unknown);
        UNIT_ASSERT(partition.Piles.at("b").SelfQuorum);
        // Reporter node/root do not redefine the configured endpoint's vantage.
        s.Replies.Connectivity.back().Report.ReporterNodeId = 12345;
        s.Replies.Connectivity.back().Report.ReporterIsRoot = true;
        AssertVerdict(s.Evaluate(), "b", EPileVerdict::Unknown);
        s.Replies.Connectivity.pop_back();
        s.Replies.Connectivity.front().Report.Piles.erase("b");
        AssertVerdict(s.Evaluate(), "b", EPileVerdict::Unknown);
    }

    Y_UNIT_TEST(OutsideReportsMustAgreeOnConfigQuorumWithoutNodeQuorum) {
        TScenario s;
        s.Replies.SelfChecks.front().Classification = Classify("fallback_grey");
        s.Cluster.Piles["c"] = EPileState::Synchronized;
        s.AddSelfCheck("c", Classify("fallback_grey"));
        s.AddConnectivity("a", true, false);
        s.AddConnectivity("c", true, true);
        AssertVerdict(s.Evaluate(), "b", EPileVerdict::Unknown);
        auto& b = s.Replies.Connectivity.back().Report.Piles["b"];
        b.HasNodeQuorum = false;
        AssertVerdict(s.Evaluate(), "b", EPileVerdict::Lost);
        b.InConfigQuorum = false;
        b.InDataQuorum = false;
        AssertVerdict(s.Evaluate(), "b", EPileVerdict::Unknown);
    }

    Y_UNIT_TEST(SuspendedOnlyHasLostRule) {
        TScenario s;
        s.Cluster.Piles["b"] = EPileState::Suspended;
        s.Replies.SelfChecks.front().Classification = Classify("bsc_red");
        AssertVerdict(s.Evaluate(), "b", EPileVerdict::Unknown);
        s.AddConnectivity("a", true, false);
        AssertVerdict(s.Evaluate(), "b", EPileVerdict::Lost);
        UNIT_ASSERT(!s.Evaluate().Participating.contains("b"));
        s.Replies.Connectivity.front().Report.Piles["b"].HasNodeQuorum = true;
        s.Replies.SelfChecks.front().Classification = {};
        AssertVerdict(s.Evaluate(), "b", EPileVerdict::Unknown);
    }

    Y_UNIT_TEST(SelfCheckValidityUsesSendTimeAndLatestResponderState) {
        for (ui32 gate = 0; gate < 5; ++gate) {
            TScenario s;
            auto& endpoint = s.Replies.Endpoints.front();
            auto& reply = s.Replies.SelfChecks.front();
            reply.Classification = Classify("bsc_red");
            switch (gate) {
                case 0: endpoint.Current = false; break;
                case 1: reply.SentAt = TMonotonic::Seconds(79); s.Now = TMonotonic::Seconds(110); break;
                case 2: endpoint.Identity = EEndpointIdentity::Mismatch; break;
                case 3: endpoint.Identity = EEndpointIdentity::Unknown; break;
                // A VERIFIED endpoint whose storage config is not ready.
                case 4: reply.ResponderPile.reset(); break;
            }
            const auto evaluation = s.Evaluate();
            UNIT_ASSERT(!evaluation.BscAvailable);
            AssertVerdict(evaluation, "b", EPileVerdict::Unknown);
        }
        TScenario boundary;
        boundary.Replies.SelfChecks.front().SentAt = boundary.Now - boundary.Params.EvidenceTtl;
        UNIT_ASSERT(boundary.Evaluate().BscAvailable);
    }

    Y_UNIT_TEST(ConnectivityValidityExcludesUnverifiedAndStale) {
        for (ui32 gate = 0; gate < 5; ++gate) {
            TScenario s;
            s.AddConnectivity("a", true, false);
            auto& report = s.Replies.Connectivity.front().Report;
            switch (gate) {
                case 0: report.Generation = 6; break;
                case 1: report.SentAt = TMonotonic::Seconds(69); break;
                case 2: report.SentAt = TMonotonic::Seconds(79); s.Now = TMonotonic::Seconds(110); break;
                case 3: s.Replies.Endpoints.front().Current = false; break;
                case 4: s.Replies.Endpoints.front().Identity = EEndpointIdentity::Mismatch; break;
            }
            UNIT_ASSERT(s.Evaluate().Connectivity.empty());
        }
    }

    Y_UNIT_TEST(EmptyPileNameNeedsExplicitIdentityAndLocationPresence) {
        auto raw = Fixture("bsc_red");
        raw.mutable_issue_log(2)->mutable_location()->mutable_storage()->mutable_pool()->mutable_group()->mutable_pile()->set_name("");
        const TNamedPileStates piles = {{"", EPileState::Primary}};
        UNIT_ASSERT(ClassifySelfCheck(raw).Parts.contains(""));
        raw.mutable_issue_log(2)->mutable_location()->mutable_storage()->mutable_pool()->mutable_group()->clear_pile();
        UNIT_ASSERT(!ClassifySelfCheck(raw).Parts.contains(""));
        UNIT_ASSERT_VALUES_EQUAL(ClassifySelfCheck(raw).UnnamedRed, 1);
        TScenario s;
        s.Cluster.Piles = piles;
        s.Replies = {};
        s.AddSelfCheck("", {});
        AssertVerdict(s.Evaluate(), "", EPileVerdict::Healthy);
        s.Replies.SelfChecks.front().ResponderPile.reset();
        AssertVerdict(s.Evaluate(), "", EPileVerdict::Unknown);
    }

    Y_UNIT_TEST(MixedBscRepliesAreUnknownAndReplicatingNeedsNoCleanReply) {
        TScenario s;
        s.Replies.SelfChecks.front().Classification = Classify("bsc_red");
        s.AddSelfCheck("b", {});
        AssertVerdict(s.Evaluate(), "b", EPileVerdict::Unknown);
        s.Replies.SelfChecks.back().Classification = Classify("blue_only");
        AssertVerdict(s.Evaluate(), "b", EPileVerdict::Replicating);
        s.Replies.SelfChecks.back().Classification = Classify("fallback_grey");
        AssertVerdict(s.Evaluate(), "b", EPileVerdict::Failed);
    }

    Y_UNIT_TEST(WarmupRequiresContinuousCleanAndResetsOnRolesOrFallback) {
        TScenario s;
        s.Params.BscWarmup = TDuration::Seconds(30);
        AssertVerdict(s.Evaluate(), "a", EPileVerdict::Unknown);
        s.Refresh(129);
        AssertVerdict(s.Evaluate(), "a", EPileVerdict::Unknown);
        s.Refresh(130);
        AssertVerdict(s.Evaluate(), "a", EPileVerdict::Healthy);
        s.Cluster.Piles["b"] = EPileState::Promoted;
        s.Refresh(131);
        AssertVerdict(s.Evaluate(), "a", EPileVerdict::Unknown);
        s.Refresh(161);
        AssertVerdict(s.Evaluate(), "a", EPileVerdict::Healthy);
        s.Cluster.Piles["a"] = EPileState::Synchronized;
        s.Cluster.Piles["b"] = EPileState::Primary;
        s.Refresh(162);
        AssertVerdict(s.Evaluate(), "a", EPileVerdict::Unknown);
        s.Refresh(192);
        AssertVerdict(s.Evaluate(), "a", EPileVerdict::Healthy);
        s.Replies.SelfChecks.front().Classification = Classify("fallback_grey");
        s.Refresh(193);
        AssertVerdict(s.Evaluate(), "a", EPileVerdict::Unknown);
        UNIT_ASSERT(s.State.CleanSince.empty());
        s.Replies.SelfChecks.front().Classification = {};
        s.Refresh(194);
        AssertVerdict(s.Evaluate(), "a", EPileVerdict::Unknown);
    }

    Y_UNIT_TEST(RoleChangeWarmupExcludesCachedPreChangeSends) {
        for (const bool promotion : {false, true}) {
            TScenario s;
            s.Params.BscWarmup = TDuration::Seconds(30);
            AssertVerdict(s.Evaluate(), "b", EPileVerdict::Unknown);
            s.Now = TMonotonic::Seconds(129);
            ++s.Cluster.Generation;
            if (promotion) {
                s.Cluster.Piles["b"] = EPileState::Promoted;
            } else {
                s.Cluster.Piles["a"] = EPileState::Synchronized;
                s.Cluster.Piles["b"] = EPileState::Primary;
            }
            // The latest successful health call was sent at 100. Fresh state
            // reads alone cannot make it support the new BSC's warm-up.
            AssertVerdict(s.Evaluate(), "b", EPileVerdict::Unknown);
            UNIT_ASSERT(s.State.CleanSince.empty());
            s.Refresh(130);
            AssertVerdict(s.Evaluate(), "b", EPileVerdict::Unknown);
            UNIT_ASSERT_VALUES_EQUAL(s.State.CleanSince.at("b").Since, TMonotonic::Seconds(130));
            s.Refresh(159);
            AssertVerdict(s.Evaluate(), "b", EPileVerdict::Unknown);
            s.Refresh(160);
            AssertVerdict(s.Evaluate(), "b", EPileVerdict::Healthy);
        }
    }

    Y_UNIT_TEST(WarmupMeasuresSendTimesAndRestartsAfterATtlGap) {
        TScenario s;
        s.Params.BscWarmup = TDuration::Seconds(30);
        // The reply sent at 100 arrives late; the window starts at its send time.
        s.Now = TMonotonic::Seconds(112);
        AssertVerdict(s.Evaluate(), "a", EPileVerdict::Unknown);
        UNIT_ASSERT_VALUES_EQUAL(s.State.CleanSince.at("a").Since, TMonotonic::Seconds(100));
        // No fresh reply at all keeps the window.
        s.Now = TMonotonic::Seconds(131);
        AssertVerdict(s.Evaluate(), "a", EPileVerdict::Unknown);
        UNIT_ASSERT_VALUES_EQUAL(s.State.CleanSince.at("a").Since, TMonotonic::Seconds(100));
        // Consecutive clean sends 20 s apart continue the window; 30 s of clean sends is Healthy.
        s.Replies.SelfChecks.front().SentAt = TMonotonic::Seconds(120);
        s.Now = TMonotonic::Seconds(132);
        AssertVerdict(s.Evaluate(), "a", EPileVerdict::Unknown);
        s.Replies.SelfChecks.front().SentAt = TMonotonic::Seconds(130);
        s.Now = TMonotonic::Seconds(142);
        AssertVerdict(s.Evaluate(), "a", EPileVerdict::Healthy);
        // A clean send more than one TTL after the previous one restarts the window.
        s.Replies.SelfChecks.front().SentAt = TMonotonic::Seconds(161);
        s.Now = TMonotonic::Seconds(162);
        AssertVerdict(s.Evaluate(), "a", EPileVerdict::Unknown);
        UNIT_ASSERT_VALUES_EQUAL(s.State.CleanSince.at("a").Since, TMonotonic::Seconds(161));
        s.Refresh(191);
        AssertVerdict(s.Evaluate(), "a", EPileVerdict::Healthy);
    }

    Y_UNIT_TEST(UnobservedBscRestartRequiresFullCleanWindow) {
        TScenario s;
        s.Params.BscWarmup = TDuration::Seconds(30);
        s.Replies.SelfChecks.front().Classification = Classify("bsc_red");
        AssertVerdict(s.Evaluate(), "b", EPileVerdict::Failed);
        s.Replies.SelfChecks.front().Classification = {};
        s.Refresh(101);
        AssertVerdict(s.Evaluate(), "b", EPileVerdict::Unknown);
        s.Refresh(130);
        AssertVerdict(s.Evaluate(), "b", EPileVerdict::Unknown);
        s.Refresh(131);
        AssertVerdict(s.Evaluate(), "b", EPileVerdict::Healthy);
    }

    Y_UNIT_TEST(OneSidedBscPartitionKeepsCleanWindowAndAbsentEndpointDoesNotStall) {
        TScenario s;
        s.Params.BscWarmup = TDuration::Seconds(30);
        s.Evaluate();
        // b's unanswered request produces no reply; fallback is only an observation
        // from that side, while a's BSC reply remains clean for a.
        s.AddSelfCheck("b", Classify("fallback_red"));
        s.Refresh(130);
        AssertVerdict(s.Evaluate(), "a", EPileVerdict::Healthy);
        s.Replies.SelfChecks.pop_back();
        s.Refresh(131);
        AssertVerdict(s.Evaluate(), "a", EPileVerdict::Healthy);
    }

    // B6: an endpoint that returns from fallback to BSC mode saw a BSC outage;
    // the first BSC replies after a restart can be clean by omission (Ov 3.2.5).
    Y_UNIT_TEST(EndpointReturningFromFallbackToBscRestartsTheWarmup) {
        TScenario s;
        s.Params.BscWarmup = TDuration::Seconds(30);
        s.Evaluate();
        s.Refresh(130);
        AssertVerdict(s.Evaluate(), "a", EPileVerdict::Healthy);
        s.AddSelfCheck("b", Classify("fallback_grey"));
        s.Refresh(131);
        AssertVerdict(s.Evaluate(), "a", EPileVerdict::Healthy);
        s.Replies.SelfChecks.back().Classification = {};
        s.Refresh(132);
        AssertVerdict(s.Evaluate(), "a", EPileVerdict::Unknown);
        AssertVerdict(s.Evaluate(), "b", EPileVerdict::Unknown);
        s.Refresh(161);
        AssertVerdict(s.Evaluate(), "a", EPileVerdict::Unknown);
        s.Refresh(162);
        AssertVerdict(s.Evaluate(), "a", EPileVerdict::Healthy);
    }

    Y_UNIT_TEST(NewestEvidenceOnlyUsesVerdictSupportingReplies) {
        TScenario s;
        s.Replies.SelfChecks.front().Classification = Classify("bsc_red");
        s.Replies.SelfChecks.front().SentAt = TMonotonic::Seconds(90);
        s.AddSelfCheck("b", Classify("fallback_grey"));
        s.AddConnectivity("b", true, true);
        auto evaluation = s.Evaluate();
        AssertVerdict(evaluation, "b", EPileVerdict::Failed);
        UNIT_ASSERT_VALUES_EQUAL(evaluation.Piles.at("b").NewestEvidenceSentAt, TMonotonic::Seconds(90));
        s.Replies.SelfChecks.front().Classification = Classify("fallback_grey");
        s.Replies.Connectivity.clear();
        s.AddConnectivity("b", false, true);
        s.Replies.Connectivity.front().Report.SentAt = TMonotonic::Seconds(95);
        evaluation = s.Evaluate();
        AssertVerdict(evaluation, "a", EPileVerdict::Lost);
        UNIT_ASSERT_VALUES_EQUAL(evaluation.Piles.at("a").NewestEvidenceSentAt, TMonotonic::Seconds(95));
    }

    // S8 (B6): a fresh fallback reply from the BSC-host pile resets the warm-up.
    Y_UNIT_TEST(FreshFallbackReplyFromBscHostResetsWarmupDespiteCachedBscReply) {
        TScenario s;
        s.Params.BscWarmup = TDuration::Seconds(30);
        s.Evaluate();
        s.Refresh(130);
        AssertVerdict(s.Evaluate(), "a", EPileVerdict::Healthy);
        AssertVerdict(s.Evaluate(), "b", EPileVerdict::Healthy);
        // A second endpoint of the BSC-host pile answers in fallback mode now, while
        // a:2135's clean BSC reply sent at 130 stays fresh.
        s.AddSelfCheck("a", Classify("fallback_grey"));
        s.Replies.Endpoints.back().Endpoint = "a2:2135";
        s.Replies.SelfChecks.back().Endpoint = "a2:2135";
        s.Now = TMonotonic::Seconds(131);
        s.Replies.SelfChecks.back().SentAt = s.Now;
        const auto evaluation = s.Evaluate();
        AssertVerdict(evaluation, "a", EPileVerdict::Unknown);
        AssertVerdict(evaluation, "b", EPileVerdict::Unknown);
        UNIT_ASSERT(s.State.CleanSince.empty());
    }

    // S10 (B7): a BSC RED part for a NOT_SYNCHRONIZED pile implies stage-2 participation.
    Y_UNIT_TEST(BscRedPartForStageTwoPileImpliesParticipation) {
        TScenario s;
        s.Cluster.Piles["b"] = EPileState::NotSynchronized;
        // No connectivity report at all; a's BSC reply attributes a RED part to b.
        s.Replies.SelfChecks.front().Classification = Classify("stage2");
        auto evaluation = s.Evaluate();
        UNIT_ASSERT(evaluation.Participating.contains("b"));
        AssertVerdict(evaluation, "b", EPileVerdict::Failed);
        // A clean reply proves nothing about the stage.
        s.Replies.SelfChecks.front().Classification = {};
        evaluation = s.Evaluate();
        UNIT_ASSERT(evaluation.Participating.contains("b"));
        AssertVerdict(evaluation, "b", EPileVerdict::Unknown);
        UNIT_ASSERT_VALUES_EQUAL(evaluation.Piles.at("b").Reasons.front(), "participation unknown");
    }

    // S11 (P1): binding connectivity reports count for self quorum.
    Y_UNIT_TEST(BindingReportsCountForSelfQuorum) {
        TScenario s;
        s.Replies.SelfChecks.front().Classification = Classify("fallback_grey");
        s.AddConnectivity("a", true, false);
        s.AddSelfCheck("b", Classify("fallback_grey"));
        s.AddConnectivity("b", false, true);
        for (auto& reply : s.Replies.Connectivity) {
            reply.Report.BindingInProgress = true;
        }
        const auto evaluation = s.Evaluate();
        UNIT_ASSERT_VALUES_EQUAL(evaluation.Connectivity.size(), 2);
        UNIT_ASSERT(evaluation.Piles.at("a").SelfQuorum);
        UNIT_ASSERT(evaluation.Piles.at("b").SelfQuorum);
        AssertVerdict(evaluation, "b", EPileVerdict::Unknown);
    }

    // S12 (P1): binding connectivity reports do not prove a pile Lost.
    Y_UNIT_TEST(BindingReportsDoNotProveLost) {
        TScenario s;
        s.Replies.SelfChecks.front().Classification = Classify("fallback_grey");
        s.AddConnectivity("a", true, false);
        s.Replies.Connectivity.front().Report.BindingInProgress = true;
        AssertVerdict(s.Evaluate(), "b", EPileVerdict::Unknown);
        s.Replies.Connectivity.front().Report.BindingInProgress = false;
        AssertVerdict(s.Evaluate(), "b", EPileVerdict::Lost);
    }

    // S16 (guard): a configured pile absent from the view is ignored until it appears in the view.
    Y_UNIT_TEST(ConfiguredPileAbsentFromViewIsIgnoredAndAppearingPileIsEvaluated) {
        TScenario s;
        // c is configured but absent from the view: its replies are not evidence.
        s.AddSelfCheck("c", Classify("bsc_red"));
        auto evaluation = s.Evaluate();
        UNIT_ASSERT(!evaluation.Piles.contains("c"));
        AssertVerdict(evaluation, "b", EPileVerdict::Healthy);
        s.Cluster.Piles["c"] = EPileState::Synchronized;
        evaluation = s.Evaluate();
        UNIT_ASSERT(evaluation.Piles.contains("c"));
        AssertVerdict(evaluation, "b", EPileVerdict::Unknown);
    }
}

} // namespace NBridgeSkipper
