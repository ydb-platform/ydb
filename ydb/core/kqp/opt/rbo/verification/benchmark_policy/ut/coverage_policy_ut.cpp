#include <ydb/core/kqp/opt/rbo/verification/benchmark_policy/coverage_policy.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/json/writer/json.h>
#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/string/builder.h>

#include <utility>

namespace NKikimr::NKqp::NVerificationCoverage {
namespace {

TString CoveragePolicyPath() {
    return ArcadiaSourceRoot() +
        "/ydb/core/kqp/opt/rbo/verification/benchmark_ut/coverage_policy.json";
}

TCoveragePolicy LoadCoveragePolicy() {
    return DecodeCoveragePolicy(
        TFileInput(CoveragePolicyPath()).ReadAll());
}

std::set<ui32> AllQueryIds(const TSuite& suite) {
    std::set<ui32> result;
    for (ui32 queryId = 1; queryId <= suite.QueryCount; ++queryId) {
        result.insert(queryId);
    }
    return result;
}

std::set<ui32> OutcomeIds(const TMap<ui32, TString>& statuses) {
    std::set<ui32> result;
    for (const auto& [queryId, status] : statuses) {
        Y_UNUSED(status);
        result.insert(queryId);
    }
    return result;
}

std::set<ui32> FormulaDashboardFloorVerifierEntries(
    const TSuiteCoveragePolicy& suitePolicy)
{
    auto result = suitePolicy.RequiredVerifierEntryQueries;
    result.insert(
        suitePolicy.RequiredFormulaQueries.begin(),
        suitePolicy.RequiredFormulaQueries.end());
    return result;
}

TMap<ui32, TString> FormulaDashboardFloorStatuses(
    const TCoveragePolicy& policy,
    const TSuite& suite)
{
    TMap<ui32, TString> result;
    const auto& suitePolicy = policy.Suites.at(suite.Name);
    for (const ui32 queryId : suitePolicy.RequiredFormulaQueries) {
        result[queryId] = "FORMULA_EMITTED";
    }
    for (const ui32 queryId : suitePolicy.RequiredVerifierEntryQueries) {
        result.try_emplace(queryId, "UNSUPPORTED");
    }
    for (const ui32 queryId : suitePolicy.RequiredSnapshotPairQueries) {
        result.try_emplace(queryId, "UNSUPPORTED");
    }
    return result;
}

} // namespace

// No host, subprocess, environment mutation, or solver is needed for these tests.
Y_UNIT_TEST_SUITE(TRBOCoveragePolicy) {
    Y_UNIT_TEST(IndependentTinyPolicyExercisesEverySemanticDepth) {
        const TSuite suite{"tiny", "tiny", "", "", 6};
        TSuiteCoveragePolicy floors;
        floors.QueryCount = 6;
        floors.RequiredPrepareSuccessQueries = {1, 6};
        floors.RequiredSnapshotPairQueries = {2};
        floors.RequiredVerifierEntryQueries = {3};
        floors.RequiredFormulaQueries = {4, 5};
        floors.RequiredVerifiedQueries = {5};
        TCoveragePolicy policy;
        policy.Suites.emplace(suite.Name, floors);
        const auto selected = AllQueryIds(suite);
        const TMap<ui32, TString> statuses = {
            {1, "UNSUPPORTED"}, {2, "UNSUPPORTED"}, {3, "UNSUPPORTED"},
            {4, "FORMULA_EMITTED"}, {5, "VERIFIED_BOUNDED"}, {6, "UNSUPPORTED"},
        };
        const auto good = EvaluateCoveragePolicy(
            policy, suite, selected, statuses, {2, 3, 4, 5}, {3, 4, 5}, {1, 6},
            ECoverageMode::FormulaDashboard);
        UNIT_ASSERT(good.Violations.empty());
        // q4 has a formula without successful preparation. Operational and
        // semantic floors are independent; deeper statuses satisfy weaker floors.
        UNIT_ASSERT(!good.PrepareSuccessQueries.contains(4));
        UNIT_ASSERT(good.FormulaEmittedQueries.contains(4));
        UNIT_ASSERT(good.VerifiedBoundedQueries.contains(5));
        const auto regressed = EvaluateCoveragePolicy(
            policy, suite, selected, statuses, {3, 4, 5}, {4, 5}, {1},
            ECoverageMode::FormulaDashboard);
        UNIT_ASSERT_VALUES_EQUAL(regressed.Violations.size(), 3);
        UNIT_ASSERT(regressed.Violations[0].Contains("q6"));
        UNIT_ASSERT(regressed.Violations[1].Contains("q2"));
        UNIT_ASSERT(regressed.Violations[2].Contains("q3"));
        const auto proof = EvaluateCoveragePolicy(
            policy, suite, {5}, statuses, {5}, {5}, {},
            ECoverageMode::ProofFloor);
        UNIT_ASSERT(proof.Violations.empty());
        auto broken = statuses;
        broken[5] = "UNKNOWN";
        const auto unknown = EvaluateCoveragePolicy(
            policy, suite, {5}, broken, {5}, {5}, {},
            ECoverageMode::ProofFloor);
        UNIT_ASSERT_VALUES_EQUAL(unknown.Violations.size(), 1);
        UNIT_ASSERT(unknown.Violations[0].Contains("q5"));
    }

    Y_UNIT_TEST(PolicyAllowsMonotonicCoverageImprovements) {
        const auto policy = LoadCoveragePolicy();
        const auto selected = AllQueryIds(Tpcds);
        auto statuses = FormulaDashboardFloorStatuses(policy, Tpcds);
        ui32 improvementQuery = 0;
        for (ui32 queryId = 1; queryId <= Tpcds.QueryCount; ++queryId) {
            if (!policy.Suites.at(Tpcds.Name)
                     .RequiredFormulaQueries.contains(queryId))
            {
                improvementQuery = queryId;
                break;
            }
        }
        UNIT_ASSERT(improvementQuery);
        statuses[improvementQuery] = "FORMULA_EMITTED";
        auto snapshotPairs =
            SnapshotPairFloorQueries(policy.Suites.at(Tpcds.Name));
        snapshotPairs.insert(improvementQuery);
        auto verifierEntries =
            FormulaDashboardFloorVerifierEntries(
                policy.Suites.at(Tpcds.Name));
        verifierEntries.insert(improvementQuery);
        const auto evaluation = EvaluateCoveragePolicy(
            policy,
            Tpcds,
            selected,
            statuses,
            snapshotPairs,
            verifierEntries,
            policy.Suites.at(Tpcds.Name).RequiredPrepareSuccessQueries,
            ECoverageMode::FormulaDashboard);
        UNIT_ASSERT(evaluation.VerifierEntryFloorEnforced);
        UNIT_ASSERT(evaluation.FormulaFloorEnforced);
        UNIT_ASSERT(!evaluation.ProofFloorEnforced);
        UNIT_ASSERT(evaluation.Violations.empty());
        for (const ui32 queryId :
             policy.Suites.at(Tpcds.Name).RequiredVerifierEntryQueries)
        {
            UNIT_ASSERT(evaluation.VerifierEntryQueries.contains(queryId));
        }
        UNIT_ASSERT(evaluation.FormulaEmittedQueries.contains(5));
        UNIT_ASSERT(evaluation.FormulaEmittedQueries.contains(65));
        UNIT_ASSERT(evaluation.FormulaEmittedQueries.contains(80));
        auto expectedFormulaQueries =
            policy.Suites.at(Tpcds.Name).RequiredFormulaQueries;
        expectedFormulaQueries.insert(improvementQuery);
        UNIT_ASSERT(
            evaluation.FormulaEmittedQueries == expectedFormulaQueries);
        UNIT_ASSERT(
            evaluation.FormulaEmittedQueries.contains(improvementQuery));

        const auto report = PolicyEvaluationJson(evaluation);
        UNIT_ASSERT(report["verifier_entry_floor_enforced"].GetBooleanSafe());
        UNIT_ASSERT_VALUES_EQUAL(
            report["required_verifier_entry_queries"].GetArraySafe().size(),
            policy.Suites.at(Tpcds.Name)
                .RequiredVerifierEntryQueries.size());
        size_t index = 0;
        for (const ui32 queryId :
             policy.Suites.at(Tpcds.Name).RequiredVerifierEntryQueries)
        {
            UNIT_ASSERT_VALUES_EQUAL(
                report["required_verifier_entry_queries"][index++].GetUIntegerSafe(),
                queryId);
        }
        UNIT_ASSERT_VALUES_EQUAL(
            report["verifier_entry_queries"].GetArraySafe().size(),
            verifierEntries.size());
    }

    Y_UNIT_TEST(PolicyTracksPreparationIndependently) {
        const auto policy = LoadCoveragePolicy();
        const auto selected = AllQueryIds(Tpcds);
        const auto statuses = FormulaDashboardFloorStatuses(policy, Tpcds);
        auto prepareSuccess = OutcomeIds(statuses);
        prepareSuccess.erase(2);

        const auto evaluation = EvaluateCoveragePolicy(
            policy,
            Tpcds,
            selected,
            statuses,
            SnapshotPairFloorQueries(policy.Suites.at(Tpcds.Name)),
            FormulaDashboardFloorVerifierEntries(
                policy.Suites.at(Tpcds.Name)),
            prepareSuccess,
            ECoverageMode::FormulaDashboard);
        UNIT_ASSERT(evaluation.PrepareSuccessFloorEnforced);
        UNIT_ASSERT(evaluation.FormulaFloorEnforced);
        UNIT_ASSERT(evaluation.FormulaEmittedQueries.contains(2));
        UNIT_ASSERT(!evaluation.PrepareSuccessQueries.contains(2));
        UNIT_ASSERT_VALUES_EQUAL(evaluation.Violations.size(), 1);
        UNIT_ASSERT(evaluation.Violations.front().Contains(
            "q2 regressed before successful query preparation"));

        const auto report = PolicyEvaluationJson(evaluation);
        UNIT_ASSERT(report["prepare_success_floor_enforced"].GetBooleanSafe());
        UNIT_ASSERT_VALUES_EQUAL(
            report["required_prepare_success_queries"].GetArraySafe().size(),
            policy.Suites.at(Tpcds.Name).RequiredPrepareSuccessQueries.size());

        auto independentPolicy = policy;
        independentPolicy.Suites.at(Tpcds.Name)
            .RequiredPrepareSuccessQueries.erase(2);
        const auto independent = EvaluateCoveragePolicy(
            independentPolicy,
            Tpcds,
            selected,
            statuses,
            SnapshotPairFloorQueries(
                independentPolicy.Suites.at(Tpcds.Name)),
            FormulaDashboardFloorVerifierEntries(
                independentPolicy.Suites.at(Tpcds.Name)),
            prepareSuccess,
            ECoverageMode::FormulaDashboard);
        UNIT_ASSERT(independent.FormulaEmittedQueries.contains(2));
        UNIT_ASSERT(independent.Violations.empty());
    }

    Y_UNIT_TEST(PolicyDocumentKeepsPreparationAndFormulaFloorsIndependent) {
        NJson::TJsonValue encoded;
        UNIT_ASSERT(NJson::ReadJsonTree(
            TFileInput(CoveragePolicyPath()).ReadAll(),
            &encoded,
            true));
        encoded["suites"][Tpcds.Name]["required_prepare_success_queries"] =
            NJson::TJsonValue(NJson::JSON_ARRAY);

        const auto decoded = DecodeCoveragePolicy(
            NJson::WriteJson(encoded, false, true));
        UNIT_ASSERT(
            decoded.Suites.at(Tpcds.Name).RequiredPrepareSuccessQueries.empty());
        UNIT_ASSERT(
            !decoded.Suites.at(Tpcds.Name).RequiredFormulaQueries.empty());
    }

    Y_UNIT_TEST(PolicyPinsTpchQ1AtFormulaConstruction) {
        const auto policy = LoadCoveragePolicy();
        const auto selected = AllQueryIds(Tpch);
        const auto statuses = FormulaDashboardFloorStatuses(policy, Tpch);

        const auto snapshotPairs =
            SnapshotPairFloorQueries(policy.Suites.at(Tpch.Name));
        auto verifierEntries =
            FormulaDashboardFloorVerifierEntries(
                policy.Suites.at(Tpch.Name));
        const auto current = EvaluateCoveragePolicy(
            policy,
            Tpch,
            selected,
            statuses,
            snapshotPairs,
            verifierEntries,
            policy.Suites.at(Tpch.Name).RequiredPrepareSuccessQueries,
            ECoverageMode::FormulaDashboard);
        UNIT_ASSERT(current.VerifierEntryFloorEnforced);
        UNIT_ASSERT(current.FormulaFloorEnforced);
        UNIT_ASSERT(current.Violations.empty());
        UNIT_ASSERT(current.VerifierEntryQueries.contains(1));
        UNIT_ASSERT(current.FormulaEmittedQueries.contains(1));

        auto regressedStatuses = statuses;
        regressedStatuses[1] = "UNSUPPORTED";
        const auto regressed = EvaluateCoveragePolicy(
            policy,
            Tpch,
            selected,
            regressedStatuses,
            snapshotPairs,
            verifierEntries,
            policy.Suites.at(Tpch.Name).RequiredPrepareSuccessQueries,
            ECoverageMode::FormulaDashboard);
        UNIT_ASSERT_VALUES_EQUAL(regressed.Violations.size(), 1);
        UNIT_ASSERT(regressed.Violations.front().Contains(
            "q1 regressed from FORMULA_EMITTED to UNSUPPORTED"));
    }

    Y_UNIT_TEST(PolicyPinsTpchFormulaFloor) {
        const auto policy = LoadCoveragePolicy();
        const auto selected = AllQueryIds(Tpch);
        const auto statuses = FormulaDashboardFloorStatuses(policy, Tpch);
        const auto current = EvaluateCoveragePolicy(
            policy,
            Tpch,
            selected,
            statuses,
            SnapshotPairFloorQueries(policy.Suites.at(Tpch.Name)),
            FormulaDashboardFloorVerifierEntries(
                policy.Suites.at(Tpch.Name)),
            policy.Suites.at(Tpch.Name).RequiredPrepareSuccessQueries,
            ECoverageMode::FormulaDashboard);
        UNIT_ASSERT(current.VerifierEntryFloorEnforced);
        UNIT_ASSERT(current.FormulaFloorEnforced);
        UNIT_ASSERT(current.Violations.empty());
        UNIT_ASSERT(
            current.FormulaEmittedQueries ==
            policy.Suites.at(Tpch.Name).RequiredFormulaQueries);

        for (const ui32 queryId :
             policy.Suites.at(Tpch.Name).RequiredFormulaQueries)
        {
            auto regressedStatuses = statuses;
            regressedStatuses[queryId] = "UNSUPPORTED";
            const auto regressed = EvaluateCoveragePolicy(
                policy,
                Tpch,
                selected,
                regressedStatuses,
                SnapshotPairFloorQueries(policy.Suites.at(Tpch.Name)),
                FormulaDashboardFloorVerifierEntries(
                    policy.Suites.at(Tpch.Name)),
                policy.Suites.at(Tpch.Name).RequiredPrepareSuccessQueries,
                ECoverageMode::FormulaDashboard);
            UNIT_ASSERT_VALUES_EQUAL(regressed.Violations.size(), 1);
            const TString expected = TStringBuilder()
                << "q" << queryId
                << " regressed from FORMULA_EMITTED to UNSUPPORTED";
            UNIT_ASSERT(regressed.Violations.front().Contains(expected));
        }
    }

    Y_UNIT_TEST(PolicyVerifierEntryFloorAcceptsDeeperOutcomes) {
        const auto policy = LoadCoveragePolicy();
        const auto selected = AllQueryIds(Tpcds);

        auto statuses = FormulaDashboardFloorStatuses(policy, Tpcds);

        for (const TString status : {"FORMULA_EMITTED", "VERIFIED_BOUNDED"}) {
            statuses[65] = status;
            const auto evaluation = EvaluateCoveragePolicy(
                policy,
                Tpcds,
                selected,
                statuses,
                SnapshotPairFloorQueries(policy.Suites.at(Tpcds.Name)),
                FormulaDashboardFloorVerifierEntries(
                    policy.Suites.at(Tpcds.Name)),
                policy.Suites.at(Tpcds.Name).RequiredPrepareSuccessQueries,
                ECoverageMode::FormulaDashboard);
            UNIT_ASSERT(evaluation.VerifierEntryFloorEnforced);
            UNIT_ASSERT(evaluation.Violations.empty());
            for (const ui32 queryId :
                 policy.Suites.at(Tpcds.Name).RequiredVerifierEntryQueries)
            {
                UNIT_ASSERT(evaluation.VerifierEntryQueries.contains(queryId));
            }
            UNIT_ASSERT(evaluation.FormulaEmittedQueries.contains(5));
            UNIT_ASSERT(evaluation.FormulaEmittedQueries.contains(80));
            if (status == "FORMULA_EMITTED") {
                UNIT_ASSERT(evaluation.FormulaEmittedQueries.contains(65));
            } else {
                UNIT_ASSERT(evaluation.VerifiedBoundedQueries.contains(65));
            }
        }
    }

    Y_UNIT_TEST(PolicyReportsVerifierEntryRegressions) {
        const auto policy = LoadCoveragePolicy();
        const auto selected = AllQueryIds(Tpcds);

        auto statuses = FormulaDashboardFloorStatuses(policy, Tpcds);
        statuses[65] = "UNSUPPORTED";
        auto snapshotPairs =
            SnapshotPairFloorQueries(policy.Suites.at(Tpcds.Name));
        auto verifierEntries =
            FormulaDashboardFloorVerifierEntries(
                policy.Suites.at(Tpcds.Name));
        verifierEntries.erase(65);

        const auto beforeVerifier = EvaluateCoveragePolicy(
            policy,
            Tpcds,
            selected,
            statuses,
            snapshotPairs,
            verifierEntries,
            policy.Suites.at(Tpcds.Name).RequiredPrepareSuccessQueries,
            ECoverageMode::FormulaDashboard);
        UNIT_ASSERT(beforeVerifier.VerifierEntryFloorEnforced);
        UNIT_ASSERT_VALUES_EQUAL(beforeVerifier.Violations.size(), 2);
        UNIT_ASSERT(beforeVerifier.Violations[0].Contains(
            "q65 regressed before verifier entry with status UNSUPPORTED"));
        UNIT_ASSERT(beforeVerifier.Violations[1].Contains(
            "q65 regressed from FORMULA_EMITTED to UNSUPPORTED"));

        statuses[65] = "OPTIMIZER_FAILURE";
        snapshotPairs.erase(65);
        const auto optimizerFailure = EvaluateCoveragePolicy(
            policy,
            Tpcds,
            selected,
            statuses,
            snapshotPairs,
            verifierEntries,
            policy.Suites.at(Tpcds.Name).RequiredPrepareSuccessQueries,
            ECoverageMode::FormulaDashboard);
        UNIT_ASSERT_VALUES_EQUAL(optimizerFailure.Violations.size(), 3);
        UNIT_ASSERT(optimizerFailure.Violations[0].Contains(
            "q65 regressed before exact Initial/Final snapshot pair with status "
            "OPTIMIZER_FAILURE"));
        UNIT_ASSERT(optimizerFailure.Violations[1].Contains(
            "q65 regressed before verifier entry with status OPTIMIZER_FAILURE"));
        UNIT_ASSERT(optimizerFailure.Violations[2].Contains(
            "q65 regressed from FORMULA_EMITTED to OPTIMIZER_FAILURE"));

        statuses.erase(65);
        const auto missing = EvaluateCoveragePolicy(
            policy,
            Tpcds,
            selected,
            statuses,
            snapshotPairs,
            verifierEntries,
            policy.Suites.at(Tpcds.Name).RequiredPrepareSuccessQueries,
            ECoverageMode::FormulaDashboard);
        UNIT_ASSERT_VALUES_EQUAL(missing.Violations.size(), 3);
        UNIT_ASSERT(missing.Violations[0].Contains(
            "q65 has no coverage outcome; expected exact Initial/Final snapshot pair"));
        UNIT_ASSERT(missing.Violations[1].Contains(
            "q65 has no coverage outcome; expected verifier entry"));
        UNIT_ASSERT(missing.Violations[2].Contains(
            "q65 has no coverage outcome; expected FORMULA_EMITTED"));
    }

    Y_UNIT_TEST(PolicyReportsQ51PairAndFormulaRegression) {
        const auto policy = LoadCoveragePolicy();
        const auto selected = AllQueryIds(Tpcds);
        auto statuses = FormulaDashboardFloorStatuses(policy, Tpcds);
        auto snapshotPairs =
            SnapshotPairFloorQueries(policy.Suites.at(Tpcds.Name));
        const auto verifierEntries =
            FormulaDashboardFloorVerifierEntries(
                policy.Suites.at(Tpcds.Name));

        const auto baseline = EvaluateCoveragePolicy(
            policy,
            Tpcds,
            selected,
            statuses,
            snapshotPairs,
            verifierEntries,
            policy.Suites.at(Tpcds.Name).RequiredPrepareSuccessQueries,
            ECoverageMode::FormulaDashboard);
        UNIT_ASSERT(baseline.SnapshotPairFloorEnforced);
        UNIT_ASSERT(baseline.Violations.empty());
        UNIT_ASSERT(baseline.RequiredSnapshotPairQueries.empty());
        UNIT_ASSERT(baseline.RequiredFormulaQueries.contains(51));
        UNIT_ASSERT_VALUES_EQUAL(baseline.SnapshotPairFloorQueries.size(), 82);
        UNIT_ASSERT_VALUES_EQUAL(baseline.SnapshotPairQueries.size(), 82);

        statuses[51] = "OPTIMIZER_FAILURE";
        snapshotPairs.erase(51);

        const auto regressed = EvaluateCoveragePolicy(
            policy,
            Tpcds,
            selected,
            statuses,
            snapshotPairs,
            verifierEntries,
            policy.Suites.at(Tpcds.Name).RequiredPrepareSuccessQueries,
            ECoverageMode::FormulaDashboard);
        UNIT_ASSERT_VALUES_EQUAL(regressed.Violations.size(), 2);
        UNIT_ASSERT(regressed.Violations[0].Contains(
            "q51 regressed before exact Initial/Final snapshot pair with status "
            "OPTIMIZER_FAILURE"));
        UNIT_ASSERT(regressed.Violations[1].Contains(
            "q51 regressed from FORMULA_EMITTED to OPTIMIZER_FAILURE"));

        const auto report = PolicyEvaluationJson(regressed);
        UNIT_ASSERT_VALUES_EQUAL(
            report["version"].GetUIntegerSafe(),
            CoveragePolicyEvaluationVersion);
        UNIT_ASSERT(report["snapshot_pair_floor_enforced"].GetBooleanSafe());
        UNIT_ASSERT(
            report["required_snapshot_pair_queries"].GetArraySafe().empty());
        UNIT_ASSERT_VALUES_EQUAL(
            report["snapshot_pair_floor_queries"].GetArraySafe().size(), 82);
        UNIT_ASSERT_VALUES_EQUAL(
            report["snapshot_pair_queries"].GetArraySafe().size(), 81);
        UNIT_ASSERT_VALUES_EQUAL(
            report["required_formula_queries"].GetArraySafe().size(), 82);
        UNIT_ASSERT_VALUES_EQUAL(
            report["formula_emitted_queries"].GetArraySafe().size(), 81);
    }

    Y_UNIT_TEST(PolicyReportsEveryFloorRegression) {
        const auto policy = LoadCoveragePolicy();
        const auto selected = AllQueryIds(Tpcds);
        auto statuses = FormulaDashboardFloorStatuses(policy, Tpcds);
        statuses[88] = "UNSUPPORTED";
        statuses.erase(96);
        auto snapshotPairs =
            SnapshotPairFloorQueries(policy.Suites.at(Tpcds.Name));
        snapshotPairs.erase(96);
        auto verifierEntries =
            FormulaDashboardFloorVerifierEntries(
                policy.Suites.at(Tpcds.Name));
        verifierEntries.erase(88);
        const auto evaluation = EvaluateCoveragePolicy(
            policy,
            Tpcds,
            selected,
            statuses,
            snapshotPairs,
            verifierEntries,
            policy.Suites.at(Tpcds.Name).RequiredPrepareSuccessQueries,
            ECoverageMode::FormulaDashboard);
        UNIT_ASSERT(evaluation.VerifierEntryFloorEnforced);
        UNIT_ASSERT(evaluation.SnapshotPairFloorEnforced);
        UNIT_ASSERT(evaluation.FormulaFloorEnforced);
        UNIT_ASSERT(!evaluation.ProofFloorEnforced);
        UNIT_ASSERT_VALUES_EQUAL(evaluation.Violations.size(), 3);
        UNIT_ASSERT(evaluation.Violations[0].Contains(
            "q96 has no coverage outcome; expected exact Initial/Final snapshot pair"));
        UNIT_ASSERT(evaluation.Violations[1].Contains(
            "q88 regressed from FORMULA_EMITTED to UNSUPPORTED"));
        UNIT_ASSERT(evaluation.Violations[2].Contains(
            "q96 has no coverage outcome"));

        const auto report = PolicyEvaluationJson(evaluation);
        UNIT_ASSERT(report["snapshot_pair_floor_enforced"].GetBooleanSafe());
        UNIT_ASSERT(report["formula_floor_enforced"].GetBooleanSafe());
        UNIT_ASSERT(!report["proof_floor_enforced"].GetBooleanSafe());
        UNIT_ASSERT_VALUES_EQUAL(
            report["violations"].GetArraySafe().size(),
            3);
    }

    Y_UNIT_TEST(PolicyEnforcesCuratedProofFloor) {
        const auto policy = LoadCoveragePolicy();
        const std::set<ui32> selected = {
            3, 8, 9, 15, 16, 19, 21, 28, 34, 38, 41, 42, 43, 48, 52, 55, 62,
            69, 73, 87, 88, 90, 93, 94, 95, 96, 97, 99};
        const TMap<ui32, TString> statuses = {
            {3, "VERIFIED_BOUNDED"},
            {8, "VERIFIED_BOUNDED"},
            {9, "VERIFIED_BOUNDED"},
            {15, "VERIFIED_BOUNDED"},
            {16, "VERIFIED_BOUNDED"},
            {19, "VERIFIED_BOUNDED"},
            {21, "VERIFIED_BOUNDED"},
            {28, "VERIFIED_BOUNDED"},
            {34, "VERIFIED_BOUNDED"},
            {38, "VERIFIED_BOUNDED"},
            {41, "VERIFIED_BOUNDED"},
            {42, "VERIFIED_BOUNDED"},
            {43, "VERIFIED_BOUNDED"},
            {48, "VERIFIED_BOUNDED"},
            {52, "VERIFIED_BOUNDED"},
            {55, "VERIFIED_BOUNDED"},
            {62, "VERIFIED_BOUNDED"},
            {69, "VERIFIED_BOUNDED"},
            {73, "VERIFIED_BOUNDED"},
            {87, "VERIFIED_BOUNDED"},
            {88, "VERIFIED_BOUNDED"},
            {90, "VERIFIED_BOUNDED"},
            {93, "VERIFIED_BOUNDED"},
            {94, "VERIFIED_BOUNDED"},
            {95, "VERIFIED_BOUNDED"},
            {96, "VERIFIED_BOUNDED"},
            {97, "VERIFIED_BOUNDED"},
            {99, "VERIFIED_BOUNDED"},
        };
        const auto evaluation = EvaluateCoveragePolicy(
            policy,
            Tpcds,
            selected,
            statuses,
            {},
            {},
            policy.Suites.at(Tpcds.Name).RequiredPrepareSuccessQueries,
            ECoverageMode::ProofFloor);
        UNIT_ASSERT(!evaluation.SnapshotPairFloorEnforced);
        UNIT_ASSERT(!evaluation.VerifierEntryFloorEnforced);
        UNIT_ASSERT(!evaluation.FormulaFloorEnforced);
        UNIT_ASSERT(evaluation.PrepareSuccessFloorEnforced);
        UNIT_ASSERT(evaluation.ProofFloorEnforced);
        UNIT_ASSERT(evaluation.Violations.empty());
        UNIT_ASSERT(
            evaluation.VerifiedBoundedQueries == selected);
        UNIT_ASSERT(
            evaluation.PrepareSuccessFloorQueries == selected);

        const auto report = PolicyEvaluationJson(evaluation);
        UNIT_ASSERT_VALUES_EQUAL(
            report["format"].GetStringSafe(),
            CoveragePolicyEvaluationFormat);
        UNIT_ASSERT_VALUES_EQUAL(
            report["version"].GetUIntegerSafe(),
            CoveragePolicyEvaluationVersion);
        UNIT_ASSERT_VALUES_EQUAL(
            report["mode"].GetStringSafe(),
            "proof_floor");
        UNIT_ASSERT(report["proof_floor_enforced"].GetBooleanSafe());
        UNIT_ASSERT_VALUES_EQUAL(
            report["prepare_success_floor_queries"].GetArraySafe().size(),
            selected.size());
        UNIT_ASSERT_VALUES_EQUAL(
            report["verified_bounded_queries"].GetArraySafe().size(),
            selected.size());
    }

    Y_UNIT_TEST(PolicyReportsEveryProofFloorRegression) {
        const auto policy = LoadCoveragePolicy();
        const TMap<ui32, TString> statuses = {
            {3, "VERIFIED_BOUNDED"},
            {8, "VERIFIED_BOUNDED"},
            {9, "VERIFIED_BOUNDED"},
            {15, "VERIFIED_BOUNDED"},
            {16, "VERIFIED_BOUNDED"},
            {19, "VERIFIED_BOUNDED"},
            {21, "VERIFIED_BOUNDED"},
            {28, "VERIFIED_BOUNDED"},
            {34, "VERIFIED_BOUNDED"},
            {38, "VERIFIED_BOUNDED"},
            {41, "VERIFIED_BOUNDED"},
            {42, "VERIFIED_BOUNDED"},
            {43, "VERIFIED_BOUNDED"},
            {48, "VERIFIED_BOUNDED"},
            {52, "UNKNOWN"},
            {55, "FORMULA_EMITTED"},
            {62, "VERIFIED_BOUNDED"},
            {69, "VERIFIED_BOUNDED"},
            {73, "VERIFIED_BOUNDED"},
            {87, "VERIFIED_BOUNDED"},
            {88, "VERIFIED_BOUNDED"},
            {90, "VERIFIED_BOUNDED"},
            {93, "UNSUPPORTED"},
            {94, "VERIFIED_BOUNDED"},
            {95, "VERIFIED_BOUNDED"},
            {97, "VERIFIED_BOUNDED"},
            {99, "VERIFIED_BOUNDED"},
        };
        const auto evaluation = EvaluateCoveragePolicy(
            policy,
            Tpcds,
            {3, 8, 9, 15, 16, 19, 21, 28, 34, 38, 41, 42, 43, 48, 52, 55, 62, 69, 73, 87, 88, 90, 93, 94, 95, 96, 97, 99},
            statuses,
            {},
            {},
            policy.Suites.at(Tpcds.Name).RequiredPrepareSuccessQueries,
            ECoverageMode::ProofFloor);
        UNIT_ASSERT(evaluation.ProofFloorEnforced);
        UNIT_ASSERT_VALUES_EQUAL(evaluation.Violations.size(), 4);
        UNIT_ASSERT(evaluation.Violations[0].Contains(
            "q52 regressed from VERIFIED_BOUNDED to UNKNOWN"));
        UNIT_ASSERT(evaluation.Violations[1].Contains(
            "q55 regressed from VERIFIED_BOUNDED to FORMULA_EMITTED"));
        UNIT_ASSERT(evaluation.Violations[2].Contains(
            "q93 regressed from VERIFIED_BOUNDED to UNSUPPORTED"));
        UNIT_ASSERT(evaluation.Violations[3].Contains(
            "q96 has no proof outcome"));

        auto optimizerFailureStatuses = statuses;
        optimizerFailureStatuses[96] = "OPTIMIZER_FAILURE";
        const auto optimizerFailure = EvaluateCoveragePolicy(
            policy,
            Tpcds,
            {3, 8, 9, 15, 16, 19, 21, 28, 34, 38, 41, 42, 43, 48, 52, 55, 62, 69, 73, 87, 88, 90, 93, 94, 95, 96, 97, 99},
            optimizerFailureStatuses,
            {},
            {},
            policy.Suites.at(Tpcds.Name).RequiredPrepareSuccessQueries,
            ECoverageMode::ProofFloor);
        UNIT_ASSERT(optimizerFailure.Violations.back().Contains(
            "q96 regressed from VERIFIED_BOUNDED to OPTIMIZER_FAILURE"));

        const auto wrongSelection = EvaluateCoveragePolicy(
            policy,
            Tpcds,
            {3, 42, 48, 52, 55, 69, 90, 93},
            statuses,
            {},
            {},
            policy.Suites.at(Tpcds.Name).RequiredPrepareSuccessQueries,
            ECoverageMode::ProofFloor);
        UNIT_ASSERT(wrongSelection.Violations.front().Contains(
            "did not select exactly"));
    }

    Y_UNIT_TEST(PolicyDoesNotGateFocusedOrSolverExperiments) {
        const auto policy = LoadCoveragePolicy();
        const TMap<ui32, TString> statuses;
        const auto focused = EvaluateCoveragePolicy(
            policy,
            Tpcds,
            {3, 42, 48, 50, 52, 55, 61, 71, 76, 88, 90, 93, 96},
            statuses,
            {},
            {},
            {},
            ECoverageMode::FormulaDashboard);
        UNIT_ASSERT(!focused.SnapshotPairFloorEnforced);
        UNIT_ASSERT(!focused.VerifierEntryFloorEnforced);
        UNIT_ASSERT(!focused.FormulaFloorEnforced);
        UNIT_ASSERT(!focused.ProofFloorEnforced);
        UNIT_ASSERT(focused.Violations.empty());

        const auto selected = AllQueryIds(Tpcds);
        const auto solver = EvaluateCoveragePolicy(
            policy,
            Tpcds,
            selected,
            statuses,
            {},
            {},
            {},
            ECoverageMode::SolverExperiment);
        UNIT_ASSERT(!solver.SnapshotPairFloorEnforced);
        UNIT_ASSERT(!solver.VerifierEntryFloorEnforced);
        UNIT_ASSERT(!solver.FormulaFloorEnforced);
        UNIT_ASSERT(!solver.ProofFloorEnforced);
        UNIT_ASSERT(solver.Violations.empty());

        const auto coincidentalProofSelection = EvaluateCoveragePolicy(
            policy,
            Tpcds,
            {3, 16, 34, 38, 42, 48, 52, 55, 69, 73, 87, 90, 93, 94, 95, 96},
            statuses,
            {},
            {},
            {},
            ECoverageMode::SolverExperiment);
        UNIT_ASSERT(!coincidentalProofSelection.SnapshotPairFloorEnforced);
        UNIT_ASSERT(!coincidentalProofSelection.ProofFloorEnforced);
        UNIT_ASSERT(coincidentalProofSelection.Violations.empty());
    }

    Y_UNIT_TEST(PolicyRejectsMalformedDocuments) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            DecodeCoveragePolicy("{"),
            yexception,
            "not valid JSON");

        NJson::TJsonValue encoded;
        UNIT_ASSERT(NJson::ReadJsonTree(
            TFileInput(CoveragePolicyPath()).ReadAll(),
            &encoded,
            true));
        const auto baseline = encoded;

        encoded["version"] = 2;
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            DecodeCoveragePolicy(NJson::WriteJson(encoded, false, true)),
            yexception,
            "unsupported version");

        encoded = baseline;
        encoded["suites"][Tpcds.Name].EraseValue(
            "required_prepare_success_queries");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            DecodeCoveragePolicy(NJson::WriteJson(encoded, false, true)),
            yexception,
            "is missing field required_prepare_success_queries");

        encoded = baseline;
        encoded["suites"][Tpcds.Name]["required_prepare_success_queries"] =
            true;
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            DecodeCoveragePolicy(NJson::WriteJson(encoded, false, true)),
            yexception,
            "required_prepare_success_queries must be an array");

        encoded = baseline;
        encoded["suites"][Tpcds.Name].EraseValue(
            "required_snapshot_pair_queries");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            DecodeCoveragePolicy(NJson::WriteJson(encoded, false, true)),
            yexception,
            "is missing field required_snapshot_pair_queries");

        encoded = baseline;
        encoded["suites"][Tpcds.Name]["required_snapshot_pair_queries"] = true;
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            DecodeCoveragePolicy(NJson::WriteJson(encoded, false, true)),
            yexception,
            "required_snapshot_pair_queries must be an array");

        encoded = baseline;
        NJson::TJsonValue unorderedSnapshotPairs(NJson::JSON_ARRAY);
        unorderedSnapshotPairs.AppendValue(51);
        unorderedSnapshotPairs.AppendValue(49);
        encoded["suites"][Tpcds.Name]["required_snapshot_pair_queries"] =
            std::move(unorderedSnapshotPairs);
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            DecodeCoveragePolicy(NJson::WriteJson(encoded, false, true)),
            yexception,
            "required_snapshot_pair_queries query ids must be strictly increasing");

        encoded = baseline;
        NJson::TJsonValue duplicateSnapshotPair(NJson::JSON_ARRAY);
        duplicateSnapshotPair.AppendValue(49);
        duplicateSnapshotPair.AppendValue(49);
        encoded["suites"][Tpcds.Name]["required_snapshot_pair_queries"] =
            std::move(duplicateSnapshotPair);
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            DecodeCoveragePolicy(NJson::WriteJson(encoded, false, true)),
            yexception,
            "required_snapshot_pair_queries query ids must be strictly increasing");

        encoded = baseline;
        NJson::TJsonValue outsideSnapshotPair(NJson::JSON_ARRAY);
        outsideSnapshotPair.AppendValue(100);
        encoded["suites"][Tpcds.Name]["required_snapshot_pair_queries"] =
            std::move(outsideSnapshotPair);
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            DecodeCoveragePolicy(NJson::WriteJson(encoded, false, true)),
            yexception,
            "required_snapshot_pair_queries query id 100 is outside the corpus");

        encoded = baseline;
        encoded["suites"][Tpcds.Name]["required_snapshot_pair_queries"] =
            NJson::TJsonValue(NJson::JSON_ARRAY);
        encoded["suites"][Tpcds.Name]["required_snapshot_pair_queries"]
            .AppendValue(5);
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            DecodeCoveragePolicy(NJson::WriteJson(encoded, false, true)),
            yexception,
            "required snapshot-pair query q5 is also a required verifier-entry query");

        encoded = baseline;
        encoded["suites"][Tpcds.Name]["required_snapshot_pair_queries"] =
            NJson::TJsonValue(NJson::JSON_ARRAY);
        encoded["suites"][Tpcds.Name]["required_snapshot_pair_queries"]
            .AppendValue(2);
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            DecodeCoveragePolicy(NJson::WriteJson(encoded, false, true)),
            yexception,
            "required snapshot-pair query q2 is also a required formula query");

        encoded = baseline;
        encoded["suites"][Tpcds.Name].EraseValue(
            "required_verifier_entry_queries");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            DecodeCoveragePolicy(NJson::WriteJson(encoded, false, true)),
            yexception,
            "is missing field required_verifier_entry_queries");

        encoded = baseline;
        encoded["suites"][Tpcds.Name]["required_verifier_entry_queries"] = true;
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            DecodeCoveragePolicy(NJson::WriteJson(encoded, false, true)),
            yexception,
            "required_verifier_entry_queries must be an array");

        encoded = baseline;
        NJson::TJsonValue duplicateVerifierEntry(NJson::JSON_ARRAY);
        duplicateVerifierEntry.AppendValue(65);
        duplicateVerifierEntry.AppendValue(65);
        encoded["suites"][Tpcds.Name]["required_verifier_entry_queries"] =
            std::move(duplicateVerifierEntry);
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            DecodeCoveragePolicy(NJson::WriteJson(encoded, false, true)),
            yexception,
            "required_verifier_entry_queries query ids must be strictly increasing");

        encoded = baseline;
        NJson::TJsonValue outsideVerifierEntry(NJson::JSON_ARRAY);
        outsideVerifierEntry.AppendValue(100);
        encoded["suites"][Tpcds.Name]["required_verifier_entry_queries"] =
            std::move(outsideVerifierEntry);
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            DecodeCoveragePolicy(NJson::WriteJson(encoded, false, true)),
            yexception,
            "required_verifier_entry_queries query id 100 is outside the corpus");

        encoded = baseline;
        encoded["suites"][Tpcds.Name].EraseValue(
            "required_verified_queries");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            DecodeCoveragePolicy(NJson::WriteJson(encoded, false, true)),
            yexception,
            "is missing field required_verified_queries");

        encoded = baseline;
        encoded["unexpected"] = true;
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            DecodeCoveragePolicy(NJson::WriteJson(encoded, false, true)),
            yexception,
            "unexpected field unexpected");

        encoded.EraseValue("unexpected");
        NJson::TJsonValue unordered(NJson::JSON_ARRAY);
        unordered.AppendValue(96);
        unordered.AppendValue(88);
        encoded["suites"][Tpcds.Name]["required_formula_queries"] =
            std::move(unordered);
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            DecodeCoveragePolicy(NJson::WriteJson(encoded, false, true)),
            yexception,
            "strictly increasing");

        encoded = baseline;
        NJson::TJsonValue duplicate(NJson::JSON_ARRAY);
        duplicate.AppendValue(3);
        duplicate.AppendValue(52);
        duplicate.AppendValue(52);
        encoded["suites"][Tpcds.Name]["required_verified_queries"] =
            std::move(duplicate);
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            DecodeCoveragePolicy(NJson::WriteJson(encoded, false, true)),
            yexception,
            "strictly increasing");

        encoded = baseline;
        NJson::TJsonValue outside(NJson::JSON_ARRAY);
        outside.AppendValue(100);
        encoded["suites"][Tpcds.Name]["required_verified_queries"] =
            std::move(outside);
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            DecodeCoveragePolicy(NJson::WriteJson(encoded, false, true)),
            yexception,
            "outside the corpus");

        encoded = baseline;
        NJson::TJsonValue notFormula(NJson::JSON_ARRAY);
        notFormula.AppendValue(3);
        notFormula.AppendValue(48);
        encoded["suites"][Tpcds.Name]["required_verified_queries"] =
            std::move(notFormula);
        encoded["suites"][Tpcds.Name]["required_formula_queries"] =
            NJson::TJsonValue(NJson::JSON_ARRAY);
        encoded["suites"][Tpcds.Name]["required_formula_queries"]
            .AppendValue(3);
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            DecodeCoveragePolicy(NJson::WriteJson(encoded, false, true)),
            yexception,
            "is not a required formula query");

        encoded = baseline;
        encoded["suites"][Tpcds.Name]["required_verified_queries"] =
            NJson::TJsonValue(NJson::JSON_ARRAY);
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            DecodeCoveragePolicy(NJson::WriteJson(encoded, false, true)),
            yexception,
            "must not be empty");
    }

}

} // namespace NKikimr::NKqp::NVerificationCoverage
