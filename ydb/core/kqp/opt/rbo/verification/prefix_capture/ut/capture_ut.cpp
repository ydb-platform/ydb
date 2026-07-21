#include "../capture.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/yexception.h>

namespace NKikimr::NKqp::NRBOPrefixCapture {
namespace {

TRBOSemanticSnapshotBoundaryResultV1 Supported(
    ERBOSemanticSnapshotBoundaryV1 boundary,
    TString json,
    TVector<TRBORuleApplicationV1> applications = {})
{
    return {
        boundary,
        std::move(json),
        {},
        std::move(applications),
    };
}

TRBOSemanticSnapshotBoundaryResultV1 Unsupported(
    ERBOSemanticSnapshotBoundaryV1 boundary,
    TString reason,
    TVector<TRBORuleApplicationV1> applications)
{
    return {
        boundary,
        {},
        std::move(reason),
        std::move(applications),
    };
}

TVector<TRBORuleApplicationV1> TwoApplications() {
    return {
        {1, "First stage", "First rule"},
        {2, "Second stage", "Second rule"},
    };
}

NJson::TJsonValue Manifest(const TCaptureOutput& capture) {
    NJson::TJsonValue result;
    const TString text = RenderManifest(capture);
    UNIT_ASSERT_C(NJson::ReadJsonTree(text, &result, true), text);
    return result;
}

} // namespace

Y_UNIT_TEST_SUITE(TRBOPrefixCapture) {
    Y_UNIT_TEST(ClassifiesAndRendersCapturedPrefix) {
        auto output = ClassifyCapture(2, false, {
            Supported(ERBOSemanticSnapshotBoundaryV1::Initial, "initial"),
            Supported(
                ERBOSemanticSnapshotBoundaryV1::RuleApplicationPrefix,
                "prefix",
                TwoApplications()),
        });

        UNIT_ASSERT(output.Status == ECaptureStatus::PrefixCaptured);
        UNIT_ASSERT_VALUES_EQUAL(output.InitialSnapshot, "initial");
        UNIT_ASSERT_VALUES_EQUAL(output.CandidateSnapshot, "prefix");
        UNIT_ASSERT(output.UnsupportedReason.empty());
        const auto manifest = Manifest(output);
        UNIT_ASSERT_VALUES_EQUAL(
            manifest["protocol"].GetStringSafe(),
            "ydb-rbo-rule-prefix-capture-v1");
        UNIT_ASSERT_VALUES_EQUAL(manifest["requested_ordinal"].GetUIntegerSafe(), 2);
        UNIT_ASSERT_VALUES_EQUAL(manifest["status"].GetStringSafe(), "PREFIX_CAPTURED");
        UNIT_ASSERT_VALUES_EQUAL(manifest["initial_snapshot"].GetStringSafe(), "initial.json");
        UNIT_ASSERT_VALUES_EQUAL(manifest["prefix_snapshot"].GetStringSafe(), "prefix.json");
        UNIT_ASSERT(!manifest.Has("final_snapshot"));
        UNIT_ASSERT(!manifest.Has("unsupported_reason"));
        const auto& applications = manifest["applications"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(applications.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(applications[0]["ordinal"].GetUIntegerSafe(), 1);
        UNIT_ASSERT_VALUES_EQUAL(applications[0]["stage"].GetStringSafe(), "First stage");
        UNIT_ASSERT_VALUES_EQUAL(applications[0]["rule"].GetStringSafe(), "First rule");
    }

    Y_UNIT_TEST(ClassifiesAndRendersUnsupportedPrefix) {
        auto output = ClassifyCapture(2, false, {
            Supported(ERBOSemanticSnapshotBoundaryV1::Initial, "initial"),
            Unsupported(
                ERBOSemanticSnapshotBoundaryV1::RuleApplicationPrefix,
                "temporary CBO tree",
                TwoApplications()),
        });

        UNIT_ASSERT(output.Status == ECaptureStatus::PrefixUnsupported);
        UNIT_ASSERT(output.CandidateSnapshot.empty());
        UNIT_ASSERT_VALUES_EQUAL(output.UnsupportedReason, "temporary CBO tree");
        const auto manifest = Manifest(output);
        UNIT_ASSERT_VALUES_EQUAL(manifest["status"].GetStringSafe(), "PREFIX_UNSUPPORTED");
        UNIT_ASSERT_VALUES_EQUAL(
            manifest["unsupported_reason"].GetStringSafe(),
            "temporary CBO tree");
        UNIT_ASSERT(!manifest.Has("prefix_snapshot"));
        UNIT_ASSERT(!manifest.Has("final_snapshot"));
    }

    Y_UNIT_TEST(ClassifiesAndRendersCompletedOptimizer) {
        auto output = ClassifyCapture(3, true, {
            Supported(ERBOSemanticSnapshotBoundaryV1::Initial, "initial"),
            Supported(
                ERBOSemanticSnapshotBoundaryV1::Final,
                "final",
                TwoApplications()),
        });

        UNIT_ASSERT(output.Status == ECaptureStatus::OptimizerComplete);
        const auto manifest = Manifest(output);
        UNIT_ASSERT_VALUES_EQUAL(manifest["status"].GetStringSafe(), "OPTIMIZER_COMPLETE");
        UNIT_ASSERT_VALUES_EQUAL(manifest["final_snapshot"].GetStringSafe(), "final.json");
        UNIT_ASSERT(!manifest.Has("prefix_snapshot"));
        UNIT_ASSERT(!manifest.Has("unsupported_reason"));
    }

    Y_UNIT_TEST(ClassifiesAndRendersUnsupportedFinal) {
        auto output = ClassifyCapture(3, true, {
            Supported(ERBOSemanticSnapshotBoundaryV1::Initial, "initial"),
            Unsupported(
                ERBOSemanticSnapshotBoundaryV1::Final,
                "unsupported physical edge",
                TwoApplications()),
        });

        UNIT_ASSERT(output.Status == ECaptureStatus::FinalUnsupported);
        UNIT_ASSERT(output.CandidateSnapshot.empty());
        const auto manifest = Manifest(output);
        UNIT_ASSERT_VALUES_EQUAL(manifest["status"].GetStringSafe(), "FINAL_UNSUPPORTED");
        UNIT_ASSERT_VALUES_EQUAL(
            manifest["unsupported_reason"].GetStringSafe(),
            "unsupported physical edge");
        UNIT_ASSERT(!manifest.Has("prefix_snapshot"));
        UNIT_ASSERT(!manifest.Has("final_snapshot"));
    }

    Y_UNIT_TEST(RejectsMalformedOrUnrelatedOptimizerOutcomes) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            ClassifyCapture(0, false, {}),
            yexception,
            "must be positive");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            ClassifyCapture(1, false, {
                Unsupported(
                    ERBOSemanticSnapshotBoundaryV1::Initial,
                    "initial gap",
                    {}),
                Supported(
                    ERBOSemanticSnapshotBoundaryV1::RuleApplicationPrefix,
                    "prefix",
                    {{1, "Stage", "Rule"}}),
            }),
            yexception,
            "initial semantic snapshot is unsupported");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            ClassifyCapture(3, false, {
                Supported(ERBOSemanticSnapshotBoundaryV1::Initial, "initial"),
                Supported(
                    ERBOSemanticSnapshotBoundaryV1::Final,
                    "final",
                    TwoApplications()),
            }, "physical lowering failed"),
            yexception,
            "without an observed rule-prefix stop");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            ClassifyCapture(2, false, {
                Supported(ERBOSemanticSnapshotBoundaryV1::Initial, "initial"),
                Supported(
                    ERBOSemanticSnapshotBoundaryV1::RuleApplicationPrefix,
                    "prefix",
                    {{1, "Stage", "Rule"}, {3, "Stage", "Rule"}}),
            }),
            yexception,
            "not contiguous");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            ClassifyCapture(1, true, {
                Supported(ERBOSemanticSnapshotBoundaryV1::Initial, "initial"),
                Supported(
                    ERBOSemanticSnapshotBoundaryV1::RuleApplicationPrefix,
                    "prefix",
                    {{1, "Stage", "Rule"}}),
            }),
            yexception,
            "preparation succeeded");
    }

    Y_UNIT_TEST(BenchmarkInputPreparationMatchesCoverageHarness) {
        const TString schema = R"(CREATE TABLE `/Root/T` (
    k Uint64,
    PRIMARY KEY (k)
);
)";
        const TString expectedSchema = R"(CREATE TABLE `/Root/T` (
    k Uint64,
    PRIMARY KEY (k)
) WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 16);;
)";
        UNIT_ASSERT_VALUES_EQUAL(
            RewriteBenchmarkSchemaToColumnStore(schema),
            expectedSchema);

        const TString query = "SELECT * FROM `/Root/T`;\n";
        const TString expectedQuery = R"(
$to_decimal = ($x) -> { return cast($x as Decimal(12, 2)); };
$to_decimal_max_precision = ($x) -> { return cast($x as Decimal(35, 2)); };
$round = ($x,$y) -> { return $x; };
)" + query;
        UNIT_ASSERT_VALUES_EQUAL(AddBenchmarkQueryPrelude(query), expectedQuery);
    }
}

} // namespace NKikimr::NKqp::NRBOPrefixCapture
