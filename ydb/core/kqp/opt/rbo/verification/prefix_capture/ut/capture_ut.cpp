#include "../capture.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/yexception.h>

namespace NKikimr::NKqp::NRBOPrefixCapture {
namespace {

TRBOSemanticSnapshotBoundaryResultV1 Supported(
    ERBOSemanticSnapshotBoundaryV1 boundary,
    TString json,
    TVector<TRBOTransformationEventV1> events = {})
{
    return {
        boundary,
        std::move(json),
        {},
        std::move(events),
    };
}

TRBOSemanticSnapshotBoundaryResultV1 Unsupported(
    ERBOSemanticSnapshotBoundaryV1 boundary,
    TString reason,
    TVector<TRBOTransformationEventV1> events)
{
    return {
        boundary,
        {},
        std::move(reason),
        std::move(events),
    };
}

TVector<TRBOTransformationEventV1> TwoEvents() {
    return {
        {1, ERBOTransformationEventKindV1::RuleApplication, "First stage", "First rule"},
        {2, ERBOTransformationEventKindV1::AtomicStageCommit, "Second stage", "Second commit"},
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
                ERBOSemanticSnapshotBoundaryV1::TransformationPrefix,
                "prefix",
                TwoEvents()),
        });

        UNIT_ASSERT(output.Status == ECaptureStatus::PrefixCaptured);
        UNIT_ASSERT_VALUES_EQUAL(output.InitialSnapshot, "initial");
        UNIT_ASSERT_VALUES_EQUAL(output.CandidateSnapshot, "prefix");
        UNIT_ASSERT(output.UnsupportedReason.empty());
        const auto manifest = Manifest(output);
        UNIT_ASSERT_VALUES_EQUAL(
            manifest["protocol"].GetStringSafe(),
            "ydb-rbo-transformation-prefix-capture-v2");
        UNIT_ASSERT_VALUES_EQUAL(manifest["requested_ordinal"].GetUIntegerSafe(), 2);
        UNIT_ASSERT_VALUES_EQUAL(manifest["status"].GetStringSafe(), "PREFIX_CAPTURED");
        UNIT_ASSERT_VALUES_EQUAL(manifest["initial_snapshot"].GetStringSafe(), "initial.json");
        UNIT_ASSERT_VALUES_EQUAL(manifest["prefix_snapshot"].GetStringSafe(), "prefix.json");
        UNIT_ASSERT(!manifest.Has("final_snapshot"));
        UNIT_ASSERT(!manifest.Has("unsupported_reason"));
        const auto& events = manifest["events"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(events.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(events[0]["ordinal"].GetUIntegerSafe(), 1);
        UNIT_ASSERT_VALUES_EQUAL(events[0]["kind"].GetStringSafe(), "RULE_APPLICATION");
        UNIT_ASSERT_VALUES_EQUAL(events[0]["stage"].GetStringSafe(), "First stage");
        UNIT_ASSERT_VALUES_EQUAL(events[0]["name"].GetStringSafe(), "First rule");
        UNIT_ASSERT_VALUES_EQUAL(events[1]["kind"].GetStringSafe(), "ATOMIC_STAGE_COMMIT");
        UNIT_ASSERT_VALUES_EQUAL(events[1]["name"].GetStringSafe(), "Second commit");
    }

    Y_UNIT_TEST(ClassifiesAndRendersUnsupportedPrefix) {
        auto output = ClassifyCapture(2, false, {
            Supported(ERBOSemanticSnapshotBoundaryV1::Initial, "initial"),
            Unsupported(
                ERBOSemanticSnapshotBoundaryV1::TransformationPrefix,
                "temporary CBO tree",
                TwoEvents()),
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
                TwoEvents()),
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
                TwoEvents()),
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
                    ERBOSemanticSnapshotBoundaryV1::TransformationPrefix,
                    "prefix",
                    {{1, ERBOTransformationEventKindV1::RuleApplication, "Stage", "Rule"}}),
            }),
            yexception,
            "initial semantic snapshot is unsupported");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            ClassifyCapture(3, false, {
                Supported(ERBOSemanticSnapshotBoundaryV1::Initial, "initial"),
                Supported(
                    ERBOSemanticSnapshotBoundaryV1::Final,
                    "final",
                    TwoEvents()),
            }, "physical lowering failed"),
            yexception,
            "without an observed transformation-prefix stop");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            ClassifyCapture(2, false, {
                Supported(ERBOSemanticSnapshotBoundaryV1::Initial, "initial"),
                Supported(
                    ERBOSemanticSnapshotBoundaryV1::TransformationPrefix,
                    "prefix",
                    {
                        {1, ERBOTransformationEventKindV1::RuleApplication, "Stage", "Rule"},
                        {3, ERBOTransformationEventKindV1::AtomicStageCommit, "Stage", "Commit"},
                    }),
            }),
            yexception,
            "not contiguous");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            ClassifyCapture(1, true, {
                Supported(ERBOSemanticSnapshotBoundaryV1::Initial, "initial"),
                Supported(
                    ERBOSemanticSnapshotBoundaryV1::TransformationPrefix,
                    "prefix",
                    {{1, ERBOTransformationEventKindV1::RuleApplication, "Stage", "Rule"}}),
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
