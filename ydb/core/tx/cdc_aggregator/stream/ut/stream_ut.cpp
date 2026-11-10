#include "helpers.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NCdcAggregator {

using namespace NTest;

Y_UNIT_TEST_SUITE(TStream) {

    Y_UNIT_TEST(ShouldAddEverySourceWithoutProgress_WhenConfigured) {
        // given
        TStream stream;
        TStreamChanges changes;

        // when
        const auto outcome = stream.Configure(MakeStreamConfig({1, 2, 3}), changes);

        // then
        UNIT_ASSERT(outcome == EConfigureOutcome::Configured);
        UNIT_ASSERT_VALUES_EQUAL(stream.GetSources().size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(stream.CountSourcesWithoutProgress(), 3);
        UNIT_ASSERT(!stream.GetReadyBarrier());
        UNIT_ASSERT(stream.GetPhase() == EStreamPhase::Configured);
        UNIT_ASSERT(changes.Config);
        UNIT_ASSERT_VALUES_EQUAL(changes.UpsertedSources.size(), 3);
    }

    Y_UNIT_TEST(ShouldKeepProgress_WhenSameConfigurationRepeats) {
        // given
        auto stream = MakeConfiguredStream({1, 2});
        TStreamChanges reportChanges;
        stream.ApplyDelivered(1, MakeBarrier(100), reportChanges);

        // when
        TStreamChanges changes;
        const auto outcome = stream.Configure(MakeStreamConfig({1, 2}), changes);

        // then
        UNIT_ASSERT(outcome == EConfigureOutcome::AlreadyConfigured);
        UNIT_ASSERT(changes.IsEmpty());
        UNIT_ASSERT_VALUES_EQUAL((*stream.FindSourceProgress(1))->Version, TRowVersion(100, 0));
    }

    Y_UNIT_TEST(ShouldRejectConfiguration_WhenItDiffersFromStoredOne) {
        // given
        auto stream = MakeConfiguredStream({1, 2});

        // when
        TStreamChanges changes;
        const auto otherSources = stream.Configure(MakeStreamConfig({1, 2, 3}), changes);
        const auto otherVersion = stream.Configure(MakeStreamConfig({1, 2}, 2), changes);

        // then
        UNIT_ASSERT(otherSources == EConfigureOutcome::Conflict);
        UNIT_ASSERT(otherVersion == EConfigureOutcome::Conflict);
        UNIT_ASSERT(changes.IsEmpty());
        UNIT_ASSERT_VALUES_EQUAL(stream.GetSources().size(), 2);
    }

    Y_UNIT_TEST(ShouldTakeMinimumOverAllSources_WhenEverySourceReported) {
        // given
        auto stream = MakeConfiguredStream({1, 2, 3});
        TStreamChanges changes;

        // when
        stream.ApplyDelivered(1, MakeBarrier(120), changes);
        stream.ApplyDelivered(2, MakeBarrier(100), changes);
        stream.ApplyDelivered(3, MakeBarrier(110), changes);

        // then
        UNIT_ASSERT(stream.GetReadyBarrier());
        UNIT_ASSERT_VALUES_EQUAL(stream.GetReadyBarrier()->Version, TRowVersion(100, 0));
        UNIT_ASSERT_VALUES_EQUAL(stream.GetReadyBarrier()->Payload, MakeBarrier(100).Payload);
        UNIT_ASSERT(changes.ReadyBarrier);
    }

    Y_UNIT_TEST(ShouldBlockBarrier_WhenSourceHasNotReported) {
        // given
        auto stream = MakeConfiguredStream({1, 2, 3});
        TStreamChanges changes;

        // when
        stream.ApplyDelivered(1, MakeBarrier(120), changes);
        stream.ApplyDelivered(3, MakeBarrier(110), changes);

        // then
        UNIT_ASSERT(!stream.GetReadyBarrier());
        UNIT_ASSERT(!changes.ReadyBarrier);
        UNIT_ASSERT_VALUES_EQUAL(stream.CountSourcesWithoutProgress(), 1);
    }

    Y_UNIT_TEST(ShouldCompareFullVersion_WhenStepsAreEqual) {
        // given
        auto stream = MakeConfiguredStream({1, 2});
        TStreamChanges changes;

        // when
        stream.ApplyDelivered(1, MakeBarrier(100, 7), changes);
        stream.ApplyDelivered(2, MakeBarrier(100, 3), changes);

        // then
        UNIT_ASSERT_VALUES_EQUAL(stream.GetReadyBarrier()->Version, TRowVersion(100, 3));
    }

    Y_UNIT_TEST(ShouldKeepProgress_WhenReportIsDuplicateOrOlder) {
        // given
        auto stream = MakeConfiguredStream({1, 2});
        TStreamChanges setup;
        stream.ApplyDelivered(1, MakeBarrier(120), setup);
        stream.ApplyDelivered(2, MakeBarrier(110), setup);

        // when
        TStreamChanges changes;
        const auto duplicate = stream.ApplyDelivered(1, MakeBarrier(120), changes);
        // Altering the resolved interval re-creates heartbeat state and may
        // emit a lower version.
        const auto older = stream.ApplyDelivered(2, MakeBarrier(70), changes);

        // then
        UNIT_ASSERT(duplicate == EDeliveredOutcome::NotAdvanced);
        UNIT_ASSERT(older == EDeliveredOutcome::NotAdvanced);
        UNIT_ASSERT(changes.IsEmpty());
        UNIT_ASSERT_VALUES_EQUAL((*stream.FindSourceProgress(2))->Version, TRowVersion(110, 0));
        UNIT_ASSERT_VALUES_EQUAL(stream.GetReadyBarrier()->Version, TRowVersion(110, 0));
    }

    Y_UNIT_TEST(ShouldNotAddSource_WhenUnknownSourceReports) {
        // given
        auto stream = MakeConfiguredStream({1});

        // when
        TStreamChanges changes;
        const auto outcome = stream.ApplyDelivered(9, MakeBarrier(100), changes);

        // then
        UNIT_ASSERT(outcome == EDeliveredOutcome::UnknownSource);
        UNIT_ASSERT(!stream.FindSourceProgress(9));
        UNIT_ASSERT(changes.IsEmpty());
    }

    Y_UNIT_TEST(ShouldReplaceParentWithChildrenAtOnce_WhenSourceSplits) {
        // given
        auto stream = MakeConfiguredStream({1, 2});
        TStreamChanges setup;
        stream.ApplyDelivered(1, MakeBarrier(100), setup);
        stream.ApplyDelivered(2, MakeBarrier(110), setup);

        // when
        TStreamChanges changes;
        const auto outcome = stream.ReplaceSource({.Parent = 1, .Children = {3, 4}, .OperationId = 77}, changes);

        // then
        UNIT_ASSERT(outcome == EReplaceOutcome::Replaced);
        UNIT_ASSERT(!stream.FindSourceProgress(1));
        UNIT_ASSERT(stream.FindSourceProgress(3) && !*stream.FindSourceProgress(3));
        UNIT_ASSERT(stream.FindSourceProgress(4) && !*stream.FindSourceProgress(4));
        UNIT_ASSERT(changes.ErasedSources.contains(1));
        UNIT_ASSERT(changes.UpsertedSources.contains(3) && changes.UpsertedSources.contains(4));
        UNIT_ASSERT_VALUES_EQUAL(changes.AddedReplacements.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stream.GetReadyBarrier()->Version, TRowVersion(100, 0));
    }

    Y_UNIT_TEST(ShouldWaitForEveryChild_WhenSourceSplits) {
        // given
        auto stream = MakeConfiguredStream({1, 2});
        TStreamChanges setup;
        stream.ApplyDelivered(1, MakeBarrier(100), setup);
        stream.ApplyDelivered(2, MakeBarrier(110), setup);
        stream.ReplaceSource({.Parent = 1, .Children = {3, 4}}, setup);

        // when
        TStreamChanges firstChild;
        stream.ApplyDelivered(3, MakeBarrier(130), firstChild);
        TStreamChanges secondChild;
        stream.ApplyDelivered(4, MakeBarrier(140), secondChild);

        // then
        UNIT_ASSERT(!firstChild.ReadyBarrier);
        UNIT_ASSERT(secondChild.ReadyBarrier);
        UNIT_ASSERT_VALUES_EQUAL(stream.GetReadyBarrier()->Version, TRowVersion(110, 0));
    }

    Y_UNIT_TEST(ShouldKeepBarrier_WhenChildReportsBelowIt) {
        // given
        auto stream = MakeConfiguredStream({1, 2});
        TStreamChanges setup;
        stream.ApplyDelivered(1, MakeBarrier(100), setup);
        stream.ApplyDelivered(2, MakeBarrier(110), setup);
        stream.ReplaceSource({.Parent = 1, .Children = {3}}, setup);

        // when
        TStreamChanges changes;
        const auto outcome = stream.ApplyDelivered(3, MakeBarrier(90), changes);

        // then
        UNIT_ASSERT(outcome == EDeliveredOutcome::Advanced);
        UNIT_ASSERT(!changes.ReadyBarrier);
        UNIT_ASSERT_VALUES_EQUAL(stream.GetReadyBarrier()->Version, TRowVersion(100, 0));
    }

    Y_UNIT_TEST(ShouldKeepChildProgress_WhenReplacementRepeats) {
        // given
        auto stream = MakeConfiguredStream({1});
        TStreamChanges setup;
        stream.ReplaceSource({.Parent = 1, .Children = {2, 3}}, setup);
        stream.ApplyDelivered(2, MakeBarrier(100), setup);

        // when
        TStreamChanges changes;
        const auto outcome = stream.ReplaceSource({.Parent = 1, .Children = {2, 3}}, changes);

        // then
        UNIT_ASSERT(outcome == EReplaceOutcome::AlreadyReplaced);
        UNIT_ASSERT(changes.IsEmpty());
        UNIT_ASSERT_VALUES_EQUAL((*stream.FindSourceProgress(2))->Version, TRowVersion(100, 0));
    }

    Y_UNIT_TEST(ShouldRejectReplacement_WhenParentWasReplacedByOtherChildren) {
        // given
        auto stream = MakeConfiguredStream({1});
        TStreamChanges setup;
        stream.ReplaceSource({.Parent = 1, .Children = {2, 3}}, setup);

        // when
        TStreamChanges changes;
        const auto outcome = stream.ReplaceSource({.Parent = 1, .Children = {4}}, changes);

        // then
        UNIT_ASSERT(outcome == EReplaceOutcome::Conflict);
        UNIT_ASSERT(changes.IsEmpty());
        UNIT_ASSERT(!stream.FindSourceProgress(4));
    }

    Y_UNIT_TEST(ShouldRejectLateReport_WhenSourceIsReplaced) {
        // given
        auto stream = MakeConfiguredStream({1, 2});
        TStreamChanges setup;
        stream.ReplaceSource({.Parent = 1, .Children = {3}}, setup);

        // when
        TStreamChanges changes;
        const auto outcome = stream.ApplyDelivered(1, MakeBarrier(100), changes);

        // then
        UNIT_ASSERT(outcome == EDeliveredOutcome::RetiredSource);
        UNIT_ASSERT(!stream.FindSourceProgress(1));
        UNIT_ASSERT(changes.IsEmpty());
    }

    Y_UNIT_TEST(ShouldKeepExistingChild_WhenSecondParentMerges) {
        // given
        auto stream = MakeConfiguredStream({1, 2});
        TStreamChanges setup;
        stream.ApplyDelivered(1, MakeBarrier(100), setup);
        stream.ApplyDelivered(2, MakeBarrier(110), setup);
        stream.ReplaceSource({.Parent = 1, .Children = {3}}, setup);
        stream.ApplyDelivered(3, MakeBarrier(120), setup);

        // when
        TStreamChanges changes;
        const auto outcome = stream.ReplaceSource({.Parent = 2, .Children = {3}}, changes);

        // then
        UNIT_ASSERT(outcome == EReplaceOutcome::Replaced);
        UNIT_ASSERT(!changes.UpsertedSources.contains(3));
        UNIT_ASSERT_VALUES_EQUAL(stream.GetSources().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL((*stream.FindSourceProgress(3))->Version, TRowVersion(120, 0));
        UNIT_ASSERT(changes.ReadyBarrier);
        UNIT_ASSERT_VALUES_EQUAL(stream.GetReadyBarrier()->Version, TRowVersion(120, 0));
    }

    Y_UNIT_TEST(ShouldRejectReplacement_WhenChildIsRetiredSource) {
        // given
        auto stream = MakeConfiguredStream({1, 2});
        TStreamChanges setup;
        stream.ReplaceSource({.Parent = 1, .Children = {3}}, setup);

        // when
        TStreamChanges changes;
        const auto outcome = stream.ReplaceSource({.Parent = 2, .Children = {1}}, changes);

        // then
        UNIT_ASSERT(outcome == EReplaceOutcome::Conflict);
        UNIT_ASSERT(stream.FindSourceProgress(2));
        UNIT_ASSERT(changes.IsEmpty());
    }

    Y_UNIT_TEST(ShouldRejectReplacement_WhenParentIsUnknown) {
        // given
        auto stream = MakeConfiguredStream({1});

        // when
        TStreamChanges changes;
        const auto outcome = stream.ReplaceSource({.Parent = 9, .Children = {3}}, changes);

        // then
        UNIT_ASSERT(outcome == EReplaceOutcome::UnknownSource);
        UNIT_ASSERT(changes.IsEmpty());
    }

    Y_UNIT_TEST(ShouldRejectReplacementShape_WhenChildrenAreInvalid) {
        // given
        TSourceReplacement noChildren{.Parent = 1};
        TSourceReplacement duplicateChild{.Parent = 1, .Children = {3, 2, 3}};
        TSourceReplacement parentAsChild{.Parent = 1, .Children = {1, 2}};
        TSourceReplacement unsorted{.Parent = 1, .Children = {3, 2}};
        TString error;

        // when / then
        UNIT_ASSERT(!NormalizeSourceReplacement(noChildren, error));
        UNIT_ASSERT(!NormalizeSourceReplacement(duplicateChild, error));
        UNIT_ASSERT(!NormalizeSourceReplacement(parentAsChild, error));
        UNIT_ASSERT(NormalizeSourceReplacement(unsorted, error));
        UNIT_ASSERT(unsorted.Children == TVector<TSourceId>({2, 3}));
    }

    Y_UNIT_TEST(ShouldActivateOnce_WhenVersionMatches) {
        // given
        auto stream = MakeConfiguredStream({1});

        // when
        TStreamChanges mismatch;
        const auto wrongVersion = stream.Activate(2, mismatch);
        TStreamChanges first;
        const auto activated = stream.Activate(1, first);
        TStreamChanges repeat;
        const auto again = stream.Activate(1, repeat);

        // then
        UNIT_ASSERT(wrongVersion == EActivateOutcome::VersionMismatch);
        UNIT_ASSERT(mismatch.IsEmpty());
        UNIT_ASSERT(activated == EActivateOutcome::Activated);
        UNIT_ASSERT(first.Config);
        UNIT_ASSERT(again == EActivateOutcome::AlreadyActive);
        UNIT_ASSERT(repeat.IsEmpty());
        UNIT_ASSERT(stream.GetPhase() == EStreamPhase::Active);
    }

    Y_UNIT_TEST(ShouldRebuildMinimum_WhenRestored) {
        // given
        THashMap<TSourceId, TSourceProgress> sources{
            {2, MakeBarrier(110)},
            {3, MakeBarrier(130)},
            {4, std::nullopt},
        };
        THashMap<TSourceId, TSourceReplacement> replacements{
            {1, TSourceReplacement{.Parent = 1, .Children = {3, 4}}},
        };
        auto stream = TStream::Restore(MakeStreamConfig({1, 2}), EStreamPhase::Active,
            std::move(sources), MakeBarrier(100), std::move(replacements));

        // when
        TStreamChanges changes;
        const auto retired = stream.ApplyDelivered(1, MakeBarrier(200), changes);
        stream.ApplyDelivered(4, MakeBarrier(120), changes);

        // then
        UNIT_ASSERT(retired == EDeliveredOutcome::RetiredSource);
        UNIT_ASSERT(changes.ReadyBarrier);
        UNIT_ASSERT_VALUES_EQUAL(stream.GetReadyBarrier()->Version, TRowVersion(110, 0));
    }
}

} // namespace NKikimr::NCdcAggregator
