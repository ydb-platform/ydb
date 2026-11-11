#include <ydb/core/tx/cdc_aggregator/stream/stream.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/builder.h>

namespace NKikimr::NCdcAggregator {

namespace {

TTrackingConfig MakeConfig(TVector<ui64> sources) {
    return TTrackingConfig{
        .StreamPathId = TPathId(72075186233409546, 42),
        .StreamPath = "/Root/table/stream",
        .SourceDataShardIds = std::move(sources),
        .TargetPartitions = {{.PartitionId = 0, .TabletId = 72075186224037900}, {.PartitionId = 1, .TabletId = 72075186224037901}},
    };
}

NPQ::THeartbeat MakeHeartbeat(ui64 step, ui64 txId = 0) {
    return NPQ::THeartbeat{
        .Version = TRowVersion(step, txId),
        .Data = TStringBuilder() << "{\"resolved\":[" << step << "," << txId << "]}",
    };
}

TStream MakeStream(TVector<ui64> sources) {
    TStream stream;
    TStreamChanges changes;
    UNIT_ASSERT(stream.Configure(MakeConfig(std::move(sources)), changes) == EConfigureOutcome::Success);
    return stream;
}

void Write(TStream& stream, ui64 source, ui64 step) {
    TStreamChanges changes;
    UNIT_ASSERT(stream.WriteHeartbeat(source, MakeHeartbeat(step), changes) == ESourceOutcome::Success);
}

void ChangePartitioning(TStream& stream, ui64 parent, TVector<ui64> children) {
    TStreamChanges changes;
    UNIT_ASSERT(stream.ChangePartitioning(parent, children, changes) == ESourceOutcome::Success);
}

} // namespace

Y_UNIT_TEST_SUITE(TCdcAggregatorStream) {

    Y_UNIT_TEST(ShouldAddSourcesWithoutHeartbeat_WhenConfigured) {
        TStream stream;
        TStreamChanges changes;

        const auto outcome = stream.Configure(MakeConfig({1, 2, 3}), changes);

        UNIT_ASSERT(outcome == EConfigureOutcome::Success);
        UNIT_ASSERT(changes.Config);
        UNIT_ASSERT_VALUES_EQUAL(changes.UpsertedSources.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(stream.GetSources().size(), 3);
        UNIT_ASSERT(!stream.GetSources().at(1));
        UNIT_ASSERT(!stream.GetCurrentHeartbeat());
    }

    Y_UNIT_TEST(ShouldKeepProgress_WhenSameConfigRepeats) {
        auto stream = MakeStream({1, 2});
        Write(stream, 1, 100);

        TStreamChanges changes;
        const auto outcome = stream.Configure(MakeConfig({1, 2}), changes);

        UNIT_ASSERT(outcome == EConfigureOutcome::Success);
        UNIT_ASSERT(changes.IsEmpty());
        UNIT_ASSERT_VALUES_EQUAL(*stream.GetSources().at(1), TRowVersion(100, 0));
    }

    Y_UNIT_TEST(ShouldReturnConflict_WhenConfigDiffers) {
        auto stream = MakeStream({1, 2});
        auto otherPartitions = MakeConfig({1, 2});
        otherPartitions.TargetPartitions.pop_back();

        TStreamChanges changes;
        const auto otherSources = stream.Configure(MakeConfig({1, 2, 3}), changes);
        const auto otherTargets = stream.Configure(otherPartitions, changes);

        UNIT_ASSERT(otherSources == EConfigureOutcome::Conflict);
        UNIT_ASSERT(otherTargets == EConfigureOutcome::Conflict);
        UNIT_ASSERT(changes.IsEmpty());
        UNIT_ASSERT_VALUES_EQUAL(stream.GetSources().size(), 2);
    }

    Y_UNIT_TEST(ShouldTakeMinimum_WhenEverySourceWrote) {
        auto stream = MakeStream({1, 2, 3});
        Write(stream, 1, 100);
        Write(stream, 2, 110);

        TStreamChanges changes;
        stream.WriteHeartbeat(3, MakeHeartbeat(120), changes);

        UNIT_ASSERT(changes.CurrentHeartbeat);
        UNIT_ASSERT_VALUES_EQUAL(stream.GetCurrentHeartbeat()->Version, TRowVersion(100, 0));
        UNIT_ASSERT_VALUES_EQUAL(stream.GetCurrentHeartbeat()->Data, MakeHeartbeat(100).Data);
    }

    Y_UNIT_TEST(ShouldBlockCurrentHeartbeat_WhenSourceHasNotWritten) {
        auto stream = MakeStream({1, 2, 3});
        Write(stream, 1, 120);

        TStreamChanges changes;
        stream.WriteHeartbeat(3, MakeHeartbeat(110), changes);

        UNIT_ASSERT(!changes.CurrentHeartbeat);
        UNIT_ASSERT(!stream.GetCurrentHeartbeat());
    }

    Y_UNIT_TEST(ShouldCompareFullVersion_WhenStepsAreEqual) {
        auto stream = MakeStream({1, 2});
        TStreamChanges changes;

        stream.WriteHeartbeat(1, MakeHeartbeat(100, 7), changes);
        stream.WriteHeartbeat(2, MakeHeartbeat(100, 3), changes);

        UNIT_ASSERT_VALUES_EQUAL(stream.GetCurrentHeartbeat()->Version, TRowVersion(100, 3));
    }

    Y_UNIT_TEST(ShouldKeepProgress_WhenHeartbeatIsEqualOrLower) {
        auto stream = MakeStream({1, 2});
        Write(stream, 1, 120);
        Write(stream, 2, 110);

        TStreamChanges changes;
        const auto equal = stream.WriteHeartbeat(1, MakeHeartbeat(120), changes);
        const auto lower = stream.WriteHeartbeat(2, MakeHeartbeat(70), changes);

        UNIT_ASSERT(equal == ESourceOutcome::Success);
        UNIT_ASSERT(lower == ESourceOutcome::Success);
        UNIT_ASSERT(changes.IsEmpty());
        UNIT_ASSERT_VALUES_EQUAL(*stream.GetSources().at(2), TRowVersion(110, 0));
        UNIT_ASSERT_VALUES_EQUAL(stream.GetCurrentHeartbeat()->Version, TRowVersion(110, 0));
    }

    Y_UNIT_TEST(ShouldTakeDataOfNewMinimum_WhenMinimumSourceAdvances) {
        auto stream = MakeStream({1, 2, 3});
        Write(stream, 1, 100);
        Write(stream, 2, 110);
        Write(stream, 3, 120);

        TStreamChanges changes;
        stream.WriteHeartbeat(1, MakeHeartbeat(130), changes);

        UNIT_ASSERT_VALUES_EQUAL(stream.GetCurrentHeartbeat()->Version, TRowVersion(110, 0));
        UNIT_ASSERT_VALUES_EQUAL(stream.GetCurrentHeartbeat()->Data, MakeHeartbeat(110).Data);
        UNIT_ASSERT(changes.ErasedHeartbeats.contains(TRowVersion(100, 0)));
        UNIT_ASSERT(changes.UpsertedHeartbeats.contains(TRowVersion(130, 0)));
    }

    Y_UNIT_TEST(ShouldStoreDataOnce_WhenSourcesWriteSameVersion) {
        auto stream = MakeStream({1, 2});
        TStreamChanges first;
        TStreamChanges second;
        TStreamChanges third;

        stream.WriteHeartbeat(1, MakeHeartbeat(100), first);
        stream.WriteHeartbeat(2, MakeHeartbeat(100), second);
        stream.WriteHeartbeat(1, MakeHeartbeat(110), third);

        UNIT_ASSERT_VALUES_EQUAL(first.UpsertedHeartbeats.size(), 1);
        UNIT_ASSERT(second.UpsertedHeartbeats.empty());
        UNIT_ASSERT(third.ErasedHeartbeats.empty());
        UNIT_ASSERT_VALUES_EQUAL(stream.GetCurrentHeartbeat()->Version, TRowVersion(100, 0));
    }

    Y_UNIT_TEST(ShouldReturnUnknownSource_WhenSourceIsNotTracked) {
        auto stream = MakeStream({1});

        TStreamChanges changes;
        const auto outcome = stream.WriteHeartbeat(9, MakeHeartbeat(100), changes);

        UNIT_ASSERT(outcome == ESourceOutcome::UnknownSource);
        UNIT_ASSERT(!stream.GetSources().contains(9));
        UNIT_ASSERT(changes.IsEmpty());
    }

    Y_UNIT_TEST(ShouldReplaceParentWithChildrenWithoutHeartbeat_WhenSplit) {
        auto stream = MakeStream({1, 2});
        Write(stream, 1, 100);
        Write(stream, 2, 110);

        TStreamChanges changes;
        const auto outcome = stream.ChangePartitioning(1, {3, 4}, changes);

        UNIT_ASSERT(outcome == ESourceOutcome::Success);
        UNIT_ASSERT(!stream.GetSources().contains(1));
        UNIT_ASSERT(!stream.GetSources().at(3));
        UNIT_ASSERT(!stream.GetSources().at(4));
        UNIT_ASSERT(changes.ErasedSources.contains(1));
        UNIT_ASSERT(changes.UpsertedSources.contains(3) && changes.UpsertedSources.contains(4));
        UNIT_ASSERT(changes.ErasedHeartbeats.contains(TRowVersion(100, 0)));
        UNIT_ASSERT_VALUES_EQUAL(stream.GetCurrentHeartbeat()->Version, TRowVersion(100, 0));
    }

    Y_UNIT_TEST(ShouldWaitForEveryChild_WhenSplit) {
        auto stream = MakeStream({1, 2});
        Write(stream, 1, 100);
        Write(stream, 2, 110);
        ChangePartitioning(stream, 1, {3, 4});

        TStreamChanges firstChild;
        stream.WriteHeartbeat(3, MakeHeartbeat(130), firstChild);
        TStreamChanges secondChild;
        stream.WriteHeartbeat(4, MakeHeartbeat(140), secondChild);

        UNIT_ASSERT(!firstChild.CurrentHeartbeat);
        UNIT_ASSERT(secondChild.CurrentHeartbeat);
        UNIT_ASSERT_VALUES_EQUAL(stream.GetCurrentHeartbeat()->Version, TRowVersion(110, 0));
    }

    Y_UNIT_TEST(ShouldKeepCurrentHeartbeat_WhenChildWritesBelowIt) {
        auto stream = MakeStream({1, 2});
        Write(stream, 1, 100);
        Write(stream, 2, 110);
        ChangePartitioning(stream, 1, {3});

        TStreamChanges changes;
        stream.WriteHeartbeat(3, MakeHeartbeat(90), changes);

        UNIT_ASSERT(!changes.CurrentHeartbeat);
        UNIT_ASSERT_VALUES_EQUAL(stream.GetCurrentHeartbeat()->Version, TRowVersion(100, 0));
    }

    Y_UNIT_TEST(ShouldKeepChildProgress_WhenPartitioningChangeRepeats) {
        auto stream = MakeStream({1});
        ChangePartitioning(stream, 1, {2, 3});
        Write(stream, 2, 100);

        TStreamChanges changes;
        const auto outcome = stream.ChangePartitioning(1, {2, 3}, changes);

        UNIT_ASSERT(outcome == ESourceOutcome::Success);
        UNIT_ASSERT(changes.IsEmpty());
        UNIT_ASSERT_VALUES_EQUAL(*stream.GetSources().at(2), TRowVersion(100, 0));
    }

    Y_UNIT_TEST(ShouldReturnUnknownSource_WhenParentAndSomeChildAreAbsent) {
        auto stream = MakeStream({1});
        ChangePartitioning(stream, 1, {2, 3});

        TStreamChanges changes;
        const auto otherChildren = stream.ChangePartitioning(1, {2, 4}, changes);
        const auto unknownParent = stream.ChangePartitioning(9, {5}, changes);

        UNIT_ASSERT(otherChildren == ESourceOutcome::UnknownSource);
        UNIT_ASSERT(unknownParent == ESourceOutcome::UnknownSource);
        UNIT_ASSERT(changes.IsEmpty());
        UNIT_ASSERT(!stream.GetSources().contains(4));
    }

    Y_UNIT_TEST(ShouldRejectLateHeartbeat_WhenParentIsReplaced) {
        auto stream = MakeStream({1, 2});
        ChangePartitioning(stream, 1, {3});

        TStreamChanges changes;
        const auto outcome = stream.WriteHeartbeat(1, MakeHeartbeat(100), changes);

        UNIT_ASSERT(outcome == ESourceOutcome::UnknownSource);
        UNIT_ASSERT(!stream.GetSources().contains(1));
        UNIT_ASSERT(changes.IsEmpty());
    }

    Y_UNIT_TEST(ShouldKeepExistingChild_WhenSecondParentMerges) {
        auto stream = MakeStream({1, 2});
        Write(stream, 1, 100);
        Write(stream, 2, 110);
        ChangePartitioning(stream, 1, {3});
        Write(stream, 3, 120);

        TStreamChanges changes;
        const auto outcome = stream.ChangePartitioning(2, {3}, changes);

        UNIT_ASSERT(outcome == ESourceOutcome::Success);
        UNIT_ASSERT(!changes.UpsertedSources.contains(3));
        UNIT_ASSERT_VALUES_EQUAL(stream.GetSources().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(*stream.GetSources().at(3), TRowVersion(120, 0));
        UNIT_ASSERT(changes.CurrentHeartbeat);
        UNIT_ASSERT_VALUES_EQUAL(stream.GetCurrentHeartbeat()->Version, TRowVersion(120, 0));
        UNIT_ASSERT_VALUES_EQUAL(stream.GetCurrentHeartbeat()->Data, MakeHeartbeat(120).Data);
    }

    Y_UNIT_TEST(ShouldRebuildMinimum_WhenRestored) {
        THashMap<ui64, std::optional<TRowVersion>> sources{
            {2, TRowVersion(110, 0)},
            {3, TRowVersion(130, 0)},
            {4, std::nullopt},
        };
        const TMap<TRowVersion, TString> heartbeats{
            {TRowVersion(110, 0), MakeHeartbeat(110).Data},
            {TRowVersion(130, 0), MakeHeartbeat(130).Data},
        };
        auto stream = TStream::Restore(MakeConfig({1, 2}), std::move(sources), heartbeats, MakeHeartbeat(100));

        TStreamChanges changes;
        stream.WriteHeartbeat(4, MakeHeartbeat(120), changes);

        UNIT_ASSERT(changes.CurrentHeartbeat);
        UNIT_ASSERT_VALUES_EQUAL(stream.GetCurrentHeartbeat()->Version, TRowVersion(110, 0));
        UNIT_ASSERT_VALUES_EQUAL(stream.GetCurrentHeartbeat()->Data, MakeHeartbeat(110).Data);
    }
}

} // namespace NKikimr::NCdcAggregator
