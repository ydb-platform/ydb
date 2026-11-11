#include <ydb/core/tx/cdc_aggregator/stream/usecases.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/builder.h>

namespace NKikimr::NCdcAggregator {

namespace {

using TConfigureResult = NKikimrCdcAggregator::TEvConfigureTrackingResult;
using TWriteResult = NKikimrCdcAggregator::TEvWriteHeartbeatsResult;
using THeartbeatResult = TWriteResult::THeartbeatResult;
using TPartitioningAck = NKikimrCdcAggregator::TEvPartitioningChangedAck;

NKikimrCdcAggregator::TEvConfigureTracking MakeConfigureRequest(TVector<ui64> sources) {
    NKikimrCdcAggregator::TEvConfigureTracking request;
    auto& config = *request.MutableConfig();
    TPathId(72075186233409546, 42).ToProto(config.MutableStreamPathId());
    config.SetStreamPath("/Root/table/stream");
    for (const auto source : sources) {
        config.AddSourceDataShardIds(source);
    }
    for (const ui32 partitionId : {0, 1}) {
        auto& partition = *config.AddTargetPartitions();
        partition.SetPartitionId(partitionId);
        partition.SetTabletId(72075186224037900 + partitionId);
    }
    return request;
}

TStream MakeStream(TVector<ui64> sources) {
    TStream stream;
    TStreamChanges changes;
    UNIT_ASSERT(ConfigureTracking(stream, MakeConfigureRequest(std::move(sources)), changes).GetStatus() == TConfigureResult::SUCCESS);
    return stream;
}

void AddHeartbeat(NKikimrCdcAggregator::TEvWriteHeartbeats& request, ui64 source, ui64 step) {
    auto& heartbeat = *request.AddHeartbeats();
    heartbeat.SetSourceDataShardId(source);
    NPQ::THeartbeat{
        .Version = TRowVersion(step, 0),
        .Data = TStringBuilder() << "{\"resolved\":[" << step << ",0]}",
    }.Serialize(*heartbeat.MutableHeartbeat());
}

NKikimrCdcAggregator::TEvWriteHeartbeats MakeWriteRequest(TVector<std::pair<ui64, ui64>> heartbeats) {
    NKikimrCdcAggregator::TEvWriteHeartbeats request;
    for (const auto& [source, step] : heartbeats) {
        AddHeartbeat(request, source, step);
    }
    return request;
}

NKikimrCdcAggregator::TEvPartitioningChanged MakePartitioningChanged(ui64 parent, TVector<ui64> children) {
    NKikimrCdcAggregator::TEvPartitioningChanged request;
    request.SetParentDataShardId(parent);
    for (const auto child : children) {
        request.AddChildDataShardIds(child);
    }
    request.SetOperationId(77);
    return request;
}

TWriteResult Write(TStream& stream, TVector<std::pair<ui64, ui64>> heartbeats) {
    TStreamChanges changes;
    return WriteHeartbeats(stream, MakeWriteRequest(std::move(heartbeats)), changes);
}

} // namespace

Y_UNIT_TEST_SUITE(TConfigureTracking) {

    Y_UNIT_TEST(ShouldConfigure_WhenTrackingIsNew) {
        TStream stream;
        TStreamChanges changes;

        const auto result = ConfigureTracking(stream, MakeConfigureRequest({3, 1, 2}), changes);

        UNIT_ASSERT(result.GetStatus() == TConfigureResult::SUCCESS);
        UNIT_ASSERT(changes.Config);
        UNIT_ASSERT(stream.GetConfig().SourceDataShardIds == TVector<ui64>({1, 2, 3}));
        UNIT_ASSERT_VALUES_EQUAL(stream.GetConfig().TargetPartitions.size(), 2);
    }

    Y_UNIT_TEST(ShouldKeepProgress_WhenSameConfigRepeats) {
        auto stream = MakeStream({1, 2});
        Write(stream, {{1, 100}});

        TStreamChanges changes;
        const auto result = ConfigureTracking(stream, MakeConfigureRequest({2, 1}), changes);

        UNIT_ASSERT(result.GetStatus() == TConfigureResult::SUCCESS);
        UNIT_ASSERT(changes.IsEmpty());
        UNIT_ASSERT_VALUES_EQUAL(*stream.GetSources().at(1), TRowVersion(100, 0));
    }

    Y_UNIT_TEST(ShouldReturnConflict_WhenConfigDiffers) {
        auto stream = MakeStream({1, 2});

        TStreamChanges changes;
        const auto result = ConfigureTracking(stream, MakeConfigureRequest({1, 2, 3}), changes);

        UNIT_ASSERT(result.GetStatus() == TConfigureResult::CONFLICT);
        UNIT_ASSERT(!result.GetError().empty());
        UNIT_ASSERT(changes.IsEmpty());
    }

    Y_UNIT_TEST(ShouldReturnBadRequest_WhenConfigIsMalformed) {
        auto noPathId = MakeConfigureRequest({1});
        noPathId.MutableConfig()->ClearStreamPathId();
        auto noSources = MakeConfigureRequest({});
        auto zeroSource = MakeConfigureRequest({0, 1});
        auto duplicateSource = MakeConfigureRequest({1, 1});
        auto noPartitions = MakeConfigureRequest({1});
        noPartitions.MutableConfig()->ClearTargetPartitions();
        auto duplicatePartition = MakeConfigureRequest({1});
        duplicatePartition.MutableConfig()->MutableTargetPartitions(1)->SetPartitionId(0);
        auto noPartitionTablet = MakeConfigureRequest({1});
        noPartitionTablet.MutableConfig()->MutableTargetPartitions(0)->ClearTabletId();

        for (const auto& request : {noPathId, noSources, zeroSource, duplicateSource, noPartitions, duplicatePartition, noPartitionTablet}) {
            TStream stream;
            TStreamChanges changes;

            const auto result = ConfigureTracking(stream, request, changes);

            UNIT_ASSERT(result.GetStatus() == TConfigureResult::BAD_REQUEST);
            UNIT_ASSERT(!result.GetError().empty());
            UNIT_ASSERT(!stream.IsConfigured());
            UNIT_ASSERT(changes.IsEmpty());
        }
    }
}

Y_UNIT_TEST_SUITE(TWriteHeartbeats) {

    Y_UNIT_TEST(ShouldReturnNotReady_WhenTrackingIsNotConfigured) {
        TStream stream;

        const auto result = Write(stream, {{1, 100}});

        UNIT_ASSERT(result.GetStatus() == TWriteResult::NOT_READY);
        UNIT_ASSERT_VALUES_EQUAL(result.ResultsSize(), 0);
    }

    Y_UNIT_TEST(ShouldTakeMinimum_WhenThreeSourcesWrite) {
        auto stream = MakeStream({1, 2, 3});
        TStreamChanges changes;

        const auto result = WriteHeartbeats(stream, MakeWriteRequest({{1, 100}, {2, 110}, {3, 120}}), changes);

        UNIT_ASSERT(result.GetStatus() == TWriteResult::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(result.ResultsSize(), 3);
        for (const auto& sourceResult : result.GetResults()) {
            UNIT_ASSERT(sourceResult.GetStatus() == THeartbeatResult::SUCCESS);
        }
        UNIT_ASSERT_VALUES_EQUAL(TRowVersion::FromProto(result.GetResults(2).GetWrittenVersion()), TRowVersion(120, 0));
        UNIT_ASSERT(changes.CurrentHeartbeat);
        UNIT_ASSERT_VALUES_EQUAL(stream.GetCurrentHeartbeat()->Version, TRowVersion(100, 0));
    }

    Y_UNIT_TEST(ShouldReturnStoredVersion_WhenHeartbeatIsLower) {
        auto stream = MakeStream({1, 2});
        Write(stream, {{1, 120}});

        TStreamChanges changes;
        const auto result = WriteHeartbeats(stream, MakeWriteRequest({{1, 100}}), changes);

        UNIT_ASSERT(result.GetResults(0).GetStatus() == THeartbeatResult::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(TRowVersion::FromProto(result.GetResults(0).GetWrittenVersion()), TRowVersion(120, 0));
        UNIT_ASSERT(changes.IsEmpty());
    }

    Y_UNIT_TEST(ShouldRejectOnlyInvalidHeartbeats_WhenBatchIsMixed) {
        auto stream = MakeStream({1, 2});
        TStreamChanges setup;
        PartitioningChanged(stream, MakePartitioningChanged(2, {3}), setup);
        auto request = MakeWriteRequest({{1, 100}, {9, 100}, {2, 100}, {0, 100}, {3, 0}, {3, 100}, {3, 100}});
        request.MutableHeartbeats(4)->ClearHeartbeat();
        request.MutableHeartbeats(5)->MutableHeartbeat()->ClearData();
        request.MutableHeartbeats(6)->MutableHeartbeat()->SetData(TString(MaxHeartbeatDataSize + 1, 'x'));
        AddHeartbeat(request, 3, 0);

        TStreamChanges changes;
        const auto result = WriteHeartbeats(stream, request, changes);

        UNIT_ASSERT(result.GetStatus() == TWriteResult::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(result.ResultsSize(), 8);
        UNIT_ASSERT(result.GetResults(0).GetStatus() == THeartbeatResult::SUCCESS);
        UNIT_ASSERT(result.GetResults(1).GetStatus() == THeartbeatResult::UNKNOWN_SOURCE);
        UNIT_ASSERT(result.GetResults(2).GetStatus() == THeartbeatResult::UNKNOWN_SOURCE);
        for (int index = 3; index < 8; ++index) {
            UNIT_ASSERT(result.GetResults(index).GetStatus() == THeartbeatResult::BAD_REQUEST);
            UNIT_ASSERT(!result.GetResults(index).HasWrittenVersion());
        }
        UNIT_ASSERT(!stream.GetSources().at(3));
    }

    Y_UNIT_TEST(ShouldReturnBadRequest_WhenBatchIsTooLarge) {
        auto stream = MakeStream({1});
        NKikimrCdcAggregator::TEvWriteHeartbeats request;
        for (size_t index = 0; index <= MaxHeartbeatsPerBatch; ++index) {
            AddHeartbeat(request, 1, 100);
        }

        TStreamChanges changes;
        const auto result = WriteHeartbeats(stream, request, changes);

        UNIT_ASSERT(result.GetStatus() == TWriteResult::BAD_REQUEST);
        UNIT_ASSERT(changes.IsEmpty());
        UNIT_ASSERT(!stream.GetSources().at(1));
    }
}

Y_UNIT_TEST_SUITE(TPartitioningChanged) {

    Y_UNIT_TEST(ShouldReturnNotReady_WhenTrackingIsNotConfigured) {
        TStream stream;
        TStreamChanges changes;

        const auto result = PartitioningChanged(stream, MakePartitioningChanged(1, {2, 3}), changes);

        UNIT_ASSERT(result.GetStatus() == TPartitioningAck::NOT_READY);
        UNIT_ASSERT(changes.IsEmpty());
    }

    Y_UNIT_TEST(ShouldReturnBadRequest_WhenChangeIsMalformed) {
        auto stream = MakeStream({1});
        const auto noChildren = MakePartitioningChanged(1, {});
        const auto zeroParent = MakePartitioningChanged(0, {2});
        const auto zeroChild = MakePartitioningChanged(1, {0, 2});
        const auto duplicateChild = MakePartitioningChanged(1, {3, 2, 3});
        const auto parentAsChild = MakePartitioningChanged(1, {1, 2});

        for (const auto& request : {noChildren, zeroParent, zeroChild, duplicateChild, parentAsChild}) {
            TStreamChanges changes;

            const auto result = PartitioningChanged(stream, request, changes);

            UNIT_ASSERT(result.GetStatus() == TPartitioningAck::BAD_REQUEST);
            UNIT_ASSERT(changes.IsEmpty());
            UNIT_ASSERT(stream.GetSources().contains(1));
        }
    }

    Y_UNIT_TEST(ShouldSucceedWithoutChanges_WhenChangeRepeats) {
        auto stream = MakeStream({1, 2});
        const auto request = MakePartitioningChanged(1, {4, 3});

        TStreamChanges first;
        const auto firstResult = PartitioningChanged(stream, request, first);
        TStreamChanges second;
        const auto secondResult = PartitioningChanged(stream, request, second);

        UNIT_ASSERT(firstResult.GetStatus() == TPartitioningAck::SUCCESS);
        UNIT_ASSERT(!first.IsEmpty());
        UNIT_ASSERT(secondResult.GetStatus() == TPartitioningAck::SUCCESS);
        UNIT_ASSERT(second.IsEmpty());
    }

    Y_UNIT_TEST(ShouldReturnUnknownSource_WhenParentIsUnknown) {
        auto stream = MakeStream({1});

        TStreamChanges changes;
        const auto result = PartitioningChanged(stream, MakePartitioningChanged(9, {3}), changes);

        UNIT_ASSERT(result.GetStatus() == TPartitioningAck::UNKNOWN_SOURCE);
        UNIT_ASSERT(changes.IsEmpty());
    }
}

Y_UNIT_TEST_SUITE(TDescribeTracking) {

    Y_UNIT_TEST(ShouldDescribeSourcesAndCurrentHeartbeat) {
        auto stream = MakeStream({1, 2, 3});
        Write(stream, {{3, 120}, {1, 100}, {2, 110}});
        TStreamChanges changes;
        PartitioningChanged(stream, MakePartitioningChanged(1, {4}), changes);

        const auto result = DescribeTracking(stream);

        UNIT_ASSERT_VALUES_EQUAL(TRowVersion::FromProto(result.GetCurrentHeartbeat()), TRowVersion(100, 0));
        UNIT_ASSERT_VALUES_EQUAL(result.SourcesSize(), 3);
        UNIT_ASSERT_VALUES_EQUAL(result.GetSources(0).GetDataShardId(), 2);
        UNIT_ASSERT_VALUES_EQUAL(TRowVersion::FromProto(result.GetSources(0).GetLastHeartbeat()), TRowVersion(110, 0));
        UNIT_ASSERT_VALUES_EQUAL(result.GetSources(1).GetDataShardId(), 3);
        UNIT_ASSERT_VALUES_EQUAL(result.GetSources(2).GetDataShardId(), 4);
        UNIT_ASSERT(!result.GetSources(2).HasLastHeartbeat());
    }
}

} // namespace NKikimr::NCdcAggregator
