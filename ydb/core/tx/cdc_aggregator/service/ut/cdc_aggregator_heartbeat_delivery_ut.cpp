#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/tx/cdc_aggregator/service/cdc_aggregator_heartbeat_delivery.h>
#include <ydb/core/tx/cdc_aggregator/service/cdc_aggregator_heartbeat_delivery_proto.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NCdcAggregator {

namespace {

using EStatus = TCdcAggregatorHeartbeatDeliveryReply::EStatus;
using TProtoResult = NKikimrCdcAggregator::TEvWriteHeartbeatsResult;

NPQ::THeartbeat MakeHeartbeat(ui64 step, const TString& data = {}) {
    return NPQ::THeartbeat{
        .Version = TRowVersion(step, 0),
        .Data = data,
    };
}

TCdcAggregatorHeartbeatDeliveryResult MakeWritten(const TVector<std::pair<ui64, ui64>>& written) {
    TCdcAggregatorHeartbeatDeliveryResult result;
    for (const auto& [sourceDataShardId, step] : written) {
        result.WrittenVersions[sourceDataShardId] = TRowVersion(step, 0);
    }
    return result;
}

TVector<ui64> GetSources(const TCdcAggregatorHeartbeatBatch& batch) {
    TVector<ui64> sources;
    for (const auto& item : batch) {
        sources.push_back(item.SourceDataShardId);
    }
    return sources;
}

} // namespace

Y_UNIT_TEST_SUITE(CdcAggregatorHeartbeatDelivery) {

    Y_UNIT_TEST(BatchesSources) {
        TCdcAggregatorHeartbeatDelivery delivery(100);
        for (ui64 sourceDataShardId : {1, 2, 3}) {
            delivery.AddHeartbeat(sourceDataShardId, MakeHeartbeat(100));
        }

        UNIT_ASSERT_VALUES_EQUAL(GetSources(delivery.PrepareNextBatchIfPossible().value()), TVector<ui64>({1, 2, 3}));
        UNIT_ASSERT(!delivery.PrepareNextBatchIfPossible());
    }

    Y_UNIT_TEST(KeepsNewestHeartbeatOfSource) {
        TCdcAggregatorHeartbeatDelivery delivery(100);
        delivery.AddHeartbeat(1, MakeHeartbeat(100, "a"));
        delivery.AddHeartbeat(1, MakeHeartbeat(120, "b"));
        delivery.AddHeartbeat(1, MakeHeartbeat(110, "c"));

        const auto batch = delivery.PrepareNextBatchIfPossible().value();
        UNIT_ASSERT_VALUES_EQUAL(batch.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(batch[0].Heartbeat.Version, TRowVersion(120, 0));
        UNIT_ASSERT_VALUES_EQUAL(batch[0].Heartbeat.Data, "b");

        const auto replies = delivery.CompleteBatch(MakeWritten({{1, 120}}));
        UNIT_ASSERT_VALUES_EQUAL(replies.size(), 1);
        UNIT_ASSERT(replies[0].Status == EStatus::Written);
        UNIT_ASSERT_VALUES_EQUAL(replies[0].WrittenVersion, TRowVersion(120, 0));
        UNIT_ASSERT(delivery.IsIdle());
    }

    Y_UNIT_TEST(SendsHeartbeatAddedInFlightInNextBatch) {
        TCdcAggregatorHeartbeatDelivery delivery(100);
        delivery.AddHeartbeat(1, MakeHeartbeat(100));
        delivery.PrepareNextBatchIfPossible().value();

        delivery.AddHeartbeat(1, MakeHeartbeat(120));
        UNIT_ASSERT(!delivery.PrepareNextBatchIfPossible());

        const auto replies = delivery.CompleteBatch(MakeWritten({{1, 100}}));
        UNIT_ASSERT_VALUES_EQUAL(replies.size(), 1);
        UNIT_ASSERT(replies[0].Status == EStatus::Written);
        UNIT_ASSERT_VALUES_EQUAL(replies[0].WrittenVersion, TRowVersion(100, 0));

        UNIT_ASSERT_VALUES_EQUAL(delivery.PrepareNextBatchIfPossible().value()[0].Heartbeat.Version, TRowVersion(120, 0));
    }

    Y_UNIT_TEST(RetriesSourcesWithoutResult) {
        TCdcAggregatorHeartbeatDelivery delivery(100);
        for (ui64 sourceDataShardId : {1, 2, 3}) {
            delivery.AddHeartbeat(sourceDataShardId, MakeHeartbeat(100));
        }

        delivery.PrepareNextBatchIfPossible().value();
        UNIT_ASSERT(!delivery.IsIdle());

        const auto replies = delivery.CompleteBatch(MakeWritten({{1, 100}, {2, 100}}));
        UNIT_ASSERT_VALUES_EQUAL(replies.size(), 3);
        UNIT_ASSERT(replies[0].Status == EStatus::Written);
        UNIT_ASSERT(replies[1].Status == EStatus::Written);
        UNIT_ASSERT_VALUES_EQUAL(replies[2].SourceDataShardId, 3);
        UNIT_ASSERT(replies[2].Status == EStatus::Retry);
        UNIT_ASSERT(delivery.IsIdle());
    }

    Y_UNIT_TEST(LimitsBatchSize) {
        TCdcAggregatorHeartbeatDelivery delivery(2);
        for (ui64 sourceDataShardId : {1, 2, 3}) {
            delivery.AddHeartbeat(sourceDataShardId, MakeHeartbeat(100));
        }

        UNIT_ASSERT_VALUES_EQUAL(GetSources(delivery.PrepareNextBatchIfPossible().value()), TVector<ui64>({1, 2}));
        UNIT_ASSERT(!delivery.PrepareNextBatchIfPossible());

        delivery.CompleteBatch(MakeWritten({{1, 100}, {2, 100}}));
        UNIT_ASSERT_VALUES_EQUAL(GetSources(delivery.PrepareNextBatchIfPossible().value()), TVector<ui64>({3}));
    }

    Y_UNIT_TEST(RetriesAllSourcesWhenBatchFails) {
        TCdcAggregatorHeartbeatDelivery delivery(100);
        delivery.AddHeartbeat(1, MakeHeartbeat(100));
        delivery.PrepareNextBatchIfPossible().value();
        delivery.AddHeartbeat(2, MakeHeartbeat(100));

        const auto replies = delivery.FailBatch("NOT_READY: ");
        UNIT_ASSERT_VALUES_EQUAL(replies.size(), 2);
        for (const auto& reply : replies) {
            UNIT_ASSERT(reply.Status == EStatus::Retry);
            UNIT_ASSERT_VALUES_EQUAL(reply.Error, "NOT_READY: ");
        }
        UNIT_ASSERT(delivery.IsIdle());
    }

    Y_UNIT_TEST(IgnoresFailureWithoutBatchInFlight) {
        TCdcAggregatorHeartbeatDelivery delivery(100);
        delivery.AddHeartbeat(1, MakeHeartbeat(100));

        UNIT_ASSERT(delivery.FailBatch("NOT_READY: ").empty());
        UNIT_ASSERT(delivery.PrepareNextBatchIfPossible());
    }

    Y_UNIT_TEST(RejectsUnknownSource) {
        TCdcAggregatorHeartbeatDelivery delivery(100);
        delivery.AddHeartbeat(1, MakeHeartbeat(100));
        delivery.PrepareNextBatchIfPossible().value();

        TCdcAggregatorHeartbeatDeliveryResult result;
        result.RejectionErrors[1] = "UNKNOWN_SOURCE";

        const auto replies = delivery.CompleteBatch(result);
        UNIT_ASSERT_VALUES_EQUAL(replies.size(), 1);
        UNIT_ASSERT(replies[0].Status == EStatus::Rejected);
        UNIT_ASSERT_VALUES_EQUAL(replies[0].Error, "UNKNOWN_SOURCE");
        UNIT_ASSERT(delivery.IsIdle());
    }

}

Y_UNIT_TEST_SUITE(CdcAggregatorHeartbeatDeliveryProtoConverter) {

    Y_UNIT_TEST(SerializesBatch) {
        const auto ev = TCdcAggregatorHeartbeatDeliveryProtoConverter().SerializeBatch({
            {.SourceDataShardId = 1, .Heartbeat = MakeHeartbeat(120, "b")},
        });

        UNIT_ASSERT_VALUES_EQUAL(ev->Record.HeartbeatsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(ev->Record.GetHeartbeats(0).GetSourceDataShardId(), 1);
        const auto heartbeat = NPQ::THeartbeat::Parse(ev->Record.GetHeartbeats(0).GetHeartbeat());
        UNIT_ASSERT_VALUES_EQUAL(heartbeat.Version, TRowVersion(120, 0));
        UNIT_ASSERT_VALUES_EQUAL(heartbeat.Data, "b");
    }

    Y_UNIT_TEST(ParsesWrittenAndRejectedSources) {
        TProtoResult proto;
        proto.SetStatus(TProtoResult::SUCCESS);
        auto& written = *proto.AddResults();
        written.SetSourceDataShardId(1);
        written.SetStatus(TProtoResult::THeartbeatResult::SUCCESS);
        TRowVersion(100, 0).ToProto(written.MutableWrittenVersion());
        auto& unknown = *proto.AddResults();
        unknown.SetSourceDataShardId(2);
        unknown.SetStatus(TProtoResult::THeartbeatResult::UNKNOWN_SOURCE);
        auto& withoutVersion = *proto.AddResults();
        withoutVersion.SetSourceDataShardId(3);
        withoutVersion.SetStatus(TProtoResult::THeartbeatResult::SUCCESS);

        const auto [result, error] = TCdcAggregatorHeartbeatDeliveryProtoConverter().ParseBatchResult(proto);
        UNIT_ASSERT_C(result, error);
        UNIT_ASSERT_VALUES_EQUAL(result->WrittenVersions.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result->WrittenVersions.at(1), TRowVersion(100, 0));
        UNIT_ASSERT_VALUES_EQUAL(result->RejectionErrors.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result->RejectionErrors.at(2), "UNKNOWN_SOURCE");
    }

    Y_UNIT_TEST(FailsBatchNotProcessedByTablet) {
        for (const auto status : {TProtoResult::NOT_READY, TProtoResult::BAD_REQUEST}) {
            TProtoResult proto;
            proto.SetStatus(status);
            proto.SetError("Stream is not configured");

            const auto [result, error] = TCdcAggregatorHeartbeatDeliveryProtoConverter().ParseBatchResult(proto);
            UNIT_ASSERT(!result);
            UNIT_ASSERT_VALUES_EQUAL(error, TProtoResult::EStatus_Name(status) + ": Stream is not configured");
        }
    }

}

} // namespace NKikimr::NCdcAggregator
