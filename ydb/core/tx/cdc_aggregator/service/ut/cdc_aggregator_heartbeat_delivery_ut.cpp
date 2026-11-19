#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/tx/cdc_aggregator/service/cdc_aggregator_heartbeat_delivery.h>
#include <ydb/core/tx/cdc_aggregator/service/cdc_aggregator_heartbeat_delivery_proto.h>

#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>

namespace NKikimr::NCdcAggregator {

namespace {

using TProtoResult = NKikimrCdcAggregator::TEvWriteHeartbeatsResult;

NPQ::THeartbeat MakeHeartbeat(ui64 step, const TString& data = {}) {
    return NPQ::THeartbeat{
        .Version = TRowVersion(step, 0),
        .Data = data,
    };
}

TCdcAggregatorWriteHeartbeatsResult MakeWritten(const TVector<std::pair<ui64, ui64>>& written) {
    TCdcAggregatorWriteHeartbeatsResult writeHeartbeatsResult;
    for (const auto& [sourceDataShardId, step] : written) {
        writeHeartbeatsResult.WrittenVersionsBySourceDataShardId[sourceDataShardId] = TRowVersion(step, 0);
    }
    return writeHeartbeatsResult;
}

TVector<ui64> GetSortedSourceDataShardIds(const TCdcAggregatorHeartbeatBatch& batch) {
    TVector<ui64> sourceDataShardIds;
    for (const auto& [sourceDataShardId, heartbeat] : batch) {
        sourceDataShardIds.push_back(sourceDataShardId);
    }
    std::sort(sourceDataShardIds.begin(), sourceDataShardIds.end());
    return sourceDataShardIds;
}

} // namespace

Y_UNIT_TEST_SUITE(CdcAggregatorHeartbeatDelivery) {

    Y_UNIT_TEST(ShouldPrepareBatchOfAllPendingSourcesWhenBelowMaxBatchSize) {
        // given
        TCdcAggregatorHeartbeatDelivery delivery(100);
        for (ui64 sourceDataShardId : {1, 2, 3}) {
            delivery.AddHeartbeat(sourceDataShardId, MakeHeartbeat(100));
        }

        // when
        const auto batch = delivery.PrepareNextBatchIfPossible();

        // then
        UNIT_ASSERT_VALUES_EQUAL(GetSortedSourceDataShardIds(batch.value()), TVector<ui64>({1, 2, 3}));
    }

    Y_UNIT_TEST(ShouldNotPrepareBatchWhenPreviousBatchAwaitsReply) {
        // given
        TCdcAggregatorHeartbeatDelivery delivery(100);
        delivery.AddHeartbeat(1, MakeHeartbeat(100));
        delivery.PrepareNextBatchIfPossible().value();
        delivery.AddHeartbeat(2, MakeHeartbeat(100));

        // when
        const auto batch = delivery.PrepareNextBatchIfPossible();

        // then
        UNIT_ASSERT(!batch);
    }

    Y_UNIT_TEST(ShouldSendNewestHeartbeatWhenSourceAddsSeveral) {
        // given
        TCdcAggregatorHeartbeatDelivery delivery(100);
        delivery.AddHeartbeat(1, MakeHeartbeat(100, "a"));
        delivery.AddHeartbeat(1, MakeHeartbeat(120, "b"));
        delivery.AddHeartbeat(1, MakeHeartbeat(110, "c"));

        // when
        const auto batch = delivery.PrepareNextBatchIfPossible().value();

        // then
        UNIT_ASSERT_VALUES_EQUAL(batch.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(batch.at(1).Version, TRowVersion(120, 0));
        UNIT_ASSERT_VALUES_EQUAL(batch.at(1).Data, "b");
    }

    Y_UNIT_TEST(ShouldSendHeartbeatInNextBatchWhenAddedWhileAwaitingReply) {
        // given
        TCdcAggregatorHeartbeatDelivery delivery(100);
        delivery.AddHeartbeat(1, MakeHeartbeat(100));
        delivery.PrepareNextBatchIfPossible().value();
        delivery.AddHeartbeat(1, MakeHeartbeat(120));
        delivery.CompleteBatch(MakeWritten({{1, 100}}));

        // when
        const auto batch = delivery.PrepareNextBatchIfPossible().value();

        // then
        UNIT_ASSERT_VALUES_EQUAL(batch.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(batch.at(1).Version, TRowVersion(120, 0));
    }

    Y_UNIT_TEST(ShouldReplyWrittenVersionWhenTabletWroteHeartbeat) {
        // given
        TCdcAggregatorHeartbeatDelivery delivery(100);
        delivery.AddHeartbeat(1, MakeHeartbeat(120));
        delivery.PrepareNextBatchIfPossible().value();

        // when
        const auto replies = delivery.CompleteBatch(MakeWritten({{1, 120}}));

        // then
        UNIT_ASSERT_VALUES_EQUAL(replies.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(std::get<TCdcAggregatorHeartbeatWritten>(replies.at(1)).Version, TRowVersion(120, 0));
        UNIT_ASSERT(delivery.IsIdle());
    }

    Y_UNIT_TEST(ShouldRetrySourceWhenTabletReplyMissesIt) {
        // given
        TCdcAggregatorHeartbeatDelivery delivery(100);
        for (ui64 sourceDataShardId : {1, 2, 3}) {
            delivery.AddHeartbeat(sourceDataShardId, MakeHeartbeat(100));
        }
        delivery.PrepareNextBatchIfPossible().value();

        // when
        const auto replies = delivery.CompleteBatch(MakeWritten({{1, 100}, {2, 100}}));

        // then
        UNIT_ASSERT_VALUES_EQUAL(replies.size(), 3);
        UNIT_ASSERT(std::holds_alternative<TCdcAggregatorHeartbeatWritten>(replies.at(1)));
        UNIT_ASSERT(std::holds_alternative<TCdcAggregatorHeartbeatWritten>(replies.at(2)));
        UNIT_ASSERT(std::holds_alternative<TCdcAggregatorHeartbeatRetry>(replies.at(3)));
        UNIT_ASSERT(delivery.IsIdle());
    }

    Y_UNIT_TEST(ShouldRejectSourceWhenTabletReportsUnknownSource) {
        // given
        TCdcAggregatorHeartbeatDelivery delivery(100);
        delivery.AddHeartbeat(1, MakeHeartbeat(100));
        delivery.PrepareNextBatchIfPossible().value();
        TCdcAggregatorWriteHeartbeatsResult writeHeartbeatsResult;
        writeHeartbeatsResult.RejectionErrorsBySourceDataShardId[1] = "UNKNOWN_SOURCE";

        // when
        const auto replies = delivery.CompleteBatch(writeHeartbeatsResult);

        // then
        UNIT_ASSERT_VALUES_EQUAL(replies.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(std::get<TCdcAggregatorHeartbeatRejected>(replies.at(1)).Error, "UNKNOWN_SOURCE");
        UNIT_ASSERT(delivery.IsIdle());
    }

    Y_UNIT_TEST(ShouldLeaveRestInQueueWhenBatchReachesMaxSize) {
        // given
        TCdcAggregatorHeartbeatDelivery delivery(2);
        for (ui64 sourceDataShardId : {1, 2, 3}) {
            delivery.AddHeartbeat(sourceDataShardId, MakeHeartbeat(100));
        }

        // when
        const auto batch = delivery.PrepareNextBatchIfPossible().value();

        // then
        UNIT_ASSERT_VALUES_EQUAL(GetSortedSourceDataShardIds(batch), TVector<ui64>({1, 2}));
    }

    Y_UNIT_TEST(ShouldPrepareRestOfQueueWhenPreviousBatchCompleted) {
        // given
        TCdcAggregatorHeartbeatDelivery delivery(2);
        for (ui64 sourceDataShardId : {1, 2, 3}) {
            delivery.AddHeartbeat(sourceDataShardId, MakeHeartbeat(100));
        }
        delivery.PrepareNextBatchIfPossible().value();
        delivery.CompleteBatch(MakeWritten({{1, 100}, {2, 100}}));

        // when
        const auto batch = delivery.PrepareNextBatchIfPossible().value();

        // then
        UNIT_ASSERT_VALUES_EQUAL(GetSortedSourceDataShardIds(batch), TVector<ui64>({3}));
    }

    Y_UNIT_TEST(ShouldRetryAwaitingAndPendingSourcesWhenBatchFails) {
        // given
        TCdcAggregatorHeartbeatDelivery delivery(100);
        delivery.AddHeartbeat(1, MakeHeartbeat(100));
        delivery.PrepareNextBatchIfPossible().value();
        delivery.AddHeartbeat(2, MakeHeartbeat(100));

        // when
        const auto replies = delivery.FailBatch("NOT_READY: ");

        // then
        UNIT_ASSERT_VALUES_EQUAL(replies.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(std::get<TCdcAggregatorHeartbeatRetry>(replies.at(1)).Error, "NOT_READY: ");
        UNIT_ASSERT_VALUES_EQUAL(std::get<TCdcAggregatorHeartbeatRetry>(replies.at(2)).Error, "NOT_READY: ");
        UNIT_ASSERT(delivery.IsIdle());
    }

    Y_UNIT_TEST(ShouldIgnoreFailureWhenNoBatchAwaitsReply) {
        // given
        TCdcAggregatorHeartbeatDelivery delivery(100);
        delivery.AddHeartbeat(1, MakeHeartbeat(100));

        // when
        const auto replies = delivery.FailBatch("NOT_READY: ");

        // then
        UNIT_ASSERT(replies.empty());
        UNIT_ASSERT(!delivery.IsIdle());
    }

}

Y_UNIT_TEST_SUITE(CdcAggregatorHeartbeatDeliveryProtoConverter) {

    Y_UNIT_TEST(ShouldSerializeSourceAndHeartbeatWhenBatchHasItem) {
        // given
        const TCdcAggregatorHeartbeatBatch batch = {
            {1, MakeHeartbeat(120, "b")},
        };

        // when
        const auto ev = TCdcAggregatorHeartbeatDeliveryProtoConverter().SerializeBatch(batch);

        // then
        UNIT_ASSERT_VALUES_EQUAL(ev->Record.HeartbeatsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(ev->Record.GetHeartbeats(0).GetSourceDataShardId(), 1);
        const auto heartbeat = NPQ::THeartbeat::Parse(ev->Record.GetHeartbeats(0).GetHeartbeat());
        UNIT_ASSERT_VALUES_EQUAL(heartbeat.Version, TRowVersion(120, 0));
        UNIT_ASSERT_VALUES_EQUAL(heartbeat.Data, "b");
    }

    Y_UNIT_TEST(ShouldParseWrittenAndRejectedSourcesWhenTabletProcessedBatch) {
        // given
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

        // when
        const auto [writeHeartbeatsResult, error] = TCdcAggregatorHeartbeatDeliveryProtoConverter().ParseWriteHeartbeatsResult(proto);

        // then
        UNIT_ASSERT_C(writeHeartbeatsResult, error);
        UNIT_ASSERT_VALUES_EQUAL(writeHeartbeatsResult->WrittenVersionsBySourceDataShardId.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(writeHeartbeatsResult->WrittenVersionsBySourceDataShardId.at(1), TRowVersion(100, 0));
        UNIT_ASSERT_VALUES_EQUAL(writeHeartbeatsResult->RejectionErrorsBySourceDataShardId.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(writeHeartbeatsResult->RejectionErrorsBySourceDataShardId.at(2), "UNKNOWN_SOURCE");
    }

    Y_UNIT_TEST(ShouldSerializeWrittenVersionWhenReplyIsWritten) {
        // given
        const TCdcAggregatorHeartbeatWriteResult writeResult = TCdcAggregatorHeartbeatWritten{.Version = TRowVersion(120, 0)};

        // when
        const auto ev = TCdcAggregatorHeartbeatDeliveryProtoConverter().SerializeWriteResult(writeResult);

        // then
        UNIT_ASSERT(ev->Status == TEvCdcAggregatorService::TEvWriteHeartbeatResult::EStatus::Written);
        UNIT_ASSERT_VALUES_EQUAL(ev->WrittenVersion, TRowVersion(120, 0));
        UNIT_ASSERT_VALUES_EQUAL(ev->Error, "");
    }

    Y_UNIT_TEST(ShouldSerializeErrorWhenReplyIsRejected) {
        // given
        const TCdcAggregatorHeartbeatWriteResult writeResult = TCdcAggregatorHeartbeatRejected{.Error = "UNKNOWN_SOURCE"};

        // when
        const auto ev = TCdcAggregatorHeartbeatDeliveryProtoConverter().SerializeWriteResult(writeResult);

        // then
        UNIT_ASSERT(ev->Status == TEvCdcAggregatorService::TEvWriteHeartbeatResult::EStatus::Rejected);
        UNIT_ASSERT_VALUES_EQUAL(ev->Error, "UNKNOWN_SOURCE");
    }

    Y_UNIT_TEST(ShouldReturnErrorWhenTabletDidNotProcessBatch) {
        for (const auto status : {TProtoResult::NOT_READY, TProtoResult::BAD_REQUEST}) {
            // given
            TProtoResult proto;
            proto.SetStatus(status);
            proto.SetError("Stream is not configured");

            // when
            const auto [writeHeartbeatsResult, error] = TCdcAggregatorHeartbeatDeliveryProtoConverter().ParseWriteHeartbeatsResult(proto);

            // then
            UNIT_ASSERT(!writeHeartbeatsResult);
            UNIT_ASSERT_VALUES_EQUAL(error, TProtoResult::EStatus_Name(status) + ": Stream is not configured");
        }
    }

}

} // namespace NKikimr::NCdcAggregator
