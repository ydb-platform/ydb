#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/tx/cdc_aggregator/service/cdc_aggregator_heartbeat_delivery.h>
#include <ydb/core/tx/cdc_aggregator/service/cdc_aggregator_heartbeat_delivery_proto.h>

#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>

namespace NKikimr::NCdcAggregator {

namespace {

using EStatus = TCdcAggregatorHeartbeatDeliveryReply::EStatus;
using TProtoResult = NKikimrCdcAggregator::TEvWriteHeartbeatsResult;

const TActorId Sender(1, 0, 1, 0);

TCdcAggregatorHeartbeatDeliveryWaiter MakeWaiter(ui64 cookie) {
    return TCdcAggregatorHeartbeatDeliveryWaiter{
        .Sender = Sender,
        .Cookie = cookie,
    };
}

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

TVector<ui64> GetSources(const TVector<TCdcAggregatorHeartbeatDeliveryItem>& batch) {
    TVector<ui64> sources;
    for (const auto& item : batch) {
        sources.push_back(item.SourceDataShardId);
    }
    return sources;
}

TVector<TCdcAggregatorHeartbeatDeliveryReply> SortByCookie(TVector<TCdcAggregatorHeartbeatDeliveryReply> replies) {
    std::sort(replies.begin(), replies.end(), [](const auto& left, const auto& right) {
        return left.Waiter.Cookie < right.Waiter.Cookie;
    });
    return replies;
}

} // namespace

Y_UNIT_TEST_SUITE(CdcAggregatorHeartbeatDelivery) {

    Y_UNIT_TEST(BatchesSources) {
        TCdcAggregatorHeartbeatDelivery delivery(100, 100);
        for (ui64 sourceDataShardId : {1, 2, 3}) {
            UNIT_ASSERT(delivery.AddHeartbeat(sourceDataShardId, MakeHeartbeat(100), MakeWaiter(sourceDataShardId)));
        }

        UNIT_ASSERT(delivery.HasBatchToSend());
        UNIT_ASSERT_VALUES_EQUAL(GetSources(delivery.TakeBatch()), TVector<ui64>({1, 2, 3}));
        UNIT_ASSERT(!delivery.HasBatchToSend());
    }

    Y_UNIT_TEST(MergesVersionsOfSource) {
        TCdcAggregatorHeartbeatDelivery delivery(100, 100);
        UNIT_ASSERT(delivery.AddHeartbeat(1, MakeHeartbeat(100, "a"), MakeWaiter(1)));
        UNIT_ASSERT(delivery.AddHeartbeat(1, MakeHeartbeat(120, "b"), MakeWaiter(2)));
        UNIT_ASSERT(delivery.AddHeartbeat(1, MakeHeartbeat(110, "c"), MakeWaiter(3)));

        const auto batch = delivery.TakeBatch();
        UNIT_ASSERT_VALUES_EQUAL(batch.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(batch[0].Heartbeat.Version, TRowVersion(120, 0));
        UNIT_ASSERT_VALUES_EQUAL(batch[0].Heartbeat.Data, "b");

        const auto replies = delivery.CompleteBatch(MakeWritten({{1, 120}}));
        UNIT_ASSERT_VALUES_EQUAL(replies.size(), 3);
        for (const auto& reply : replies) {
            UNIT_ASSERT(reply.Status == EStatus::Written);
            UNIT_ASSERT_VALUES_EQUAL(reply.WrittenVersion, TRowVersion(120, 0));
        }
        UNIT_ASSERT(delivery.IsIdle());
    }

    Y_UNIT_TEST(ConfirmsOnlyWrittenVersion) {
        TCdcAggregatorHeartbeatDelivery delivery(100, 100);
        UNIT_ASSERT(delivery.AddHeartbeat(1, MakeHeartbeat(100), MakeWaiter(1)));
        delivery.TakeBatch();

        UNIT_ASSERT(delivery.AddHeartbeat(1, MakeHeartbeat(120), MakeWaiter(2)));
        UNIT_ASSERT(!delivery.HasBatchToSend());

        auto replies = delivery.CompleteBatch(MakeWritten({{1, 100}}));
        UNIT_ASSERT_VALUES_EQUAL(replies.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(replies[0].Waiter.Cookie, 1);
        UNIT_ASSERT(replies[0].Status == EStatus::Written);
        UNIT_ASSERT_VALUES_EQUAL(replies[0].WrittenVersion, TRowVersion(100, 0));
        UNIT_ASSERT_VALUES_EQUAL(delivery.GetWaitersCount(), 1);

        UNIT_ASSERT(delivery.HasBatchToSend());
        const auto batch = delivery.TakeBatch();
        UNIT_ASSERT_VALUES_EQUAL(batch[0].Heartbeat.Version, TRowVersion(120, 0));

        replies = delivery.CompleteBatch(MakeWritten({{1, 120}}));
        UNIT_ASSERT_VALUES_EQUAL(replies.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(replies[0].Waiter.Cookie, 2);
        UNIT_ASSERT(replies[0].Status == EStatus::Written);
        UNIT_ASSERT(delivery.IsIdle());
    }

    Y_UNIT_TEST(KeepsWaitersUntilResult) {
        TCdcAggregatorHeartbeatDelivery delivery(100, 100);
        for (ui64 sourceDataShardId : {1, 2, 3}) {
            UNIT_ASSERT(delivery.AddHeartbeat(sourceDataShardId, MakeHeartbeat(100), MakeWaiter(sourceDataShardId)));
        }

        delivery.TakeBatch();
        UNIT_ASSERT_VALUES_EQUAL(delivery.GetWaitersCount(), 3);
        UNIT_ASSERT(!delivery.IsIdle());

        const auto replies = SortByCookie(delivery.CompleteBatch(MakeWritten({{1, 100}, {2, 100}})));
        UNIT_ASSERT_VALUES_EQUAL(replies.size(), 3);
        UNIT_ASSERT(replies[0].Status == EStatus::Written);
        UNIT_ASSERT(replies[1].Status == EStatus::Written);
        UNIT_ASSERT_VALUES_EQUAL(replies[2].Waiter.Cookie, 3);
        UNIT_ASSERT(replies[2].Status == EStatus::Retry);
        UNIT_ASSERT(delivery.IsIdle());
    }

    Y_UNIT_TEST(LimitsBatchSize) {
        TCdcAggregatorHeartbeatDelivery delivery(2, 100);
        for (ui64 sourceDataShardId : {1, 2, 3}) {
            UNIT_ASSERT(delivery.AddHeartbeat(sourceDataShardId, MakeHeartbeat(100), MakeWaiter(sourceDataShardId)));
        }

        UNIT_ASSERT_VALUES_EQUAL(GetSources(delivery.TakeBatch()), TVector<ui64>({1, 2}));
        UNIT_ASSERT(!delivery.HasBatchToSend());

        delivery.CompleteBatch(MakeWritten({{1, 100}, {2, 100}}));
        UNIT_ASSERT(delivery.HasBatchToSend());
        UNIT_ASSERT_VALUES_EQUAL(GetSources(delivery.TakeBatch()), TVector<ui64>({3}));
    }

    Y_UNIT_TEST(RetriesAboveLimit) {
        TCdcAggregatorHeartbeatDelivery delivery(100, 2);
        UNIT_ASSERT(delivery.AddHeartbeat(1, MakeHeartbeat(100), MakeWaiter(1)));
        UNIT_ASSERT(delivery.AddHeartbeat(2, MakeHeartbeat(100), MakeWaiter(2)));
        UNIT_ASSERT(!delivery.AddHeartbeat(3, MakeHeartbeat(100), MakeWaiter(3)));

        delivery.TakeBatch();
        UNIT_ASSERT(!delivery.AddHeartbeat(3, MakeHeartbeat(100), MakeWaiter(3)));

        const auto replies = delivery.RetryAllWaiters("Pipe destroyed");
        UNIT_ASSERT_VALUES_EQUAL(replies.size(), 2);
        for (const auto& reply : replies) {
            UNIT_ASSERT(reply.Status == EStatus::Retry);
        }
        UNIT_ASSERT(delivery.IsIdle());
        UNIT_ASSERT(delivery.AddHeartbeat(3, MakeHeartbeat(100), MakeWaiter(3)));
    }

    Y_UNIT_TEST(RetriesAllWaitersWhenBatchFails) {
        TCdcAggregatorHeartbeatDelivery delivery(100, 100);
        UNIT_ASSERT(delivery.AddHeartbeat(1, MakeHeartbeat(100), MakeWaiter(1)));
        delivery.TakeBatch();
        UNIT_ASSERT(delivery.AddHeartbeat(2, MakeHeartbeat(100), MakeWaiter(2)));

        const auto replies = delivery.FailBatch("NOT_READY: ");
        UNIT_ASSERT_VALUES_EQUAL(replies.size(), 2);
        for (const auto& reply : replies) {
            UNIT_ASSERT(reply.Status == EStatus::Retry);
            UNIT_ASSERT_VALUES_EQUAL(reply.Error, "NOT_READY: ");
        }
        UNIT_ASSERT(delivery.IsIdle());
    }

    Y_UNIT_TEST(IgnoresFailureWithoutBatchInFlight) {
        TCdcAggregatorHeartbeatDelivery delivery(100, 100);
        UNIT_ASSERT(delivery.AddHeartbeat(1, MakeHeartbeat(100), MakeWaiter(1)));

        UNIT_ASSERT(delivery.FailBatch("NOT_READY: ").empty());
        UNIT_ASSERT_VALUES_EQUAL(delivery.GetWaitersCount(), 1);
        UNIT_ASSERT(delivery.HasBatchToSend());
    }

    Y_UNIT_TEST(RejectsUnknownSource) {
        TCdcAggregatorHeartbeatDelivery delivery(100, 100);
        UNIT_ASSERT(delivery.AddHeartbeat(1, MakeHeartbeat(100), MakeWaiter(1)));
        delivery.TakeBatch();

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
