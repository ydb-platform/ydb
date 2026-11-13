#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/tx/cdc_aggregator/service/heartbeat_queue.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NCdcAggregator {

namespace {

using EStatus = THeartbeatQueue::EStatus;
using TResult = NKikimrCdcAggregator::TEvWriteHeartbeatsResult;

const TActorId Sender(1, 0, 1, 0);

NPQ::THeartbeat MakeHeartbeat(ui64 step, const TString& data = {}) {
    return NPQ::THeartbeat{
        .Version = TRowVersion(step, 0),
        .Data = data,
    };
}

TResult MakeResult(const TVector<std::pair<ui64, ui64>>& written) {
    TResult record;
    record.SetStatus(TResult::SUCCESS);
    for (const auto& [sourceDataShardId, step] : written) {
        auto& result = *record.AddResults();
        result.SetSourceDataShardId(sourceDataShardId);
        result.SetStatus(TResult::THeartbeatResult::SUCCESS);
        TRowVersion(step, 0).ToProto(result.MutableWrittenVersion());
    }
    return record;
}

TVector<ui64> GetSources(const TEvCdcAggregator::TEvWriteHeartbeats& ev) {
    TVector<ui64> sources;
    for (const auto& heartbeat : ev.Record.GetHeartbeats()) {
        sources.push_back(heartbeat.GetSourceDataShardId());
    }
    return sources;
}

} // namespace

Y_UNIT_TEST_SUITE(CdcAggregatorHeartbeatQueue) {

    Y_UNIT_TEST(BatchesSources) {
        THeartbeatQueue queue(100, 100);
        for (ui64 sourceDataShardId : {1, 2, 3}) {
            UNIT_ASSERT(queue.Enqueue(sourceDataShardId, MakeHeartbeat(100), Sender, sourceDataShardId));
        }

        UNIT_ASSERT(queue.CanSend());
        auto ev = queue.NextBatch();
        UNIT_ASSERT_VALUES_EQUAL(GetSources(*ev), TVector<ui64>({1, 2, 3}));
        UNIT_ASSERT(!queue.CanSend());
    }

    Y_UNIT_TEST(MergesVersionsOfSource) {
        THeartbeatQueue queue(100, 100);
        UNIT_ASSERT(queue.Enqueue(1, MakeHeartbeat(100, "a"), Sender, 1));
        UNIT_ASSERT(queue.Enqueue(1, MakeHeartbeat(120, "b"), Sender, 2));
        UNIT_ASSERT(queue.Enqueue(1, MakeHeartbeat(110, "c"), Sender, 3));

        auto ev = queue.NextBatch();
        UNIT_ASSERT_VALUES_EQUAL(ev->Record.HeartbeatsSize(), 1);
        const auto heartbeat = NPQ::THeartbeat::Parse(ev->Record.GetHeartbeats(0).GetHeartbeat());
        UNIT_ASSERT_VALUES_EQUAL(heartbeat.Version, TRowVersion(120, 0));
        UNIT_ASSERT_VALUES_EQUAL(heartbeat.Data, "b");

        auto replies = queue.OnResult(MakeResult({{1, 120}}));
        UNIT_ASSERT_VALUES_EQUAL(replies.size(), 3);
        for (const auto& reply : replies) {
            UNIT_ASSERT_VALUES_EQUAL(reply.Status, EStatus::Written);
            UNIT_ASSERT_VALUES_EQUAL(reply.WrittenVersion, TRowVersion(120, 0));
        }
        UNIT_ASSERT(queue.IsIdle());
    }

    Y_UNIT_TEST(ConfirmsOnlyWrittenVersion) {
        THeartbeatQueue queue(100, 100);
        UNIT_ASSERT(queue.Enqueue(1, MakeHeartbeat(100), Sender, 1));
        queue.NextBatch();

        UNIT_ASSERT(queue.Enqueue(1, MakeHeartbeat(120), Sender, 2));
        UNIT_ASSERT(!queue.CanSend());

        auto replies = queue.OnResult(MakeResult({{1, 100}}));
        UNIT_ASSERT_VALUES_EQUAL(replies.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(replies[0].Cookie, 1);
        UNIT_ASSERT_VALUES_EQUAL(replies[0].Status, EStatus::Written);
        UNIT_ASSERT_VALUES_EQUAL(replies[0].WrittenVersion, TRowVersion(100, 0));
        UNIT_ASSERT_VALUES_EQUAL(queue.GetWaitersCount(), 1);

        UNIT_ASSERT(queue.CanSend());
        auto ev = queue.NextBatch();
        UNIT_ASSERT_VALUES_EQUAL(ev->Record.GetHeartbeats(0).GetHeartbeat().GetStep(), 120);

        replies = queue.OnResult(MakeResult({{1, 120}}));
        UNIT_ASSERT_VALUES_EQUAL(replies.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(replies[0].Cookie, 2);
        UNIT_ASSERT_VALUES_EQUAL(replies[0].Status, EStatus::Written);
        UNIT_ASSERT(queue.IsIdle());
    }

    Y_UNIT_TEST(KeepsWaitersUntilResult) {
        THeartbeatQueue queue(100, 100);
        for (ui64 sourceDataShardId : {1, 2, 3}) {
            UNIT_ASSERT(queue.Enqueue(sourceDataShardId, MakeHeartbeat(100), Sender, sourceDataShardId));
        }

        queue.NextBatch();
        UNIT_ASSERT_VALUES_EQUAL(queue.GetWaitersCount(), 3);
        UNIT_ASSERT(!queue.IsIdle());

        auto replies = queue.OnResult(MakeResult({{1, 100}, {2, 100}}));
        UNIT_ASSERT_VALUES_EQUAL(replies.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(replies[0].Status, EStatus::Written);
        UNIT_ASSERT_VALUES_EQUAL(replies[1].Status, EStatus::Written);
        UNIT_ASSERT_VALUES_EQUAL(replies[2].Cookie, 3);
        UNIT_ASSERT_VALUES_EQUAL(replies[2].Status, EStatus::Retry);
        UNIT_ASSERT(queue.IsIdle());
    }

    Y_UNIT_TEST(LimitsBatchSize) {
        THeartbeatQueue queue(2, 100);
        for (ui64 sourceDataShardId : {1, 2, 3}) {
            UNIT_ASSERT(queue.Enqueue(sourceDataShardId, MakeHeartbeat(100), Sender, sourceDataShardId));
        }

        auto first = queue.NextBatch();
        UNIT_ASSERT_VALUES_EQUAL(GetSources(*first), TVector<ui64>({1, 2}));
        UNIT_ASSERT(!queue.CanSend());

        queue.OnResult(MakeResult({{1, 100}, {2, 100}}));
        UNIT_ASSERT(queue.CanSend());
        auto second = queue.NextBatch();
        UNIT_ASSERT_VALUES_EQUAL(GetSources(*second), TVector<ui64>({3}));
    }

    Y_UNIT_TEST(RetriesAboveLimit) {
        THeartbeatQueue queue(100, 2);
        UNIT_ASSERT(queue.Enqueue(1, MakeHeartbeat(100), Sender, 1));
        UNIT_ASSERT(queue.Enqueue(2, MakeHeartbeat(100), Sender, 2));
        UNIT_ASSERT(!queue.Enqueue(3, MakeHeartbeat(100), Sender, 3));

        queue.NextBatch();
        UNIT_ASSERT(!queue.Enqueue(3, MakeHeartbeat(100), Sender, 3));

        auto replies = queue.Reset("Pipe destroyed");
        UNIT_ASSERT_VALUES_EQUAL(replies.size(), 2);
        for (const auto& reply : replies) {
            UNIT_ASSERT_VALUES_EQUAL(reply.Status, EStatus::Retry);
        }
        UNIT_ASSERT(queue.IsIdle());
        UNIT_ASSERT(queue.Enqueue(3, MakeHeartbeat(100), Sender, 3));
    }

    Y_UNIT_TEST(RetriesWhenNotReady) {
        THeartbeatQueue queue(100, 100);
        UNIT_ASSERT(queue.Enqueue(1, MakeHeartbeat(100), Sender, 1));
        queue.NextBatch();
        UNIT_ASSERT(queue.Enqueue(2, MakeHeartbeat(100), Sender, 2));

        TResult record;
        record.SetStatus(TResult::NOT_READY);
        auto replies = queue.OnResult(record);
        UNIT_ASSERT_VALUES_EQUAL(replies.size(), 2);
        for (const auto& reply : replies) {
            UNIT_ASSERT_VALUES_EQUAL(reply.Status, EStatus::Retry);
        }
        UNIT_ASSERT(queue.IsIdle());
    }

    Y_UNIT_TEST(RejectsUnknownSource) {
        THeartbeatQueue queue(100, 100);
        UNIT_ASSERT(queue.Enqueue(1, MakeHeartbeat(100), Sender, 1));
        queue.NextBatch();

        auto record = MakeResult({});
        auto& result = *record.AddResults();
        result.SetSourceDataShardId(1);
        result.SetStatus(TResult::THeartbeatResult::UNKNOWN_SOURCE);

        auto replies = queue.OnResult(record);
        UNIT_ASSERT_VALUES_EQUAL(replies.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(replies[0].Status, EStatus::Rejected);
        UNIT_ASSERT_VALUES_EQUAL(replies[0].Error, "UNKNOWN_SOURCE");
        UNIT_ASSERT(queue.IsIdle());
    }

}

} // namespace NKikimr::NCdcAggregator
