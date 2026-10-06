#include "yql_qyt_blocking_queue.h"

#include <ydb/library/yql/providers/abstract/message_stream/message_stream_session.h>

#include <util/datetime/base.h>

#include <library/cpp/testing/unittest/registar.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/unversioned_row.h>

namespace NYql {

////////////////////////////////////////////////////////////////////////////
// Test: TBlockingEQueue TryPush (used by session close path)
////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TQytTopicSessionTest) {

    Y_UNIT_TEST(TryPush) {
        TBlockingEQueue<TString> queue(20);
        queue.Push(TString("aaaaa"), 5);
        queue.Push(TString("bbbbb"), 5);

        // TryPush should succeed when there is room.
        bool result = queue.TryPush(TString("cccc"), 4);
        UNIT_ASSERT(result);

        auto v1 = queue.Pop(false);
        UNIT_ASSERT(v1.has_value());
        auto v2 = queue.Pop(false);
        UNIT_ASSERT(v2.has_value());
        auto v3 = queue.Pop(false);
        UNIT_ASSERT(v3.has_value());
        UNIT_ASSERT_EQUAL(*v3, TString("cccc"));
    }

    Y_UNIT_TEST(TryPushWhenFull) {
        TBlockingEQueue<TString> queue(10);
        queue.Push(TString("aaaaa"), 5);
        queue.Push(TString("bbbbb"), 5);

        // TryPush should return false when queue is full (respects backpressure).
        bool result = queue.TryPush(TString("cccc"), 4);
        UNIT_ASSERT(!result);
    }

    Y_UNIT_TEST(TryPushAfterStop) {
        TBlockingEQueue<TString> queue(1024);
        queue.Stop();
        bool result = queue.TryPush(TString("hello"), 5);
        UNIT_ASSERT(!result);
    }

////////////////////////////////////////////////////////////////////////////
// Test: CanPushPredicate
////////////////////////////////////////////////////////////////////////////

    Y_UNIT_TEST(CanPushPredicate) {
        TBlockingEQueue<TString> queue(10);
        // CanPushPredicate() with no args checks if queue can accept any push.
        UNIT_ASSERT(queue.CanPushPredicate());
        queue.Push(TString("aaaaa"), 5);
        UNIT_ASSERT(queue.CanPushPredicate());
        queue.Push(TString("bbbbb"), 5);
        // Queue is full at 10 bytes.
        UNIT_ASSERT(!queue.CanPushPredicate());
    }

////////////////////////////////////////////////////////////////////////////
// Test: Read session close event resolution
////////////////////////////////////////////////////////////////////////////

    Y_UNIT_TEST(ReadSessionCloseEventResolution) {
        TBlockingEQueue<NFq::TMessageStreamReadEvent> eventsQ(16 << 20);

        eventsQ.TryPush(NFq::TMessageStreamSessionClosedEvent{NFq::EMessageStreamStatus::Success, {}}, 0);
        eventsQ.Stop();

        auto event = eventsQ.Pop(false);
        UNIT_ASSERT(event.has_value());
        UNIT_ASSERT(std::holds_alternative<NFq::TMessageStreamSessionClosedEvent>(*event));
        auto& closeEvent = std::get<NFq::TMessageStreamSessionClosedEvent>(*event);
        UNIT_ASSERT_EQUAL(closeEvent.Status, NFq::EMessageStreamStatus::Success);
    }

////////////////////////////////////////////////////////////////////////////
// Test: Row data extraction logic
////////////////////////////////////////////////////////////////////////////

    Y_UNIT_TEST(ExtractRowDataLogic) {
        auto nameTable = NYT::New<NYT::NTableClient::TNameTable>();
        const int dataColumnId = nameTable->GetIdOrRegisterName("data");

        NYT::NTableClient::TUnversionedRowBuilder builder;
        const TString payload("hello world");
        builder.AddValue(NYT::NTableClient::MakeUnversionedStringValue(
            TStringBuf(payload.data(), payload.size()), dataColumnId));
        auto row = builder.GetRow();

        std::optional<int> dataId = nameTable->FindId("data");
        TStringBuf extractedData;
        for (const auto& value : row) {
            if (dataId && value.Id != static_cast<ui16>(*dataId)) {
                continue;
            }
            if (value.Type == NYT::NTableClient::EValueType::String) {
                extractedData = TStringBuf(value.Data.String, value.Length);
                break;
            }
        }

        UNIT_ASSERT_EQUAL(TString(extractedData), payload);
    }

////////////////////////////////////////////////////////////////////////////
// Test: Retry backoff pattern
////////////////////////////////////////////////////////////////////////////

    Y_UNIT_TEST(RetryBackoffPattern) {
        const TDuration MinBackoff = TDuration::MilliSeconds(100);
        const TDuration MaxBackoff = TDuration::Seconds(30);
        TDuration backoff = MinBackoff;

        std::vector<TDuration> backoffs;
        for (int i = 0; i < 10; ++i) {
            backoffs.push_back(backoff);
            backoff = std::min(backoff * 2, MaxBackoff);
        }

        UNIT_ASSERT_EQUAL(backoffs[0], TDuration::MilliSeconds(100));
        UNIT_ASSERT_EQUAL(backoffs[1], TDuration::MilliSeconds(200));
        UNIT_ASSERT_EQUAL(backoffs[2], TDuration::MilliSeconds(400));
        UNIT_ASSERT_EQUAL(backoffs[3], TDuration::MilliSeconds(800));
        UNIT_ASSERT_EQUAL(backoffs.back(), TDuration::Seconds(30));
    }

////////////////////////////////////////////////////////////////////////////
// Test: Multiple close events don't corrupt queue
////////////////////////////////////////////////////////////////////////////

    Y_UNIT_TEST(MultipleCloseEvents) {
        TBlockingEQueue<NFq::TMessageStreamReadEvent> eventsQ(16 << 20);

        // Push multiple close events (simulates race between TryPush and Stop).
        eventsQ.TryPush(NFq::TMessageStreamSessionClosedEvent{NFq::EMessageStreamStatus::Success, {}}, 0);
        eventsQ.TryPush(NFq::TMessageStreamSessionClosedEvent{NFq::EMessageStreamStatus::Success, {}}, 0);
        eventsQ.Stop();

        int closeCount = 0;
        while (auto ev = eventsQ.Pop(false)) {
            if (std::holds_alternative<NFq::TMessageStreamSessionClosedEvent>(*ev)) {
                ++closeCount;
            }
        }
        UNIT_ASSERT_EQUAL(closeCount, 2);
    }

} // Y_UNIT_TEST_SUITE(TQytTopicSessionTest)

} // namespace NYql
