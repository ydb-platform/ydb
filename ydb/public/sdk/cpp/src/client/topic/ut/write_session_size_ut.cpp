#include <ydb/public/sdk/cpp/src/client/topic/impl/common.h>

#include <ydb/public/api/protos/ydb_topic.pb.h>

#include <google/protobuf/util/time_util.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::inline Dev::NTopic::NTests {
namespace {

namespace NGrpc = NWriteSessionGrpc;

struct TTestBlock {
    size_t MessageCount = 0;
    ui32 CodecID = static_cast<ui32>(ECodec::RAW);
    size_t OriginalSize = 0;
    std::vector<std::string_view> OriginalDataRefs;
    std::string Data;
    bool Compressed = false;
};

struct TTestMessage {
    TInstant CreatedAt;
    std::vector<std::pair<std::string, std::string>> MessageMeta;
    std::optional<TTransactionId> Tx;
};

void FillMetadata(
        Ydb::Topic::StreamWriteMessage::WriteRequest::MessageData& message,
        const std::vector<std::pair<std::string, std::string>>& metadata) {
    for (const auto& [key, value] : metadata) {
        auto* item = message.add_metadata_items();
        item->set_key(key);
        item->set_value(value);
    }
}

void FillTx(Ydb::Topic::StreamWriteMessage::WriteRequest& request, const std::optional<TTransactionId>& tx) {
    if (!tx) {
        return;
    }
    request.mutable_tx()->set_id(tx->TxId);
    request.mutable_tx()->set_session(tx->SessionId);
}

size_t BuildTopicWriteRequestBlockSize(
        const TTestBlock& block,
        const std::vector<TTestMessage>& messages,
        bool includeRequestFields) {
    Y_ABORT_UNLESS(!messages.empty());

    Ydb::Topic::StreamWriteMessage::WriteRequest request;
    if (includeRequestFields) {
        request.set_codec(static_cast<i32>(block.CodecID));
        FillTx(request, messages.front().Tx);
    }

    if (block.MessageCount > 1) {
        auto* message = request.add_messages();
        message->set_seq_no(-1);
        *message->mutable_created_at() =
            google::protobuf::util::TimeUtil::MillisecondsToTimestamp(messages.front().CreatedAt.MilliSeconds());
        FillMetadata(*message, messages.front().MessageMeta);
        for (size_t i = 1; i < block.MessageCount; ++i) {
            for (const auto& item : messages[i].MessageMeta) {
                if (item.first == NGrpc::PARTITION_KEY_META_KEY) {
                    FillMetadata(*message, {item});
                }
            }
        }
        message->set_uncompressed_size(static_cast<i64>(block.OriginalSize));
        message->set_data(block.Data);
        return request.ByteSizeLong();
    }

    auto* message = request.add_messages();
    message->set_seq_no(-1);
    *message->mutable_created_at() =
        google::protobuf::util::TimeUtil::MillisecondsToTimestamp(messages.front().CreatedAt.MilliSeconds());
    FillMetadata(*message, messages.front().MessageMeta);
    message->set_uncompressed_size(static_cast<i64>(block.OriginalSize));
    if (block.Compressed) {
        message->set_data(block.Data);
    } else {
        Y_ABORT_UNLESS(block.OriginalDataRefs.size() == 1);
        message->set_data(block.OriginalDataRefs.front().data(), block.OriginalDataRefs.front().size());
    }
    return request.ByteSizeLong();
}

void AssertEstimateCloseToActual(size_t estimate, size_t actual) {
    static constexpr size_t MaxAllowedOverestimate = 16;

    UNIT_ASSERT_GE_C(estimate, actual, "estimate=" << estimate << ", actual=" << actual);
    UNIT_ASSERT_LE_C(estimate, actual + MaxAllowedOverestimate, "estimate=" << estimate << ", actual=" << actual);
}

} // namespace

Y_UNIT_TEST_SUITE(WriteSessionGrpcSize) {
    Y_UNIT_TEST(RequestSizeLimiterBoundaries) {
        const size_t maxSize = NGrpc::ProtoMessageFieldSize(2, 10);
        NGrpc::TRequestSizeLimiter limiter(2, maxSize);

        // The first block is allowed even when it is larger than the limit:
        // otherwise a single oversized block would make the send loop stuck.
        UNIT_ASSERT(limiter.Empty());
        UNIT_ASSERT(limiter.CanAdd(maxSize * 2));

        limiter.Add(5);
        UNIT_ASSERT(!limiter.Empty());

        // For non-empty requests the limiter checks the full serialized
        // envelope size: field #2 tag + body length + body bytes.
        UNIT_ASSERT(limiter.CanAdd(5));
        UNIT_ASSERT(!limiter.CanAdd(6));
    }

    Y_UNIT_TEST(EstimateTopicWriteRequestStandardBlockSize) {
        const std::string data = "payload";
        TTestBlock block;
        block.MessageCount = 1;
        block.CodecID = static_cast<ui32>(ECodec::RAW);
        block.OriginalSize = data.size();
        block.OriginalDataRefs = {data};

        std::vector<TTestMessage> messages = {{
            .CreatedAt = TInstant::MilliSeconds(123456789),
            .MessageMeta = {{"meta-key", "meta-value"}, {std::string(NGrpc::PARTITION_KEY_META_KEY), "partition-key"}},
            .Tx = TTransactionId{.SessionId = "session-id", .TxId = "tx-id"},
        }};

        const size_t estimate = NGrpc::EstimateTopicWriteRequestBlockSize(block, messages, true);
        const size_t actual = BuildTopicWriteRequestBlockSize(block, messages, true);
        AssertEstimateCloseToActual(estimate, actual);
    }

    Y_UNIT_TEST(EstimateTopicWriteRequestBatchBlockSize) {
        TTestBlock block;
        block.MessageCount = 3;
        block.CodecID = static_cast<ui32>(ECodec::GZIP);
        block.OriginalSize = 100;
        block.Data = "compressed-batch";
        block.Compressed = true;

        std::vector<TTestMessage> messages = {
            {
                .CreatedAt = TInstant::MilliSeconds(223456789),
                .MessageMeta = {{"first-key", "first-value"}},
                .Tx = TTransactionId{.SessionId = "session-id", .TxId = "tx-id"},
            },
            {
                .CreatedAt = TInstant::MilliSeconds(323456789),
                .MessageMeta = {{std::string(NGrpc::PARTITION_KEY_META_KEY), "partition-key-1"}, {"ignored", "ignored-value"}},
                .Tx = TTransactionId{.SessionId = "session-id", .TxId = "tx-id"},
            },
            {
                .CreatedAt = TInstant::MilliSeconds(423456789),
                .MessageMeta = {{std::string(NGrpc::PARTITION_KEY_META_KEY), "partition-key-2"}},
                .Tx = TTransactionId{.SessionId = "session-id", .TxId = "tx-id"},
            },
        };

        const size_t estimate = NGrpc::EstimateTopicWriteRequestBlockSize(block, messages, true);
        const size_t actual = BuildTopicWriteRequestBlockSize(block, messages, true);
        AssertEstimateCloseToActual(estimate, actual);
    }
}

} // namespace NYdb::NTopic::NTests
