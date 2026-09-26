#include <ydb/core/kqp/runtime/kqp_write_table.h>

#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme_types/scheme_type_info.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>
#include <yql/essentials/minikql/mkql_alloc.h>

#include <library/cpp/testing/unittest/registar.h>

#include <memory>

namespace {

using namespace NKikimr;
using namespace NKikimr::NKqp;

constexpr ui64 TestShardId = 1;
constexpr ui64 UnknownShardId = 12345;

TVector<TKeyDesc::TPartitionInfo> MakeSingleShardPartitions() {
    TVector<TKeyDesc::TPartitionInfo> partitions;
    // A default range (empty EndKeyPrefix) covers all keys up to +inf, so every
    // written row lands on TestShardId.
    auto& partition = partitions.emplace_back(TestShardId);
    partition.Range = TKeyDesc::TPartitionRangeInfo{};
    return partitions;
}

NKikimrKqp::TKqpColumnMetadataProto MakeColumn(const TString& name, const ui32 id, const ui32 typeId) {
    NKikimrKqp::TKqpColumnMetadataProto column;
    column.SetName(name);
    column.SetId(id);
    column.SetTypeId(typeId);
    return column;
}

class TShardedWriteControllerFixture : public NUnitTest::TBaseTestCase {
public:
    TShardedWriteControllerFixture()
        : Alloc(std::make_shared<NMiniKQL::TScopedAlloc>(__LOCATION__))
    {
        Controller = CreateShardedWriteController(TShardedWriteControllerSettings{}, Alloc);
        Controller->OnPartitioningChanged(std::make_shared<TPartitioning>(MakeSingleShardPartitions()));
    }

    // Opens the token, writes a single row (a single message round on the shard),
    // closes the token and flushes the batch to the shard.
    void WriteRound(const ui64 key, const ui64 value, const IShardedWriteController::TWriteToken token) {
        TVector<NKikimrKqp::TKqpColumnMetadataProto> keyColumns;
        keyColumns.push_back(MakeColumn("key", 1, NScheme::NTypeIds::Uint64));

        TVector<NKikimrKqp::TKqpColumnMetadataProto> inputColumns;
        inputColumns.push_back(MakeColumn("key", 1, NScheme::NTypeIds::Uint64));
        inputColumns.push_back(MakeColumn("value", 2, NScheme::NTypeIds::Uint64));

        Controller->Open(
            token,
            TTableId(1, 2),
            NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
            std::move(keyColumns),
            std::move(inputColumns),
            /* defaultColumnsCount */ 0,
            /* priority */ 0,
            /* mvccSnapshot */ std::nullopt);

        auto batcher = CreateRowsBatcher(/* columnsCount */ 2, Alloc);
        batcher->AddCell(TCell::Make(key));
        batcher->AddCell(TCell::Make(value));
        batcher->AddRow();
        Controller->Write(token, batcher->Flush());
        Controller->FlushBuffers();

        Controller->Close(token);
        Controller->FlushBuffers();
    }

protected:
    std::shared_ptr<NMiniKQL::TScopedAlloc> Alloc;
    IShardedWriteControllerPtr Controller;
};

IShardedWriteController::TMessageMetadata MetadataWithCookie(const ui64 cookie) {
    IShardedWriteController::TMessageMetadata metadata;
    metadata.Cookie = cookie;
    return metadata;
}

} // namespace

Y_UNIT_TEST_SUITE(KqpWriteTable) {
    Y_UNIT_TEST(SupersededWriteResultFilter) {
        // A result echoing the cookie of the shard's last sent message is processed.
        UNIT_ASSERT(!IsSupersededWriteResult(7, MetadataWithCookie(7)));
        // A result of a superseded message (a resend went out with a fresh cookie,
        // or the message was already acknowledged) is dropped.
        UNIT_ASSERT(IsSupersededWriteResult(6, MetadataWithCookie(7)));
        // A result for a shard unknown to the controller is dropped.
        UNIT_ASSERT(IsSupersededWriteResult(7, std::nullopt));
        // Zero-cookie results (replies of shards that do not echo cookies, e.g.
        // 26-3 datashards; distributed/volatile commit completions) are not tied
        // to a specific message and always pass.
        UNIT_ASSERT(!IsSupersededWriteResult(0, std::nullopt));
        UNIT_ASSERT(!IsSupersededWriteResult(0, MetadataWithCookie(7)));
    }

    Y_UNIT_TEST_F(FreshCookieForEachMessage, TShardedWriteControllerFixture) {
        WriteRound(1, 11, 0);

        // Before the first send the shard holds a not-yet-used marker cookie; every
        // outbound message, the first attempt or a resend, mints its own fresh cookie.
        // The send-path lookup builds the pending batches into flight.
        const auto firstMetadata = Controller->PrepareMessageMetadata(TestShardId);
        UNIT_ASSERT_VALUES_EQUAL(firstMetadata.OperationsCount, 1);

        const ui64 firstCookie = Controller->AllocateMessageCookie(TestShardId);
        Controller->OnMessageSent(TestShardId, firstCookie);

        // A resend mints a distinct fresh cookie.
        const ui64 resendCookie = Controller->AllocateMessageCookie(TestShardId);
        UNIT_ASSERT(resendCookie != firstCookie);
        Controller->OnMessageSent(TestShardId, resendCookie);

        // Only the result echoing the last minted cookie acknowledges the round.
        // Note: a zero-cookie result (e.g. a reply of a pre-26-4 shard) can never
        // acknowledge the round - callers never pass such a cookie here (see the
        // AFL_ENSURE in OnMessageAcknowledged).
        UNIT_ASSERT(!Controller->OnMessageAcknowledged(TestShardId, firstCookie));

        const auto acknowledged = Controller->OnMessageAcknowledged(TestShardId, resendCookie);
        UNIT_ASSERT(acknowledged);
        UNIT_ASSERT(acknowledged->IsShardEmpty);
        UNIT_ASSERT(!Controller->GetMessageMetadata(TestShardId));

        // A second round mints a fresh cookie again; belated duplicates of the first
        // round's messages are both filtered out and ignored by OnMessageAcknowledged.
        WriteRound(2, 22, 1);
        const auto secondMetadata = Controller->PrepareMessageMetadata(TestShardId);
        const ui64 secondCookie = Controller->AllocateMessageCookie(TestShardId);
        UNIT_ASSERT(secondCookie != firstCookie && secondCookie != resendCookie);
        Controller->OnMessageSent(TestShardId, secondCookie);

        UNIT_ASSERT(IsSupersededWriteResult(firstCookie, secondMetadata));
        UNIT_ASSERT(IsSupersededWriteResult(resendCookie, Controller->GetMessageMetadata(TestShardId)));
        UNIT_ASSERT(!Controller->OnMessageAcknowledged(TestShardId, firstCookie));
        UNIT_ASSERT(!Controller->OnMessageAcknowledged(TestShardId, resendCookie));

        const auto currentMetadata = Controller->GetMessageMetadata(TestShardId);
        UNIT_ASSERT(currentMetadata);
        UNIT_ASSERT_VALUES_EQUAL(currentMetadata->Cookie, secondCookie);

        // The result of the second round is acknowledged.
        const auto secondAck = Controller->OnMessageAcknowledged(TestShardId, secondCookie);
        UNIT_ASSERT(secondAck);
        UNIT_ASSERT(secondAck->IsShardEmpty);
        UNIT_ASSERT(!Controller->GetMessageMetadata(TestShardId));
    }

    Y_UNIT_TEST_F(MetadataLookupDoesNotCreateShardEntries, TShardedWriteControllerFixture) {
        WriteRound(1, 11, 0);

        // A lookup for an unknown shard (e.g. a COMMIT-mode completion from a
        // lock-only participant) reports no metadata and must not leak into the
        // shard set used for external-prepare exclusion.
        UNIT_ASSERT(!Controller->GetMessageMetadata(UnknownShardId));
        UNIT_ASSERT(!Controller->OnMessageAcknowledged(UnknownShardId, 1));

        const auto shardIds = Controller->GetShardsIds();
        UNIT_ASSERT_VALUES_EQUAL(shardIds.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(shardIds[0], TestShardId);
        UNIT_ASSERT_VALUES_EQUAL(Controller->GetShardsCount(), 1);
    }
}
