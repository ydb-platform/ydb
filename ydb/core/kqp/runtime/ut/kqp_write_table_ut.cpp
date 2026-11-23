#include <ydb/core/kqp/runtime/kqp_write_table.h>

#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme_types/scheme_type_info.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>
#include <ydb/core/tx/data_events/payload_helper.h>
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
    void WriteRound(const ui64 key, const ui64 value, const IShardedWriteController::TWriteToken token,
            bool flush = true) {
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
        if (!flush) {
            return;
        }
        Controller->FlushBuffers();

        Controller->Close(token);
        Controller->FlushBuffers();
    }

protected:
    std::shared_ptr<NMiniKQL::TScopedAlloc> Alloc;
    IShardedWriteControllerPtr Controller;
};

class TAncestorsControllerFixture : public TShardedWriteControllerFixture {
public:
    TAncestorsControllerFixture() {
        TShardedWriteControllerSettings settings;
        settings.EnableWriteSeqNum = true;
        settings.WriterIndex = 7;
        Controller = CreateShardedWriteController(settings, Alloc);
        SetPartitions({{TestShardId, std::nullopt}});
    }

    void SetPartitions(const TVector<std::pair<ui64, std::optional<ui64>>>& ranges) {
        TVector<TKeyDesc::TPartitionInfo> partitions;
        for (const auto& [shardId, end] : ranges) {
            auto& partition = partitions.emplace_back(shardId);
            partition.Range = TKeyDesc::TPartitionRangeInfo{};
            if (end) {
                const TVector<TCell> cells{TCell::Make(*end)};
                partition.Range->EndKeyPrefix = TSerializedCellVec(cells);
            }
        }
        Controller->OnPartitioningChanged(std::make_shared<TPartitioning>(std::move(partitions)));
    }

    void Reroute() {
        Controller->ReRouteShards(Controller->GetDeletedShards());
    }

    std::unique_ptr<NEvents::TDataEvents::TEvWrite> Capture(ui64 shardId) {
        auto ev = std::make_unique<NEvents::TDataEvents::TEvWrite>(NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
        Controller->PrepareMessageMetadata(shardId);
        Controller->SerializeMessageToPayload(shardId, *ev, false);
        return ev;
    }

    static void Check(const NEvents::TDataEvents::TEvWrite& ev, size_t index,
            const TVector<ui64>& ancestors, ui64 wsn, const TVector<ui64>& keys) {
        UNIT_ASSERT(index < ev.Record.OperationsSize());
        const auto& op = ev.Record.GetOperations(index);
        UNIT_ASSERT_VALUES_EQUAL(op.GetWriteSeqNum().GetWriterIndex(), 7);
        UNIT_ASSERT_VALUES_EQUAL(op.GetWriteSeqNum().GetWriteSeqNum(), wsn);
        UNIT_ASSERT_VALUES_EQUAL(op.AncestorsSize(), ancestors.size());
        for (size_t i = 0; i < ancestors.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(op.GetAncestors(i), ancestors[i]);
        }
        NEvWrite::TPayloadReader<NEvents::TDataEvents::TEvWrite> reader(ev);
        TSerializedCellMatrix matrix;
        UNIT_ASSERT(TSerializedCellMatrix::TryParse(reader.GetDataFromPayload(op.GetPayloadIndex()), matrix));
        UNIT_ASSERT_VALUES_EQUAL(matrix.GetRowCount(), keys.size());
        for (size_t i = 0; i < keys.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(matrix.GetCell(i, 0).AsValue<ui64>(), keys[i]);
        }
    }
};

IShardedWriteController::TMessageMetadata MetadataWithCookie(const ui64 cookie) {
    IShardedWriteController::TMessageMetadata metadata;
    metadata.Cookie = cookie;
    return metadata;
}

} // namespace

Y_UNIT_TEST_SUITE(KqpWriteTable) {
    Y_UNIT_TEST_F(ReroutePreservesOversizedRow, TAncestorsControllerFixture) {
        const TString value(9 * 1024 * 1024, 'x');
        for (const bool enableWriteSeqNum : {false, true}) {
            TShardedWriteControllerSettings settings;
            settings.EnableWriteSeqNum = enableWriteSeqNum;
            settings.Inconsistent = !enableWriteSeqNum;
            settings.WriterIndex = 7;
            Controller = CreateShardedWriteController(settings, Alloc);
            SetPartitions({{1, std::nullopt}});

            TVector<NKikimrKqp::TKqpColumnMetadataProto> keyColumns{
                MakeColumn("key", 1, NScheme::NTypeIds::Uint64)};
            TVector<NKikimrKqp::TKqpColumnMetadataProto> inputColumns{
                keyColumns.front(), MakeColumn("value", 2, NScheme::NTypeIds::String)};
            Controller->Open(0, TTableId(1, 2),
                NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
                std::move(keyColumns), std::move(inputColumns), 0, 0, std::nullopt);

            auto batcher = CreateRowsBatcher(2, Alloc);
            batcher->AddCell(TCell::Make(ui64(10)));
            batcher->AddCell(TCell(value.data(), value.size()));
            batcher->AddRow();
            Controller->Write(0, batcher->Flush());
            Controller->Close(0);
            Controller->FlushBuffers();

            auto initial = Capture(1);
            UNIT_ASSERT_VALUES_EQUAL(initial->Record.OperationsSize(), 1);
            NEvWrite::TPayloadReader<NEvents::TDataEvents::TEvWrite> initialReader(*initial);
            const auto originalSize = initialReader.GetDataFromPayload(
                initial->Record.GetOperations(0).GetPayloadIndex()).size();
            UNIT_ASSERT(originalSize > 8 * 1024 * 1024);

            SetPartitions({{2, 50}, {3, std::nullopt}});
            Reroute();
            auto rerouted = Capture(2);
            UNIT_ASSERT_VALUES_EQUAL(rerouted->Record.OperationsSize(), 1);
            NEvWrite::TPayloadReader<NEvents::TDataEvents::TEvWrite> reader(*rerouted);
            const auto payload = reader.GetDataFromPayload(rerouted->Record.GetOperations(0).GetPayloadIndex());
            UNIT_ASSERT_VALUES_EQUAL(payload.size(), originalSize);
            TSerializedCellMatrix matrix;
            UNIT_ASSERT(TSerializedCellMatrix::TryParse(payload, matrix));
            UNIT_ASSERT_VALUES_EQUAL(matrix.GetRowCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(matrix.GetCell(0, 0).AsValue<ui64>(), 10);
            UNIT_ASSERT_C(matrix.GetCell(0, 1).AsBuf() == value, "Reroute changed the oversized value");
        }
    }

    Y_UNIT_TEST_F(RetryAndUnsentReroutesPreserveAncestors, TAncestorsControllerFixture) {
        WriteRound(10, 100, 0);
        auto initial = Capture(1);
        Check(*initial, 0, {}, 1, {10});
        auto retry = Capture(1);
        Check(*retry, 0, {}, 1, {10});

        SetPartitions({{2, std::nullopt}});
        Reroute();
        // Nothing is sent to 2, but it must still appear in the routing history.
        SetPartitions({{3, std::nullopt}});
        Reroute();
        auto rerouted = Capture(3);
        UNIT_ASSERT_VALUES_EQUAL(rerouted->Record.OperationsSize(), 1);
        Check(*rerouted, 0, {1, 2}, 1, {10});
        Check(*Capture(3), 0, {1, 2}, 1, {10});

        Controller->OnMessageAcknowledged(3, Controller->GetMessageMetadata(3)->Cookie);
        WriteRound(20, 200, 1);
        // Rerouted WSNs never allocate positions in the destination's current chain.
        Check(*Capture(3), 0, {}, 1, {20});
    }

    Y_UNIT_TEST_F(SplitMergeSplitKeepsBranchRangesAndEmptyFragments, TAncestorsControllerFixture) {
        WriteRound(10, 100, 0);
        SetPartitions({{2, 50}, {3, std::nullopt}});
        Reroute();
        Check(*Capture(2), 0, {1}, 1, {10});
        Check(*Capture(3), 0, {1}, 1, {});

        SetPartitions({{4, std::nullopt}});
        Reroute();
        auto merged = Capture(4);
        UNIT_ASSERT_VALUES_EQUAL(merged->Record.OperationsSize(), 2);
        // Source iteration order is unspecified; independent branches stay separate.
        for (size_t i = 0; i < 2; ++i) {
            const auto source = merged->Record.GetOperations(i).GetAncestors(1);
            UNIT_ASSERT(source == 2 || source == 3);
            Check(*merged, i, {1, source}, 1, source == 2 ? TVector<ui64>{10} : TVector<ui64>{});
        }
        UNIT_ASSERT(merged->Record.GetOperations(0).GetAncestors(1)
            != merged->Record.GetOperations(1).GetAncestors(1));

        SetPartitions({{5, 25}, {6, std::nullopt}});
        Reroute();
        auto left = Capture(5);
        UNIT_ASSERT_VALUES_EQUAL(left->Record.OperationsSize(), 1);
        Check(*left, 0, {1, 2, 4}, 1, {10});
        auto right = Capture(6);
        UNIT_ASSERT_VALUES_EQUAL(right->Record.OperationsSize(), 2);
        for (size_t i = 0; i < 2; ++i) {
            const auto source = right->Record.GetOperations(i).GetAncestors(1);
            Check(*right, i, {1, source, 4}, 1, {});
        }

        // Touching exclusive boundaries do not expand either branch across 50.
        SetPartitions({{5, 25}, {7, 50}, {8, std::nullopt}});
        Reroute();
        auto middle = Capture(7);
        UNIT_ASSERT_VALUES_EQUAL(middle->Record.OperationsSize(), 1);
        Check(*middle, 0, {1, 2, 4, 6}, 1, {});
        auto upper = Capture(8);
        UNIT_ASSERT_VALUES_EQUAL(upper->Record.OperationsSize(), 1);
        Check(*upper, 0, {1, 3, 4, 6}, 1, {});
    }

    Y_UNIT_TEST_F(ReroutePreservesSourceFifoAcrossOwners, TAncestorsControllerFixture) {
        WriteRound(10, 100, 0);
        SetPartitions({{2, std::nullopt}});
        Reroute();
        WriteRound(20, 200, 1);
        WriteRound(30, 300, 2);
        SetPartitions({{3, std::nullopt}});
        Reroute();
        auto ev = Capture(3);
        UNIT_ASSERT_VALUES_EQUAL(ev->Record.OperationsSize(), 3);
        Check(*ev, 0, {1, 2}, 1, {10});
        Check(*ev, 1, {2}, 1, {20});
        Check(*ev, 2, {2}, 2, {30});
    }

    Y_UNIT_TEST_F(FirstReroutedRequestIncludesCompleteTail, TAncestorsControllerFixture) {
        WriteRound(10, 100, 0);
        Controller->PrepareMessageMetadata(1);
        Controller->OnMessageSent(1, Controller->AllocateMessageCookie(1));
        // These batches are queued behind an already prepared/in-flight request.
        WriteRound(20, 200, 1);
        WriteRound(30, 300, 2);
        Controller->Close();
        SetPartitions({{2, std::nullopt}});
        Reroute();
        auto ev = Capture(2);
        UNIT_ASSERT_VALUES_EQUAL(ev->Record.OperationsSize(), 3);
        Check(*ev, 0, {1}, 1, {10});
        Check(*ev, 1, {1}, 2, {20});
        Check(*ev, 2, {1}, 3, {30});
        UNIT_ASSERT(Controller->GetMessageMetadata(2)->IsFinal);
        UNIT_ASSERT(!Controller->HasShard(1));
    }

    Y_UNIT_TEST_F(IncomingAncestorsPrecedePendingCurrent, TAncestorsControllerFixture) {
        WriteRound(10, 100, 0);
        SetPartitions({{2, std::nullopt}});
        WriteRound(20, 200, 1);
        auto current = Capture(2);
        Check(*current, 0, {}, 1, {20});
        const auto oldMetadata = *Controller->GetMessageMetadata(2);
        Controller->OnMessageSent(2, oldMetadata.Cookie);

        Reroute();
        UNIT_ASSERT(IsSupersededWriteResult(oldMetadata.Cookie, Controller->GetMessageMetadata(2)));
        auto ev = Capture(2);
        UNIT_ASSERT_VALUES_EQUAL(ev->Record.OperationsSize(), 2);
        Check(*ev, 0, {1}, 1, {10});
        Check(*ev, 1, {}, 1, {20});
    }

    Y_UNIT_TEST_F(PartitioningChangeFlushesUsingSourceRange, TAncestorsControllerFixture) {
        WriteRound(10, 100, 0, false);
        SetPartitions({{2, 50}, {3, std::nullopt}});
        Reroute();
        Check(*Capture(2), 0, {1}, 1, {10});
        Check(*Capture(3), 0, {1}, 1, {});
    }

    Y_UNIT_TEST(SupersededWriteResultFilter) {
        // A result echoing the cookie of the shard's last sent message is processed.
        UNIT_ASSERT(!IsSupersededWriteResult(7, MetadataWithCookie(7)));
        // A result of a superseded message (a resend went out with a fresh cookie,
        // or the message was already acknowledged) is dropped.
        UNIT_ASSERT(IsSupersededWriteResult(6, MetadataWithCookie(7)));
        // A result for a shard unknown to the controller is dropped.
        UNIT_ASSERT(IsSupersededWriteResult(7, std::nullopt));
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

        // Only the result echoing the last minted cookie may acknowledge the
        // round: a stale cookie (e.g. the result of the original attempt after
        // a resend) is a superseded result - the write actor filters it out
        // with IsSupersededWriteResult before it ever reaches the controller,
        // and OnMessageAcknowledged treats it as a bug.
        const auto acknowledged = Controller->OnMessageAcknowledged(TestShardId, resendCookie);
        UNIT_ASSERT(acknowledged.IsShardEmpty);
        UNIT_ASSERT(!Controller->GetMessageMetadata(TestShardId));

        // A second round mints a fresh cookie again; belated duplicates of the
        // first round's messages carry stale cookies and are filtered out by
        // the write actor with IsSupersededWriteResult before they reach the
        // controller.
        WriteRound(2, 22, 1);
        const auto secondMetadata = Controller->PrepareMessageMetadata(TestShardId);
        const ui64 secondCookie = Controller->AllocateMessageCookie(TestShardId);
        UNIT_ASSERT(secondCookie != firstCookie && secondCookie != resendCookie);
        Controller->OnMessageSent(TestShardId, secondCookie);

        UNIT_ASSERT(IsSupersededWriteResult(firstCookie, secondMetadata));
        UNIT_ASSERT(IsSupersededWriteResult(resendCookie, Controller->GetMessageMetadata(TestShardId)));

        const auto currentMetadata = Controller->GetMessageMetadata(TestShardId);
        UNIT_ASSERT(currentMetadata);
        UNIT_ASSERT_VALUES_EQUAL(currentMetadata->Cookie, secondCookie);

        // The result of the second round is acknowledged.
        const auto secondAck = Controller->OnMessageAcknowledged(TestShardId, secondCookie);
        UNIT_ASSERT(secondAck.IsShardEmpty);
        UNIT_ASSERT(!Controller->GetMessageMetadata(TestShardId));
    }

    Y_UNIT_TEST_F(ReadOnlyLookupsDoNotAlterShardSet, TShardedWriteControllerFixture) {
        WriteRound(1, 11, 0);

        Controller->PrepareMessageMetadata(TestShardId);
        const ui64 cookie = Controller->AllocateMessageCookie(TestShardId);
        Controller->OnMessageSent(TestShardId, cookie);
        UNIT_ASSERT(Controller->GetMessageMetadata(TestShardId));

        // Acknowledge the round: the shard stays registered (needed for the
        // external-prepare exclusion) but is empty, and a read-only metadata
        // lookup for it returns nothing without removing or altering the entry.
        const auto acknowledged = Controller->OnMessageAcknowledged(TestShardId, cookie);
        UNIT_ASSERT(acknowledged.IsShardEmpty);
        UNIT_ASSERT(!Controller->GetMessageMetadata(TestShardId));

        // Lookups for shards unknown to the controller must not create entries
        // (the write actor drops results of such shards before any lookup, and
        // direct metadata lookups for them are asserted in the controller), so
        // the unknown shard stays unknown and does not leak into the shard set.
        UNIT_ASSERT(!Controller->HasShard(UnknownShardId));

        const auto shardIds = Controller->GetShardsIds();
        UNIT_ASSERT_VALUES_EQUAL(shardIds.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(shardIds[0], TestShardId);
        UNIT_ASSERT_VALUES_EQUAL(Controller->GetShardsCount(), 1);
    }
}
