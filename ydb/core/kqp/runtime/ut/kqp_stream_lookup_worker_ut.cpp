#include <ydb/core/kqp/runtime/kqp_stream_lookup_worker.h>
#include <ydb/core/kqp/runtime/kqp_read_iterator_common.h>
#include <ydb/core/protos/base.pb.h>
#include <ydb/core/protos/kqp_physical.pb.h>
#include <ydb/core/protos/kqp_tablemetadata.pb.h>

#include <yql/essentials/minikql/mkql_node_serialization.h>
#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>

namespace NKikimr::NKqp {
namespace {
    using namespace NMiniKQL;
    using TRead = TEvDataShard::TEvRead;
    constexpr ui64 ShardId = 1001;

    struct TLimitScope {
        TLimitScope(ui64 entries, ui64 bytes) {
            auto settings = MakeIntrusive<TIteratorReadBackoffSettings>();
            settings->MaxReadRequestEntriesStreamLookup = entries;
            settings->MaxReadRequestBytesStreamLookup = bytes;
            SetReadIteratorBackoffSettings(settings);
        }

        ~TLimitScope() {
            SetReadIteratorBackoffSettings(MakeIntrusive<TIteratorReadBackoffSettings>());
        }
    };

    TPartitioning::TCPtr Partitioning(bool split = false) {
        TVector<TKeyDesc::TPartitionInfo> parts;
        if (split) {
            TKeyDesc::TPartitionInfo first(ShardId);
            first.Range.ConstructInPlace();
            first.Range->EndKeyPrefix = TSerializedCellVec(TVector<TCell>{ TCell::Make<ui64>(2) });
            first.Range->IsInclusive = false;
            parts.emplace_back(std::move(first));
        }
        TKeyDesc::TPartitionInfo last(split ? ShardId + 1 : ShardId);
        last.Range.ConstructInPlace();
        parts.emplace_back(std::move(last));
        return std::make_shared<TPartitioning>(std::move(parts));
    }

    size_t WorstWireBytes(const TRead& request) {
        auto record = request.Record;
        record.SetReadId(Max<ui64>());
        record.MutableSnapshot()->SetStep(Max<ui64>());
        record.MutableSnapshot()->SetTxId(Max<ui64>());
        record.SetLockTxId(Max<ui64>());
        record.SetLockNodeId(Max<ui32>());
        record.SetLockMode(NKikimrDataEvents::PESSIMISTIC_EXCLUSIVE);
        record.SetQuerySpanId(Max<ui64>());
        record.SetMaxRows(Max<ui32>());
        record.SetMaxBytes(Max<ui32>());
        record.SetTotalRowsLimit(Max<ui64>());
        record.SetResultFormat(NKikimrDataEvents::FORMAT_CELLVEC);
        for (const auto& key : request.Keys) {
            record.AddKeys(key.GetBuffer());
        }
        for (const auto& range : request.Ranges) {
            range.Serialize(*record.AddRanges());
        }
        return record.ByteSizeLong();
    }

    struct TFixture {
        TLimitScope Limits;
        TScopedAlloc Alloc{ __LOCATION__ };
        TMemoryUsageInfo MemoryInfo{ "StreamLookupBatchTest" };
        TTypeEnvironment TypeEnv{ Alloc };
        THolderFactory HolderFactory{ Alloc.Ref(), MemoryInfo };
        NYql::NDqProto::TTaskInput InputDesc;
        std::unique_ptr<TKqpStreamLookupWorker> Worker;
        ui64 ReadId = 0;

        TFixture(ui64 entries, ui64 bytes, bool prefix = false, bool stringKey = false, bool join = false)
            : Limits(entries, bytes)
        {
            const NScheme::TTypeInfo keyType(stringKey ? NScheme::NTypeIds::String : NScheme::NTypeIds::Uint64);
            TSysTables::TTableColumnInfo key("key", 1, keyType, "", 0);
            if (!join) {
                TLookupSettings settings;
                settings.TablePath = "/Root/Table";
                settings.TableId = TTableId(1, 2, 3);
                settings.Database = "/Root";
                settings.PoolId = "pool";
                settings.AllowNullKeysPrefixSize = 0;
                settings.KeepRowsOrder = false;
                settings.LookupStrategy = NKqpProto::EStreamLookupStrategy::LOOKUP;
                settings.KeyColumns.emplace("key", key);
                settings.InputColumns.push_back(key);
                settings.Columns.push_back(key);
                if (prefix) {
                    TSysTables::TTableColumnInfo suffix(
                        "suffix", 2, NScheme::TTypeInfo(NScheme::NTypeIds::String), "", 1);
                    settings.KeyColumns.emplace("suffix", suffix);
                    settings.Columns.push_back(suffix);
                }
                Worker = CreateLookupWorker(std::move(settings), TypeEnv, HolderFactory);
                return;
            }
            NKikimrKqp::TKqpStreamLookupSettings settings;
            settings.MutableTable()->SetOwnerId(1);
            settings.MutableTable()->SetTableId(2);
            settings.MutableTable()->SetVersion(3);
            settings.MutableTable()->SetPath("/Root/Table");
            settings.SetLookupStrategy(NKqpProto::EStreamLookupStrategy::JOIN);
            settings.SetKeepRowsOrder(true);
            auto* column = settings.AddKeyColumns();
            column->SetName("key");
            column->SetId(1);
            column->SetTypeId(NScheme::NTypeIds::Uint64);
            *settings.AddInputColumns() = *column;
            if (prefix) {
                auto* suffix = settings.AddKeyColumns();
                suffix->SetName("suffix");
                suffix->SetId(2);
                suffix->SetTypeId(NScheme::NTypeIds::String);
            }
            auto* value = settings.AddColumns();
            value->SetName("value");
            value->SetId(3);
            value->SetTypeId(NScheme::NTypeIds::Uint64);
            auto* integer = TDataType::Create(NUdf::TDataType<ui64>::Id, TypeEnv);
            const std::pair<TString, TType*> leftMembers[] = { { "left", integer } };
            const std::pair<TString, TType*> keyMembers[] = { { "key", integer } };
            const std::pair<TString, TType*> rightMembers[] = { { "value", integer } };
            auto* left = TStructType::Create(leftMembers, 1, TypeEnv);
            auto* joinKey = TStructType::Create(keyMembers, 1, TypeEnv);
            auto* right = TStructType::Create(rightMembers, 1, TypeEnv);
            TType* input[] = { left, joinKey };
            TType* output[] = { left, right, integer };
            InputDesc.MutableTransform()->SetInputType(SerializeNode(TTupleType::Create(2, input, TypeEnv), TypeEnv));
            InputDesc.MutableTransform()->SetOutputType(SerializeNode(TTupleType::Create(3, output, TypeEnv), TypeEnv));
            Worker = CreateStreamLookupWorker(std::move(settings), 0, TypeEnv, HolderFactory, InputDesc, nullptr);
        }

        void Add(ui64 value) {
            const TCell cell = TCell::Make(value);
            Worker->AddInputRow(TConstArrayRef<TCell>(&cell, 1));
        }

        void AddString(const TString& value) {
            const TCell cell(value.data(), value.size());
            Worker->AddInputRow(TConstArrayRef<TCell>(&cell, 1));
        }

        void AddJoin(ui64 ordinal, ui64 key) {
            NUdf::TUnboxedValue* items = nullptr;
            auto row = HolderFactory.CreateDirectArrayHolder(2, items);
            NUdf::TUnboxedValue* left = nullptr;
            items[0] = HolderFactory.CreateDirectArrayHolder(1, left);
            left[0] = NUdf::TUnboxedValuePod(ordinal);
            NUdf::TUnboxedValue* keyItems = nullptr;
            items[1] = HolderFactory.CreateDirectArrayHolder(1, keyItems);
            keyItems[0] = NUdf::TUnboxedValuePod(key);
            Worker->AddInputRow(std::move(row));
        }

        void Result(THolder<TEvDataShard::TEvReadResult> result) {
            auto envelope = MakeHolder<IEventHandle>(TActorId(), TActorId(), result.Release());
            // Typed handles are the actor runtime's view of the same envelope.
            THolder<TEventHandle<TEvDataShard::TEvReadResult>> typed(
                reinterpret_cast<TEventHandle<TEvDataShard::TEvReadResult>*>(envelope.Release()));
            Worker->AddResult(TStreamLookupShardReadResult(ShardId, std::move(typed), &Alloc.Ref()));
        }

        std::vector<ui64> DrainKeys(size_t maxEntries = 0, size_t maxBytes = 0) {
            std::vector<ui64> values;
            while (auto request = Worker->PopNextRequest().second) {
                if (maxEntries) {
                    UNIT_ASSERT(request->GetKeysSize() + request->GetRangesSize() <= maxEntries);
                }
                if (maxBytes) {
                    UNIT_ASSERT(WorstWireBytes(*request) <= maxBytes);
                }
                for (const auto& key : request->Keys) {
                    values.push_back(key.GetCells()[0].AsValue<ui64>());
                }
            }
            return values;
        }
    };

    void CheckJoin(bool prefix, bool retry, bool repartition) {
        TFixture fixture(2, 1024, prefix, false, true);
        const std::vector<ui64> keys = { 3, 1, 3, 2, 1 };
        for (size_t i = 0; i < keys.size(); ++i) {
            fixture.AddJoin(i, keys[i]);
        }
        fixture.Worker->BuildRequests(Partitioning(), fixture.ReadId);
        if (retry || repartition) {
            auto first = fixture.Worker->PopNextRequest();
            UNIT_ASSERT(first.second);
            if (repartition) {
                fixture.Worker->ResetRowsProcessing(first.second->Record.GetReadId());
                fixture.Worker->BuildRequests(Partitioning(true), fixture.ReadId);
            } else {
                fixture.Worker->RebuildRequest(first.first, first.second->Record.GetReadId(), fixture.ReadId);
            }
        }
        std::vector<THolder<TRead>> requests;
        while (auto request = fixture.Worker->PopNextRequest().second) {
            UNIT_ASSERT(request->GetKeysSize() + request->GetRangesSize() <= 2);
            UNIT_ASSERT(WorstWireBytes(*request) <= 1024);
            requests.push_back(std::move(request));
        }
        std::reverse(requests.begin(), requests.end());
        for (const auto& request : requests) {
            auto result = MakeHolder<TEvDataShard::TEvReadResult>();
            result->Record.SetReadId(request->Record.GetReadId());
            result->Record.SetSeqNo(1);
            result->Record.SetFinished(true);
            result->Record.MutableStatus()->SetCode(Ydb::StatusIds::SUCCESS);
            std::vector<ui64> values;
            for (const auto& key : request->Keys) {
                values.push_back(key.GetCells()[0].AsValue<ui64>());
            }
            for (const auto& range : request->Ranges) {
                values.push_back(range.From.GetCells()[0].AsValue<ui64>());
            }
            TOwnedCellVecBatch rows;
            for (ui64 key : values) {
                for (ui64 variant = 0; variant < 2; ++variant) {
                    rows.Append({ TCell::Make(key), TCell::Make(key * 10 + variant) });
                }
            }
            result->Record.SetRowCount(rows.Size());
            result->SetBatch(std::move(rows));
            fixture.Result(std::move(result));
        }
        TUnboxedValueBatch batch;
        fixture.Worker->ReplyResult(batch, Max<i64>());
        UNIT_ASSERT_VALUES_EQUAL(batch.RowCount(), 2 * keys.size());
        size_t index = 0;
        batch.ForEachRow([&](const NUdf::TUnboxedValue& row) {
            UNIT_ASSERT_VALUES_EQUAL(row.GetElement(0).GetElement(0).Get<ui64>(), index / 2);
            UNIT_ASSERT_VALUES_EQUAL(row.GetElement(1).GetElement(0).Get<ui64>(), keys[index / 2] * 10 + index % 2);
            ++index;
        });
        UNIT_ASSERT(fixture.Worker->AllRowsProcessed());
    }
} // namespace

Y_UNIT_TEST_SUITE(KqpStreamLookupBatching) {
    Y_UNIT_TEST(CountCapPreservesDuplicateKeyOrder) {
        TFixture fixture(3, 0);
        const std::vector<ui64> keys = { 3, 1, 3, 2, 1, 9, 8, 7, 6, 5 };
        for (auto key : keys) {
            fixture.Add(key);
        }
        fixture.Worker->BuildRequests(Partitioning(), fixture.ReadId);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Worker->ScheduledRequestsCount(), 4);
        UNIT_ASSERT_VALUES_EQUAL(fixture.DrainKeys(3), keys);
    }

    Y_UNIT_TEST(ByteCapIncludesLateControlFields) {
        TFixture fixture(0, 1024, false, true);
        for (size_t i = 0; i < 20; ++i) {
            fixture.AddString(TString(180, 'x') + ToString(i));
        }
        fixture.Worker->BuildRequests(Partitioning(), fixture.ReadId);
        UNIT_ASSERT(fixture.Worker->ScheduledRequestsCount() > 1);
        size_t count = 0;
        while (auto request = fixture.Worker->PopNextRequest().second) {
            UNIT_ASSERT(WorstWireBytes(*request) <= 1024);
            count += request->GetKeysSize();
        }
        UNIT_ASSERT_VALUES_EQUAL(count, 20);
    }

    Y_UNIT_TEST(PrefixRangesStayBounded) {
        TFixture fixture(3, 512, true);
        for (ui64 i = 0; i < 10; ++i) {
            fixture.Add(i);
        }
        fixture.Worker->BuildRequests(Partitioning(), fixture.ReadId);
        size_t count = 0;
        while (auto request = fixture.Worker->PopNextRequest().second) {
            UNIT_ASSERT(request->Ranges.size() <= 3);
            UNIT_ASSERT(WorstWireBytes(*request) <= 512);
            for (const auto& range : request->Ranges) {
                UNIT_ASSERT_VALUES_EQUAL(range.From.GetCells()[0].AsValue<ui64>(), count++);
                UNIT_ASSERT(range.ToInclusive);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(count, 10);
    }

    Y_UNIT_TEST(OversizedEntryIsSentAlone) {
        TFixture fixture(0, 1_MB, false, true);
        const std::vector<TString> keys = { "before", TString(1_MB + 512, 'x'), "after" };
        for (const auto& key : keys) {
            fixture.AddString(key);
        }
        fixture.Worker->BuildRequests(Partitioning(), fixture.ReadId);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Worker->ScheduledRequestsCount(), keys.size());
        size_t index = 0;
        while (auto request = fixture.Worker->PopNextRequest().second) {
            UNIT_ASSERT_VALUES_EQUAL(request->GetKeysSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(request->Keys[0].GetCells()[0].AsBuf(), keys.at(index));
            UNIT_ASSERT_VALUES_EQUAL(WorstWireBytes(*request) > 1_MB, index == 1);
            ++index;
        }
        UNIT_ASSERT_VALUES_EQUAL(index, keys.size());
    }

    Y_UNIT_TEST(OversizedControlHeaderUsesSingletons) {
        TFixture fixture(0, 1);
        const std::vector<ui64> keys = { 1, 2, 3 };
        for (auto key : keys) {
            fixture.Add(key);
        }
        fixture.Worker->BuildRequests(Partitioning(), fixture.ReadId);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Worker->ScheduledRequestsCount(), keys.size());
        UNIT_ASSERT_VALUES_EQUAL(fixture.DrainKeys(1), keys);
    }

    Y_UNIT_TEST(ZeroDisablesConfiguredCaps) {
        TFixture fixture(0, 0);
        for (ui64 i = 0; i < 10; ++i) {
            fixture.Add(i);
        }
        fixture.Worker->BuildRequests(Partitioning(), fixture.ReadId);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Worker->ScheduledRequestsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.DrainKeys().size(), 10);
    }

    Y_UNIT_TEST(RetryAndRepartitionUseTheSameCaps) {
        for (bool repartition : { false, true }) {
            TFixture fixture(3, 512);
            for (ui64 i = 0; i < 10; ++i) {
                fixture.Add(i);
            }
            fixture.Worker->BuildRequests(Partitioning(), fixture.ReadId);
            auto first = fixture.Worker->PopNextRequest();
            if (repartition) {
                fixture.Worker->ResetRowsProcessing(first.second->Record.GetReadId());
                fixture.Worker->BuildRequests(Partitioning(true), fixture.ReadId);
            } else {
                fixture.Worker->RebuildRequest(first.first, first.second->Record.GetReadId(), fixture.ReadId);
            }
            auto keys = fixture.DrainKeys(3, 512);
            std::sort(keys.begin(), keys.end());
            const std::vector<ui64> expected = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
            UNIT_ASSERT_VALUES_EQUAL(keys, expected);
        }
    }

    Y_UNIT_TEST(PartialRangeRetryResplitsExpandedContinuation) {
        TFixture fixture(0, 512, true);
        for (ui64 i = 0; i < 10; ++i) {
            fixture.Add(i);
        }
        fixture.Worker->BuildRequests(Partitioning(), fixture.ReadId);
        auto first = fixture.Worker->PopNextRequest();
        UNIT_ASSERT(first.second->Ranges.size() > 1);
        auto result = MakeHolder<TEvDataShard::TEvReadResult>();
        result->Record.SetReadId(first.second->Record.GetReadId());
        result->Record.SetSeqNo(1);
        result->Record.MutableStatus()->SetCode(Ydb::StatusIds::SUCCESS);
        NKikimrTxDataShard::TReadContinuationToken token;
        token.SetFirstUnprocessedQuery(0);
        const TString suffix(200, 'x');
        token.SetLastProcessedKey(
            TSerializedCellVec::Serialize({ TCell::Make<ui64>(0), TCell(suffix.data(), suffix.size()) }));
        UNIT_ASSERT(token.SerializeToString(result->Record.MutableContinuationToken()));
        fixture.Result(std::move(result));
        fixture.Worker->ReadAllResult([](TConstArrayRef<TCell>) {
        });
        const ui64 oldMaxId = fixture.ReadId;
        fixture.Worker->RebuildRequest(first.first, first.second->Record.GetReadId(), fixture.ReadId);
        size_t retried = 0;
        while (auto request = fixture.Worker->PopNextRequest().second) {
            UNIT_ASSERT(WorstWireBytes(*request) <= 512);
            if (request->Record.GetReadId() > oldMaxId) {
                ++retried;
            }
        }
        UNIT_ASSERT(retried > 1);
    }

    Y_UNIT_TEST(JoinPreservesOrderAndMultiplicityWithOutOfOrderReplies) {
        CheckJoin(false, false, false);
    }
    Y_UNIT_TEST(PrefixJoinPreservesMultiplicity) {
        CheckJoin(true, false, false);
    }
    Y_UNIT_TEST(JoinRetryPreservesCompletionAccounting) {
        CheckJoin(false, true, false);
    }
    Y_UNIT_TEST(JoinRepartitionPreservesCompletionAccounting) {
        CheckJoin(false, false, true);
    }
}
} // namespace NKikimr::NKqp
