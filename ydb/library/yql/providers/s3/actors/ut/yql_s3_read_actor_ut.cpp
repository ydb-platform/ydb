#include <ydb/library/yql/providers/s3/actors/yql_s3_read_actor.h>
#include <ydb/library/yql/providers/s3/actors/yql_arrow_push_down.h>
#include <ydb/library/yql/providers/s3/proto/range.pb.h>

#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_default_retry_policy.h>
#include <ydb/library/yql/providers/common/ut_helpers/dq_fake_ca.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/memory.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/arrow/reader.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/arrow/writer.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/size_literals.h>
#include <util/stream/str.h>

#include <algorithm>
#include <deque>

namespace NYql::NDq {
namespace {

using namespace NActors;
using namespace NKikimr::NMiniKQL;

constexpr ui64 RowGroupCount = 10;
constexpr ui64 Second = 1000000;

// One distinct timestamp per row group, with statistics for predicate pruning.
TString MakeParquet() {
    arrow::TimestampBuilder builder(arrow::timestamp(arrow::TimeUnit::MICRO), arrow::default_memory_pool());
    for (ui64 group = 0; group < RowGroupCount; ++group) {
        UNIT_ASSERT(builder.Append(group * Second).ok());
    }
    std::shared_ptr<arrow::Array> array;
    UNIT_ASSERT(builder.Finish(&array).ok());
    auto table = arrow::Table::Make(
        arrow::schema({arrow::field("ts", arrow::timestamp(arrow::TimeUnit::MICRO), false)}), {array});
    auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
    UNIT_ASSERT(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), sink, 1).ok());
    const auto buffer = sink->Finish().ValueOrDie();
    return TString(reinterpret_cast<const char*>(buffer->data()), buffer->size());
}

using TPredicate = NConnector::NApi::TPredicate;

TPredicate TimestampComparison(TPredicate::TComparison::EOperation operation, ui64 group) {
    TPredicate predicate;
    auto* comparison = predicate.mutable_comparison();
    comparison->set_operation(operation);
    comparison->mutable_left_value()->set_column("ts");
    auto* value = comparison->mutable_right_value()->mutable_typed_value();
    value->mutable_type()->set_type_id(Ydb::Type::TIMESTAMP);
    value->mutable_value()->set_int64_value(group * Second);
    return predicate;
}

TPredicate AlternatingGroups(ui64 first) {
    TPredicate predicate;
    for (ui64 group = first; group < RowGroupCount; group += 2) {
        *predicate.mutable_disjunction()->add_operands() = TimestampComparison(TPredicate::TComparison::EQ, group);
    }
    return predicate;
}

// Ported from the historical TS3ReadCoro harness. Hold ranged callbacks so that
// the test drives readiness explicitly, including completion in reverse order.
class TFakeS3Gateway : public IHTTPGateway {
public:
    struct TRequest {
        size_t Offset;
        size_t Size;
    };

    struct TPendingRequest : TRequest {
        TOnResult Callback;
    };

    const TString Body = MakeParquet();
    std::vector<TRequest> Requests;
    std::deque<TPendingRequest> Pending;

    void Upload(TString, THeaders, TString, TOnResult, bool, TRetryPolicy::TPtr, IHttpRequestContext::TPtr) override {
        UNIT_FAIL("unexpected upload");
    }

    void Delete(TString, THeaders, TOnResult, TRetryPolicy::TPtr, IHttpRequestContext::TPtr) override {
        UNIT_FAIL("unexpected delete");
    }

    void Download(TString, THeaders, size_t offset, size_t size, TOnResult callback, TString,
                  TRetryPolicy::TPtr, IHttpRequestContext::TPtr) override {
        UNIT_ASSERT(offset <= Body.size() && size <= Body.size() - offset);
        Requests.push_back({offset, size});
        Pending.push_back({{offset, size}, std::move(callback)});
    }

    TCancelHook Download(TString, THeaders, size_t, size_t, TOnDownloadStart, TOnNewDataPart, TOnDownloadFinish,
                         const ::NMonitoring::TDynamicCounters::TCounterPtr&, IHttpRequestContext::TPtr) override {
        UNIT_FAIL("unexpected streaming download");
        return {};
    }

    ui64 GetBuffersSizePerStream() override {
        return 0;
    }

    void UpdatePoolCaps(THashMap<TWorkScope, size_t>) override {}

    void CompleteOne(bool reverse) {
        UNIT_ASSERT(!Pending.empty());
        auto request = std::move(reverse ? Pending.back() : Pending.front());
        if (reverse) {
            Pending.pop_back();
        } else {
            Pending.pop_front();
        }
        request.Callback(TResult(TContent(Body.substr(request.Offset, request.Size), 206)));
    }
};

struct TSimulatedCA {
    const TActorId FakeActorId{0, "FakeActor"};
    std::shared_ptr<TAsyncInputPromises> InputPromises = std::make_shared<TAsyncInputPromises>();
    std::shared_ptr<TAsyncOutputPromises> OutputPromises = std::make_shared<TAsyncOutputPromises>();
    TVector<ui64> Rows;
    TIssues Errors;
    ui64 Notifications = 0;
    bool Finished = false;
    // Destroy actors before the state borrowed by the observer.
    TTestActorRuntimeBase Runtime{1};

    TSimulatedCA() {
        Runtime.AddLocalService(FakeActorId,
            TActorSetupCmd(new TFakeActor(InputPromises, OutputPromises), TMailboxType::Simple, 0));
        Runtime.Initialize();
        Runtime.GetLogSettings(0)->Append(NKikimrServices::EServiceKikimr_MIN,
            NKikimrServices::EServiceKikimr_MAX, NKikimrServices::EServiceKikimr_Name);
        Runtime.SetDispatchTimeout(TDuration::Seconds(1));
        Runtime.SetObserverFunc([this](TAutoPtr<IEventHandle>& event) {
            if (event->Recipient == FakeActorId) {
                if (event->GetTypeRewrite() == IDqComputeActorAsyncInput::TEvNewAsyncInputDataArrived::EventType) {
                    ++Notifications;
                } else if (event->GetTypeRewrite() == IDqComputeActorAsyncInput::TEvAsyncInputError::EventType) {
                    Errors.AddIssues(event->Get<IDqComputeActorAsyncInput::TEvAsyncInputError>()->Issues);
                }
            }
            return TTestActorRuntimeBase::EEventAction::PROCESS;
        });
    }

    ~TSimulatedCA() {
        try {
            Execute([](TFakeActor& actor) { actor.Terminate(); });
        } catch (...) {
        }
    }

    void Execute(TCallback callback) {
        auto exception = std::make_shared<std::exception_ptr>();
        auto promise = NThreading::NewPromise();
        // TEvExecute borrows its exception slot; keep it alive if dispatch fails.
        auto ownedCallback = [callback = std::move(callback), exception](TFakeActor& actor) {
            try {
                callback(actor);
            } catch (...) {
                *exception = std::current_exception();
            }
        };
        Runtime.Send(new IEventHandle(FakeActorId, {},
            new TEvPrivate::TEvExecute(promise, std::move(ownedCallback), *exception)));
        TDispatchOptions options;
        options.CustomFinalCondition = [&] { return promise.HasValue(); };
        Runtime.DispatchEvents(options, TDuration::Seconds(10));
        UNIT_ASSERT_C(promise.HasValue(), "fake compute actor did not run the callback");
        if (*exception) {
            std::rethrow_exception(*exception);
        }
    }

    void CreateReader(const std::shared_ptr<TFakeS3Gateway>& gateway, NS3::TSource source) {
        NS3::TRange range;
        auto* path = range.AddPaths();
        path->SetName("file.parquet");
        path->SetSize(gateway->Body.size());
        path->SetRead(true);
        TStringStream out;
        range.Save(&out);
        Execute([&](TFakeActor& actor) {
            auto [input, inputActor] = CreateS3ReadActor(actor.TypeEnv, actor.HolderFactory,
                std::shared_ptr<TScopedAlloc>(&actor.Alloc, [](TScopedAlloc*) {}),
                gateway, std::move(source), 0, TCollectStatsLevel::None, TTxId{}, {}, {},
                {out.Str()}, FakeActorId, CreateStructuredTokenCredentialsFactory(),
                GetHTTPDefaultRetryPolicy(), TS3ReadActorFactoryConfig{}, nullptr, nullptr,
                std::make_shared<TGuaranteeQuotaManager>(1_GB, 1_GB), false, nullptr);
            actor.InitAsyncInput(input, inputActor);
        });
    }

    void ReadToEnd(TFakeS3Gateway& gateway, bool reverse) {
        for (ui32 step = 0; step < 100; ++step) {
            const auto notifications = Notifications;
            Execute([&](TFakeActor& actor) {
                TMaybe<TInstant> watermark;
                TUnboxedValueBatch buffer;
                actor.DqAsyncInput->GetAsyncInputData(buffer, watermark, Finished, 1_MB);
                buffer.ForEachRow([&](const NUdf::TUnboxedValue& row) {
                    // Struct members are sorted: _yql_block_length, ts.
                    const auto block = row.GetElement(1);
                    const auto array = TArrowBlock::From(block).GetDatum().make_array();
                    UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(array->type_id()), static_cast<int>(arrow::Type::UINT64));
                    const auto& timestamps = static_cast<const arrow::UInt64Array&>(*array);
                    UNIT_ASSERT_VALUES_EQUAL(timestamps.null_count(), 0);
                    for (i64 i = 0; i < timestamps.length(); ++i) {
                        Rows.push_back(timestamps.Value(i));
                    }
                });
            });
            if (Finished || !Errors.Empty()) {
                return;
            }
            if (gateway.Pending.empty() && Notifications == notifications) {
                TDispatchOptions options;
                options.CustomFinalCondition = [&] {
                    return !gateway.Pending.empty() || Notifications != notifications || !Errors.Empty();
                };
                try {
                    Runtime.DispatchEvents(options, TDuration::Seconds(10));
                } catch (const TEmptyEventQueueException&) {
                    return; // No callback or notification can make further progress.
                }
            }
            if (!gateway.Pending.empty()) {
                gateway.CompleteOne(reverse);
            }
        }
    }
};

void CheckRead(const TPredicate& predicate, const TVector<ui64>& groups, ui64 parallel, bool reordering, bool reverse) {
    const TString context = TStringBuilder() << "parallel=" << parallel << " reordering=" << reordering << " reverse=" << reverse;
    auto gateway = std::make_shared<TFakeS3Gateway>();
    auto file = std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromString(gateway->Body));
    std::unique_ptr<parquet::arrow::FileReader> reader;
    UNIT_ASSERT(parquet::arrow::OpenFile(file, arrow::default_memory_pool(), &reader).ok());
    const auto metadata = reader->parquet_reader()->metadata();
    UNIT_ASSERT_VALUES_EQUAL(metadata->num_row_groups(), RowGroupCount);
    if (predicate.payload_case() != TPredicate::PAYLOAD_NOT_SET) {
        UNIT_ASSERT_VALUES_EQUAL(MatchedRowGroups(metadata, predicate), groups);
    }

    TSimulatedCA ca;
    NS3::TSource source;
    source.SetUrl("http://fake/");
    source.SetFormat("parquet");
    source.SetRowType(R"(["StructType";[["ts";["DataType";"Timestamp"]]]])");
    source.SetParallelRowGroupCount(parallel);
    source.SetRowGroupReordering(reordering);
    *source.MutablePredicate() = predicate;
    ca.CreateReader(gateway, std::move(source));
    ca.ReadToEnd(*gateway, reverse);
    UNIT_ASSERT_C(ca.Errors.Empty(), context << " " << ca.Errors.ToOneLineString());
    UNIT_ASSERT_C(ca.Finished, context << " unfinished, rows=" << ca.Rows.size() << " expected=" << groups.size());
    UNIT_ASSERT(gateway->Pending.empty());
    TVector<ui64> expected;
    for (const auto group : groups) {
        expected.push_back(group * Second);
    }
    if (reordering) {
        std::sort(ca.Rows.begin(), ca.Rows.end());
    }
    UNIT_ASSERT_VALUES_EQUAL(ca.Rows, expected);

    // Check the actual HTTP prefetch ranges, including refills. Decoding must
    // use these cache entries and never request a skipped physical row group.
    for (ui64 group = 0; group < RowGroupCount; ++group) {
        const auto column = metadata->RowGroup(group)->ColumnChunk(0);
        const auto offset = column->has_dictionary_page() ? column->dictionary_page_offset() : column->data_page_offset();
        const auto requests = std::count_if(gateway->Requests.begin(), gateway->Requests.end(), [&](const auto& request) {
            return request.Offset == ui64(offset) && request.Size == ui64(column->total_compressed_size());
        });
        const bool selected = std::find(groups.begin(), groups.end(), group) != groups.end();
        UNIT_ASSERT_VALUES_EQUAL_C(requests, selected ? 1 : 0, context << " physical group=" << group);
    }
}

TVector<ui64> GroupsFrom(ui64 first, ui64 stride = 1) {
    TVector<ui64> groups;
    for (ui64 group = first; group < RowGroupCount; group += stride) {
        groups.push_back(group);
    }
    return groups;
}

void CheckParallelReads(const TPredicate& predicate, const TVector<ui64>& groups) {
    for (ui64 parallel : {0, 1, 2, 3, 5}) {
        for (bool reordering : {false, true}) {
            for (bool reverse : {false, true}) {
                CheckRead(predicate, groups, parallel, reordering, reverse);
            }
        }
    }
}

} // namespace

Y_UNIT_TEST_SUITE(TS3ReadCoro) {
    Y_UNIT_TEST(ParquetParallelRowGroupsWithPushdown) {
        for (ui64 skipped : {1, 2}) {
            CheckParallelReads(TimestampComparison(TPredicate::TComparison::GE, skipped), GroupsFrom(skipped));
        }
    }

    Y_UNIT_TEST(ParquetRefillReadinessAfterLeadingSkip) {
        CheckRead(TimestampComparison(TPredicate::TComparison::GE, 1), GroupsFrom(1), 2, false, false);
    }

    Y_UNIT_TEST(ParquetRefillCacheRangeAfterLeadingSkip) {
        CheckRead(TimestampComparison(TPredicate::TComparison::GE, 1), GroupsFrom(1), 1, true, false);
    }

    Y_UNIT_TEST(ParquetParallelAlternatingRowGroupsWithPushdown) {
        for (ui64 first : {0, 1}) {
            CheckParallelReads(AlternatingGroups(first), GroupsFrom(first, 2));
        }
    }

    Y_UNIT_TEST(ParquetReadWithoutPredicate) {
        CheckParallelReads({}, GroupsFrom(0));
    }

    Y_UNIT_TEST(ParquetPushdownWithoutSkippedGroups) {
        CheckParallelReads(TimestampComparison(TPredicate::TComparison::GE, 0), GroupsFrom(0));
    }

    Y_UNIT_TEST(ParquetPushdownClampsReadersAndSkipsAllGroups) {
        for (bool reordering : {false, true}) {
            CheckRead(TimestampComparison(TPredicate::TComparison::GE, 9), GroupsFrom(9), 5, reordering, true);
            CheckRead(TimestampComparison(TPredicate::TComparison::GE, 10), {}, 5, reordering, true);
        }
    }
}

} // namespace NYql::NDq
