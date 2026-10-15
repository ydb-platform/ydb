// ClickHouse headers go first: memlog.h (via the actor headers) defines NO_SANITIZE_THREAD differently.
#include <ydb/library/yql/udfs/common/clickhouse/client/src/Formats/FormatFactory.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/Formats/registerFormats.h>

#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_default_retry_policy.h>
#include <ydb/library/yql/providers/common/ut_helpers/dq_fake_ca.h>
#include <ydb/library/yql/providers/s3/actors/yql_s3_read_actor.h>
#include <ydb/library/yql/providers/s3/events/events.h>
#include <ydb/library/yql/providers/s3/proto/range.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/scope.h>
#include <util/generic/size_literals.h>
#include <util/generic/yexception.h>
#include <util/random/fast.h>
#include <util/stream/str.h>
#include <util/stream/zlib.h>
#include <util/system/env.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/memory.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/arrow/writer.h>

#include <atomic>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <vector>

namespace NYql::NDq {

using namespace NActors;
using namespace NKikimr::NMiniKQL;

namespace {

// Fake S3: streaming (CSV/JSON/...) downloads and ranged (parquet, raw) downloads are scripted per URL.
class TFakeS3Gateway : public IHTTPGateway {
public:
    using TStreamScript = std::function<void(const TString& url, int attempt, TOnDownloadStart&, TOnNewDataPart&, TOnDownloadFinish&)>;
    using TRangeScript = std::function<TResult(const TString& url, size_t offset, size_t size)>;

    TStreamScript StreamScript;
    std::function<TCancelHook(const TString& url, int attempt)> StreamCancelHook;
    // Called with the callback; the script decides whether (and how) to answer. Default: serve Body.
    std::function<void(const TString& url, size_t offset, size_t size, TOnResult& callback)> RangeScript;
    TString Body;

    std::atomic<int> StreamCalls = 0;
    std::atomic<int> RangeCalls = 0;

    int StreamCallsFor(const TString& url) {
        std::lock_guard lock(Mutex);
        return StreamCallsByUrl[url];
    }

    void Upload(TString, THeaders, TString, TOnResult, bool, TRetryPolicy::TPtr, IHttpRequestContext::TPtr) override {
        UNIT_FAIL("unexpected upload");
    }

    void Delete(TString, THeaders, TOnResult, TRetryPolicy::TPtr, IHttpRequestContext::TPtr) override {
        UNIT_FAIL("unexpected delete");
    }

    void Download(TString url, THeaders, std::size_t offset, std::size_t sizeLimit, TOnResult callback, TString, TRetryPolicy::TPtr, IHttpRequestContext::TPtr) override {
        ++RangeCalls;
        if (RangeScript) {
            RangeScript(url, offset, sizeLimit, callback);
            return;
        }
        callback(TResult(TContent(Body.substr(offset, sizeLimit), 206)));
    }

    TCancelHook Download(TString url, THeaders, std::size_t, std::size_t, TOnDownloadStart onStart, TOnNewDataPart onData, TOnDownloadFinish onFinish,
                         const ::NMonitoring::TDynamicCounters::TCounterPtr&, IHttpRequestContext::TPtr) override {
        ++StreamCalls;
        int attempt;
        {
            std::lock_guard lock(Mutex);
            attempt = ++StreamCallsByUrl[url];
        }
        StreamScript(url, attempt, onStart, onData, onFinish);
        if (StreamCancelHook) {
            return StreamCancelHook(url, attempt);
        }
        return [](TIssue) {};
    }

    ui64 GetBuffersSizePerStream() override {
        return 0;
    }

    void UpdatePoolCaps(THashMap<NDq::TWorkScope, size_t>) override {}

    TCountedContent Content(TString data) {
        return TCountedContent(std::move(data), Counter, nullptr, {}, Max<size_t>());
    }

    // Answers every streaming request for any URL with the same successful body.
    void ServeStream(const TString& data) {
        StreamScript = [this, data](const TString&, int, TOnDownloadStart& onStart, TOnNewDataPart& onData, TOnDownloadFinish& onFinish) {
            onStart(CURLE_OK, 206);
            onData(Content(data));
            onFinish(CURLE_OK, TIssues{});
        };
    }

private:
    std::shared_ptr<std::atomic_size_t> Counter = std::make_shared<std::atomic_size_t>(0);
    std::mutex Mutex;
    THashMap<TString, int> StreamCallsByUrl;
};

TString EncodeRanges(ui32 files, ui64 size) {
    NS3::TRange range;
    for (ui32 i = 0; i < files; ++i) {
        auto* path = range.AddPaths();
        path->SetName(TStringBuilder() << "file" << i);
        path->SetSize(size);
        path->SetRead(true);
    }
    TStringStream out;
    range.Save(&out);
    return out.Str();
}

NS3::TSource CsvSource(const TString& columnType = "String") {
    NS3::TSource source;
    source.SetUrl("http://fake/");
    source.SetFormat("csv_with_names");
    source.SetRowType(TStringBuilder() << R"(["StructType";[["a";["DataType";")" << columnType << R"("]]]])");
    return source;
}

NS3::TSource ParquetSource() {
    NS3::TSource source;
    source.SetUrl("http://fake/");
    source.SetFormat("parquet");
    source.SetRowType(R"(["StructType";[["ts";["DataType";"Timestamp"]]]])");
    return source;
}

// Parquet file with one Timestamp column "ts" = i seconds and one row per row group.
TString MakeParquet(int rows) {
    arrow::TimestampBuilder builder(arrow::timestamp(arrow::TimeUnit::MICRO), arrow::default_memory_pool());
    for (int i = 0; i < rows; ++i) {
        UNIT_ASSERT(builder.Append(i * 1000000LL).ok());
    }
    std::shared_ptr<arrow::Array> array;
    UNIT_ASSERT(builder.Finish(&array).ok());
    auto table = arrow::Table::Make(arrow::schema({arrow::field("ts", arrow::timestamp(arrow::TimeUnit::MICRO), false)}), {array});
    auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
    UNIT_ASSERT(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), sink, /* chunk_size */ 1).ok());
    auto buffer = sink->Finish().ValueOrDie();
    return TString(reinterpret_cast<const char*>(buffer->data()), buffer->size());
}

TString Gzip(const TString& data) {
    TString result;
    TStringOutput out(result);
    {
        TZLibCompress compress(&out, ZLib::GZip);
        compress.Write(data);
        compress.Finish();
    }
    return result;
}

TString S3Error(const TString& code, const TString& message) {
    return TStringBuilder() << "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>" << code << "</Code><Message>" << message << "</Message></Error>";
}

struct TSourceError {
    NDqProto::StatusIds::StatusCode Code;
    TIssues Issues;
};

// Stands for the compute actor as the recipient of source notifications: records errors with their status codes.
class TSourceEventsCollector : public TActor<TSourceEventsCollector> {
public:
    TSourceEventsCollector(std::vector<TSourceError>& errors, std::mutex& mutex)
        : TActor(&TSourceEventsCollector::StateFunc)
        , Errors(errors)
        , Mutex(mutex)
    {}

    STRICT_STFUNC(StateFunc,
        hFunc(IDqComputeActorAsyncInput::TEvAsyncInputError, Handle);
        IgnoreFunc(IDqComputeActorAsyncInput::TEvNewAsyncInputDataArrived);
    )

    void Handle(IDqComputeActorAsyncInput::TEvAsyncInputError::TPtr& ev) {
        std::lock_guard lock(Mutex);
        Errors.push_back({ev->Get()->FatalCode, ev->Get()->Issues});
    }

private:
    std::vector<TSourceError>& Errors;
    std::mutex& Mutex;
};

// Fake compute actor. By default the runtime runs in simulated time (no sleeps, deterministic),
// scheduled download retries fire when Settle() advances the time.
struct TSimulatedCA {
    using TError = TSourceError;

    const bool RealThreads;
    const TActorId FakeActorId{0, "FakeActor"};
    std::shared_ptr<TAsyncInputPromises> InputPromises = std::make_shared<TAsyncInputPromises>();
    std::shared_ptr<TAsyncOutputPromises> OutputPromises = std::make_shared<TAsyncOutputPromises>();
    std::vector<std::function<void(TAutoPtr<IEventHandle>&)>> Observers;
    std::vector<TError> Errors; // retriable (UNSPECIFIED) and fatal errors reported by the source, guarded by ErrorsMutex
    mutable std::mutex ErrorsMutex;
    TActorId EventsCollectorId; // passed to the source as its compute actor
    bool Terminated = false;
    ui64 Rows = 0; // items returned by the source: ClickHouse blocks or arrow batches (the tests use one row per block/batch)
    bool Finished = false;
    // Stop worker threads before destroying the state referenced by actors and observers.
    TTestActorRuntimeBase Runtime;

    explicit TSimulatedCA(bool realThreads = false)
        : RealThreads(realThreads)
        , Runtime(1, realThreads)
    {
        Runtime.AddLocalService(FakeActorId, TActorSetupCmd(new TFakeActor(InputPromises, OutputPromises), TMailboxType::Simple, 0));
        Runtime.Initialize();
        Runtime.GetLogSettings(0)->Append(NKikimrServices::EServiceKikimr_MIN, NKikimrServices::EServiceKikimr_MAX, NKikimrServices::EServiceKikimr_Name);
        if (GetEnv("S3_UT_DEBUG")) {
            Runtime.SetLogPriority(NKikimrServices::KQP_COMPUTE, NLog::PRI_TRACE);
        }
        Runtime.SetObserverFunc([this](TAutoPtr<IEventHandle>& ev) {
            for (const auto& observer : Observers) {
                observer(ev);
            }
            return TTestActorRuntimeBase::EEventAction::PROCESS;
        });
        // The default filter drops every scheduled event; keep download retries so that they fire in simulated time
        // (other scheduled events, e.g. periodic resume attempts of throttled work, stay dropped).
        Runtime.SetScheduledEventFilter([](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev, TDuration, TInstant&) {
            return ev->GetTypeRewrite() != TEvS3Provider::TEvRetryEventFunc::EventType;
        });
        EventsCollectorId = Runtime.Register(new TSourceEventsCollector(Errors, ErrorsMutex));
        if (!NDB::FormatFactory::instance().getAllFormats().contains("CSVWithNames")) {
            NDB::registerFormats();
        }
    }

    ~TSimulatedCA() {
        // Pass the source away while the runtime is alive, also when an assertion failed mid-test.
        try {
            Terminate();
        } catch (...) {
        }
    }

    void Execute(TCallback callback, TDuration timeout = TDuration::Seconds(10)) {
        auto exception = std::make_shared<std::exception_ptr>();
        auto promise = NThreading::NewPromise();
        // The queued event borrows its result slot. Keep it alive even if the caller times out.
        auto ownedCallback = [callback = std::move(callback), exception](TFakeActor& actor) {
            try {
                callback(actor);
            } catch (...) {
                *exception = std::current_exception();
            }
        };
        Runtime.Send(new IEventHandle(FakeActorId, Runtime.AllocateEdgeActor(), new TEvPrivate::TEvExecute(promise, std::move(ownedCallback), *exception)));
        if (RealThreads) {
            promise.GetFuture().Wait(timeout);
        } else {
            TDispatchOptions options;
            options.CustomFinalCondition = [&] { return promise.HasValue(); };
            Runtime.DispatchEvents(options, timeout);
        }
        if (!promise.HasValue()) {
            ythrow yexception() << "fake compute actor did not run the callback";
        }
        if (*exception) {
            std::rethrow_exception(*exception);
        }
    }

    // Advances simulated time and processes everything that becomes ready
    // (with real threads: lets the actors run for up to 2 s of wall time).
    void Settle(TDuration delta = TDuration::MilliSeconds(200)) {
        if (RealThreads) {
            Sleep(Min(delta, TDuration::Seconds(2)));
            return;
        }
        Runtime.AdvanceCurrentTime(delta);
        Runtime.SetDispatchTimeout(TDuration::MilliSeconds(300));
        TDispatchOptions options;
        options.CustomFinalCondition = [] { return false; };
        try {
            Runtime.DispatchEvents(options);
        } catch (const TEmptyEventQueueException&) {
            // the queue is drained
        }
    }

    void CreateReader(std::shared_ptr<TFakeS3Gateway> gateway, NS3::TSource source, ui32 files, ui64 fileSize,
                      IHTTPGateway::TRetryPolicy::TPtr retryPolicy = GetHTTPDefaultRetryPolicy(),
                      TS3ReadActorFactoryConfig config = {}) {
        Execute([this, gateway = std::move(gateway), source = std::move(source), files, fileSize,
                 retryPolicy = std::move(retryPolicy), config](TFakeActor& actor) mutable {
            auto [input, inputActor] = CreateS3ReadActor(actor.TypeEnv, actor.HolderFactory,
                std::shared_ptr<TScopedAlloc>(&actor.Alloc, [](TScopedAlloc*) {}),
                gateway, std::move(source), 0, TCollectStatsLevel::None, TTxId{}, THashMap<TString, TString>{}, THashMap<TString, TString>{},
                TVector<TString>{EncodeRanges(files, fileSize)}, EventsCollectorId, CreateStructuredTokenCredentialsFactory(),
                retryPolicy, config, nullptr, nullptr,
                std::make_shared<TGuaranteeQuotaManager>(1_GB, 1_GB), false, nullptr);
            actor.InitAsyncInput(input, inputActor);
        });
    }

    void Read() {
        Execute([&](TFakeActor& actor) {
            TMaybe<TInstant> watermark;
            TUnboxedValueBatch buffer;
            bool finished = false;
            actor.DqAsyncInput->GetAsyncInputData(buffer, watermark, finished, 1_MB);
            Rows += buffer.RowCount();
            Finished = finished;
        });
    }

    void Terminate() {
        if (!Terminated) {
            Terminated = true;
            Execute([](TFakeActor& actor) { actor.Terminate(); });
        }
    }

    std::vector<TError> AllErrors() const {
        std::lock_guard lock(ErrorsMutex);
        return Errors;
    }

    std::vector<TError> FatalErrors() const {
        std::lock_guard lock(ErrorsMutex);
        std::vector<TError> result;
        for (const auto& error : Errors) {
            if (error.Code != NDqProto::StatusIds::UNSPECIFIED) {
                result.push_back(error);
            }
        }
        return result;
    }

    // Reads until the source reports "finished" or a fatal error, within a bounded amount of simulated time.
    void ReadToEnd(TDuration step = TDuration::MilliSeconds(200), ui32 maxSteps = 100) {
        for (ui32 i = 0; i < maxSteps && !Finished && FatalErrors().empty(); ++i) {
            Settle(step);
            Read();
        }
    }

    TString Describe() const {
        TStringBuilder result;
        result << "rows=" << Rows << " finished=" << Finished << " errors=[";
        for (const auto& error : AllErrors()) {
            result << "{" << NDqProto::StatusIds::StatusCode_Name(error.Code) << ": " << error.Issues.ToOneLineString() << "}";
        }
        return result << "]";
    }
};

} // anonymous namespace

Y_UNIT_TEST_SUITE(TS3ReadCoro) {
    Y_UNIT_TEST(ExecuteCallbackOutlivesTimeout) {
        auto entered = NThreading::NewPromise();
        auto release = NThreading::NewPromise();
        std::atomic<int> callbackCalls = 0;
        TSimulatedCA ca(/* realThreads */ true);
        Y_DEFER { release.TrySetValue(); };
        UNIT_ASSERT_EXCEPTION_CONTAINS(ca.Execute([&, entered, release](TFakeActor&) mutable {
            entered.SetValue();
            UNIT_ASSERT_C(release.GetFuture().Wait(TDuration::Seconds(10)), "late callback was not released");
            ++callbackCalls;
            ythrow yexception() << "late callback failure";
        }, TDuration::MilliSeconds(20)), yexception, "fake compute actor did not run the callback");
        UNIT_ASSERT_C(entered.GetFuture().Wait(TDuration::Seconds(10)), "timed-out callback never entered");
        release.SetValue();
        ca.Execute([](TFakeActor&) {}); // same-mailbox barrier: the late exception has been handled
        UNIT_ASSERT_VALUES_EQUAL(callbackCalls.load(), 1);
    }

    Y_UNIT_TEST(CsvReadFinishes) {
        TSimulatedCA ca;
        auto gateway = std::make_shared<TFakeS3Gateway>();
        const TString data = "a\nx\n";
        gateway->ServeStream(data);
        ca.CreateReader(gateway, CsvSource(), 3, data.size());
        ca.ReadToEnd();
        UNIT_ASSERT_C(ca.Finished && ca.Rows == 3 && ca.AllErrors().empty(), ca.Describe());
        UNIT_ASSERT_VALUES_EQUAL(gateway->StreamCalls.load(), 3);
    }

    Y_UNIT_TEST(ParquetReadWithoutPredicate) {
        for (ui64 parallelRowGroups : {0, 1, 2, 5}) {
            TSimulatedCA ca;
            auto gateway = std::make_shared<TFakeS3Gateway>();
            gateway->Body = MakeParquet(10);
            auto source = ParquetSource();
            source.SetParallelRowGroupCount(parallelRowGroups);
            ca.CreateReader(gateway, std::move(source), 1, gateway->Body.size());
            ca.ReadToEnd();
            UNIT_ASSERT_C(ca.Finished && ca.Rows == 10 && ca.AllErrors().empty(), "parallelRowGroups=" << parallelRowGroups << " " << ca.Describe());
        }
    }

    // A retriable HTTP error that carries a body must not poison the next attempt.
    Y_UNIT_TEST(RetryAfterErrorBodySucceeds) {
        struct TCase {
            long HttpCode;
            TString Body;
            int Failures;
        };
        const TCase cases[] = {
            {503, S3Error("SlowDown", "Please reduce your request rate."), 1},
            {500, S3Error("InternalError", "We encountered an internal error."), 2},
        };
        for (const auto& c : cases) {
            TSimulatedCA ca;
            auto gateway = std::make_shared<TFakeS3Gateway>();
            const TString data = "a\nx\n";
            gateway->StreamScript = [&, gateway = gateway.get()](const TString&, int attempt, auto& onStart, auto& onData, auto& onFinish) {
                if (attempt <= c.Failures) {
                    onStart(CURLE_OK, c.HttpCode);
                    onData(gateway->Content(c.Body));
                } else {
                    onStart(CURLE_OK, 206);
                    onData(gateway->Content(data));
                }
                onFinish(CURLE_OK, TIssues{});
            };
            ca.CreateReader(gateway, CsvSource(), 1, data.size());
            ca.ReadToEnd(TDuration::MilliSeconds(100));
            UNIT_ASSERT_C(ca.Finished && ca.Rows == 1 && ca.FatalErrors().empty(), "http=" << c.HttpCode << " downloads=" << gateway->StreamCalls.load() << " " << ca.Describe());
            UNIT_ASSERT_VALUES_EQUAL(gateway->StreamCalls.load(), c.Failures + 1);
            UNIT_ASSERT_VALUES_EQUAL(ca.AllErrors().size(), size_t(c.Failures)); // transient issues only
        }
    }

    // Issues of a retried attempt are reported as retriable when it fails; a fatal error of a later
    // attempt (here: a parse error) must not repeat them as its cause.
    Y_UNIT_TEST(FatalErrorAfterRetryHasNoIssuesOfEarlierAttempt) {
        TSimulatedCA ca;
        auto gateway = std::make_shared<TFakeS3Gateway>();
        TStringBuilder csv;
        csv << "a\nnot-a-number\n";
        for (ui32 i = 0; i < 1000; ++i) {
            csv << i << "\n";
        }
        const TString data = csv;
        gateway->StreamScript = [&, gateway = gateway.get()](const TString&, int attempt, auto& onStart, auto& onData, auto& onFinish) {
            if (attempt == 1) {
                onStart(CURLE_OK, 503);
                onData(gateway->Content(S3Error("SlowDown", "Please reduce your request rate.")));
                onFinish(CURLE_OK, TIssues{});
            } else {
                onStart(CURLE_OK, 206);
                onData(gateway->Content(data)); // the download is still in progress
            }
        };
        ca.CreateReader(gateway, CsvSource("Int32"), 1, data.size());
        ca.ReadToEnd(TDuration::MilliSeconds(100));
        UNIT_ASSERT_VALUES_EQUAL(gateway->StreamCalls.load(), 2);
        const auto errors = ca.AllErrors();
        UNIT_ASSERT_VALUES_EQUAL_C(errors.size(), 2, ca.Describe());
        UNIT_ASSERT_C(errors[0].Code == NDqProto::StatusIds::UNSPECIFIED && errors[0].Issues.ToOneLineString().Contains("SlowDown"), ca.Describe());
        UNIT_ASSERT_C(errors[1].Code == NDqProto::StatusIds::BAD_REQUEST, ca.Describe());
        const TString fatal = errors[1].Issues.ToOneLineString();
        UNIT_ASSERT_STRING_CONTAINS(fatal, "failed to parse data in column");
        UNIT_ASSERT_C(!fatal.Contains("SlowDown") && !fatal.Contains("503"), fatal);
    }

    // When retries are exhausted, the fatal error carries the issues of the last attempt only.
    Y_UNIT_TEST(RetryExhaustedReportsLastAttempt) {
        TSimulatedCA ca;
        auto gateway = std::make_shared<TFakeS3Gateway>();
        gateway->StreamScript = [&, gateway = gateway.get()](const TString&, int attempt, auto& onStart, auto& onData, auto& onFinish) {
            onStart(CURLE_OK, 503);
            onData(gateway->Content(S3Error("SlowDown", TStringBuilder() << "attempt-" << attempt)));
            onFinish(CURLE_OK, TIssues{});
        };
        auto retryPolicy = IHTTPGateway::TRetryPolicy::GetFixedIntervalPolicy(
            [](CURLcode, long) { return ERetryErrorClass::ShortRetry; }, TDuration::MilliSeconds(10), TDuration::MilliSeconds(10), 2);
        ca.CreateReader(gateway, CsvSource(), 1, 10, retryPolicy);
        ca.ReadToEnd(TDuration::MilliSeconds(100));
        UNIT_ASSERT_VALUES_EQUAL(gateway->StreamCalls.load(), 3);
        const auto fatal = ca.FatalErrors();
        UNIT_ASSERT_VALUES_EQUAL_C(fatal.size(), 1, ca.Describe());
        const TString text = fatal.front().Issues.ToOneLineString();
        const int last = gateway->StreamCalls.load();
        UNIT_ASSERT_STRING_CONTAINS(text, TStringBuilder() << "attempt-" << last);
        for (int i = 1; i < last; ++i) {
            UNIT_ASSERT_C(!text.Contains(TStringBuilder() << "attempt-" << i), text);
        }
    }

    // A final error without an S3 body has the same status with and without a preceding retry.
    Y_UNIT_TEST(RetryDoesNotInheritFatalStatus) {
        for (bool asyncDecoding : {false, true}) {
            for (bool curlFailure : {false, true}) {
                for (bool failFirst : {false, true}) {
                    TSimulatedCA ca;
                    auto gateway = std::make_shared<TFakeS3Gateway>();
                    gateway->StreamScript = [=, gateway = gateway.get()](const TString&, int attempt, auto& onStart, auto& onData, auto& onFinish) {
                        if (failFirst && attempt == 1) {
                            onStart(CURLE_OK, 503);
                            onData(gateway->Content("<html>Service temporarily unavailable</html>"));
                            onFinish(CURLE_OK, TIssues{});
                        } else if (curlFailure) {
                            onStart(CURLE_COULDNT_CONNECT, 0);
                            onFinish(CURLE_COULDNT_CONNECT, TIssues{TIssue("Cannot connect to object storage")});
                        } else {
                            onStart(CURLE_OK, 404);
                            onFinish(CURLE_OK, TIssues{});
                        }
                    };
                    auto source = CsvSource();
                    source.SetAsyncDecoding(asyncDecoding);
                    auto retryPolicy = IHTTPGateway::TRetryPolicy::GetFixedIntervalPolicy(
                        [](CURLcode, long code) { return code == 503 ? ERetryErrorClass::ShortRetry : ERetryErrorClass::NoRetry; },
                        TDuration::MilliSeconds(10), TDuration::MilliSeconds(10));
                    ca.CreateReader(gateway, std::move(source), 1, 100, retryPolicy);
                    ca.ReadToEnd(TDuration::MilliSeconds(100));
                    UNIT_ASSERT_VALUES_EQUAL(gateway->StreamCalls.load(), failFirst ? 2 : 1);
                    const auto fatal = ca.FatalErrors();
                    UNIT_ASSERT_VALUES_EQUAL_C(fatal.size(), 1, ca.Describe());
                    UNIT_ASSERT_C(fatal.front().Code == NDqProto::StatusIds::EXTERNAL_ERROR, ca.Describe());
                    UNIT_ASSERT_STRING_CONTAINS(fatal.front().Issues.ToOneLineString(), curlFailure ? "Cannot connect" : "404");
                }
            }
        }
    }

    // A gateway-wide failure can finish a queued request before its start callback runs.
    Y_UNIT_TEST(RetryFailureBeforeStartDoesNotInheritErrorState) {
        for (bool asyncDecoding : {false, true}) {
            TSimulatedCA ca;
            auto gateway = std::make_shared<TFakeS3Gateway>();
            gateway->StreamScript = [gateway = gateway.get()](const TString&, int attempt, auto& onStart, auto& onData, auto& onFinish) {
                if (attempt == 1) {
                    onStart(CURLE_OK, 503);
                    onData(gateway->Content("<html>Service temporarily unavailable</html>"));
                    onFinish(CURLE_OK, TIssues{});
                } else {
                    // TEasyCurlStream::Fail, unlike Done, does not call MaybeStart.
                    onFinish(CURLE_OK, TIssues{TIssue("multi handle failure")});
                }
            };
            auto source = CsvSource();
            source.SetAsyncDecoding(asyncDecoding);
            auto retryPolicy = IHTTPGateway::TRetryPolicy::GetFixedIntervalPolicy(
                [](CURLcode, long code) { return code == 503 ? ERetryErrorClass::ShortRetry : ERetryErrorClass::NoRetry; },
                TDuration::MilliSeconds(10), TDuration::MilliSeconds(10), 1);
            ca.CreateReader(gateway, std::move(source), 1, 100, retryPolicy);
            ca.ReadToEnd(TDuration::MilliSeconds(100));
            UNIT_ASSERT_VALUES_EQUAL(gateway->StreamCalls.load(), 2);
            const auto fatal = ca.FatalErrors();
            UNIT_ASSERT_VALUES_EQUAL_C(fatal.size(), 1, ca.Describe());
            UNIT_ASSERT_C(fatal.front().Code == NDqProto::StatusIds::EXTERNAL_ERROR, ca.Describe());
            const TString text = fatal.front().Issues.ToOneLineString();
            UNIT_ASSERT_STRING_CONTAINS(text, "multi handle failure");
            UNIT_ASSERT_C(!text.Contains("503") && !text.Contains("temporarily unavailable"), text);
        }
    }

    // Every attempt must execute in the actor that owns the stream and its cancellation hook,
    // even when decoding has its own mailbox.
    Y_UNIT_TEST(RetryRunsInReaderActor) {
        for (bool asyncDecoding : {false, true}) {
            std::vector<TActorId> attempts;
            TSimulatedCA ca;
            auto gateway = std::make_shared<TFakeS3Gateway>();
            const TString data = "a\nx\n";
            gateway->StreamScript = [&, gateway = gateway.get()](const TString&, int attempt, auto& onStart, auto& onData, auto& onFinish) {
                attempts.push_back(TActivationContext::AsActorContext().SelfID);
                if (attempt == 1) {
                    onStart(CURLE_OK, 503);
                    onData(gateway->Content(S3Error("SlowDown", "Retry later")));
                } else {
                    onStart(CURLE_OK, 206);
                    onData(gateway->Content(data));
                }
                onFinish(CURLE_OK, TIssues{});
            };
            auto source = CsvSource();
            source.SetAsyncDecoding(asyncDecoding);
            ca.CreateReader(gateway, std::move(source), 1, data.size());
            ca.ReadToEnd(TDuration::MilliSeconds(100));
            UNIT_ASSERT_C(ca.Finished && ca.Rows == 1 && ca.FatalErrors().empty(), ca.Describe());
            UNIT_ASSERT_VALUES_EQUAL(attempts.size(), 2);
            UNIT_ASSERT_C(attempts[0] == attempts[1], "asyncDecoding=" << asyncDecoding << ": retry ran in a different actor");
        }
    }

    // A reader waiting for downstream capacity must still process its retry timer.
    Y_UNIT_TEST(RetryWhileDownstreamPaused) {
        for (bool asyncDecoding : {false, true}) {
            auto gateway = std::make_shared<TFakeS3Gateway>();
            // Leave enough buffered data for the parser to publish a block before it needs EOF.
            const TString firstPart = TStringBuilder() << "a\n" << TString(128_KB, 'x') << "\ny\n";
            gateway->StreamScript = [gateway = gateway.get(), firstPart](const TString&, int attempt, auto& onStart, auto& onData, auto& onFinish) {
                onStart(CURLE_OK, 206);
                onData(gateway->Content(attempt == 1 ? firstPart : "z\n"));
                onFinish(attempt == 1 ? CURLE_RECV_ERROR : CURLE_OK,
                    attempt == 1 ? TIssues{TIssue("Connection interrupted")} : TIssues{});
            };
            bool blockArrived = false;
            TAutoPtr<IEventHandle> retry;
            TSimulatedCA ca;
            ca.Runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
                if (ev->GetTypeRewrite() == TEvS3Provider::TEvNextBlock::EventType) {
                    blockArrived = true;
                }
                if (ev->GetTypeRewrite() == TEvS3Provider::TEvRetryEventFunc::EventType) {
                    retry = ev.Release();
                    return TTestActorRuntimeBase::EEventAction::DROP;
                }
                return TTestActorRuntimeBase::EEventAction::PROCESS;
            });
            auto source = CsvSource();
            source.SetAsyncDecoding(asyncDecoding);
            TS3ReadActorFactoryConfig config;
            config.RowsInBatch = 1;
            config.DataInflight = 1; // the first unread row exhausts downstream capacity
            auto retryPolicy = IHTTPGateway::TRetryPolicy::GetFixedIntervalPolicy(
                [](CURLcode, long) { return ERetryErrorClass::ShortRetry; }, TDuration::Seconds(1), TDuration::Seconds(1));
            ca.CreateReader(gateway, std::move(source), 1, firstPart.size() + 2, retryPolicy, config);
            ca.Settle(TDuration::MilliSeconds(10));
            UNIT_ASSERT_C(blockArrived, "parser did not publish a block before retry: " << ca.Describe());
            UNIT_ASSERT(retry);
            UNIT_ASSERT_VALUES_EQUAL(gateway->StreamCalls.load(), 1);
            UNIT_ASSERT_VALUES_EQUAL(ca.Rows, 0); // no read/TEvContinue yet
            ca.Runtime.SetObserverFunc(&TTestActorRuntimeBase::DefaultObserverFunc);
            ca.Runtime.Send(retry.Release());
            ca.Settle(TDuration::Seconds(2));
            UNIT_ASSERT_VALUES_EQUAL_C(gateway->StreamCalls.load(), 2, "paused reader did not retry");
            ca.ReadToEnd();
            UNIT_ASSERT_C(ca.Finished && ca.Rows == 3 && ca.FatalErrors().empty(), ca.Describe());
        }
    }

    // Queue cancellation while the gateway is still returning a retry's hook. The stream owner
    // must process it after installing that hook, not cancel the previous attempt on another thread.
    Y_UNIT_TEST(AsyncRetryCancellationDuringHookPublication) {
        auto entered = NThreading::NewPromise<TActorId>();
        auto owner = NThreading::NewPromise<TActorId>();
        auto release = NThreading::NewPromise();
        auto cancelled = NThreading::NewPromise();
        auto previousCancelled = NThreading::NewPromise();
        std::atomic<int> firstCancelled = 0;
        std::atomic<int> retryCancelled = 0;
        auto gateway = std::make_shared<TFakeS3Gateway>();
        gateway->StreamScript = [&, gateway = gateway.get()](const TString&, int attempt, auto& onStart, auto& onData, auto& onFinish) {
            if (attempt == 1) {
                owner.SetValue(TActivationContext::AsActorContext().SelfID);
                onStart(CURLE_OK, 503);
                onData(gateway->Content(S3Error("SlowDown", "Retry later")));
                onFinish(CURLE_OK, TIssues{});
            } else {
                onStart(CURLE_OK, 206); // leave the stream open until cancelled
            }
        };
        gateway->StreamCancelHook = [&](const TString&, int attempt) -> IHTTPGateway::TCancelHook {
            if (attempt == 1) {
                return [&](TIssue) {
                    ++firstCancelled;
                    previousCancelled.TrySetValue();
                    cancelled.TrySetValue();
                };
            }
            entered.SetValue(TActivationContext::AsActorContext().SelfID);
            UNIT_ASSERT_C(release.GetFuture().Wait(TDuration::Seconds(10)), "retry hook was not released");
            // If the retry escaped to another actor, that mailbox can run cancellation while
            // Download is blocked. Force this reachable interleaving; never wait on our own mailbox.
            if (TActivationContext::AsActorContext().SelfID != owner.GetFuture().GetValue()) {
                UNIT_ASSERT_C(previousCancelled.GetFuture().Wait(TDuration::Seconds(10)), "stream owner did not process cancellation");
            }
            return [&](TIssue) {
                ++retryCancelled;
                cancelled.TrySetValue();
            };
        };
        // Construct last so that the runtime stops before destroying anything captured by the gateway.
        TSimulatedCA ca(/* realThreads */ true);
        auto source = CsvSource();
        source.SetAsyncDecoding(true);
        auto retryPolicy = IHTTPGateway::TRetryPolicy::GetFixedIntervalPolicy(
            [](CURLcode, long) { return ERetryErrorClass::ShortRetry; }, TDuration::MilliSeconds(10), TDuration::MilliSeconds(10));
        ca.CreateReader(gateway, std::move(source), 1, 100, retryPolicy);
        const bool retryEntered = entered.GetFuture().Wait(TDuration::Seconds(10));
        if (retryEntered) {
            ca.Runtime.Send(new IEventHandle(owner.GetFuture().GetValue(), {}, new TEvents::TEvPoison()));
        }
        release.TrySetValue(); // also unblock worker teardown if the precondition failed
        UNIT_ASSERT_C(retryEntered, "retry did not reach hook publication");
        UNIT_ASSERT_C(cancelled.GetFuture().Wait(TDuration::Seconds(10)), "stream was not cancelled");
        UNIT_ASSERT_VALUES_EQUAL(firstCancelled.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(retryCancelled.load(), 1);
        UNIT_ASSERT_C(owner.GetFuture().GetValue() == entered.GetFuture().GetValue(), "retry escaped the stream owner's mailbox");
        UNIT_ASSERT_VALUES_EQUAL(gateway->StreamCalls.load(), 2);
        ca.Terminate();
    }

    // A retry scheduled before the coroutine was cancelled (LIMIT reached)
    // must not start a new download.
    Y_UNIT_TEST(RetryStopsOnCancel) {
        for (bool asyncDecoding : {false, true}) {
            TAutoPtr<IEventHandle> retry;
            TActorId retryReader;
            std::function<void()> finishFile0;
            bool failedStreamCancelled = false;
            TSimulatedCA ca;
            auto gateway = std::make_shared<TFakeS3Gateway>();
            // A limit above 1000 rows keeps parallel downloads on (a smaller one forces ParallelDownloadCount = 1).
            constexpr ui64 rowsLimit = 1001;
            TStringBuilder data;
            data << "a\n";
            for (ui64 i = 0; i < rowsLimit; ++i) {
                data << "x\n";
            }
            gateway->StreamScript = [&, gateway = gateway.get()](const TString& url, int, auto& onStart, auto& onData, auto& onFinish) {
                if (url.EndsWith("file0")) {
                    onStart(CURLE_OK, 206);
                    // Do not reach LIMIT until the peer's retry timer is held.
                    finishFile0 = [gateway, data = TString(data), onData, onFinish] {
                        onData(gateway->Content(data));
                        onFinish(CURLE_OK, TIssues{});
                    };
                } else {
                    onStart(CURLE_OK, 503);
                    onData(gateway->Content(S3Error("SlowDown", "Please reduce your request rate.")));
                    onFinish(CURLE_OK, TIssues{});
                }
            };
            gateway->StreamCancelHook = [&](const TString& url, int) -> IHTTPGateway::TCancelHook {
                return [&, failedStream = url.EndsWith("file1")](TIssue) {
                    if (failedStream) {
                        failedStreamCancelled = true;
                    }
                };
            };
            ca.Runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
                if (ev->GetTypeRewrite() == TEvS3Provider::TEvRetryEventFunc::EventType) {
                    retryReader = ev->Sender;
                    retry = ev.Release();
                    return TTestActorRuntimeBase::EEventAction::DROP;
                }
                return TTestActorRuntimeBase::EEventAction::PROCESS;
            });
            auto source = CsvSource();
            source.SetAsyncDecoding(asyncDecoding);
            source.SetRowsLimitHint(rowsLimit);
            source.MutableSettings()->insert({"fileQueueBatchObjectCountLimit", "2"});
            source.MutableSettings()->insert({"fileQueueBatchSizeLimit", "1000000"});
            auto retryPolicy = IHTTPGateway::TRetryPolicy::GetFixedIntervalPolicy(
                [](CURLcode, long) { return ERetryErrorClass::LongRetry; }, TDuration::Seconds(1), TDuration::Seconds(1));
            ca.CreateReader(gateway, std::move(source), 2, data.size(), retryPolicy);
            ca.Settle();
            UNIT_ASSERT(retry);
            UNIT_ASSERT(finishFile0);
            UNIT_ASSERT(ca.Runtime.FindActor(retryReader));
            ca.Runtime.SetObserverFunc(&TTestActorRuntimeBase::DefaultObserverFunc);
            finishFile0();
            ca.ReadToEnd(TDuration::MilliSeconds(50));
            UNIT_ASSERT_C(ca.Finished && ca.FatalErrors().empty(), ca.Describe());
            UNIT_ASSERT(failedStreamCancelled);
            UNIT_ASSERT(!ca.Runtime.FindActor(retryReader));
            ca.Runtime.Send(retry.Release()); // deliver the queued timer only after cancellation/detachment
            ca.Settle(TDuration::Seconds(2));
            UNIT_ASSERT_VALUES_EQUAL(gateway->StreamCallsFor("http://fake/file0"), 1);
            UNIT_ASSERT_VALUES_EQUAL_C(gateway->StreamCallsFor("http://fake/file1"), 1, "retry started after the coroutine had been cancelled");
        }
    }

    // The async decompressor actor dies with its coroutine when parsing fails.
    Y_UNIT_TEST(DecompressorDiesOnParseError) {
        TSimulatedCA ca;
        auto gateway = std::make_shared<TFakeS3Gateway>();
        TStringBuilder csv;
        csv << "a\nnot-a-number\n";
        TReallyFastRng32 rng(42);
        for (ui32 i = 0; i < 100000; ++i) {
            csv << rng() << "\n";
        }
        const TString compressed = Gzip(csv);
        UNIT_ASSERT(compressed.size() > 64_KB);
        gateway->StreamScript = [&, gateway = gateway.get()](const TString&, int, auto& onStart, auto& onData, auto&) {
            onStart(CURLE_OK, 206);
            // The first part of the object arrived, the rest of the download is still in progress.
            onData(gateway->Content(compressed.substr(0, 64_KB)));
        };
        TActorId decompressor;
        ca.Observers.push_back([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvS3Provider::TEvDecompressDataRequest::EventType) {
                decompressor = ev->GetRecipientRewrite();
            }
        });
        auto source = CsvSource("Int32");
        source.SetAsyncDecompressing(true);
        source.MutableSettings()->insert({"compression", "gzip"});
        ca.CreateReader(gateway, std::move(source), 1, compressed.size());
        ca.ReadToEnd();
        const auto fatal = ca.FatalErrors();
        UNIT_ASSERT_C(!fatal.empty() && fatal.front().Code == NDqProto::StatusIds::BAD_REQUEST, ca.Describe());
        ca.Settle();
        UNIT_ASSERT(decompressor);
        UNIT_ASSERT_C(!ca.Runtime.FindActor(decompressor), "decompressor actor outlived its coroutine");
        ca.Terminate();
    }

    // The async decompressor actor dies with its coroutine when the coroutine stops on the LIMIT
    // while the download is still in progress.
    Y_UNIT_TEST(DecompressorDiesOnLimit) {
        TSimulatedCA ca;
        auto gateway = std::make_shared<TFakeS3Gateway>();
        TStringBuilder csv;
        csv << "a\n";
        TReallyFastRng32 rng(42);
        for (ui32 i = 0; i < 100000; ++i) {
            csv << (rng() % 1000000) << "\n";
        }
        const TString compressed = Gzip(csv);
        UNIT_ASSERT(compressed.size() > 64_KB);
        gateway->StreamScript = [&, gateway = gateway.get()](const TString&, int, auto& onStart, auto& onData, auto&) {
            onStart(CURLE_OK, 206);
            onData(gateway->Content(compressed.substr(0, 64_KB)));
        };
        TActorId decompressor;
        ca.Observers.push_back([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvS3Provider::TEvDecompressDataRequest::EventType) {
                decompressor = ev->GetRecipientRewrite();
            }
        });
        auto source = CsvSource("Int32");
        source.SetAsyncDecompressing(true);
        source.SetRowsLimitHint(10);
        source.MutableSettings()->insert({"compression", "gzip"});
        ca.CreateReader(gateway, std::move(source), 1, compressed.size());
        ca.ReadToEnd();
        ca.Settle();
        UNIT_ASSERT_C(ca.FatalErrors().empty() && ca.Rows > 0, ca.Describe());
        UNIT_ASSERT(decompressor);
        UNIT_ASSERT_C(!ca.Runtime.FindActor(decompressor), "decompressor actor outlived its coroutine");
        ca.Terminate();
    }
}

Y_UNIT_TEST_SUITE(TS3ReadActors) {
    // The file queue registered by the reader (runtime listing off) dies with it.
    Y_UNIT_TEST(LocalFileQueueDiesWithReader) {
        for (bool raw : {false, true}) {
            TSimulatedCA ca;
            auto gateway = std::make_shared<TFakeS3Gateway>();
            gateway->StreamScript = [](const TString&, int, auto&, auto&, auto&) {}; // slow objects
            gateway->RangeScript = [](const TString&, size_t, size_t, IHTTPGateway::TOnResult&) {};
            TActorId queue;
            ca.Observers.push_back([&](TAutoPtr<IEventHandle>& ev) {
                if (ev->GetTypeRewrite() == TEvS3Provider::TEvGetNextBatch::EventType) {
                    queue = ev->GetRecipientRewrite();
                }
            });
            NS3::TSource source;
            if (raw) {
                source.SetUrl("http://fake/");
            } else {
                source = CsvSource();
            }
            ca.CreateReader(gateway, std::move(source), 10, 100);
            ca.Settle();
            UNIT_ASSERT(queue);
            UNIT_ASSERT(ca.Runtime.FindActor(queue));
            ca.Terminate(); // LIMIT, cancel or an error elsewhere: the compute actor passes the source away
            ca.Settle(TDuration::Minutes(40));
            UNIT_ASSERT_C(!ca.Runtime.FindActor(queue), "raw=" << raw << ": local file queue actor outlived its reader");
        }
    }

}

} // namespace NYql::NDq
