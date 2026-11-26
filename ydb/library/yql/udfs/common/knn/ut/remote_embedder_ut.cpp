#include <ydb/library/yql/udfs/common/knn/knn-serializer-shared.h>
#include <ydb/library/yql/udfs/common/knn/remote_embedder_http.h>

#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/computation/mkql_value_builder.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/mkql_utils.h>
#include <yql/essentials/public/udf/udf_type_printer.h>

#include <ydb/public/sdk/cpp/tests/unit/client/oidc/helpers/test_server.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/testing/mock_server/server.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/time_provider/monotonic.h>

#include <util/system/event.h>

#include <mutex>

namespace {

namespace NUdf = NYql::NUdf;
using namespace NKikimr::NMiniKQL;

class TServer {
public:
    struct TRequest {
        TString FirstLine;
        TString Authorization;
        TString ContentType;
        TString Host;
        NJson::TJsonValue Body;
    };

    struct TReply {
        HttpCodes Status;
        TString Body;
        bool OmitContentLength = false;
        TString RawResponse = {};
    };

    explicit TServer(TVector<TReply> replies = {}, bool ipv6 = false)
        : Port_(Ports_.GetPort())
        , Ipv6_(ipv6)
        , Replies_(std::move(replies))
        , Server_(Address(), [this] { return new TReplier(*this); })
    {
    }

    TString Url() const {
        return TStringBuilder() << "http://" << Authority() << "/v1/";
    }

    TString Authority() const {
        return TStringBuilder() << (Ipv6_ ? "[::1]" : "localhost") << ':' << Port_;
    }

    TVector<TRequest> Requests() const {
        std::lock_guard lock(Mutex_);
        return Requests_;
    }

private:
    THttpServerOptions Address() const {
        THttpServerOptions options;
        options.AddBindAddress(Ipv6_ ? "::1" : "localhost", Port_);
        options.SetThreads(1);
        options.SetMaxConnections(300);
        options.SetMaxQueueSize(30);
        return options;
    }

    class TReplier: public TRequestReplier {
    public:
        explicit TReplier(TServer& owner)
            : Owner_(owner)
        {
        }

        bool DoReply(const TReplyParams& params) override {
            TRequest request;
            request.FirstLine = params.Input.FirstLine();
            if (const auto* header = params.Input.Headers().FindHeader("Authorization")) {
                request.Authorization = header->Value();
            }
            if (const auto* header = params.Input.Headers().FindHeader("Content-Type")) {
                request.ContentType = header->Value();
            }
            if (const auto* header = params.Input.Headers().FindHeader("Host")) {
                request.Host = header->Value();
            }
            NJson::ReadJsonTree(params.Input.ReadAll(), &request.Body);
            const auto& input = request.Body["input"];
            const size_t count = input.IsString() ? 1 : input.GetArray().size();
            NJson::TJsonValue body(NJson::JSON_MAP);
            body["data"].SetType(NJson::JSON_ARRAY);
            // Deliberately return the vectors in reverse order.
            for (size_t i = count; i > 0; --i) {
                NJson::TJsonValue item(NJson::JSON_MAP);
                item["index"] = i - 1;
                item["embedding"].SetType(NJson::JSON_ARRAY);
                item["embedding"].AppendValue((input.IsString() ? input : input[i - 1]).GetString().size());
                item["embedding"].AppendValue(-0.25);
                body["data"].AppendValue(std::move(item));
            }
            body["usage"]["prompt_tokens"] = ui64(1);
            TReply reply{HTTP_OK, NJson::WriteJson(body)};
            {
                std::lock_guard lock(Owner_.Mutex_);
                const auto index = Owner_.Requests_.size();
                Owner_.Requests_.push_back(std::move(request));
                if (index < Owner_.Replies_.size()) {
                    reply = Owner_.Replies_[index];
                }
            }
            if (reply.RawResponse) {
                params.Output.EnableBodyEncoding(false);
                params.Output.Write(reply.RawResponse);
                return true;
            }
            if (reply.OmitContentLength) {
                params.Output << "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n" << reply.Body;
                return true;
            }
            THttpResponse response(reply.Status);
            response.SetContent(reply.Body);
            response.OutTo(params.Output);
            return true;
        }

    private:
        TServer& Owner_;
    };

    TPortManager Ports_;
    ui16 Port_;
    bool Ipv6_;
    mutable std::mutex Mutex_;
    TVector<TRequest> Requests_;
    TVector<TReply> Replies_;
    NMock::TMockServer Server_;
};

struct TSetup {
    TScopedAlloc Alloc{__LOCATION__};
    TTypeEnvironment Env{Alloc};
    TMemoryUsageInfo Memory{"RemoteEmbedderTest"};
    THolderFactory Holders{Alloc.Ref(), Memory};
    TDefaultValueBuilder Builder{Holders, NUdf::EValidatePolicy::Exception};
    TIntrusivePtr<IMutableFunctionRegistry> Registry = CreateFunctionRegistry({})->Clone();
    NYql::TRuntimeSettings::TConstPtr RuntimeSettings = NYql::MakeRuntimeSettings();
    std::unique_ptr<NUdf::ISecureParamsProvider> Secrets = MakeSimpleSecureParamsProvider({{"secret", "test-token"}});
    TOnlyThrowingBindTerminator Terminator;
    const NUdf::TSourcePosition* CalleePosition = nullptr;

    TSetup() {
        FillStaticModules(*Registry);
        Builder.SetSecureParamsProvider(Secrets.get());
        Builder.SetCalleePositionHolder(CalleePosition);
    }

    TFunctionTypeInfo Resolve(TType* userType, bool typesOnly = false) {
        TFunctionTypeInfo info;
        auto status = Registry->FindFunctionTypeInfo(NYql::UnknownLangVersion, *RuntimeSettings, Env, new TTypeInfoHelper(), nullptr,
            "Knn.ApplyRemoteEmbedder", userType, {},
            typesOnly ? NUdf::IUdfModule::TFlags::TypesOnly : 0, NUdf::TSourcePosition(), Secrets.get(), nullptr, &info);
        UNIT_ASSERT_C(status.IsOk(), status.GetError());
        return info;
    }

    TType* UserType(bool stream, bool valid = true, bool listHint = false) {
        TFunctionTypeInfoBuilder types(NYql::UnknownLangVersion, *RuntimeSettings, Env, new TTypeInfoHelper(), "", nullptr, NUdf::TSourcePosition());
        const NUdf::TType* first = types.SimpleType<char*>();
        if (stream) {
            auto row = types.Struct(2)->AddField<ui64>("PassThrough", nullptr).AddField<char*>(valid ? NUdf::TStringRef::Of("Text") : NUdf::TStringRef::Of("Other"), nullptr).Build();
            first = listHint ? types.List()->Item(row).Build() : types.Stream()->Item(row).Build();
        }
        auto args = types.Tuple(4)->Add(first).Add<char*>().Add<char*>().Add<char*>().Build();
        return static_cast<TType*>(types.Tuple(3)->Add(args).Add(types.Struct(0)->Build()).Add(types.Tuple(0)->Build()).Build());
    }

    NUdf::TUnboxedValue Function(bool stream) {
        auto info = Resolve(UserType(stream));
        return NUdf::TUnboxedValuePod(info.Implementation.Release());
    }

    NUdf::TUnboxedValue String(TStringBuf text) {
        return Builder.NewString(text);
    }

    NUdf::TUnboxedValue Scalar(const TServer& server, ui32 retries = 0, TStringBuf text = "Дом") {
        const NUdf::TUnboxedValue args[] = {String(text), String("model"), String(server.Url()), String("secret"), NUdf::TUnboxedValuePod(retries)};
        return Function(false).Run(&Builder, args);
    }
};

class TInputStream: public NUdf::TBoxedValue {
public:
    TInputStream(TSetup& setup, ui32 rows, bool yield = false, TString text = "text")
        : Setup_(setup)
        , Rows_(rows)
        , Yield_(yield)
        , Text_(std::move(text))
    {
    }

private:
    NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
        if (Yield_ && !Yielded_) {
            Yielded_ = true;
            result = NUdf::TUnboxedValuePod(ui64(42));
            return NUdf::EFetchStatus::Yield;
        }
        if (Index_ == Rows_) {
            return NUdf::EFetchStatus::Finish;
        }
        Yielded_ = false;
        NUdf::TUnboxedValue* fields;
        result = Setup_.Builder.NewArray(2, fields);
        fields[0] = NUdf::TUnboxedValuePod(ui64(++Index_));
        fields[1] = Setup_.String(Text_ + TString(Index_, '!'));
        return NUdf::EFetchStatus::Ok;
    }

    TSetup& Setup_;
    ui32 Rows_;
    bool Yield_;
    TString Text_;
    ui32 Index_ = 0;
    bool Yielded_ = false;
};

TVector<NUdf::TUnboxedValue> RunStream(TSetup& setup, const TServer& server, ui32 rows, ui32 batchSize, bool yield = false, ui32 rate = 0, ui32 retries = 0, TString text = "text") {
    const NUdf::TUnboxedValue args[] = {
        NUdf::TUnboxedValuePod(new TInputStream(setup, rows, yield, std::move(text))),
        setup.String("model"), setup.String(server.Url()), setup.String("secret"),
        NUdf::TUnboxedValuePod(batchSize), NUdf::TUnboxedValuePod(rate), NUdf::TUnboxedValuePod(retries)};
    auto stream = setup.Function(true).Run(&setup.Builder, args);
    TVector<NUdf::TUnboxedValue> result;
    for (NUdf::TUnboxedValue row; ; ) {
        const auto status = stream.Fetch(row);
        if (status == NUdf::EFetchStatus::Finish) {
            UNIT_ASSERT_EQUAL(stream.Fetch(row), NUdf::EFetchStatus::Finish);
            return result;
        }
        if (status == NUdf::EFetchStatus::Ok) {
            result.push_back(std::move(row));
        }
    }
}

void CheckEmbedding(const NUdf::TUnboxedValue& row, float expected) {
    const auto embedding = row.GetElement(0);
    NKnnVectorSerialization::TDeserializer<float> decoder(embedding.AsStringRef());
    UNIT_ASSERT_VALUES_EQUAL(decoder.GetElementCount(), 2);
    TVector<float> values;
    decoder.DoDeserialize([&](float value) { values.push_back(value); });
    UNIT_ASSERT_VALUES_EQUAL(values[0], expected);
    UNIT_ASSERT_VALUES_EQUAL(values[1], -0.25f);
}

ui32 EmbeddingSize(const NUdf::TUnboxedValue& row) {
    const auto embedding = row.GetElement(0);
    return embedding.AsStringRef().Size();
}

} // namespace

Y_UNIT_TEST_SUITE(TRemoteEmbedder) {
    Y_UNIT_TEST(TransportSendsBearerTokenOverTls) {
        TOidcTestServer server;
        const TString url(server.Issuer());
        server.Enqueue(R"({"data":[{"index":0,"embedding":[1]}]})", HTTP_OK);

        TSetup setup;
        const NUdf::TUnboxedValue args[] = {
            setup.String("text"), setup.String("model"), setup.String(url), setup.String("secret"),
            NUdf::TUnboxedValuePod(ui32(0)),
        };
        const auto result = setup.Function(false).Run(&setup.Builder, args);
        UNIT_ASSERT_VALUES_EQUAL(result.GetElement(1).Get<ui32>(), 200);
        const auto requests = server.Requests();
        UNIT_ASSERT_VALUES_EQUAL(requests.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(requests[0].Path, "/realm/embeddings");
        UNIT_ASSERT_VALUES_EQUAL(requests[0].Authorization, "Bearer test-token");
    }

    Y_UNIT_TEST(HttpAndTimersRunIndependentlyOfTheCaller) {
        class TSlowReply : public TRequestReplier {
        public:
            TSlowReply(TManualEvent& entered, TManualEvent& release)
                : Entered_(entered)
                , Release_(release)
            {}

            bool DoReply(const TReplyParams& params) override {
                Entered_.Signal();
                Release_.WaitT(TDuration::Seconds(5));
                THttpResponse(HTTP_NO_CONTENT).OutTo(params.Output);
                return true;
            }

        private:
            TManualEvent& Entered_;
            TManualEvent& Release_;
        };

        TManualEvent entered;
        TManualEvent release;
        TPortManager ports;
        const auto port = ports.GetPort();
        NMock::TMockServer server(port, [&] { return new TSlowReply(entered, release); });
        const TString post = "POST /embeddings HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        auto request = NKnn::PostRemoteEmbedderRequest("localhost", port, false, post);
        UNIT_ASSERT(entered.WaitT(TDuration::Seconds(5)));
        UNIT_ASSERT(!request.HasValue());
        const auto timer = NKnn::WaitBeforeRemoteEmbedderRequest(TDuration::MilliSeconds(10));
        UNIT_ASSERT(timer.Wait(TDuration::Seconds(2)));
        timer.GetValueSync();
        UNIT_ASSERT(!request.HasValue());
        release.Signal();
        UNIT_ASSERT_VALUES_EQUAL(request.ExtractValueSync().Status, HTTP_NO_CONTENT);

        entered.Reset();
        release.Reset();
        auto timedRequest = NKnn::PostRemoteEmbedderRequest("localhost", port, false, post, TDuration::Seconds(2));
        const bool reachedServer = entered.WaitT(TDuration::Seconds(3));
        const bool completed = timedRequest.Wait(TDuration::Seconds(3));
        release.Signal();
        UNIT_ASSERT(reachedServer);
        UNIT_ASSERT(completed);
        UNIT_ASSERT_VALUES_EQUAL(timedRequest.ExtractValue().Status, 0);
    }

    Y_UNIT_TEST(ScalarRequestAndBinaryEmbedding) {
        TServer server;
        TSetup setup;
        const TString text = "Дом \"quoted\"\nline";
        const auto result = setup.Scalar(server, 0, text);
        UNIT_ASSERT_VALUES_EQUAL(result.GetElement(1).Get<ui32>(), 200);
        CheckEmbedding(result, text.size());
        const auto requests = server.Requests();
        UNIT_ASSERT_VALUES_EQUAL(requests.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(requests[0].FirstLine, "POST /v1/embeddings HTTP/1.1");
        UNIT_ASSERT_VALUES_EQUAL(requests[0].Authorization, "Bearer test-token");
        UNIT_ASSERT_VALUES_EQUAL(requests[0].ContentType, "application/json");
        UNIT_ASSERT_VALUES_EQUAL(requests[0].Host, server.Authority());
        UNIT_ASSERT_VALUES_EQUAL(requests[0].Body["model"].GetString(), "model");
        UNIT_ASSERT_VALUES_EQUAL(requests[0].Body["encoding_format"].GetString(), "float");
        UNIT_ASSERT_VALUES_EQUAL(requests[0].Body["input"].GetString(), text);
    }

    Y_UNIT_TEST(Ipv6AuthorityContainsPort) {
        TServer server({}, /*ipv6=*/true);
        TSetup setup;
        UNIT_ASSERT_VALUES_EQUAL(setup.Scalar(server).GetElement(1).Get<ui32>(), 200);
        const auto requests = server.Requests();
        UNIT_ASSERT_VALUES_EQUAL(requests.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(requests[0].Host, server.Authority());
    }

    Y_UNIT_TEST(BatchesPreservePassThroughAcrossYields) {
        TServer server;
        TSetup setup;
        const auto rows = RunStream(setup, server, 5, 2, true);
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 5);
        for (size_t i = 0; i < rows.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(rows[i].GetElement(1).Get<ui64>(), i + 1);
            UNIT_ASSERT_VALUES_EQUAL(rows[i].GetElement(2).Get<ui32>(), 200);
            CheckEmbedding(rows[i], 5 + i);
        }
        const auto requests = server.Requests();
        UNIT_ASSERT_VALUES_EQUAL(requests.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(requests[0].Body["input"].GetArray().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(requests[1].Body["input"].GetArray().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(requests[2].Body["input"].GetArray().size(), 1);
    }

    Y_UNIT_TEST(EmptyStreamMakesNoRequest) {
        TServer server;
        TSetup setup;
        UNIT_ASSERT(RunStream(setup, server, 0, 2).empty());
        UNIT_ASSERT(server.Requests().empty());
    }

    Y_UNIT_TEST(BatchesFlushAtByteLimit) {
        TServer server;
        TSetup setup;
        const auto rows = RunStream(setup, server, 3, 10, true, 0, 0, TString(600 * 1024, 'x'));
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 3);
        const auto requests = server.Requests();
        UNIT_ASSERT_VALUES_EQUAL(requests.size(), 3);
        for (size_t i = 0; i < rows.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(rows[i].GetElement(1).Get<ui64>(), i + 1);
            UNIT_ASSERT_VALUES_EQUAL(requests[i].Body["input"].GetArray().size(), 1);
        }
    }

    Y_UNIT_TEST(RejectOversizedInputBeforeRequest) {
        TServer server;
        TSetup setup;
        const TString text(1024 * 1024 + 1, 'x');
        UNIT_ASSERT_EXCEPTION_CONTAINS(setup.Scalar(server, 0, text), yexception, "request byte limit");
        UNIT_ASSERT_EXCEPTION_CONTAINS(RunStream(setup, server, 1, 10, false, 0, 0, text), yexception, "batch byte limit");
        UNIT_ASSERT(server.Requests().empty());
    }

    Y_UNIT_TEST(DefaultOptions) {
        TServer server;
        TSetup setup;
        const NUdf::TUnboxedValue args[] = {
            NUdf::TUnboxedValuePod(new TInputStream(setup, 33)),
            setup.String("model"), setup.String(server.Url()), setup.String("secret"), {}, {}, {}};
        auto stream = setup.Function(true).Run(&setup.Builder, args);
        size_t count = 0;
        for (NUdf::TUnboxedValue row; stream.Fetch(row) == NUdf::EFetchStatus::Ok; ++count) {
            UNIT_ASSERT_VALUES_EQUAL(row.GetElement(2).Get<ui32>(), 200);
        }
        UNIT_ASSERT_VALUES_EQUAL(count, 33);
        const auto requests = server.Requests();
        UNIT_ASSERT_VALUES_EQUAL(requests.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(requests[0].Body["input"].GetArray().size(), 32);
        UNIT_ASSERT_VALUES_EQUAL(requests[1].Body["input"].GetArray().size(), 1);
        const NUdf::TUnboxedValue scalarArgs[] = {setup.String("text"), setup.String("model"), setup.String(server.Url()), setup.String("secret"), {}};
        UNIT_ASSERT_VALUES_EQUAL(setup.Function(false).Run(&setup.Builder, scalarArgs).GetElement(1).Get<ui32>(), 200);
    }

    Y_UNIT_TEST(InvalidConfigurationDoesNotSendRequests) {
        TServer server;
        TSetup setup;
        UNIT_ASSERT_EXCEPTION_CONTAINS(RunStream(setup, server, 1, 0), yexception, "BatchSize must be positive");
        const NUdf::TUnboxedValue args[] = {setup.String("text"), setup.String("model"), setup.String(server.Url()), setup.String("missing-secret"), {}};
        UNIT_ASSERT_EXCEPTION_CONTAINS(setup.Function(false).Run(&setup.Builder, args), yexception, "authentication secret is not available");
        UNIT_ASSERT(server.Requests().empty());
    }

    Y_UNIT_TEST(TransportFailureReturnsStatusZero) {
        TPortManager ports;
        const auto port = ports.GetPort();
        TSetup setup;
        const NUdf::TUnboxedValue args[] = {setup.String("text"), setup.String("model"),
            setup.String(TStringBuilder() << "http://localhost:" << port << "/v1"), setup.String("secret"), NUdf::TUnboxedValuePod(ui32(0))};
        const auto result = setup.Function(false).Run(&setup.Builder, args);
        UNIT_ASSERT_VALUES_EQUAL(result.GetElement(1).Get<ui32>(), 0);
        UNIT_ASSERT_VALUES_EQUAL(EmbeddingSize(result), 0);
    }

    Y_UNIT_TEST(RetryTransientHttpErrors) {
        TServer server({{HTTP_TOO_MANY_REQUESTS, "rate limit"}, {HTTP_SERVICE_UNAVAILABLE, "unavailable"}});
        TSetup setup;
        const auto result = setup.Scalar(server, 2);
        UNIT_ASSERT_VALUES_EQUAL(result.GetElement(1).Get<ui32>(), 200);
        UNIT_ASSERT_VALUES_EQUAL(server.Requests().size(), 3);
    }

    Y_UNIT_TEST(DoNotRetryPermanentErrors) {
        TServer server({{HTTP_UNAUTHORIZED, "unauthorized"}});
        TSetup setup;
        const auto result = setup.Scalar(server, 5);
        UNIT_ASSERT_VALUES_EQUAL(result.GetElement(1).Get<ui32>(), 401);
        UNIT_ASSERT_VALUES_EQUAL(EmbeddingSize(result), 0);
        UNIT_ASSERT_VALUES_EQUAL(server.Requests().size(), 1);
    }

    Y_UNIT_TEST(ExhaustedRetriesPreserveEveryRow) {
        TServer server({{HTTP_SERVICE_UNAVAILABLE, "failure"}, {HTTP_SERVICE_UNAVAILABLE, "failure"}});
        TSetup setup;
        const auto rows = RunStream(setup, server, 2, 2, false, 0, 1);
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 2);
        for (size_t i = 0; i < rows.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(rows[i].GetElement(1).Get<ui64>(), i + 1);
            UNIT_ASSERT_VALUES_EQUAL(rows[i].GetElement(2).Get<ui32>(), 503);
            UNIT_ASSERT_VALUES_EQUAL(EmbeddingSize(rows[i]), 0);
        }
        UNIT_ASSERT_VALUES_EQUAL(server.Requests().size(), 2);
    }

    Y_UNIT_TEST(RejectMalformedResponses) {
        const TVector<TString> bodies = {
            "not json", "{}", R"({"data":[]})",
            R"({"data":[{"embedding":[1]}]})",
            R"({"data":[{"index":-1,"embedding":[1]}]})",
            R"({"data":[{"index":1,"embedding":[1]}]})",
            R"({"data":[{"index":0,"embedding":[]}]})",
            R"({"data":[{"index":0,"embedding":["bad"]}]})",
            R"({"data":[{"index":0,"embedding":[1e100]}]})"};
        TSetup setup;
        for (const auto& body : bodies) {
            TServer server({{HTTP_OK, body}});
            const auto result = setup.Scalar(server);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetElement(1).Get<ui32>(), 502, body);
            UNIT_ASSERT_VALUES_EQUAL(EmbeddingSize(result), 0);
        }
    }

    Y_UNIT_TEST(RejectOversizedResponseWithoutRetry) {
        TSetup setup;
        const TString body(16 * 1024 * 1024 + 1, 'x');
        for (const bool omitContentLength : {false, true}) {
            TServer server({{HTTP_OK, body, omitContentLength}});
            const auto result = setup.Scalar(server, 1);
            UNIT_ASSERT_VALUES_EQUAL(result.GetElement(1).Get<ui32>(), 502);
            UNIT_ASSERT_VALUES_EQUAL(EmbeddingSize(result), 0);
            UNIT_ASSERT_VALUES_EQUAL(server.Requests().size(), 1);
        }
    }

    Y_UNIT_TEST(RejectUnsupportedContentEncodingWithoutRetry) {
        TServer server({{.Status = HTTP_OK, .Body = {}, .RawResponse =
            "HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}"}});
        TSetup setup;
        const auto result = setup.Scalar(server, 1);
        UNIT_ASSERT_VALUES_EQUAL(result.GetElement(1).Get<ui32>(), 502);
        UNIT_ASSERT_VALUES_EQUAL(server.Requests().size(), 1);
    }

    Y_UNIT_TEST(ReadsEmbeddingBeforeIndexAndSkipsUnknownFields) {
        TServer server({{HTTP_OK, R"({"model":"test","data":[{"embedding":[5,-0.25],"extra":[{},[1]],"index":0}]})"}});
        TSetup setup;
        const auto result = setup.Scalar(server);
        UNIT_ASSERT_VALUES_EQUAL(result.GetElement(1).Get<ui32>(), 200);
        CheckEmbedding(result, 5);
    }

    Y_UNIT_TEST(AcceptsChunkedResponseWithExtensionAndTrailer) {
        TServer server({{.Status = HTTP_OK, .Body = {}, .RawResponse =
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
            "2c;name=value\r\n"
            R"({"data":[{"index":0,"embedding":[5,-0.25]}]})"
            "\r\n0\r\nX-Checksum: example\r\n\r\n"}});
        TSetup setup;
        const auto result = setup.Scalar(server);
        UNIT_ASSERT_VALUES_EQUAL(result.GetElement(1).Get<ui32>(), 200);
        CheckEmbedding(result, 5);
        UNIT_ASSERT_VALUES_EQUAL(server.Requests().size(), 1);
    }

    Y_UNIT_TEST(RejectOversizedHttpMetadataWithoutRetry) {
        const TString chunkedHead = "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n";
        const TString json = R"({"data":[{"index":0,"embedding":[5,-0.25]}]})";
        const TString chunk = "2c\r\n" + json + "\r\n";
        const TString longLine(8 * 1024, 'x');
        TString headersAndTrailers;
        TString chunkFraming;
        for (size_t i = 0; i < 17; ++i) {
            headersAndTrailers += "X-Padding: " + TString(4 * 1024, 'x') + "\r\n";
        }
        const TString paddedExtension = "1;name=" + TString(4 * 1024, 'x') + "\r\n";
        for (size_t i = 0; i < 256; ++i) {
            chunkFraming += paddedExtension;
            chunkFraming += i < json.size() ? json[i] : ' ';
            chunkFraming += "\r\n";
        }

        const TVector<std::pair<TString, TString>> responses = {
            {"status line", "HTTP/1.1 503 " + longLine + "\r\nContent-Length: 0\r\n\r\n"},
            {"header line", "HTTP/1.1 503 Unavailable\r\nX-Padding: " + longLine + "\r\nContent-Length: 0\r\n\r\n"},
            {"header bytes", "HTTP/1.1 503 Unavailable\r\n" + headersAndTrailers + "Content-Length: 0\r\n\r\n"},
            {"trailer line", chunkedHead + chunk + "0\r\nX-Padding: " + longLine + "\r\n\r\n"},
            {"trailer bytes", chunkedHead + chunk + "0\r\n" + headersAndTrailers + "\r\n"},
            {"chunk extension", chunkedHead + "2c;name=" + longLine + "\r\n" + json + "\r\n0\r\n\r\n"},
            {"chunk framing bytes", chunkedHead + chunkFraming + "0\r\n\r\n"},
        };
        TSetup setup;
        for (const auto& [name, response] : responses) {
            TServer server({{.Status = HTTP_OK, .Body = {}, .RawResponse = response}});
            const auto result = setup.Scalar(server, 1);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetElement(1).Get<ui32>(), 502, name);
            UNIT_ASSERT_VALUES_EQUAL_C(EmbeddingSize(result), 0, name);
            UNIT_ASSERT_VALUES_EQUAL_C(server.Requests().size(), 1, name);
        }
    }

    Y_UNIT_TEST(RejectDeeplyNestedResponse) {
        const TString body = TStringBuilder()
            << R"({"data":[{"index":0,"embedding":[1]}],"extra":)"
            << TString(4096, '[') << '0' << TString(4096, ']') << '}';
        TServer server({{HTTP_OK, body}});
        TSetup setup;
        const auto result = setup.Scalar(server);
        UNIT_ASSERT_VALUES_EQUAL(result.GetElement(1).Get<ui32>(), 502);
        UNIT_ASSERT_VALUES_EQUAL(EmbeddingSize(result), 0);
    }

    Y_UNIT_TEST(RejectDuplicateIndicesAndInconsistentDimensions) {
        TSetup setup;
        for (const TString body : {
                 R"({"data":[{"index":0,"embedding":[1]},{"index":0,"embedding":[2]}]})",
                 R"({"data":[{"index":0,"embedding":[1]},{"index":1,"embedding":[2,3]}]})"}) {
            TServer server({{HTTP_OK, body}});
            const auto rows = RunStream(setup, server, 2, 2);
            for (const auto& row : rows) {
                UNIT_ASSERT_VALUES_EQUAL(row.GetElement(2).Get<ui32>(), 502);
            }
        }
    }

    Y_UNIT_TEST(RateLimitSpacesRequests) {
        TServer server;
        TSetup setup;
        const auto start = NMonotonic::TMonotonic::Now();
        UNIT_ASSERT_VALUES_EQUAL(RunStream(setup, server, 3, 1, false, 40).size(), 3);
        UNIT_ASSERT(NMonotonic::TMonotonic::Now() - start >= TDuration::MilliSeconds(200));
    }

    Y_UNIT_TEST(ResolveSignaturesAndRejectInvalidStream) {
        TSetup setup;
        auto scalar = setup.Resolve(setup.UserType(false), true);
        UNIT_ASSERT_VALUES_EQUAL(scalar.FunctionType->GetArgumentsCount(), 5);
        UNIT_ASSERT_VALUES_EQUAL(scalar.FunctionType->GetOptionalArgumentsCount(), 1);
        auto stream = setup.Resolve(setup.UserType(true), true);
        UNIT_ASSERT_VALUES_EQUAL(stream.FunctionType->GetArgumentsCount(), 7);
        UNIT_ASSERT_VALUES_EQUAL(stream.FunctionType->GetOptionalArgumentsCount(), 3);
        TStringBuilder out;
        NUdf::TTypePrinter(*NUdf::ITypeInfoHelper::TPtr(new TTypeInfoHelper()), stream.FunctionType->GetReturnType()).Out(out.Out);
        UNIT_ASSERT_VALUES_EQUAL(out, "Stream<Struct<'Embedding':String,'PassThrough':Uint64,'Status':Uint32>>");
        TFunctionTypeInfo invalid;
        const auto status = setup.Registry->FindFunctionTypeInfo(NYql::UnknownLangVersion, *setup.RuntimeSettings, setup.Env, new TTypeInfoHelper(), nullptr,
            "Knn.ApplyRemoteEmbedder", setup.UserType(true, false), {}, NUdf::IUdfModule::TFlags::TypesOnly, NUdf::TSourcePosition(), nullptr, nullptr, &invalid);
        UNIT_ASSERT(!status.IsOk());
        UNIT_ASSERT_STRING_CONTAINS(status.GetError(), "Text:String");
    }

    Y_UNIT_TEST(ProcessListHintDeclaresStreamArgument) {
        TSetup setup;
        const auto info = setup.Resolve(setup.UserType(true, true, true), true);
        UNIT_ASSERT_EQUAL(info.FunctionType->GetArgumentType(0)->GetKind(), TType::EKind::Stream);
        UNIT_ASSERT_EQUAL(info.FunctionType->GetReturnType()->GetKind(), TType::EKind::Stream);
    }
}
