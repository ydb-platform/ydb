#include <ydb/library/yql/udfs/common/knn/knn-serializer-shared.h>

#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/computation/mkql_value_builder.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/mkql_utils.h>
#include <yql/essentials/public/udf/udf_type_printer.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/testing/mock_server/server.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/time_provider/monotonic.h>

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
        NJson::TJsonValue Body;
    };

    struct TReply {
        HttpCodes Status;
        TString Body;
    };

    explicit TServer(TVector<TReply> replies = {})
        : Port_(Ports_.GetPort())
        , Replies_(std::move(replies))
        , Server_(Port_, [this] { return new TReplier(*this); })
    {
    }

    TString Url() const {
        return TStringBuilder() << "http://localhost:" << Port_ << "/v1/";
    }

    TVector<TRequest> Requests() const {
        std::lock_guard lock(Mutex_);
        return Requests_;
    }

private:
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
            NJson::ReadJsonTree(params.Input.ReadAll(), &request.Body);
            const auto& inputs = request.Body["input"].GetArray();
            NJson::TJsonValue body(NJson::JSON_MAP);
            body["data"].SetType(NJson::JSON_ARRAY);
            // Deliberately return the vectors in reverse order.
            for (size_t i = inputs.size(); i > 0; --i) {
                NJson::TJsonValue item(NJson::JSON_MAP);
                item["index"] = i - 1;
                item["embedding"].SetType(NJson::JSON_ARRAY);
                item["embedding"].AppendValue(inputs[i - 1].GetString().size());
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

    TFunctionTypeInfo Resolve(bool stream, TType* userType = nullptr, bool typesOnly = false) {
        TFunctionTypeInfo info;
        auto status = Registry->FindFunctionTypeInfo(NYql::UnknownLangVersion, *RuntimeSettings, Env, new TTypeInfoHelper(), nullptr,
            stream ? "RemoteEmbedder.ApplyRemoteEmbedderStream" : "RemoteEmbedder.ApplyRemoteEmbedder", userType, {},
            typesOnly ? NUdf::IUdfModule::TFlags::TypesOnly : 0, NUdf::TSourcePosition(), Secrets.get(), nullptr, &info);
        UNIT_ASSERT_C(status.IsOk(), status.GetError());
        return info;
    }

    TType* StreamUserType(bool valid = true, bool listHint = false) {
        TFunctionTypeInfoBuilder types(NYql::UnknownLangVersion, *RuntimeSettings, Env, new TTypeInfoHelper(), "", nullptr, NUdf::TSourcePosition());
        auto row = types.Struct(2)->AddField<ui64>("PassThrough", nullptr).AddField<char*>(valid ? NUdf::TStringRef::Of("Text") : NUdf::TStringRef::Of("Other"), nullptr).Build();
        auto stream = listHint ? types.List()->Item(row).Build() : types.Stream()->Item(row).Build();
        auto args = types.Tuple(4)->Add<char*>().Add<char*>().Add<char*>().Add(stream).Build();
        return static_cast<TType*>(types.Tuple(3)->Add(args).Add(types.Struct(0)->Build()).Add(types.Tuple(0)->Build()).Build());
    }

    NUdf::TUnboxedValue Function(bool stream) {
        auto info = Resolve(stream, stream ? StreamUserType() : nullptr);
        return NUdf::TUnboxedValuePod(info.Implementation.Release());
    }

    NUdf::TUnboxedValue String(TStringBuf text) {
        return Builder.NewString(text);
    }

    NUdf::TUnboxedValue Scalar(const TServer& server, ui32 retries = 0, TStringBuf text = "Дом") {
        const NUdf::TUnboxedValue args[] = {String("model"), String(server.Url()), String("secret"), String(text), NUdf::TUnboxedValuePod(retries)};
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

TVector<NUdf::TUnboxedValue> RunStream(TSetup& setup, const TServer& server, ui32 rows, ui32 batchSize, bool yield = false, ui32 rate = 0, ui32 retries = 0) {
    const NUdf::TUnboxedValue args[] = {
        setup.String("model"), setup.String(server.Url()), setup.String("secret"),
        NUdf::TUnboxedValuePod(new TInputStream(setup, rows, yield)),
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
        UNIT_ASSERT_VALUES_EQUAL(requests[0].Body["model"].GetString(), "model");
        UNIT_ASSERT_VALUES_EQUAL(requests[0].Body["encoding_format"].GetString(), "float");
        UNIT_ASSERT_VALUES_EQUAL(requests[0].Body["input"][0].GetString(), text);
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

    Y_UNIT_TEST(DefaultOptions) {
        TServer server;
        TSetup setup;
        const NUdf::TUnboxedValue args[] = {
            setup.String("model"), setup.String(server.Url()), setup.String("secret"),
            NUdf::TUnboxedValuePod(new TInputStream(setup, 33)), {}, {}, {}};
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
        const NUdf::TUnboxedValue scalarArgs[] = {setup.String("model"), setup.String(server.Url()), setup.String("secret"), setup.String("text"), {}};
        UNIT_ASSERT_VALUES_EQUAL(setup.Function(false).Run(&setup.Builder, scalarArgs).GetElement(1).Get<ui32>(), 200);
    }

    Y_UNIT_TEST(InvalidConfigurationDoesNotSendRequests) {
        TServer server;
        TSetup setup;
        UNIT_ASSERT_EXCEPTION_CONTAINS(RunStream(setup, server, 1, 0), yexception, "BatchSize must be positive");
        const NUdf::TUnboxedValue args[] = {setup.String("model"), setup.String(server.Url()), setup.String("missing-secret"), setup.String("text"), {}};
        UNIT_ASSERT_EXCEPTION_CONTAINS(setup.Function(false).Run(&setup.Builder, args), yexception, "authentication secret is not available");
        UNIT_ASSERT(server.Requests().empty());
    }

    Y_UNIT_TEST(TransportFailureReturnsStatusZero) {
        TPortManager ports;
        const auto port = ports.GetPort();
        TSetup setup;
        const NUdf::TUnboxedValue args[] = {setup.String("model"),
            setup.String(TStringBuilder() << "http://localhost:" << port << "/v1"), setup.String("secret"),
            setup.String("text"), NUdf::TUnboxedValuePod(ui32(0))};
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
        auto scalar = setup.Resolve(false, nullptr, true);
        UNIT_ASSERT_VALUES_EQUAL(scalar.FunctionType->GetArgumentsCount(), 5);
        UNIT_ASSERT_VALUES_EQUAL(scalar.FunctionType->GetOptionalArgumentsCount(), 1);
        auto stream = setup.Resolve(true, setup.StreamUserType(), true);
        UNIT_ASSERT_VALUES_EQUAL(stream.FunctionType->GetArgumentsCount(), 7);
        UNIT_ASSERT_VALUES_EQUAL(stream.FunctionType->GetOptionalArgumentsCount(), 3);
        TStringBuilder out;
        NUdf::TTypePrinter(*NUdf::ITypeInfoHelper::TPtr(new TTypeInfoHelper()), stream.FunctionType->GetReturnType()).Out(out.Out);
        UNIT_ASSERT_VALUES_EQUAL(out, "Stream<Struct<'Embedding':String,'PassThrough':Uint64,'Status':Uint32>>");
        TFunctionTypeInfo invalid;
        const auto status = setup.Registry->FindFunctionTypeInfo(NYql::UnknownLangVersion, *setup.RuntimeSettings, setup.Env, new TTypeInfoHelper(), nullptr,
            "RemoteEmbedder.ApplyRemoteEmbedderStream", setup.StreamUserType(false), {}, NUdf::IUdfModule::TFlags::TypesOnly, NUdf::TSourcePosition(), nullptr, nullptr, &invalid);
        UNIT_ASSERT(!status.IsOk());
        UNIT_ASSERT_STRING_CONTAINS(status.GetError(), "Text:String");
    }

    Y_UNIT_TEST(ProcessListHintDeclaresStreamArgument) {
        TSetup setup;
        const auto info = setup.Resolve(true, setup.StreamUserType(true, true), true);
        UNIT_ASSERT_EQUAL(info.FunctionType->GetArgumentType(3)->GetKind(), TType::EKind::Stream);
        UNIT_ASSERT_EQUAL(info.FunctionType->GetReturnType()->GetKind(), TType::EKind::Stream);
    }
}
