#include <yql/essentials/public/udf/udf_helpers.h>

#include <ydb/library/yql/udfs/common/knn/knn-serializer-shared.h>

#include <library/cpp/http/simple/http_client.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/time_provider/monotonic.h>
#include <library/cpp/uri/uri.h>

#include <util/stream/str.h>

#include <cmath>

using namespace NYql::NUdf;
using NMonotonic::TMonotonic;

namespace {

constexpr ui32 DefaultBatchSize = 32;
constexpr ui32 DefaultMaxRetries = 5;
constexpr ui32 TransportError = 0;
constexpr ui32 InvalidResponse = 502;

struct TBatchResult {
    ui32 Status = TransportError;
    TVector<TString> Embeddings;
};

// Each invocation owns its connection, retry budget and rate limiter.
class TEmbedderClient {
public:
    TEmbedderClient(TString model, TStringBuf baseUrl, TString token, ui32 maxRetries, ui32 tokensPerSecond)
        : Model_(std::move(model))
        , MaxRetries_(maxRetries)
        , TokensPerSecond_(tokensPerSecond)
    {
        NUri::TUri url;
        Y_ENSURE(url.ParseAbs(baseUrl) == NUri::TState::ParsedOK &&
            (url.GetScheme() == NUri::TScheme::SchemeHTTP || url.GetScheme() == NUri::TScheme::SchemeHTTPS) &&
            !url.GetHost().empty() && !(url.GetFieldMask() & (NUri::TField::FlagAuth | NUri::TField::FlagQuery | NUri::TField::FlagFrag)),
            "RemoteEmbedder: BaseUrl must be an HTTP(S) base URL without credentials, query or fragment");
        Y_ENSURE(!Model_.empty(), "RemoteEmbedder: Model must not be empty");
        Y_ENSURE(!token.empty() && token.find_first_of("\r\n") == TString::npos,
            "RemoteEmbedder: invalid authentication secret");
        Path_ = url.GetField(NUri::TField::FieldPath);
        while (Path_.EndsWith('/')) {
            Path_.pop_back();
        }
        Path_ += "/embeddings";
        const TString host = TStringBuilder() << url.GetField(NUri::TField::FieldScheme) << "://" << url.GetHost();
        Client_ = MakeHolder<TKeepAliveHttpClient>(host, url.GetPort(), TDuration::Seconds(60), TDuration::Seconds(10), true, false, true);
        Headers_["Content-Type"] = "application/json";
        Headers_["Authorization"] = "Bearer " + token;
    }

    TBatchResult Apply(const TVector<TString>& texts) {
        NJson::TJsonValue request(NJson::JSON_MAP);
        request["model"] = Model_;
        request["encoding_format"] = "float";
        auto& input = request["input"];
        input.SetType(NJson::JSON_ARRAY);
        ui64 estimatedTokens = 0;
        for (const auto& text : texts) {
            input.AppendValue(text);
            // No model-specific tokenizer is available. Bytes are a conservative
            // estimate for byte-based tokenizers, including non-ASCII input.
            estimatedTokens += Max<size_t>(1, text.size());
        }
        const TString body = NJson::WriteJson(request);
        for (ui64 attempt = 0; ; ++attempt) {
            Throttle(estimatedTokens);
            TStringStream response;
            ui32 status = TransportError;
            try {
                status = Client_->DoPost(Path_, body, &response, Headers_);
            } catch (const std::exception&) {
                // HTTP diagnostics may contain credentials or input text.
                Client_->ResetConnection();
            }
            if (status >= 200 && status < 300) {
                return ParseResponse(response.Str(), texts.size(), status, estimatedTokens);
            }
            if (attempt >= MaxRetries_ || !(status == TransportError || status == 408 || status == 429 || status >= 500)) {
                return {status, {}};
            }
            Sleep(TDuration::MilliSeconds(Min<ui64>(10000, 100ULL << Min<ui64>(attempt, 7))));
        }
    }

private:
    void Throttle(ui64 tokens) {
        if (!TokensPerSecond_) {
            return;
        }
        auto now = TMonotonic::Now();
        if (NextRequest_ > now) {
            Sleep(NextRequest_ - now);
            now = TMonotonic::Now();
        }
        NextRequest_ = now + TokenDuration(tokens);
    }

    TDuration TokenDuration(ui64 tokens) const {
        const ui64 remainder = tokens % TokensPerSecond_;
        return TDuration::Seconds(tokens / TokensPerSecond_) +
            TDuration::MicroSeconds((remainder * 1000000 + TokensPerSecond_ - 1) / TokensPerSecond_);
    }

    TBatchResult ParseResponse(TStringBuf body, size_t count, ui32 status, ui64 estimatedTokens) {
        NJson::TJsonValue response;
        if (!NJson::ReadJsonTree(body, &response) || !response["data"].IsArray() || response["data"].GetArray().size() != count) {
            return {InvalidResponse, {}};
        }
        const auto& usage = response["usage"]["prompt_tokens"];
        if (TokensPerSecond_ && usage.IsUInteger() && usage.GetUInteger() > estimatedTokens) {
            NextRequest_ += TokenDuration(usage.GetUInteger() - estimatedTokens);
        }
        TBatchResult result{status, TVector<TString>(count)};
        size_t dimensions = 0;
        for (const auto& item : response["data"].GetArray()) {
            const auto& index = item["index"];
            const auto& embedding = item["embedding"];
            if (!index.IsUInteger() || index.GetUInteger() >= count || !embedding.IsArray() || embedding.GetArray().empty()) {
                return {InvalidResponse, {}};
            }
            auto& output = result.Embeddings[index.GetUInteger()];
            if (!output.empty() || (dimensions && dimensions != embedding.GetArray().size())) {
                return {InvalidResponse, {}};
            }
            dimensions = embedding.GetArray().size();
            TStringOutput stream(output);
            NKnnVectorSerialization::TSerializer<float> serializer(&stream);
            for (const auto& coordinate : embedding.GetArray()) {
                const double value = coordinate.GetDoubleRobust();
                if (!(coordinate.IsInteger() || coordinate.IsUInteger() || coordinate.IsDouble()) || !std::isfinite(value) || !std::isfinite(static_cast<float>(value))) {
                    return {InvalidResponse, {}};
                }
                serializer.HandleElement(value);
            }
        }
        return result;
    }

    TString Model_;
    TString Path_;
    TKeepAliveHttpClient::THeaders Headers_;
    THolder<TKeepAliveHttpClient> Client_;
    ui32 MaxRetries_;
    ui32 TokensPerSecond_;
    TMonotonic NextRequest_;
};

TString GetToken(const IValueBuilder* builder, TStringRef name) {
    TStringRef token;
    Y_ENSURE(builder->GetSecureParam(name, token), "RemoteEmbedder: authentication secret is not available");
    return TString(token);
}

TUnboxedValue MakeResult(const IValueBuilder* builder, const TBatchResult& batch, size_t index, const TUnboxedValue* passThrough = nullptr) {
    TUnboxedValue* items = nullptr;
    auto result = builder->NewArray(passThrough ? 3 : 2, items);
    const TStringBuf embedding = batch.Embeddings.empty() ? TStringBuf() : TStringBuf(batch.Embeddings[index]);
    items[0] = builder->NewString(embedding);
    if (passThrough) {
        items[1] = *passThrough;
    }
    items[passThrough ? 2 : 1] = TUnboxedValuePod(batch.Status);
    return result;
}

class TEmbedderStream: public TBoxedValue {
public:
    TEmbedderStream(const IValueBuilder* builder, const TUnboxedValuePod* args, ui32 textIndex, ui32 passThroughIndex, TSourcePosition pos)
        : Builder_(builder)
        , Input_(args[3])
        , Client_(TString(args[0].AsStringRef()), args[1].AsStringRef(), GetToken(builder, args[2].AsStringRef()),
              args[6] ? args[6].Get<ui32>() : DefaultMaxRetries, args[5] ? args[5].Get<ui32>() : 0)
        , BatchSize_(args[4] ? args[4].Get<ui32>() : DefaultBatchSize)
        , TextIndex_(textIndex)
        , PassThroughIndex_(passThroughIndex)
        , Pos_(pos)
    {
        Y_ENSURE(BatchSize_ > 0, "RemoteEmbedder: BatchSize must be positive");
    }

private:
    EFetchStatus Fetch(TUnboxedValue& result) override try {
        if (BatchReady_ && OutputIndex_ < PassThrough_.size()) {
            result = MakeResult(Builder_, Batch_, OutputIndex_, &PassThrough_[OutputIndex_]);
            PassThrough_[OutputIndex_++].Clear();
            return EFetchStatus::Ok;
        }
        if (OutputIndex_) {
            PassThrough_.clear();
            Texts_.clear();
            Batch_ = {};
            BatchReady_ = false;
            OutputIndex_ = 0;
        }
        while (!Finished_ && Texts_.size() < BatchSize_) {
            TUnboxedValue row;
            switch (Input_.Fetch(row)) {
                case EFetchStatus::Yield:
                    return EFetchStatus::Yield;
                case EFetchStatus::Finish:
                    Finished_ = true;
                    break;
                case EFetchStatus::Ok: {
                    const auto text = row.GetElement(TextIndex_);
                    Texts_.emplace_back(text.AsStringRef());
                    PassThrough_.push_back(row.GetElement(PassThroughIndex_));
                    break;
                }
            }
        }
        if (Texts_.empty()) {
            return EFetchStatus::Finish;
        }
        Batch_ = Client_.Apply(Texts_);
        BatchReady_ = true;
        return Fetch(result);
    } catch (const std::exception& e) {
        UdfTerminate((TStringBuilder() << Builder_->WithCalleePosition(Pos_) << " " << e.what()).c_str());
    }

    const IValueBuilder* Builder_;
    TUnboxedValue Input_;
    TEmbedderClient Client_;
    ui32 BatchSize_;
    ui32 TextIndex_;
    ui32 PassThroughIndex_;
    TSourcePosition Pos_;
    TVector<TString> Texts_;
    TVector<TUnboxedValue> PassThrough_;
    TBatchResult Batch_;
    size_t OutputIndex_ = 0;
    bool BatchReady_ = false;
    bool Finished_ = false;
};

class TApplyRemoteEmbedder: public TBoxedValue {
public:
    TApplyRemoteEmbedder(TSourcePosition pos, bool stream, ui32 textIndex = 0, ui32 passThroughIndex = 0)
        : Pos_(pos)
        , Stream_(stream)
        , TextIndex_(textIndex)
        , PassThroughIndex_(passThroughIndex)
    {
    }

private:
    TUnboxedValue Run(const IValueBuilder* builder, const TUnboxedValuePod* args) const override try {
        if (Stream_) {
            return TUnboxedValuePod(new TEmbedderStream(builder, args, TextIndex_, PassThroughIndex_, Pos_));
        }
        TEmbedderClient client(TString(args[0].AsStringRef()), args[1].AsStringRef(), GetToken(builder, args[2].AsStringRef()),
            args[4] ? args[4].Get<ui32>() : DefaultMaxRetries, 0);
        return MakeResult(builder, client.Apply({TString(args[3].AsStringRef())}), 0);
    } catch (const std::exception& e) {
        UdfTerminate((TStringBuilder() << builder->WithCalleePosition(Pos_) << " " << e.what()).c_str());
    }

    TSourcePosition Pos_;
    bool Stream_;
    ui32 TextIndex_;
    ui32 PassThroughIndex_;
};

class TRemoteEmbedderModule: public IUdfModule {
public:
    TStringRef Name() const {
        return TStringRef::Of("RemoteEmbedder");
    }

    void CleanupOnTerminate() const override {
    }

    void GetAllFunctions(IFunctionsSink& sink) const override {
        sink.Add(TStringRef::Of("ApplyRemoteEmbedder"));
        sink.Add(TStringRef::Of("ApplyRemoteEmbedderStream"))->SetTypeAwareness();
    }

    void BuildFunctionTypeInfo(const TStringRef& name, TType* userType, const TStringRef&, ui32 flags, IFunctionTypeInfoBuilder& builder) const override try {
        const bool stream = name == TStringRef::Of("ApplyRemoteEmbedderStream");
        if (!stream && name != TStringRef::Of("ApplyRemoteEmbedder")) {
            builder.SetError("RemoteEmbedder: unknown function");
            return;
        }
        const TType* inputType = nullptr;
        const TType* passThroughType = nullptr;
        ui32 textIndex = 0;
        ui32 passThroughIndex = 0;
        if (stream) {
            const auto helper = builder.TypeInfoHelper();
            const TTupleTypeInspector user(*helper, userType);
            const TTupleTypeInspector args(*helper, user && user.GetElementsCount() ? user.GetElementType(0) : nullptr);
            const auto sequenceType = args && args.GetElementsCount() > 3 ? args.GetElementType(3) : nullptr;
            const TStreamTypeInspector input(*helper, sequenceType);
            const TListTypeInspector listHint(*helper, sequenceType);
            // PROCESS supplies a List hint for TableRows(), then SqlProcess
            // adapts its input to the Stream argument declared below.
            const auto rowType = input ? input.GetItemType() : listHint ? listHint.GetItemType() : nullptr;
            const TStructTypeInspector row(*helper, rowType);
            if (!row || row.GetMembersCount() != 2 ||
                (textIndex = row.GetMemberIndex("Text")) == Max<ui32>() ||
                (passThroughIndex = row.GetMemberIndex("PassThrough")) == Max<ui32>() ||
                TDataTypeInspector(*helper, row.GetMemberType(textIndex)).GetTypeId() != TDataType<char*>::Id) {
                builder.SetError("RemoteEmbedder: expected Stream<Struct<Text:String,PassThrough:T>>");
                return;
            }
            inputType = builder.Stream()->Item(rowType).Build();
            passThroughType = row.GetMemberType(passThroughIndex);
            builder.UserType(userType);
        }
        auto result = builder.Struct(stream ? 3 : 2);
        result->AddField<char*>("Embedding", nullptr);
        if (stream) {
            result->AddField("PassThrough", passThroughType, nullptr);
        }
        result->AddField<ui32>("Status", nullptr);
        const auto resultType = result->Build();
        builder.Returns(stream ? builder.Stream()->Item(resultType).Build() : resultType).OptionalArgs(stream ? 3 : 1);
        auto args = builder.Args(stream ? 7 : 5);
        args->Add<char*>().Name("Model");
        args->Add<char*>().Name("BaseUrl");
        args->Add<char*>().Name("AuthSecretName");
        if (stream) {
            args->Add(inputType).Name("InputStream");
            args->Add<TOptional<ui32>>().Name("BatchSize");
            args->Add<TOptional<ui32>>().Name("MaxTokensPerSecond");
        } else {
            args->Add<char*>().Name("Text");
        }
        args->Add<TOptional<ui32>>().Name("MaxRetries");
        args->Done();
        if (!(flags & TFlags::TypesOnly)) {
            builder.Implementation(new TApplyRemoteEmbedder(builder.GetSourcePosition(), stream, textIndex, passThroughIndex));
        }
    } catch (const std::exception& e) {
        builder.SetError(TStringBuf(e.what()));
    }
};

} // namespace

REGISTER_MODULES(TRemoteEmbedderModule)
