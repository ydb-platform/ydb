#include "remote_embedder.h"
#include "remote_embedder_http.h"

#include "knn-serializer-shared.h"

#include <yql/essentials/public/udf/udf_helpers.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/time_provider/monotonic.h>
#include <library/cpp/uri/uri.h>

#include <util/stream/str.h>
#include <util/string/ascii.h>

#include <algorithm>
#include <cmath>
#include <optional>

using namespace NYql::NUdf;
using NMonotonic::TMonotonic;

namespace {

constexpr ui32 DefaultBatchSize = 32;
constexpr ui32 DefaultMaxRetries = 5;
constexpr ui32 TransportError = 0;
constexpr ui32 InvalidResponse = 502;
constexpr size_t MaxBatchRows = 1024;
constexpr size_t MaxBatchBytes = 1024 * 1024;

bool IsSafeHttpRequestPart(TStringBuf part) {
    return std::ranges::none_of(part, [](unsigned char c) {
        return c <= ' ' || c == 0x7f;
    });
}

struct TBatchResult {
    ui32 Status = TransportError;
    TVector<TString> Embeddings;
};

// Each invocation owns its retry budget and rate limiter.
class TEmbedderClient {
public:
    TEmbedderClient(TString model, TStringBuf baseUrl, TString token, TStringBuf secretName, ui32 maxRetries, ui32 tokensPerSecond)
        : Model_(std::move(model))
        , SecretName_(secretName)
        , MaxRetries_(maxRetries)
        , TokensPerSecond_(tokensPerSecond)
    {
        NUri::TUri url;
        Y_ENSURE(url.ParseAbs(baseUrl, NUri::TFeature::FeaturesDefaultOrSchemeKnown) == NUri::TState::ParsedOK &&
            (url.GetScheme() == NUri::TScheme::SchemeHTTP || url.GetScheme() == NUri::TScheme::SchemeHTTPS) &&
            !url.GetHost().empty() && !(url.GetFieldMask() & (NUri::TField::FlagAuth | NUri::TField::FlagQuery | NUri::TField::FlagFrag)),
            "RemoteEmbedder: BaseUrl must be an HTTP(S) base URL without credentials, query or fragment");
        Y_ENSURE(!Model_.empty(), "RemoteEmbedder: Model must not be empty");
        Y_ENSURE(Model_.size() <= MaxBatchBytes, "RemoteEmbedder: Model exceeds the request byte limit");
        Y_ENSURE(!token.empty() && token.find_first_of("\r\n") == TString::npos,
            "RemoteEmbedder: invalid authentication secret");
        Path_ = url.GetField(NUri::TField::FieldPath);
        while (Path_.EndsWith('/')) {
            Path_.pop_back();
        }
        Path_ += "/embeddings";
        UseTls_ = url.GetScheme() == NUri::TScheme::SchemeHTTPS;
        Port_ = url.GetPort();
        const auto host = url.GetHost();
        DialHost_ = host.StartsWith('[') && host.EndsWith(']') ? host.SubStr(1, host.size() - 2) : host;
        NKnn::ValidateRemoteEmbedderDestination(SecretName_, DialHost_, Port_, UseTls_);
        Authority_ = host;
        if (Port_ != (UseTls_ ? 443 : 80)) {
            Authority_ += TStringBuilder() << ':' << Port_;
        }
        Y_ENSURE(IsSafeHttpRequestPart(Path_) && IsSafeHttpRequestPart(Authority_),
            "RemoteEmbedder: invalid HTTP request target");
        Authorization_ = "Bearer " + token;
    }

    TBatchResult Apply(const TString& text) {
        Y_ENSURE(text.size() <= MaxBatchBytes, "RemoteEmbedder: Text exceeds the request byte limit");
        return ApplyRequest(NJson::TJsonValue(text), 1, Max<size_t>(1, text.size()));
    }

    TBatchResult Apply(const TVector<TString>& texts) {
        NJson::TJsonValue input(NJson::JSON_ARRAY);
        ui64 estimatedTokens = 0;
        for (const auto& text : texts) {
            input.AppendValue(text);
            // No model-specific tokenizer is available. Bytes are a conservative
            // estimate for byte-based tokenizers, including non-ASCII input.
            estimatedTokens += Max<size_t>(1, text.size());
        }
        return ApplyRequest(std::move(input), texts.size(), estimatedTokens);
    }

private:
    TBatchResult ApplyRequest(NJson::TJsonValue input, size_t count, ui64 estimatedTokens) {
        NJson::TJsonValue request(NJson::JSON_MAP);
        request["model"] = Model_;
        request["encoding_format"] = "float";
        request["input"] = std::move(input);
        const TString body = NJson::WriteJson(request);
        const TString httpRequest = TStringBuilder()
            << "POST " << Path_ << " HTTP/1.1\r\nHost: " << Authority_
            << "\r\nConnection: close\r\nContent-Type: application/json\r\nAuthorization: " << Authorization_
            << "\r\nContent-Length: " << body.size() << "\r\n\r\n" << body;
        for (ui64 attempt = 0; ; ++attempt) {
            Throttle(estimatedTokens);
            NKnn::ValidateRemoteEmbedderDestination(SecretName_, DialHost_, Port_, UseTls_);
            const auto response = NKnn::PostRemoteEmbedderRequest(DialHost_, Port_, UseTls_, httpRequest).ExtractValueSync();
            const auto status = response.Status;
            if (status >= 200 && status < 300) {
                return ParseResponse(response.Body, count, status, estimatedTokens);
            }
            if (response.TooLarge || attempt >= MaxRetries_
                || !(status == TransportError || status == 408 || status == 429 || status >= 500)) {
                return {status, {}};
            }
            NKnn::WaitBeforeRemoteEmbedderRequest(
                TDuration::MilliSeconds(Min<ui64>(10000, 100ULL << Min<ui64>(attempt, 7)))).GetValueSync();
        }
    }

    void Throttle(ui64 tokens) {
        if (!TokensPerSecond_) {
            return;
        }
        auto now = TMonotonic::Now();
        if (NextRequest_ > now) {
            NKnn::WaitBeforeRemoteEmbedderRequest(NextRequest_ - now).GetValueSync();
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
        NJson::TJsonReaderConfig config;
        config.UseIterativeParser = true;
        config.MaxDepth = 16;
        if (!NJson::ReadJsonTree(body, &config, &response) || !response["data"].IsArray() || response["data"].GetArray().size() != count) {
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
    TString SecretName_;
    TString Path_;
    TString DialHost_;
    TString Authority_;
    TString Authorization_;
    ui16 Port_;
    bool UseTls_;
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

std::optional<size_t> EstimatePassThroughBytes(const ITypeInfoHelper& helper, const TType* type,
    const TUnboxedValuePod& value, size_t limit)
{
    size_t size = sizeof(TUnboxedValue);
    if (value.IsString()) {
        return size + value.AsStringRef().Size();
    }
    if (!value.IsBoxed() || size > limit) {
        return size;
    }
    if (const TOptionalTypeInspector optional(helper, type); optional) {
        return EstimatePassThroughBytes(helper, optional.GetItemType(), value.GetOptionalValue(), limit);
    }
    if (const TTaggedTypeInspector tagged(helper, type); tagged) {
        return EstimatePassThroughBytes(helper, tagged.GetBaseType(), value, limit);
    }

    const TStructTypeInspector structure(helper, type);
    const TTupleTypeInspector tuple(helper, type);
    const TListTypeInspector list(helper, type);
    const auto* elements = list ? value.GetElements() : nullptr;
    size_t count = 0;
    if (structure) {
        count = structure.GetMembersCount();
    } else if (tuple) {
        count = tuple.GetElementsCount();
    } else if (elements && value.HasFastListLength()) {
        count = value.GetListLength();
    } else {
        // Traversing other values can consume a lazy input or evaluate user code.
        return std::nullopt;
    }

    for (size_t i = 0; i < count; ++i) {
        const auto* itemType = structure ? structure.GetMemberType(i) : tuple ? tuple.GetElementType(i) : list.GetItemType();
        const auto item = elements ? elements[i] : value.GetElement(i);
        const auto bytes = EstimatePassThroughBytes(helper, itemType, item, limit - size);
        if (!bytes) {
            return std::nullopt;
        }
        size += *bytes;
        if (size > limit) {
            break;
        }
    }
    return size;
}

class TEmbedderStream: public TBoxedValue {
public:
    TEmbedderStream(const IValueBuilder* builder, const TUnboxedValuePod* args, ui32 textIndex, ui32 passThroughIndex,
        TSourcePosition pos, ITypeInfoHelper::TPtr typeHelper, const TType* passThroughType)
        : Builder_(builder)
        , Input_(args[0])
        , Client_(TString(args[1].AsStringRef()), args[2].AsStringRef(), GetToken(builder, args[3].AsStringRef()), args[3].AsStringRef(),
              args[6] ? args[6].Get<ui32>() : DefaultMaxRetries, args[5] ? args[5].Get<ui32>() : 0)
        , BatchSize_(args[4] ? args[4].Get<ui32>() : DefaultBatchSize)
        , TextIndex_(textIndex)
        , PassThroughIndex_(passThroughIndex)
        , Pos_(pos)
        , TypeHelper_(std::move(typeHelper))
        , PassThroughType_(passThroughType)
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
            BatchBytes_ = 0;
            Batch_ = {};
            BatchReady_ = false;
            OutputIndex_ = 0;
        }
        while (!Finished_ && Texts_.size() < Min<size_t>(BatchSize_, MaxBatchRows)) {
            if (!PendingRow_) {
                TUnboxedValue row;
                switch (Input_.Fetch(row)) {
                    case EFetchStatus::Yield:
                        return EFetchStatus::Yield;
                    case EFetchStatus::Finish:
                        Finished_ = true;
                        break;
                    case EFetchStatus::Ok:
                        PendingRow_ = std::move(row);
                        break;
                }
            }
            if (Finished_) {
                break;
            }
            const auto text = PendingRow_.GetElement(TextIndex_);
            const auto textRef = text.AsStringRef();
            auto passThrough = PendingRow_.GetElement(PassThroughIndex_);
            const auto passThroughBytes = EstimatePassThroughBytes(*TypeHelper_, PassThroughType_, passThrough, MaxBatchBytes);
            const size_t rowBytes = size_t(textRef.Size()) + sizeof(TString) + passThroughBytes.value_or(sizeof(TUnboxedValue));
            Y_ENSURE(rowBytes <= MaxBatchBytes, "RemoteEmbedder: stream row exceeds the batch byte limit");
            if (rowBytes > MaxBatchBytes - BatchBytes_ || (!passThroughBytes && !Texts_.empty())) {
                break;
            }
            BatchBytes_ += rowBytes;
            Texts_.emplace_back(textRef);
            PassThrough_.push_back(std::move(passThrough));
            PendingRow_.Clear();
            // Hold at most one pass-through value whose storage cannot be estimated.
            if (!passThroughBytes) {
                break;
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
    ITypeInfoHelper::TPtr TypeHelper_;
    const TType* PassThroughType_;
    TVector<TString> Texts_;
    TVector<TUnboxedValue> PassThrough_;
    TUnboxedValue PendingRow_;
    size_t BatchBytes_ = 0;
    TBatchResult Batch_;
    size_t OutputIndex_ = 0;
    bool BatchReady_ = false;
    bool Finished_ = false;
};

class TApplyRemoteEmbedder: public TBoxedValue {
public:
    TApplyRemoteEmbedder(TSourcePosition pos, bool stream, ui32 textIndex, ui32 passThroughIndex,
        ITypeInfoHelper::TPtr typeHelper, const TType* passThroughType)
        : Pos_(pos)
        , Stream_(stream)
        , TextIndex_(textIndex)
        , PassThroughIndex_(passThroughIndex)
        , TypeHelper_(std::move(typeHelper))
        , PassThroughType_(passThroughType)
    {
    }

private:
    TUnboxedValue Run(const IValueBuilder* builder, const TUnboxedValuePod* args) const override try {
        if (Stream_) {
            return TUnboxedValuePod(new TEmbedderStream(builder, args, TextIndex_, PassThroughIndex_, Pos_, TypeHelper_, PassThroughType_));
        }
        TEmbedderClient client(TString(args[1].AsStringRef()), args[2].AsStringRef(), GetToken(builder, args[3].AsStringRef()), args[3].AsStringRef(),
            args[4] ? args[4].Get<ui32>() : DefaultMaxRetries, 0);
        return MakeResult(builder, client.Apply(TString(args[0].AsStringRef())), 0);
    } catch (const std::exception& e) {
        UdfTerminate((TStringBuilder() << builder->WithCalleePosition(Pos_) << " " << e.what()).c_str());
    }

    TSourcePosition Pos_;
    bool Stream_;
    ui32 TextIndex_;
    ui32 PassThroughIndex_;
    ITypeInfoHelper::TPtr TypeHelper_;
    const TType* PassThroughType_;
};

void BuildRemoteEmbedderSignature(TType* userType, IFunctionTypeInfoBuilder& builder, bool typesOnly) try {
    const auto helper = builder.TypeInfoHelper();
    const TTupleTypeInspector user(*helper, userType);
    const TTupleTypeInspector positional(*helper, user && user.GetElementsCount() ? user.GetElementType(0) : nullptr);
    const auto* firstArg = positional && positional.GetElementsCount() ? positional.GetElementType(0) : nullptr;
    if (!firstArg && user && user.GetElementsCount() > 1) {
        const TStructTypeInspector named(*helper, user.GetElementType(1));
        if (named) {
            for (ui32 i = 0; i < named.GetMembersCount(); ++i) {
                if (AsciiEqualsIgnoreCase(TStringBuf(named.GetMemberName(i)), "InputStream")) {
                    firstArg = named.GetMemberType(i);
                    break;
                }
            }
        }
    }
    const TStreamTypeInspector input(*helper, firstArg);
    const TListTypeInspector listHint(*helper, firstArg);
    const bool stream = input || listHint;
    const TType* inputType = nullptr;
    const TType* passThroughType = nullptr;
    ui32 textIndex = 0;
    ui32 passThroughIndex = 0;
    if (stream) {
        // PROCESS supplies a List hint for TableRows(), then SqlProcess
        // adapts its input to the Stream argument declared below.
        const auto rowType = input ? input.GetItemType() : listHint.GetItemType();
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
    }
    builder.UserType(userType);
    auto result = builder.Struct(stream ? 3 : 2);
    result->AddField<char*>("Embedding", nullptr);
    if (stream) {
        result->AddField("PassThrough", passThroughType, nullptr);
    }
    result->AddField<ui32>("Status", nullptr);
    const auto resultType = result->Build();
    builder.Returns(stream ? builder.Stream()->Item(resultType).Build() : resultType).OptionalArgs(stream ? 3 : 1);
    auto args = builder.Args(stream ? 7 : 5);
    if (stream) {
        args->Add(inputType).Name("InputStream");
    } else {
        args->Add<char*>().Name("Text");
    }
    args->Add<char*>().Name("Model");
    args->Add<char*>().Name("BaseUrl");
    args->Add<char*>().Name("AuthSecretName");
    if (stream) {
        args->Add<TOptional<ui32>>().Name("BatchSize");
        args->Add<TOptional<ui32>>().Name("MaxTokensPerSecond");
    }
    args->Add<TOptional<ui32>>().Name("MaxRetries");
    args->Done();
    if (!typesOnly) {
        builder.Implementation(new TApplyRemoteEmbedder(builder.GetSourcePosition(), stream, textIndex, passThroughIndex, helper, passThroughType));
    }
} catch (const std::exception& e) {
    builder.SetError(TStringBuf(e.what()));
}

} // namespace

namespace NKnn {

bool TRemoteEmbedder::DeclareSignature(const TStringRef& name, TType* userType, IFunctionTypeInfoBuilder& builder, bool typesOnly) {
    if (name != Name()) {
        return false;
    }
    BuildRemoteEmbedderSignature(userType, builder, typesOnly);
    return true;
}

} // namespace NKnn
