#include "error.h"
#include "serialize.h"
#include "origin_attributes.h"

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <yt/yt/core/yson/tokenizer.h>

#include <yt/yt/core/ytree/attributes.h>
#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt_proto/yt/core/misc/proto/error.pb.h>

#include <library/cpp/yt/global/variable.h>

namespace NYT {

using namespace NTracing;
using namespace NYTree;
using namespace NYson;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf OriginalErrorDepthAttribute = "original_error_depth";

////////////////////////////////////////////////////////////////////////////////

namespace NOrigin {

namespace {

struct TExtensionData
{
    NConcurrency::TFiberId Fid = NConcurrency::InvalidFiberId;
    const char* HostName = nullptr;
    TTraceId TraceId = InvalidTraceId;
    TSpanId SpanId = InvalidSpanId;
};

TOriginAttributes::TErasedExtensionData Encode(TExtensionData data)
{
    return TOriginAttributes::TErasedExtensionData{data};
}

TExtensionData Decode(const TOriginAttributes::TErasedExtensionData& storage)
{
    return storage.AsConcrete<TExtensionData>();
}

void TryExtractHost(const TOriginAttributes& attributes)
{
    if (attributes.Host || !attributes.ExtensionData) {
        return;
    }

    auto [
        fid,
        name,
        traceId,
        spanId
    ] = Decode(*attributes.ExtensionData);

    attributes.Host = name;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

bool HasHost(const TOriginAttributes& attributes) noexcept
{
    TryExtractHost(attributes);
    return attributes.Host.operator bool();
}

TStringBuf GetHost(const TOriginAttributes& attributes) noexcept
{
    TryExtractHost(attributes);
    return attributes.Host;
}

NConcurrency::TFiberId GetFid(const TOriginAttributes& attributes) noexcept
{
    if (attributes.ExtensionData.has_value()) {
        return Decode(*attributes.ExtensionData).Fid;
    }
    return NConcurrency::InvalidFiberId;
}

NTracing::TTraceId GetTraceId(const TOriginAttributes& attributes) noexcept
{
    if (attributes.ExtensionData.has_value()) {
        return Decode(*attributes.ExtensionData).TraceId;
    }
    return InvalidTraceId;
}

NTracing::TSpanId GetSpanId(const TOriginAttributes& attributes) noexcept
{
    if (attributes.ExtensionData.has_value()) {
        return Decode(*attributes.ExtensionData).SpanId;
    }
    return InvalidSpanId;
}

bool HasTracingAttributes(const TOriginAttributes& attributes) noexcept
{
    return GetTraceId(attributes) != InvalidTraceId;
}

void UpdateTracingAttributes(TOriginAttributes* attributes, const NTracing::TTracingAttributes& tracingAttributes)
{
    if (attributes->ExtensionData.has_value()) {
        auto ext = Decode(*attributes->ExtensionData);
        attributes->ExtensionData.emplace(Encode(TExtensionData{
            .Fid = ext.Fid,
            .HostName = ext.HostName,
            .TraceId = tracingAttributes.TraceId,
            .SpanId = tracingAttributes.SpanId,
        }));
    }

    attributes->ExtensionData.emplace(Encode(TExtensionData{
        .TraceId = tracingAttributes.TraceId,
        .SpanId = tracingAttributes.SpanId,
    }));
}

////////////////////////////////////////////////////////////////////////////////

TOriginAttributes::TErasedExtensionData GetExtensionDataOverride()
{
    TExtensionData result;
    result.Fid = NConcurrency::GetCurrentFiberId();
    result.HostName = NNet::ReadLocalHostName();

    if (auto* traceContext = NTracing::TryGetCurrentTraceContext()) {
        result.TraceId = traceContext->GetTraceId();
        result.SpanId = traceContext->GetSpanId();
    }

    return TOriginAttributes::TErasedExtensionData{result};
}

TString FormatOriginOverride(const TOriginAttributes& attributes)
{
    TryExtractHost(attributes);
    return Format("%v (pid %v, thread %v, fid %x)",
        attributes.Host,
        attributes.Pid,
        MakeFormatterWrapper([&] (auto* builder) {
            auto threadName = attributes.ThreadName.ToStringBuf();
            if (threadName.empty()) {
                FormatValue(builder, attributes.Tid, "v");
                return;
            }
            FormatValue(builder, threadName, "v");
        }),
        GetFid(attributes));
}

TOriginAttributes ExtractFromDictionaryOverride(const NYTree::IAttributeDictionaryPtr& attributes)
{
    auto result = NYT::NDetail::ExtractFromDictionaryDefault(attributes);

    TExtensionData ext;

    static const TString FidKey("fid");
    ext.Fid = attributes->GetAndRemove<NConcurrency::TFiberId>(FidKey, NConcurrency::InvalidFiberId);

    static const TString TraceIdKey("trace_id");
    ext.TraceId = attributes->GetAndRemove<NTracing::TTraceId>(TraceIdKey, NTracing::InvalidTraceId);

    static const TString SpanIdKey("span_id");
    ext.SpanId = attributes->GetAndRemove<NTracing::TSpanId>(SpanIdKey, NTracing::InvalidSpanId);

    result.ExtensionData = Encode(ext);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

void EnableOriginOverrides()
{
    static NGlobal::TVariable<std::byte> getExtensionDataOverride{
        NYT::NDetail::GetExtensionDataTag,
        +[] () noexcept {
            return NGlobal::TErasedStorage{&GetExtensionDataOverride};
        }};

    static NGlobal::TVariable<std::byte> formatOriginOverride{
        NYT::NDetail::FormatOriginTag,
        +[] () noexcept {
            return NGlobal::TErasedStorage{&FormatOriginOverride};
        }};

    static NGlobal::TVariable<std::byte> extractFromDictionaryOverride{
        NYT::NDetail::ExtractFromDictionaryTag,
        +[] () noexcept {
            return NGlobal::TErasedStorage{&ExtractFromDictionaryOverride};
        }};

    getExtensionDataOverride.Get();
    formatOriginOverride.Get();
    extractFromDictionaryOverride.Get();
}

} // namespace NOrigin

////////////////////////////////////////////////////////////////////////////////

bool HasHost(const TError& error) noexcept
{
    if (auto* attributes = error.MutableOriginAttributes()) {
        return NOrigin::HasHost(*attributes);
    }
    return false;
}

TStringBuf GetHost(const TError& error) noexcept
{
    if (auto* attributes = error.MutableOriginAttributes()) {
        return NOrigin::GetHost(*attributes);
    }
    return {};
}

NConcurrency::TFiberId GetFid(const TError& error) noexcept
{
    if (auto* attributes = error.MutableOriginAttributes()) {
        return NOrigin::GetFid(*attributes);
    }
    return NConcurrency::InvalidFiberId;
}

bool HasTracingAttributes(const TError& error) noexcept
{
    if (auto* attributes = error.MutableOriginAttributes()) {
        return NOrigin::HasTracingAttributes(*attributes);
    }
    return false;
}

NTracing::TTraceId GetTraceId(const TError& error) noexcept
{
    if (auto* attributes = error.MutableOriginAttributes()) {
        return NOrigin::GetTraceId(*attributes);
    }
    return NTracing::InvalidTraceId;
}

NTracing::TSpanId GetSpanId(const TError& error) noexcept
{
    if (auto* attributes = error.MutableOriginAttributes()) {
        return NOrigin::GetSpanId(*attributes);
    }
    return NTracing::InvalidSpanId;
}

void SetTracingAttributes(TError* error, const NTracing::TTracingAttributes& attributes)
{
    auto* originAttributes = error->MutableOriginAttributes();

    if (!originAttributes) {
        return;
    }

    NOrigin::UpdateTracingAttributes(originAttributes, attributes);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

// Errors whose depth exceeds |ErrorSerializationDepthLimit| are serialized
// as children of their ancestor on depth |ErrorSerializationDepthLimit - 1|.
[[maybe_unused]]
void SerializeInnerErrors(TFluentMap fluent, const TError& error, int depth)
{
    if (depth >= ErrorSerializationDepthLimit) {
        // Ignore deep inner errors.
        return;
    }

    auto visit = [&] (auto fluent, const TError& error, int depth) {
        fluent
            .Item().Do([&] (auto fluent) {
                Serialize(error, fluent.GetConsumer(), /*valueProduce*/ nullptr, depth);
            });
    };

    fluent
        .Item("inner_errors").DoListFor(error.InnerErrors(), [&] (auto fluent, const TError& innerError) {
            if (depth < ErrorSerializationDepthLimit - 1) {
                visit(fluent, innerError, depth + 1);
            } else {
                YT_VERIFY(depth == ErrorSerializationDepthLimit - 1);
                TraverseError(
                    innerError,
                    [&] (const TError& e, int depth) {
                        visit(fluent, e, depth);
                    },
                    depth + 1);
            }
        });
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void Serialize(
    const TError& error,
    IYsonConsumer* consumer,
    const std::function<void(IYsonConsumer*)>* valueProducer,
    int depth)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("code").Value(error.GetCode())
            .Item("message").Value(error.GetMessage())
            .Item("attributes").DoMap([&] (auto fluent) {
                if (error.HasOriginAttributes()) {
                    fluent
                        .Item("host").Value(GetHost(error))
                        .Item("pid").Value(error.GetPid())
                        .Item("tid").Value(error.GetTid())
                        .Item("thread").Value(error.GetThreadName())
                        .Item("fid").Value(GetFid(error));
                } else if (IsErrorSanitizerEnabled() && HasHost(error)) {
                    fluent
                        .Item("host").Value(GetHost(error));
                }
                if (error.HasDatetime()) {
                    fluent
                        .Item("datetime").Value(error.GetDatetime());
                }
                if (HasTracingAttributes(error)) {
                    fluent
                        .Item("trace_id").Value(GetTraceId(error))
                        .Item("span_id").Value(GetSpanId(error));
                }
                if (depth > ErrorSerializationDepthLimit && !error.Attributes().Contains(OriginalErrorDepthAttribute)) {
                    fluent
                        .Item(OriginalErrorDepthAttribute).Value(depth);
                }
                for (const auto& [key, value] : error.Attributes().ListPairs()) {
                    fluent
                        .Item(key).Value(value);
                }
            })
            .DoIf(!error.InnerErrors().empty(), [&] (auto fluent) {
                SerializeInnerErrors(fluent, error, depth);
            })
            .DoIf(valueProducer != nullptr, [&] (auto fluent) {
                auto* consumer = fluent.GetConsumer();
                // NB: we are forced to deal with a bare consumer here because
                // we can't use void(TFluentMap) in a function signature as it
                // will lead to the inclusion of fluent.h in error.h and a cyclic
                // inclusion error.h -> fluent.h -> callback.h -> error.h
                consumer->OnKeyedItem(TStringBuf("value"));
                (*valueProducer)(consumer);
            })
        .EndMap();
}

void Deserialize(TError& error, const NYTree::INodePtr& node)
{
    error = {};

    auto mapNode = node->AsMap();

    static const TString CodeKey("code");
    auto code = TErrorCode(mapNode->GetChildValueOrThrow<i64>(CodeKey));
    if (code == NYT::EErrorCode::OK) {
        return;
    }

    error.SetCode(code);

    static const TString MessageKey("message");
    error.SetMessage(mapNode->GetChildValueOrThrow<TString>(MessageKey));

    static const TString AttributesKey("attributes");
    auto attributes = IAttributeDictionary::FromMap(mapNode->GetChildOrThrow(AttributesKey)->AsMap());

    error.SetAttributes(std::move(attributes));

    static const TString InnerErrorsKey("inner_errors");
    if (auto innerErrorsNode = mapNode->FindChild(InnerErrorsKey)) {
        for (const auto& innerErrorNode : innerErrorsNode->AsList()->GetChildren()) {
            error.MutableInnerErrors()->push_back(ConvertTo<TError>(innerErrorNode));
        }
    }
}

void Deserialize(TError& error, NYson::TYsonPullParserCursor* cursor)
{
    Deserialize(error, ExtractTo<INodePtr>(cursor));
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NYT::NProto::TError* protoError, const TError& error)
{
    if (error.IsOK()) {
        protoError->set_code(static_cast<int>(NYT::EErrorCode::OK));
        protoError->clear_message();
        return;
    }

    protoError->set_code(error.GetCode());
    protoError->set_message(error.GetMessage());

    protoError->clear_attributes();
    if (error.HasAttributes()) {
        ToProto(protoError->mutable_attributes(), error.Attributes());
    }

    auto addAttribute = [&] (const TString& key, const auto& value) {
        auto* protoItem = protoError->mutable_attributes()->add_attributes();
        protoItem->set_key(key);
        protoItem->set_value(ConvertToYsonString(value).ToString());
    };

    if (error.HasOriginAttributes()) {
        static const TString HostKey("host");
        addAttribute(HostKey, GetHost(error));

        static const TString PidKey("pid");
        addAttribute(PidKey, error.GetPid());

        static const TString TidKey("tid");
        addAttribute(TidKey, error.GetTid());

        static const TString ThreadName("thread");
        addAttribute(ThreadName, error.GetThreadName());

        static const TString FidKey("fid");
        addAttribute(FidKey, GetFid(error));
    } else if (IsErrorSanitizerEnabled() && HasHost(error)) {
        static const TString HostKey("host");
        addAttribute(HostKey, GetHost(error));
    }

    if (error.HasDatetime()) {
        static const TString DatetimeKey("datetime");
        addAttribute(DatetimeKey, error.GetDatetime());
    }

    if (HasTracingAttributes(error)) {
        static const TString TraceIdKey("trace_id");
        addAttribute(TraceIdKey, GetTraceId(error));

        static const TString SpanIdKey("span_id");
        addAttribute(SpanIdKey, GetSpanId(error));
    }

    protoError->clear_inner_errors();
    for (const auto& innerError : error.InnerErrors()) {
        ToProto(protoError->add_inner_errors(), innerError);
    }
}

void FromProto(TError* error, const NYT::NProto::TError& protoError)
{
    *error = {};

    if (protoError.code() == static_cast<int>(NYT::EErrorCode::OK)) {
        return;
    }

    error->SetCode(TErrorCode(protoError.code()));
    error->SetMessage(FromProto<TString>(protoError.message()));
    if (protoError.has_attributes()) {
        auto attributes = FromProto(protoError.attributes());

        error->SetAttributes(std::move(attributes));
    }
    *error->MutableInnerErrors() = FromProto<std::vector<TError>>(protoError.inner_errors());
}

////////////////////////////////////////////////////////////////////////////////

void TErrorSerializer::Save(TStreamSaveContext& context, const TErrorCode& errorCode)
{
    NYT::Save(context, static_cast<int>(errorCode));
}

void TErrorSerializer::Load(TStreamLoadContext& context, TErrorCode& errorCode)
{
    int value = 0;
    NYT::Load(context, value);
    errorCode = TErrorCode{value};
}

////////////////////////////////////////////////////////////////////////////////

void TErrorSerializer::Save(TStreamSaveContext& context, const TError& error)
{
    using NYT::Save;

    if (error.IsOK()) {
        // Fast path.
        Save(context, TErrorCode(NYT::EErrorCode::OK)); // code
        Save(context, TStringBuf());                    // message
        Save(context, IAttributeDictionaryPtr());       // attributes
        Save(context, std::vector<TError>());           // inner errors
        return;
    }

    Save(context, error.GetCode());
    Save(context, error.GetMessage());

    // Cf. TAttributeDictionaryValueSerializer.
    auto attributePairs = error.Attributes().ListPairs();
    size_t attributeCount = attributePairs.size();
    if (error.HasOriginAttributes()) {
        attributeCount += 5;
    }
    if (error.HasDatetime()) {
        attributeCount += 1;
    }
    if (HasTracingAttributes(error)) {
        attributeCount += 2;
    }

    if (attributeCount > 0) {
        // Cf. TAttributeDictionaryRefSerializer.
        Save(context, true);

        TSizeSerializer::Save(context, attributeCount);

        auto saveAttribute = [&] (const TString& key, const auto& value) {
            Save(context, key);
            Save(context, ConvertToYsonString(value));
        };

        if (error.HasOriginAttributes()) {
            static const TString HostKey("host");
            saveAttribute(HostKey, GetHost(error));

            static const TString PidKey("pid");
            saveAttribute(PidKey, error.GetPid());

            static const TString TidKey("tid");
            saveAttribute(TidKey, error.GetTid());

            static const TString ThreadNameKey("thread");
            saveAttribute(ThreadNameKey, error.GetThreadName());

            static const TString FidKey("fid");
            saveAttribute(FidKey, GetFid(error));
        }

        if (error.HasDatetime()) {
            static const TString DatetimeKey("datetime");
            saveAttribute(DatetimeKey, error.GetDatetime());
        }

        if (HasTracingAttributes(error)) {
            static const TString TraceIdKey("trace_id");
            saveAttribute(TraceIdKey, GetTraceId(error));

            static const TString SpanIdKey("span_id");
            saveAttribute(SpanIdKey, GetSpanId(error));
        }

        std::sort(attributePairs.begin(), attributePairs.end(), [] (const auto& lhs, const auto& rhs) {
            return lhs.first < rhs.first;
        });
        for (const auto& [key, value] : attributePairs) {
            Save(context, key);
            Save(context, value);
        }
    } else {
        Save(context, false);
    }

    Save(context, error.InnerErrors());
}

void TErrorSerializer::Load(TStreamLoadContext& context, TError& error)
{
    using NYT::Load;

    error = {};

    auto code = Load<TErrorCode>(context);
    auto message = Load<TString>(context);

    IAttributeDictionaryPtr attributes;
    if (Load<bool>(context)) {
        attributes = CreateEphemeralAttributes();
        TAttributeDictionarySerializer::LoadNonNull(context, attributes);
    }

    auto innerErrors = Load<std::vector<TError>>(context);

    if (code == NYT::EErrorCode::OK) {
        // Fast path.
        return;
    }

    error.SetCode(code);
    error.SetMessage(std::move(message));
    error.SetAttributes(std::move(attributes));
    *error.MutableInnerErrors() = std::move(innerErrors);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
