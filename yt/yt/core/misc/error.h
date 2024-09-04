#pragma once

#include "public.h"
#include "stripped_error.h"

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/core/tracing/public.h>

#include <yt/yt/core/concurrency/public.h>

#include <library/cpp/yt/error/origin_attributes.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NOrigin {

void EnableOriginOverrides();

} // namespace NOrigin

////////////////////////////////////////////////////////////////////////////////

bool HasHost(const TError& error) noexcept;
TStringBuf GetHost(const TError& error) noexcept;

NConcurrency::TFiberId GetFid(const TError& error) noexcept;

bool HasTracingAttributes(const TError& error) noexcept;
NTracing::TTraceId GetTraceId(const TError& error) noexcept;
NTracing::TSpanId GetSpanId(const TError& error) noexcept;

void SetTracingAttributes(TError* error, const NTracing::TTracingAttributes& attributes);

////////////////////////////////////////////////////////////////////////////////

constexpr int ErrorSerializationDepthLimit = 16;

////////////////////////////////////////////////////////////////////////////////

void Serialize(
    const TError& error,
    NYson::IYsonConsumer* consumer,
    const std::function<void(NYson::IYsonConsumer*)>* valueProducer = nullptr,
    int depth = 0);
void Deserialize(
    TError& error,
    const NYTree::INodePtr& node);
void Deserialize(
    TError& error,
    NYson::TYsonPullParserCursor* cursor);

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TError* protoError, const TError& error);
void FromProto(TError* error, const NProto::TError& protoError);

////////////////////////////////////////////////////////////////////////////////

struct TErrorSerializer
{
    static void Save(TStreamSaveContext& context, const TErrorCode& errorCode);
    static void Load(TStreamLoadContext& context, TErrorCode& errorCode);

    static void Save(TStreamSaveContext& context, const TError& error);
    static void Load(TStreamLoadContext& context, TError& error);
};

template <class C, class Cond>
struct TSerializerTraits<
    TErrorCode,
    C,
    Cond>
{
    using TSerializer = TErrorSerializer;
};

template <class C, class Cond>
struct TSerializerTraits<
    TError,
    C,
    Cond>
{
    using TSerializer = TErrorSerializer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
