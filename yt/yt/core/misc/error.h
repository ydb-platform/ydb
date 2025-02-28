#pragma once

#include "public.h"

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/core/tracing/public.h>

#include <yt/yt/core/concurrency/public.h>

#include <library/cpp/yt/error/error.h>
#include <library/cpp/yt/error/origin_attributes.h>

#include <util/generic/noncopyable.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

void EnableErrorOriginOverrides();

} // namespace NDetail

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

// A fiber-local set of attributes to enrich all errors with.
class TErrorCodicils
{
public:
    using TGetter = std::function<std::string()>;

    class TGuard
        : public TNonCopyable
    {
    public:
        ~TGuard();

    private:
        friend class TErrorCodicils;
        TGuard(std::string key, TGetter oldGetter);
        std::string Key_;
        TGetter OldGetter_;
    };

    // Call from single-threaded bootstrapping code. Errors will not be enriched otherwise.
    static void Initialize();

    // Gets or creates an instance for this fiber.
    static TErrorCodicils& GetOrCreate();

    // Gets the instance for this fiber if one was created previously.
    static TErrorCodicils* MaybeGet();

    // Evaluates the codicil for the key if one was set.
    static std::optional<std::string> MaybeEvaluate(const std::string& key);

    // Sets the getter and returns an RAII object to restore the previous value on desctruction.
    static TGuard Guard(std::string key, TGetter getter);

    // Adds error attributes.
    void Apply(TError& error) const;

    // Sets the getter (or erases if the getter is empty).
    void Set(std::string key, TGetter getter);

    // Gets the getter.
    TGetter Get(const std::string& key) const;

private:
    THashMap<std::string, TGetter> Getters_;
    static bool Initialized_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define ERROR_INL_H_
#include "error-inl.h"
#undef ERROR_INL_H_
