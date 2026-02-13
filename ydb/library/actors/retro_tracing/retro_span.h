#pragma once

#include <util/system/compiler.h>
#include <util/system/types.h>
#include <util/system/yassert.h>

#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/actors/wilson/wilson_trace.h>

#include <concepts>
#include <memory>

namespace NRetroTracing {

class TRetroSpan {
protected:
    using TStatusCode = NWilson::NTraceProto::Status::StatusCode;
    using EStatusCode = NWilson::NTraceProto::Status;

private:
    // User of the library must provide the definition of this method
    // See UT for implementation example
    static TRetroSpan* DeserializeImpl(ui32 type, ui32 size, const void* data);

    static TRetroSpan* Deserialize(const void* data);

public:
    TRetroSpan(ui32 type, ui32 size);
    virtual ~TRetroSpan();

    TRetroSpan(TRetroSpan&&) = default;
    TRetroSpan(const TRetroSpan&) = default;

    TRetroSpan& operator=(TRetroSpan&&) = default;
    TRetroSpan& operator=(const TRetroSpan&) = default;

    static std::unique_ptr<TRetroSpan> DeserializeToUnique(const void* data);

    ui32 GetType() const;
    ui32 GetSize() const;

    TStatusCode GetStatusCode() const;

    const void* GetData() const;
    void* GetDataMut();

    NWilson::TTraceId GetParentId() const;
    NWilson::TTraceId GetTraceId() const;
    void AttachToTrace(const NWilson::TTraceId& parentId);

    virtual void Serialize(void* destination) const;
    virtual std::unique_ptr<NWilson::TSpan> MakeWilsonSpan();

    template <class T>
    requires std::derived_from<T, TRetroSpan>
    const T* Cast() const {
        return dynamic_cast<const T*>(this);
    }

    template <class T>
    requires std::derived_from<T, TRetroSpan>
    T* CastMut() {
        return dynamic_cast<T*>(this);
    }

    virtual TString GetName() const;
    virtual TString ToString() const;

    void End();
    void EndError();
    void EndOk();
    bool IsEnded() const;

    TInstant GetStartTs() const;
    TInstant GetEndTs() const;

    void EnableAutoEnd();

public:
    static constexpr ui8 DefaultVerbosity = 1;

private:
    ui32 Type = 0;
    ui32 Size = 0;

protected:
    TStatusCode StatusCode = EStatusCode::STATUS_CODE_UNSET;
    NWilson::TFlags Flags = NWilson::EFlags::NONE;

    NWilson::TTraceId ParentId = NWilson::TTraceId{};
    NWilson::TTraceId SpanId = NWilson::TTraceId{};

    TInstant StartTs = TInstant::Zero();
    TInstant EndTs = TInstant::Zero();
};

} // namespace NRetroTracing
