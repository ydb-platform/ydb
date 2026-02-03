#pragma once

#include <util/generic/size_literals.h>
#include <util/system/compiler.h>
#include <util/system/types.h>
#include <util/system/yassert.h>

#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/actors/wilson/wilson_trace.h>

#include <concepts>
#include <memory>

namespace NRetroTracing {

class TRetroSpan {
private:
    // User of the library must provide the definition of this method
    // See UT for implementation example
    static TRetroSpan* DeserializeImpl(ui32 type, ui32 size, const char* data);

    static TRetroSpan* Deserialize(const char* data);

public:
    TRetroSpan(ui32 type, ui32 size, NWilson::TTraceId&& traceId);
    virtual ~TRetroSpan() = default;

    static std::unique_ptr<TRetroSpan> DeserializeToUnique(const char* data);
    static std::shared_ptr<TRetroSpan> DeserializeToShared(const char* data);

    ui32 GetType() const;
    ui32 GetSize() const;

    const void* GetData() const;
    void* GetDataMut();

    NWilson::TTraceId GetTraceId() const;
    void SetTraceId(NWilson::TTraceId&& traceId);

    virtual void Serialize(void* destination) const;
    virtual std::unique_ptr<NWilson::TSpan> MakeWilsonSpan();

    template <class T>
    const T* Cast() const {
        return reinterpret_cast<const T*>(this);
    }

    template <class T>
    T* Cast() {
        return reinterpret_cast<T*>(this);
    }

public:
    static constexpr ui32 MaxPossibleSpanSize = 1_KB;

private:
    ui32 Type = 0;
    ui32 Size = 0;

    NWilson::TTraceId TraceId = NWilson::TTraceId{};
};

template <ui32 Id, typename T>
class TTypedRetroSpan : public TRetroSpan {
protected:
    static constexpr ui32 TypeId = Id;
    using TThis = TTypedRetroSpan<TypeId, T>;

public:
    TTypedRetroSpan(NWilson::TTraceId&& traceId = NWilson::TTraceId{})
        : TRetroSpan(Id, sizeof(T), std::move(traceId))
    {}

    // Constructs retro-span from args for wilson span
    static T Construct(ui8 verbosity, const NWilson::TTraceId& parentId, const char* name,
            NWilson::TFlags flags = NWilson::EFlags::NONE,
            NActors::TActorSystem* actorSystem = nullptr) {
        Y_UNUSED(verbosity);
        Y_UNUSED(name);
        Y_UNUSED(flags);
        Y_UNUSED(actorSystem);
        T res;
        res.SetTraceId(NWilson::TTraceId(parentId));
        return res;
    }

    virtual void Serialize(void* destination) const override {
        std::memcpy(destination, GetData(), GetSize());
    }
};

} // namespace NRetroTracing
