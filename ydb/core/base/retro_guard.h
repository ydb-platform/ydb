#pragma once

#include <concepts>
#include <optional>

#include <ydb/core/util/stlog.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/base/appdata_fwd.h>
#include <ydb/library/retro_tracing/retro_span_base.h>

namespace NKikimr {

template <class TEvent>
constexpr bool EventSupportsParentRetroSpan() {
    return std::is_same_v<TEvent, TEvBlobStorage::TEvVPut>;
}

template <class TSpan> requires std::derived_from<TSpan, NRetro::TRetroSpan>
struct TRetroGuard {
public:
    TRetroGuard() = default;

    template <class TEvent>
    TRetroGuard(const TEvent* ev) {
        if constexpr (EventSupportsParentRetroSpan<TEvent>()) {
            if (ev->Record.HasParentRetroSpan()) {
                NRetro::TFullSpanId spanId = NRetro::SpanIdToFullSpanId(
                        ev->Record.GetParentRetroSpan());
                Emplace(spanId, TActivationContext::Now());
            }
        }
    }

    ~TRetroGuard() {
        Drop();
    }

    TSpan* operator->() {
        return Span ? &*Span : nullptr;
    }

    const TSpan* operator->() const {
        return Span ? &*Span : nullptr;
    }

    TSpan* GetPtr() {
        return Span ? &*Span : nullptr;
    }

    template <class... Args>
    void Emplace(Args&&... args) {
        Span.emplace(std::forward<Args>(args)...);
    }

    void Drop() {
        if (Span) {
            Span->End(TActivationContext::Now());
            Span.reset();
        }
    }

    operator bool() const {
        return Span.has_value();
    }

private:
    std::optional<TSpan> Span;
};

} // namespace NKikimr
