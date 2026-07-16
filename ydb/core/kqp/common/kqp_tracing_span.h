#pragma once

#include <ydb/library/actors/wilson/wilson_span.h>

namespace NKikimr::NKqp {

// Pairs the dev (Wilson) span with its user-facing counterpart so a call site emits both
// trees at once. Either side may be empty (unsampled channel) — every method is a no-op there.
// The user tree is a pruned projection of the dev tree: where a phase has no user meaning,
// callers use Dev() directly and the user side simply stops.
class TSpanBundle {
public:
    TSpanBundle() = default;
    TSpanBundle(NWilson::TSpan dev, NWilson::TSpan user)
        : Dev_(std::move(dev))
        , User_(std::move(user))
    {}

    // Per-channel access: channel-specific attributes, and the dev reference the planner holds.
    NWilson::TSpan& Dev() { return Dev_; }
    NWilson::TSpan& User() { return User_; }
    const NWilson::TSpan& Dev() const { return Dev_; }
    const NWilson::TSpan& User() const { return User_; }

    // Default parent is dev — children spawned off GetTraceId() join the dev tree.
    NWilson::TTraceId GetTraceId() const { return Dev_.GetTraceId(); }
    NWilson::TTraceId UserTraceId() const { return User_.GetTraceId(); }

    explicit operator bool() const { return Dev_ || User_; }

    // Shared attribute — fans out to whichever side is live. Channel-specific attributes
    // (dev internals vs user semconv) go through Dev()/User() instead.
    template <class TName, class TValue>
    TSpanBundle& Attribute(TName&& name, TValue&& value) {
        if (Dev_) {
            Dev_.Attribute(name, value);
        }
        if (User_) {
            User_.Attribute(std::forward<TName>(name), std::forward<TValue>(value));
        }
        return *this;
    }

    // Dual child. Names differ by design: dev is verbose/operational, user is curated.
    TSpanBundle CreateChild(ui8 devVerbosity, ui8 userVerbosity,
            std::variant<std::optional<TString>, const char*> devName,
            std::variant<std::optional<TString>, const char*> userName,
            NWilson::TFlags flags = NWilson::EFlags::AUTO_END) const {
        return TSpanBundle(
            Dev_ ? Dev_.CreateChild(devVerbosity, std::move(devName), flags) : NWilson::TSpan{},
            User_ ? User_.CreateChild(userVerbosity, std::move(userName), flags) : NWilson::TSpan{});
    }

    void EndOk() {
        if (Dev_) {
            Dev_.EndOk();
        }
        if (User_) {
            User_.EndOk();
        }
    }

    template <class T>
    void EndError(T&& error) {
        if (Dev_) {
            Dev_.EndError(error);
        }
        if (User_) {
            User_.EndError(std::forward<T>(error));
        }
    }

    void End() {
        if (Dev_) {
            Dev_.End();
        }
        if (User_) {
            User_.End();
        }
    }

private:
    NWilson::TSpan Dev_;
    NWilson::TSpan User_;
};

} // namespace NKikimr::NKqp
