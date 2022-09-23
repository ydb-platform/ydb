#pragma once
#include "wilson_span.h"
#include <library/cpp/json/writer/json_value.h>

namespace NWilson {

class TProfileSpan: public TSpan {
private:
    using TBase = TSpan;
    class TMinMaxPair {
    private:
        std::optional<TInstant> MinMinInstance;
        std::optional<TInstant> MaxMinInstance;
        std::optional<TInstant> MaxInstance;
    public:
        static TMinMaxPair BuildMin(const TInstant value);
        static TMinMaxPair BuildMax(const TInstant value);
        void AddMax(const TInstant instance);
        void AddMin(const TInstant instance);
        TString ToString() const;
    };
    mutable NJson::TJsonValue ResultTimes = NJson::JSON_MAP;
    std::map<TString, TMinMaxPair> PairInstances;
    std::vector<NJson::TJsonValue*> CurrentJsonPath;
    mutable TInstant LastNoGuards = Now();
    const TInstant StartTime = Now();
    bool Enabled = true;

    void FlushNoGuards() const;
    TProfileSpan() = default;
public:
    TProfileSpan(const ui8 verbosity, TTraceId parentId, std::optional<TString> name);
    ~TProfileSpan();

    TProfileSpan BuildChildrenSpan(std::optional<TString> name, const ui8 verbosity = 0) const;

    using TBase::TBase;
    TString ProfileToString() const;

    TProfileSpan& SetEnabled(const bool value) {
        Enabled = value;
        return *this;
    }

    class TGuard {
    private:
        TProfileSpan& Owner;
        const TInstant Start = Now();
        NJson::TJsonValue* CurrentNodeDuration;
    public:
        TGuard(const TString& event, TProfileSpan& owner, const TString& info);
        ~TGuard();
    };

    template <class TEventId, class T = TString>
    TGuard StartStackTimeGuard(const TEventId event, const T& info = Default<T>()) {
        return TGuard(::ToString(event), *this, ::ToString(info));
    }

    template <class TEventId, class T = TString>
    void AddMin(const TEventId event, const T& info = Default<T>()) {
        AddMin(::ToString(event), ::ToString(info));
    }

    template <class TEventId, class T = TString>
    void AddMax(const TEventId event, const T& info = Default<T>()) {
        AddMax(::ToString(event), ::ToString(info));
    }

    void AddMin(const TString& eventId, const TString& info);
    void AddMax(const TString& eventId, const TString& info);

};

} // NWilson
