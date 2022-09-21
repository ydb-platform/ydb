#pragma once
#include "defs.h"
#include "traceid.h"
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/threading/light_rw_lock/lightrwlock.h>

namespace NKikimr {
namespace NTracing {

struct TTimestampInfo {
    enum EMode : ui32 {
        ModeDefault,
        ModeDisabled,
        ModeAbsolute,
        ModeFirst,
        ModePrevious
    } Mode;

    enum EPrecision : ui32 {
        PrecisionDefault,
        PrecisionSeconds,
        PrecisionMilliseconds,
        PrecisionMicroseconds
    } Precision;

    TTimestampInfo()
        : Mode(ModeDefault)
        , Precision(PrecisionDefault)
    {}

    TTimestampInfo(EMode mode, EPrecision precision)
        : Mode(mode)
        , Precision(precision)
    {}

    static TString GetTextMode(EMode mode) {
        switch (mode) {
        case ModeDisabled:
            return "Disabled";
        case ModeAbsolute:
        case ModeDefault:
            return "Absolute";
        case ModeFirst:
            return "Relative to first";
        case ModePrevious:
            return "Relative to previous";
        default:
            return "Unknown mode";
        }
    }

    static TString GetTextPrecision(EPrecision precision) {
        switch (precision) {
        case PrecisionSeconds:
            return "Seconds";
        case PrecisionMilliseconds:
        case PrecisionDefault:
            return "Milliseconds";
        case PrecisionMicroseconds:
            return "Microseconds";
        default:
            return "Unknown precision";
        }
    }
};

struct TTimestampData : TTimestampInfo {
    TTimestampData(const TTimestampInfo& timestampInfo, TInstant baseTime)
        : TTimestampInfo(timestampInfo)
        , BaseTime(baseTime)
    {}

    TInstant BaseTime;
};

struct TTraceInfo {
    ui32 NodeId;
    ui64 TabletId;
    TTraceID TraceId;
    TTimestampInfo TimestampInfo;
};

class ITraceSignal {
public:
    typedef ui32 EType;
    enum ETypeSpace {
        ES_TABLET_BOOTSTRAP = 1
        // ES_NEXT_TYPE_GROUP = 1000
    };
    virtual ~ITraceSignal() = default;

    virtual bool SerializeToString(TString& str) const = 0;
    virtual EType GetType() const = 0;

    virtual void OutHtmlHeader(
        TStringStream& str,
        TTimestampData& tsData,
        std::function<TString()> getMyId
    ) const = 0;

    virtual void OutHtmlBody(
        TStringStream& str,
        const TTimestampInfo& tsInfo,
        std::function<TString()> getMyId,
        TList<ui64>& signalAddress
    ) const = 0;

    virtual void OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix = TString()) const = 0;
};

class ITrace {
public:
    enum EType {
        TypeSysTabletBootstrap,
        TypeReqRebuildHistoryGraph
    };

    struct TSerializedSignal {
        ITraceSignal::EType Type;
        TString Signal;
    };

    virtual ~ITrace() = default;

    virtual ITrace* CreateTrace(ITrace::EType type) = 0;
    virtual bool Attach(THolder<ITraceSignal> signal) = 0;
    virtual TTraceID GetSelfID() const = 0;
    virtual TTraceID GetParentID() const = 0;
    virtual TTraceID GetRootID() const = 0;
    virtual ui64 GetSize() const = 0;
    // Start of recursion
    virtual void OutHtml(TStringStream& str, TTraceInfo& traceInfo) const = 0;
    virtual void OutHtml(TStringStream& str, const TTimestampInfo& tsInfo, std::function<TString()> getMyId) const = 0;
    // Start of recursion
    virtual void OutSignalHtmlBody(TStringStream& str, const TTimestampInfo& tsInfo, TString signalId) = 0;
    virtual void OutSignalHtmlBody(
        TStringStream& str,
        const TTimestampInfo& tsInfo,
        std::function<TString()> getMyId,
        TList<ui64>& signalAddress
    ) = 0;
    virtual void OutText(TStringStream& str, TTimestampInfo& tsInfo, const TString& prefix = TString()) const = 0;
    virtual EType GetType() const = 0;
    virtual bool SerializeToString(TString& str) const = 0;
};

ITrace* CreateTrace(ITrace::EType type);
ITrace* CreateTrace(const TString& serializedTrace);

class ISignalCreator {
public:
    virtual ITraceSignal* Create(const TString& serializedSignal) const = 0;
    virtual ~ISignalCreator() = default;
};

template <typename TSignalType>
class TSignalCreator : public ISignalCreator {
public:
    virtual ITraceSignal* Create(const TString& serializedSignal) const {
        return new TSignalType(serializedSignal);
    }
};

class TSignalFactory {
public:
    static TSignalFactory& Instance() {
        static TSignalFactory SingleFactory;
        return SingleFactory;
    }

    template <typename TSignalType>
    void Add(ITraceSignal::EType signalType) {
        TLightWriteGuard g{ Lock };
        auto it = Factorymap.find(signalType);
        if (it == Factorymap.end()) {
            Factorymap[signalType] = MakeHolder<TSignalCreator<TSignalType>>();
        }
    }

    ITraceSignal* Create(const ITrace::TSerializedSignal& signal) {
        TLightReadGuard g{ Lock };
        auto it = Factorymap.find(signal.Type);
        if (it != Factorymap.end()) {
            return it->second->Create(signal.Signal);
        }
        return nullptr;
    }

private:
    using TFactoryMap = THashMap<ui32, THolder<ISignalCreator>>;
    TFactoryMap Factorymap;
    TLightRWLock Lock;
};

class ITraceCollection {
public:
    virtual ~ITraceCollection() = default;
    virtual void AddTrace(ui64 tabletID, ITrace* trace) = 0;
    // Shows all tablet IDs for which it has data
    virtual void GetTabletIDs(TVector<ui64>& tabletIDs) const = 0;
    virtual bool HasTabletID(ui64 tabletID) const = 0;
    // Returns a list of introspection trace ids and creation times for given tabletId
    virtual bool GetTraces(ui64 tabletID, TVector<TTraceID>& result) = 0;
    virtual ITrace* GetTrace(ui64 tabletID, TTraceID& traceID) = 0;
};

ITraceCollection* CreateTraceCollection(TIntrusivePtr<::NMonitoring::TDynamicCounters> counters = nullptr);

}
}

template<>
inline void Out<NKikimr::NTracing::TTimestampInfo::EMode>(IOutputStream& o
    , const NKikimr::NTracing::TTimestampInfo::EMode x) {
    o << static_cast<ui32>(x);
}

template<>
inline void Out<NKikimr::NTracing::TTimestampInfo::EPrecision>(IOutputStream& o
    , const NKikimr::NTracing::TTimestampInfo::EPrecision x) {
    o << static_cast<ui32>(x);
}
