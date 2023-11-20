#pragma once
#include <ydb/core/base/defs.h>
#include <ydb/core/base/tracing.h>
#include <util/string/builder.h>
#include "http.h"

namespace NKikimr {
namespace NTracing {

template <size_t N>
struct TPad {
    int I;
};

template <size_t N>
inline TPad<N> Pad(int i) {
    return {i};
}

inline TStringBuilder& operator<<(TStringBuilder& o, const TPad<2>& p) {
    if (p.I < 10) {
        if (p.I >= 0) {
            o << '0';
        }
    }

    return o << p.I;
}

inline TStringBuilder& operator<<(TStringBuilder& o, const TPad<3>& p) {
    if (p.I < 100) {
        if (p.I >= 0) {
            if (p.I < 10) {
                o << '0' << '0';
            } else {
                o << '0';
            }
        }
    }

    return o << p.I;
}

inline TStringBuilder& operator<<(TStringBuilder& o, const TPad<6>& p) {
    if (p.I < 100000) {
        if (p.I >= 0) {
            if (p.I < 10) {
                o << '0' << '0' << '0' << '0' << '0';
            } else if (p.I < 100) {
                o << '0' << '0' << '0' << '0';
            } else if (p.I < 1000) {
                o << '0' << '0' << '0';
            } else if (p.I < 10000) {
                o << '0' << '0';
            } else {
                o << '0';
            }
        }
    }

    return o << p.I;
}

inline void WriteMicroSecondsToStream(TStringBuilder& os, ui32 microSeconds) {
    os << '.' << Pad<6>(microSeconds);
}

inline void WriteMilliSecondsToStream(TStringBuilder& os, ui32 milliSeconds) {
    os << '.' << Pad<3>(milliSeconds);
}

template <typename TDerived, typename TPbType, ITraceSignal::EType TSignalType>
class TTraceSignal : public ITraceSignal {
public:
    TTraceSignal() {
        PbSignal.SetTimeStamp(TInstant::Now().MicroSeconds());
    }

    TTraceSignal(const TString& serializedSignal) {
        Y_PROTOBUF_SUPPRESS_NODISCARD PbSignal.ParseFromString(serializedSignal);
    }

    bool SerializeToString(TString& str) const override {
        return PbSignal.SerializeToString(&str);
    }

    EType GetType() const override {
        return TSignalType;
    }

    // Default realizations:

    virtual void OutHtmlHeader(TStringStream& str, TTimestampData& tsData, std::function<TString()> getMyId) const override {
        Y_UNUSED(getMyId);
        HTML(str) {
            DIV_CLASS("row") {
                DIV_CLASS("col-md-12") {
                    OutText(str, tsData);
                }
            }
        }
    }

    virtual void OutHtmlBody(
        TStringStream& str,
        const TTimestampInfo& tsInfo,
        std::function<TString()> getMyId,
        TList<ui64>& signalAddress
    ) const override {
        Y_UNUSED(tsInfo);
        Y_UNUSED(getMyId);
        Y_UNUSED(signalAddress);
        str << "ITraceSignal::OutHtmlBody is undefined for type " << TSignalType;
    }

protected:
    TString TimeStamp(TTimestampData& tsData) const {
        TStringBuilder str;
        switch (tsData.Mode) {
        case TTimestampInfo::ModeDisabled: {
            return TString();
        }
        case TTimestampInfo::ModeAbsolute:
        case TTimestampInfo::ModeDefault: {
            TInstant timestamp = TInstant::MicroSeconds(PbSignal.GetTimeStamp());
            str << timestamp.FormatLocalTime("%Y-%m-%d %H:%M:%S");
            WriteWithPrecision(str, timestamp, tsData.Precision);
            break;
        }
        case TTimestampInfo::ModeFirst:
        case TTimestampInfo::ModePrevious: {
            TInstant timestamp = TInstant::MicroSeconds(PbSignal.GetTimeStamp());
            TDuration diffTime = timestamp - tsData.BaseTime;
            str << diffTime.Hours() << "h " << Pad<2>(diffTime.Minutes() % 60) << "m "
                << Pad<2>(diffTime.Seconds() % 60);
            WriteWithPrecision(str, diffTime, tsData.Precision);
            str << "s";
            if (tsData.Mode == TTimestampInfo::ModePrevious) {
                tsData.BaseTime = timestamp;
            }
            break;
        }
        }
        return str;
    }

    template <typename TimeType>
    void WriteWithPrecision(TStringBuilder& str, const TimeType& time, TTimestampInfo::EPrecision precision) const {
        switch (precision) {
        case TTimestampInfo::PrecisionSeconds:
            break;
        case TTimestampInfo::PrecisionMilliseconds:
        case TTimestampInfo::PrecisionDefault:
            WriteMilliSecondsToStream(str, time.MilliSecondsOfSecond());
            break;
        case TTimestampInfo::PrecisionMicroseconds:
            WriteMicroSecondsToStream(str, time.MicroSecondsOfSecond());
            break;
        }
    }

    using MyPbType = TPbType;
    using TMyBase = TTraceSignal<TDerived, TPbType, TSignalType>;
    MyPbType PbSignal;
};


}
}
