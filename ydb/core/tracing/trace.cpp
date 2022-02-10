#include "trace.h"
#include "http.h"

#include <ydb/core/protos/tracing.pb.h>

#include <util/string/cast.h>

namespace NKikimr {
namespace NTracing {

TTrace::TTrace(ITrace::EType type, ITrace* parent)
    : Type(type)
    , SelfID(TTraceID::GenerateNew())
    , ParentID(parent ? parent->GetSelfID() : SelfID)
    , RootID(parent ? parent->GetRootID() : SelfID)
{}

TTraceID GetPbTraceID(const NKikimrTracing::TTraceID& proto) {
    return TTraceID(proto.GetRandomID(), proto.GetCreationTime());
}

TTrace::TTrace(const TString& serializedTrace) {
    NKikimrTracing::TTraceSignal pbTraceSignal;
    Y_PROTOBUF_SUPPRESS_NODISCARD pbTraceSignal.ParseFromString(serializedTrace);
    Type = static_cast<ITrace::EType>(pbTraceSignal.GetType());
    SelfID = GetPbTraceID(pbTraceSignal.GetSelfID());
    ParentID = GetPbTraceID(pbTraceSignal.GetParentID());
    RootID = GetPbTraceID(pbTraceSignal.GetRootID());
    for (size_t i = 0; i < pbTraceSignal.SignalsSize(); ++i) {
        auto& subSignal = pbTraceSignal.signals(i);
        Signals.push_back({ subSignal.GetType(), std::move(subSignal.GetSignal()) });
    }
}

ITrace* TTrace::CreateTrace(ITrace::EType type) {
    return new TTrace(type, this);
}

bool TTrace::Attach(THolder<ITraceSignal> signal) {
    TString str;
    bool result = signal->SerializeToString(str);
    Signals.push_back({ signal->GetType(), std::move(str) });
    return result;
}

TTraceID TTrace::GetSelfID() const {
    return SelfID;
}

TTraceID TTrace::GetParentID() const {
    return ParentID;
}

TTraceID TTrace::GetRootID() const {
    return RootID;
}

ui64 TTrace::GetSize() const {
    ui64 result = sizeof(TTrace);
    for (auto& serializedSignal : Signals)
        result += serializedSignal.Signal.length();
    return result;
}

void TTrace::OutHtml(TStringStream& str, TTraceInfo& traceInfo) const {
    NHttp::OutputTimeDropdown(str,traceInfo);
    OutHtml(str, traceInfo.TimestampInfo, []() { return TString(); });
}

void TTrace::OutHtml(TStringStream& str, const TTimestampInfo& tsInfo, std::function<TString()> getMyId) const {
    TSignalFactory& signalFactory = TSignalFactory::Instance();
    ui32 i = 0;
    TTimestampData tsData(tsInfo, TInstant::MicroSeconds(SelfID.CreationTime));
    for (auto& serializedSignal : Signals) {
        THolder<ITraceSignal> signal(signalFactory.Create(serializedSignal));
        if (signal) {
            signal->OutHtmlHeader(
                str,
                tsData,
                [&]() {
                    TString myId = getMyId();
                    return myId.length() ? myId + "_" + ToString(i) : ToString(i);
                }
            );
        } else {
            HTML(str) {
                DIV_CLASS("row") {
                    DIV_CLASS("col-md-12") {
                        str << "<Unknown signal type:" << serializedSignal.Type << ">";
                    }
                }
            }
        }
        ++i;
    }
}

void TTrace::OutText(TStringStream& str, TTimestampInfo& tsInfo, const TString& prefix) const {
    TSignalFactory& signalFactory = TSignalFactory::Instance();
    TTimestampData tsData(tsInfo, TInstant::MicroSeconds(SelfID.CreationTime));
    for (auto& serializedSignal : Signals) {
        THolder<ITraceSignal> signal(signalFactory.Create(serializedSignal));
        if (signal) {
            signal->OutText(
                str,
                tsData,
                prefix
            );
        } else {
            str << prefix << "<Unknown signal type:" << serializedSignal.Type << ">" << Endl;
        }
    }
}

ITrace::EType TTrace::GetType() const {
    return Type;
}

void SetPbTraceID(NKikimrTracing::TTraceID* proto, const TTraceID& traceId) {
    proto->SetRandomID(traceId.RandomID);
    proto->SetCreationTime(traceId.CreationTime);
}

bool TTrace::SerializeToString(TString& str) const {
    NKikimrTracing::TTraceSignal pbTraceSignal;
    pbTraceSignal.SetType(Type);
    SetPbTraceID(pbTraceSignal.MutableSelfID(), SelfID);
    SetPbTraceID(pbTraceSignal.MutableParentID(), ParentID);
    SetPbTraceID(pbTraceSignal.MutableRootID(), RootID);
    auto& pbTraces = *pbTraceSignal.MutableSignals();
    for (auto& signal : Signals) {
        auto& pbSignal = *pbTraces.Add();
        pbSignal.SetType(signal.Type);
        pbSignal.SetSignal(signal.Signal);
    }
    return pbTraceSignal.SerializeToString(&str);
}

void TTrace::OutSignalHtmlBody(TStringStream& str, const TTimestampInfo& tsInfo, TString signalId) {
    size_t prevPos = 0;
    size_t currPos = signalId.find('_');
    TList<ui64> signalAddress;
    while (currPos != TString::npos) {
        signalAddress.push_back(FromString<ui64>(signalId.substr(prevPos, currPos)));
        prevPos = currPos;
        currPos = signalId.find('_', currPos + 1);
    }
    // Last one
    signalAddress.push_back(FromString<ui64>(signalId.substr(prevPos, signalId.length())));
    // Start of recursion
    OutSignalHtmlBody(str, tsInfo, []() { return TString(); }, signalAddress);
}

void TTrace::OutSignalHtmlBody(TStringStream& str
    , const TTimestampInfo& tsInfo
    , std::function<TString()> getMyId
    , TList<ui64>& signalAddress
) {
    ui64 signalIndex = signalAddress.front();
    if (signalIndex >= Signals.size()) {
        str << "Signal index is out of range. Trace has " << Signals.size() << " signals.";
        return;
    }
    auto signalIterator = Signals.begin();
    for (ui64 i = 0; i < signalIndex; ++i) {
        ++signalIterator;
    }
    signalAddress.pop_front();
    TSignalFactory& signalFactory = TSignalFactory::Instance();
    THolder<ITraceSignal> signal(signalFactory.Create(*signalIterator));
    if (signal) {
        signal->OutHtmlBody(
            str,
            tsInfo,
            [signalIndex, getMyId]() {
                TString myId = getMyId();
                return myId.length() ? myId + "_" + ToString(signalIndex) : ToString(signalIndex);
            },
            signalAddress
            );
    } else {
        str << "<Unknown signal type:" << signalIterator->Type << ">" << Endl;
    }
}

ITrace* CreateTrace(ITrace::EType type) {
    return new TTrace(type, nullptr);
}

ITrace* CreateTrace(const TString& serializedTrace) {
    return new TTrace(serializedTrace);
}

}
}
