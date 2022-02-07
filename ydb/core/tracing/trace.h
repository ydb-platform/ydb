#pragma once
#include <ydb/core/base/defs.h>
#include <ydb/core/base/tracing.h>

namespace NKikimr {
namespace NTracing {

class TTrace : public ITrace {
public:
    TTrace(ITrace::EType type, ITrace* parent);
    TTrace(const TString& serializedTrace);

    ITrace* CreateTrace(ITrace::EType type) override;
    bool Attach(THolder<ITraceSignal> signal) override;
    TTraceID GetSelfID() const override;
    TTraceID GetParentID() const override;
    TTraceID GetRootID() const override;
    ui64 GetSize() const override;
    void OutHtml(TStringStream& str, TTraceInfo& traceInfo) const override;
    void OutHtml(TStringStream& str, const TTimestampInfo& tsInfo, std::function<TString()> getMyId) const override;
    void OutSignalHtmlBody(TStringStream& str, const TTimestampInfo& tsInfo, TString signalId) override;
    void OutSignalHtmlBody(TStringStream& str, const TTimestampInfo& tsInfo, std::function<TString()> getMyId, TList<ui64>& signalAddress) override;
    void OutText(TStringStream& str, TTimestampInfo& tsInfo, const TString& prefix) const override;
    ITrace::EType GetType() const override;
    bool SerializeToString(TString& str) const override;

private:
    ITrace::EType Type;
    TTraceID SelfID;
    TTraceID ParentID;
    TTraceID RootID;
    TList<TSerializedSignal> Signals;
};

}
}
