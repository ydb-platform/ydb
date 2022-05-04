#pragma once

#include <library/cpp/lwtrace/protos/lwtrace.pb.h>
#include <library/cpp/monlib/service/monservice.h>
#include <library/cpp/lwtrace/control.h>

#include <util/generic/vector.h>

namespace NLwTraceMonPage {

class TDashboardRegistry {
    THashMap<TString, NLWTrace::TDashboard> Dashboards;
    TMutex Mutex;
public:
    void Register(const NLWTrace::TDashboard& dashboard);
    void Register(const TVector<NLWTrace::TDashboard>& dashboards);
    void Register(const TString& dashText);
    bool Get(const TString& name, NLWTrace::TDashboard& dash);
    void Output(TStringStream& ss);
};

void RegisterPages(NMonitoring::TIndexMonPage* index, bool allowUnsafe = false);
NLWTrace::TProbeRegistry& ProbeRegistry(); // This is not safe to use this function before main()
NLWTrace::TManager& TraceManager(bool allowUnsafe = false);
TDashboardRegistry& DashboardRegistry();

} // namespace NLwTraceMonPage
