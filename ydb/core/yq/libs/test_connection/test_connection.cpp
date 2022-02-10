#include "events/events.h" 
#include "probes.h" 
#include "test_connection.h" 
 
#include <library/cpp/actors/core/actor_bootstrapped.h> 
#include <library/cpp/lwtrace/mon/mon_lwtrace.h> 
#include <library/cpp/monlib/service/pages/templates.h> 
 
#include <ydb/core/mon/mon.h> 
#include <ydb/core/yq/libs/config/yq_issue.h> 
 
namespace NYq { 
 
using namespace NActors; 
 
class TTestConnectionActor : public NActors::TActorBootstrapped<TTestConnectionActor> { 
    NMonitoring::TDynamicCounterPtr Counters; 
    NConfig::TTestConnectionConfig Config; 
 
public: 
    TTestConnectionActor(const NConfig::TTestConnectionConfig& config, const NMonitoring::TDynamicCounterPtr& counters) 
        : Counters(counters) 
        , Config(config) 
    { 
    } 
 
    static constexpr char ActorName[] = "YQ_TEST_CONNECTION"; 
 
    void Bootstrap() { 
        TC_LOG_D("Starting yandex query test connection. Actor id: " << SelfId()); 
 
        NLwTraceMonPage::ProbeRegistry().AddProbesList(LWTRACE_GET_PROBES(YQ_TEST_CONNECTION_PROVIDER)); 
 
        Become(&TTestConnectionActor::StateFunc); 
    } 
 
    STRICT_STFUNC(StateFunc, 
        hFunc(TEvTestConnection::TEvTestConnectionRequest, Handle); 
        hFunc(NMon::TEvHttpInfo, Handle); 
    ) 
 
    void Handle(TEvTestConnection::TEvTestConnectionRequest::TPtr& ev) { 
        YandexQuery::TestConnectionRequest request = std::move(ev->Get()->Request); 
        TC_LOG_T("TestConnectionRequest: " << request.DebugString()); 
        Send(ev->Sender, new TEvTestConnection::TEvTestConnectionResponse(NYql::TIssues{MakeErrorIssue(TIssuesIds::INTERNAL_ERROR, "Unimplemented yet")}), 0, ev->Cookie); 
    } 
 
    void Handle(NMon::TEvHttpInfo::TPtr& ev) { 
        TStringStream str; 
        HTML(str) { 
            PRE() { 
                str << "Current config:" << Endl; 
                str << Config.DebugString() << Endl; 
                str << Endl; 
            } 
        } 
        Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str())); 
    } 
}; 
 
NActors::TActorId TestConnectionActorId() { 
    constexpr TStringBuf name = "TSTCONN"; 
    return NActors::TActorId(0, name); 
} 
 
NActors::IActor* CreateTestConnectionActor(const NConfig::TTestConnectionConfig& config, const NMonitoring::TDynamicCounterPtr& counters) { 
    return new TTestConnectionActor(config, counters); 
} 
 
} // namespace NYq 
