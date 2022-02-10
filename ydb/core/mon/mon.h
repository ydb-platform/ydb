#pragma once

#include <library/cpp/monlib/service/monservice.h> 
#include <library/cpp/monlib/dynamic_counters/counters.h> 
#include <library/cpp/monlib/service/pages/tablesorter/css_mon_page.h> 
#include <library/cpp/monlib/service/pages/tablesorter/js_mon_page.h> 

#include <library/cpp/actors/core/mon.h>

namespace NActors {

    class TActorSystem;
    struct TActorId; 

    class TMon : public NMonitoring::TMonService2 {
    public:
        using TRequestAuthorizer = std::function<IEventHandle*(const NActors::TActorId& owner, NMonitoring::IMonHttpRequest& request)>;

        struct TConfig {
            ui16 Port = 0;
            TString Address;
            ui32 Threads = 10;
            TString Title;
            TString Host;
            TRequestAuthorizer Authorizer = DefaultAuthorizer;
            TVector<TString> AllowedSIDs;
            TString RedirectMainPageTo;
        };

        TMon(TConfig config);
        virtual ~TMon();
        void Start();
        void Stop();

        void Register(NMonitoring::IMonPage *page);
        NMonitoring::TIndexMonPage *RegisterIndexPage(const TString &path, const TString &title);

        struct TRegisterActorPageFields {
            TString Title;
            TString RelPath;
            TActorSystem* ActorSystem;
            NMonitoring::TIndexMonPage* Index;
            bool PreTag = false;
            TActorId ActorId;
            bool UseAuth = true;
            TVector<TString> AllowedSIDs;
        };

        NMonitoring::IMonPage* RegisterActorPage(TRegisterActorPageFields fields);
        NMonitoring::IMonPage *RegisterActorPage(NMonitoring::TIndexMonPage *index, const TString &relPath,
            const TString &title, bool preTag, TActorSystem *actorSystem, const TActorId &actorId, bool useAuth = true); 
        NMonitoring::IMonPage *RegisterCountersPage(const TString &path, const TString &title, TIntrusivePtr<NMonitoring::TDynamicCounters> counters);
        NMonitoring::IMonPage *FindPage(const TString &relPath);
        NMonitoring::TIndexMonPage *FindIndexPage(const TString &relPath);
        void OutputIndexPage(IOutputStream& out) override;
        void SetAllowedSIDs(const TVector<TString>& sids); // sets allowed users/groups for this web interface
        ui16 GetListenPort();

        static NActors::IEventHandle* DefaultAuthorizer(const NActors::TActorId& owner, NMonitoring::IMonHttpRequest& request);

    protected:
        typedef NMonitoring::TMonService2 TBase;
        TConfig Config;
    };

} // NActors
