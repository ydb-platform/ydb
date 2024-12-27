#pragma once

#include <future>
#include <library/cpp/monlib/service/monservice.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/service/pages/index_mon_page.h>
#include <library/cpp/monlib/service/pages/resources/css_mon_page.h>
#include <library/cpp/monlib/service/pages/resources/fonts_mon_page.h>
#include <library/cpp/monlib/service/pages/resources/js_mon_page.h>
#include <library/cpp/monlib/service/pages/tablesorter/css_mon_page.h>
#include <library/cpp/monlib/service/pages/tablesorter/js_mon_page.h>

#include <ydb/library/actors/core/mon.h>
#include <ydb/library/actors/http/http.h>
#include <yql/essentials/public/issue/yql_issue.h>
#include <ydb/public/sdk/cpp/client/ydb_types/status/status.h>

#include "mon.h"

namespace NActors {

void MakeJsonErrorReply(NJson::TJsonValue& jsonResponse, TString& message, const NYdb::TStatus& status);
void MakeJsonErrorReply(NJson::TJsonValue& jsonResponse, TString& message, const NYql::TIssues& issues, NYdb::EStatus status);

class TMon {
public:
    using TRequestAuthorizer = std::function<IEventHandle*(const TActorId& owner, NHttp::THttpIncomingRequest* request)>;

    static IEventHandle* DefaultAuthorizer(const TActorId& owner, NHttp::THttpIncomingRequest* request);

    struct TConfig {
        ui16 Port = 0;
        TString Address;
        ui32 Threads = 10;
        TString Title;
        TString Host;
        TRequestAuthorizer Authorizer = DefaultAuthorizer;
        TVector<TString> AllowedSIDs;
        TString RedirectMainPageTo;
        TString Certificate;
        ui32 MaxRequestsPerSecond = 0;
        TDuration InactivityTimeout = TDuration::Minutes(2);
    };

    TMon(TConfig config);
    virtual ~TMon() = default;

    std::future<void> Start(TActorSystem* actorSystem); // signals when monitoring is ready
    void Stop();

    void Register(NMonitoring::IMonPage* page);
    NMonitoring::TIndexMonPage* RegisterIndexPage(const TString& path, const TString& title);

    struct TRegisterActorPageFields {
        TString Title;
        TString RelPath;
        TActorSystem* ActorSystem;
        NMonitoring::TIndexMonPage* Index;
        bool PreTag = false;
        TActorId ActorId;
        bool UseAuth = true;
        TVector<TString> AllowedSIDs;
        bool SortPages = true;
        TString MonServiceName = "utils";
    };

    NMonitoring::IMonPage* RegisterActorPage(TRegisterActorPageFields fields);
    NMonitoring::IMonPage* RegisterActorPage(NMonitoring::TIndexMonPage* index, const TString& relPath,
        const TString& title, bool preTag, TActorSystem* actorSystem, const TActorId& actorId, bool useAuth = true, bool sortPages = true);
    NMonitoring::IMonPage* RegisterCountersPage(const TString& path, const TString& title, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters);
    NMonitoring::IMonPage* FindPage(const TString& relPath);

    struct TRegisterHandlerFields {
        TString Path;
        TActorId Handler;
        bool UseAuth = true;
        TVector<TString> AllowedSIDs;
    };

    void RegisterActorHandler(const TRegisterHandlerFields& fields);

    void RegisterHandler(const TString& path, const TActorId& handler) {
        RegisterActorHandler({
            .Path = path,
            .Handler = handler
        });
    }

protected:
    TConfig Config;
    TIntrusivePtr<NMonitoring::TIndexMonPage> IndexMonPage;
    TActorSystem* ActorSystem = {};
    TActorId HttpProxyActorId;
    TActorId HttpMonServiceActorId;
    TActorId NodeProxyServiceActorId;

    struct TActorMonPageInfo {
        NMonitoring::TMonPagePtr Page;
        std::optional<TRegisterHandlerFields> Handler;
        TString Path;
    };

    TMutex Mutex;
    std::vector<TActorMonPageInfo> ActorMonPages;
    THashMap<TString, TActorId> ActorServices;

    void RegisterActorMonPage(const TActorMonPageInfo& pageInfo);
};

} // NActors
