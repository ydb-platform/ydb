#pragma once

#include <future>
#include <library/cpp/monlib/service/monservice.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/dynamic_counters/page.h>
#include <library/cpp/monlib/service/pages/index_mon_page.h>
#include <library/cpp/monlib/service/pages/resources/css_mon_page.h>
#include <library/cpp/monlib/service/pages/resources/fonts_mon_page.h>
#include <library/cpp/monlib/service/pages/resources/js_mon_page.h>
#include <library/cpp/monlib/service/pages/tablesorter/css_mon_page.h>
#include <library/cpp/monlib/service/pages/tablesorter/js_mon_page.h>

#include <ydb/library/actors/core/mon.h>
#include <ydb/library/actors/http/http.h>
#include <yql/essentials/public/issue/yql_issue.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>

namespace NKikimr {
    struct TAppData;
}

namespace NActors {

void MakeJsonErrorReply(NJson::TJsonValue& jsonResponse, TString& message, const NYdb::TStatus& status);
void MakeJsonErrorReply(NJson::TJsonValue& jsonResponse, TString& message, const NYql::TIssues& issues, NYdb::EStatus status);

class TMon {
public:
    enum class EAuthMode {
        Disabled,      // Don't check authorization
        Enforce,       // Check authorization in monitoring layer
        ExtractOnly    // Extract token only, check authorization in handler
    };

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
        TString Certificate; // certificate/private key data in PEM format
        TString CertificateFile; // certificate file path in PEM format (OpenSSL feature: may optionally contain both certificate chain and private key in the same PEM file if PrivateKeyFile is not set)
        TString PrivateKeyFile; // private key file path for the certificate in PEM format
        TString CaFile; // CA certificate file path for verifying client certificates (mTLS)
        ui32 MaxRequestsPerSecond = 0;
        TDuration InactivityTimeout = TDuration::Minutes(2);
        TString AllowOrigin;
        bool RequireCountersAuthentication = false;
        bool RequireHealthcheckAuthentication = false;
    };

    TMon(TConfig config);
    virtual ~TMon();

    std::future<void> Start(TActorSystem* actorSystem); // signals when monitoring is ready
    void Stop();

    void Register(NMonitoring::IMonPage* page);
    NMonitoring::TIndexMonPage* RegisterIndexPage(const TString& path, const TString& title);
    void RegisterLwtrace();

    struct TRegisterActorPageFields {
        TString Title;
        TString RelPath;
        TActorSystem* ActorSystem;
        NMonitoring::TIndexMonPage* Index;
        bool PreTag = false;
        TActorId ActorId;
        EAuthMode AuthMode = EAuthMode::Enforce;
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
        EAuthMode AuthMode = EAuthMode::Enforce;
        TVector<TString> AllowedSIDs;
    };

    void RegisterActorHandler(const TRegisterHandlerFields& fields);

    void RegisterHandler(const TString& path, const TActorId& handler) {
        RegisterActorHandler({
            .Path = path,
            .Handler = handler
        });
    }

    const TConfig& GetConfig() const {
        return Config;
    }

protected:
    TConfig Config;
    TIntrusivePtr<NMonitoring::TIndexMonPage> IndexMonPage;
    TActorSystem* ActorSystem = {};
    TActorId HttpProxyActorId;
    TActorId HttpMonServiceActorId;
    TActorId HttpAuthMonServiceActorId;
    TActorId NodeProxyServiceActorId;
    TActorId CountersServiceActorId;
    TActorId PingServiceActorId;
    TIntrusivePtr<NMonitoring::TDynamicCountersPage> CountersMonPage;

    struct TActorMonPageInfo {
        NMonitoring::TMonPagePtr Page;
        std::optional<TRegisterHandlerFields> Handler;
        TString Path;
    };

    TMutex Mutex;
    std::vector<TActorMonPageInfo> ActorMonPages;
    THashMap<TString, TActorId> ActorServices;
    std::shared_ptr<NMonitoring::IMetricFactory> Metrics;

    void RegisterActorMonPage(const TActorMonPageInfo& pageInfo);

    static TVector<TString> GetCountersAllowedSIDs(const NKikimr::TAppData* appData);
};

} // namespace NActors
