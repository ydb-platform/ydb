#include "database_resolver.h"

#include <ydb/core/fq/libs/events/events.h>
#include <ydb/core/fq/libs/common/cache.h>
#include <ydb/core/util/tuples.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/yql/providers/common/db_id_async_resolver/db_async_resolver.h>
#include <ydb/library/yql/utils/url_builder.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/http/http.h>
#include <library/cpp/actors/http/http_proxy.h>
#include <library/cpp/json/json_reader.h>

#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::FQ_DATABASE_RESOLVER, "TraceId: " << TraceId << " " << stream)
#define LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::FQ_DATABASE_RESOLVER, "TraceId: " << TraceId << " " << stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::FQ_DATABASE_RESOLVER, "TraceId: " << TraceId << " " << stream)
#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::FQ_DATABASE_RESOLVER, "TraceId: " << TraceId << " " << stream)

namespace NFq {

using namespace NActors;
using namespace NYql;

using TDatabaseDescription = NYql::TDatabaseResolverResponse::TDatabaseDescription;
using TParser = std::function<TDatabaseDescription(NJson::TJsonValue& body, const NYql::IMdbEndpointGenerator::TPtr& mdbEndpointGenerator, bool useTls)>;
using TParsers = THashMap<NYql::EDatabaseType, TParser>;

struct TResolveParams {
    // Treat ID as:
    // - cluster ID (ClickHouse, PostgreSQL)
    // - database ID (YDB)
    TString Id; 
    NYql::EDatabaseType DatabaseType = NYql::EDatabaseType::Ydb;
    NYql::TDatabaseAuth DatabaseAuth;

    TString ToDebugString() const {
        return TStringBuilder() << "id=" << Id 
                                << ", database_type=" << DatabaseType;
    }
};

using TCache = TTtlCache<std::tuple<TString, NYql::EDatabaseType, NYql::TDatabaseAuth>, std::variant<TDatabaseDescription, TString>>;
using TRequestMap = THashMap<NHttp::THttpOutgoingRequestPtr, TResolveParams>;

class TResponseProcessor : public TActorBootstrapped<TResponseProcessor>
{
public:
    enum EWakeUp {
        WU_DIE_ON_TTL
    };

    TResponseProcessor(
        const TActorId sender,
        TCache& cache,
        const TDatabaseResolverResponse::TDatabaseDescriptionMap& ready,
        const TRequestMap& requests,
        const TString& traceId,
        const NYql::IMdbEndpointGenerator::TPtr& mdbEndpointGenerator,
        const TParsers& parsers)
        : Sender(sender)
        , Cache(cache)
        , Requests(requests)
        , TraceId(traceId)
        , MdbEndpointGenerator(mdbEndpointGenerator)
        , DatabaseId2Description(ready)
        , Parsers(parsers)
    { }

    static constexpr char ActorName[] = "YQ_RESPONSE_PROCESSOR";

    void Bootstrap() {
        Schedule(ResolvingTtl, new NActors::TEvents::TEvWakeup(WU_DIE_ON_TTL));
        Become(&TResponseProcessor::StateFunc);
    }

    STRICT_STFUNC(StateFunc, {
        hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
        hFunc(NActors::TEvents::TEvWakeup, HandleWakeup)
        cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
    });

private:
    void HandleWakeup(NActors::TEvents::TEvWakeup::TPtr& ev) {
        LOG_T("ResponseProcessor::HandleWakeup: tag=" << ev->Get()->Tag);
        auto tag = ev->Get()->Tag;
        switch (tag) {
        case WU_DIE_ON_TTL:
            DieOnTtl();
            break;
        }
    }

    void DieOnTtl() {
        Success = false;

        auto errorMsg  = TStringBuilder() << "Could not resolve database ids: ";
        bool firstUnresolvedDbId = true;
        for (const auto& [_, params]: Requests) {
            if (const auto it = DatabaseId2Description.find(std::make_pair(params.Id, params.DatabaseType)); it == DatabaseId2Description.end()) {
                errorMsg << firstUnresolvedDbId ? TString{""} : TString{", "};
                errorMsg << params.Id;
                if (firstUnresolvedDbId)
                    firstUnresolvedDbId = false;
            }
        }
        errorMsg << " in " << ResolvingTtl << " seconds.";
        LOG_E("ResponseProcessor::DieOnTtl: errorMsg=" << errorMsg);

        SendResolvedEndpointsAndDie(errorMsg);
    }

    void SendResolvedEndpointsAndDie(const TString& errorMsg) {
        NYql::TIssues issues;
        if (errorMsg) {
            issues.AddIssue(errorMsg);
        }

        Send(Sender,
            new TEvents::TEvEndpointResponse(
                NYql::TDatabaseResolverResponse(std::move(DatabaseId2Description), Success, issues)));
        PassAway();
        LOG_D("ResponseProcessor::SendResolvedEndpointsAndDie: passed away");
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr& ev)
    {
        TString status;
        TString errorMessage;
        TMaybe<TDatabaseDescription> result;
        auto requestIter = Requests.find(ev->Get()->Request);
        HandledIds++;

        LOG_T("ResponseProcessor::Handle(HttpIncomingResponse): got MDB API response: code=" << ev->Get()->Response->Status);

        if (ev->Get()->Error.empty() && (ev->Get()->Response && ((status = ev->Get()->Response->Status) == "200"))) {
            NJson::TJsonReaderConfig jsonConfig;
            NJson::TJsonValue databaseInfo;

            if (requestIter == Requests.end()) {
                errorMessage = "unknown request";
            } else {
                const auto& params = requestIter->second;
                const bool parseJsonOk = NJson::ReadJsonTree(ev->Get()->Response->Body, &jsonConfig, &databaseInfo);
                TParsers::const_iterator parserIt;
                if (parseJsonOk && (parserIt = Parsers.find(params.DatabaseType)) != Parsers.end()) {
                    try {
                        auto description = parserIt->second(databaseInfo, MdbEndpointGenerator, params.DatabaseAuth.UseTls);
                        LOG_D("ResponseProcessor::Handle(HttpIncomingResponse): got description" << ": params: " << params.ToDebugString()
                                                                                                 << ", description: " << description.ToDebugString());
                        DatabaseId2Description[std::make_pair(params.Id, params.DatabaseType)] = description;
                        result.ConstructInPlace(description);
                    } catch (...) {
                        errorMessage = TStringBuilder()
                            << "response parser error: "
                            << ", params: " << params.ToDebugString() << Endl
                            << CurrentExceptionMessage();
                    }
                } else {
                    errorMessage = TStringBuilder() << "JSON parser error: " << ", params: " << params.ToDebugString();
                }
            }
        } else {
            errorMessage = ev->Get()->Error;
            const TString error = TStringBuilder()
                << "Cannot resolve database id (status = " << status << "). "
                << "Response body from " << ev->Get()->Request->URL << ": " << (ev->Get()->Response ? ev->Get()->Response->Body : "empty");
            if (!errorMessage.empty()) {
                errorMessage += '\n';
            }
            errorMessage += error;
        }

        if (errorMessage) {
            LOG_E("ResponseProcessor::Handle(HttpIncomingResponse): error=" << errorMessage);
            Success = false;
        } else {
            const auto& params = requestIter->second;
            auto key = std::make_tuple(params.Id, params.DatabaseType, params.DatabaseAuth);
            if (errorMessage) {
                LOG_T("ResponseProcessor::Handle(HttpIncomingResponse): put value in cache"
                      << "; params: " << params.ToDebugString()
                      << ", error: " << errorMessage);
                Cache.Put(key, errorMessage);
            } else {
                LOG_T("ResponseProcessor::Handle(HttpIncomingResponse): put value in cache"
                      << "; params: " << params.ToDebugString()
                      << ", result: " << result->ToDebugString());
                Cache.Put(key, result);
            }
        }

        LOG_D("ResponseProcessor::Handle(HttpIncomingResponse): progress: " 
              << DatabaseId2Description.size() << " of " << Requests.size() << " requests are done");

        if (HandledIds == Requests.size()) {
            SendResolvedEndpointsAndDie(errorMessage);
        }
    }

private:
    const TActorId Sender;
    TCache& Cache;
    const TRequestMap Requests;
    const TString TraceId;
    const NYql::IMdbEndpointGenerator::TPtr MdbEndpointGenerator;
    TDatabaseResolverResponse::TDatabaseDescriptionMap DatabaseId2Description;
    size_t HandledIds = 0;
    bool Success = true;
    const TParsers& Parsers;
    TDuration ResolvingTtl = TDuration::Seconds(30); //TODO: Use cfg
};

class TDatabaseResolver: public TActor<TDatabaseResolver>
{
public:
    TDatabaseResolver(TActorId httpProxy, ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory)
        : TActor<TDatabaseResolver>(&TDatabaseResolver::State)
        , HttpProxy(httpProxy)
        , CredentialsFactory(credentialsFactory)
        , Cache(
            TTtlCacheSettings()
            .SetTtl(TDuration::Minutes(10))
            .SetErrorTtl(TDuration::Minutes(1))
            .SetMaxSize(1000000))
    {
        auto ydbParser = [](NJson::TJsonValue& databaseInfo, const NYql::IMdbEndpointGenerator::TPtr&, bool) {
            bool secure = false;
            TString endpoint = databaseInfo.GetMap().at("endpoint").GetStringRobust();
            TString prefix("/?database=");
            TString database;
            auto pos = endpoint.rfind(prefix);
            if (pos != TString::npos) {
                database = endpoint.substr(pos+prefix.length());
                endpoint = endpoint.substr(0, pos);
            }
            if (endpoint.StartsWith("grpcs://")) {
                secure = true;
            }
            pos = endpoint.find("://");
            if (pos != TString::npos) {
                endpoint = endpoint.substr(pos+3);
            }

            Y_ENSURE(endpoint);
            return TDatabaseDescription{endpoint, "", 0, database, secure};
        };
        Parsers[NYql::EDatabaseType::Ydb] = ydbParser;
        Parsers[NYql::EDatabaseType::DataStreams] = [ydbParser](NJson::TJsonValue& databaseInfo, const NYql::IMdbEndpointGenerator::TPtr& mdbEndpointGenerator, bool useTls)
        {
            bool isDedicatedDb  = databaseInfo.GetMap().contains("storageConfig");
            auto ret = ydbParser(databaseInfo, mdbEndpointGenerator, useTls);
            // TODO: Take explicit field from MVP
            if (!isDedicatedDb && ret.Endpoint.StartsWith("ydb.")) {
                // Replace "ydb." -> "yds."
                ret.Endpoint[2] = 's';
            }
            return ret;
        };
        Parsers[NYql::EDatabaseType::ClickHouse] = [](NJson::TJsonValue& databaseInfo, const NYql::IMdbEndpointGenerator::TPtr& mdbEndpointGenerator, bool useTls) {
            NYql::IMdbEndpointGenerator::TEndpoint endpoint;
            TVector<TString> aliveHosts;

            for (const auto& host : databaseInfo.GetMap().at("hosts").GetArraySafe()) {
                if (host["health"].GetString() == "ALIVE" && host["type"].GetString() == "CLICKHOUSE") {
                    aliveHosts.push_back(host["name"].GetString());
                }
            }

            if (aliveHosts.empty()) {
                ythrow yexception() << "No ALIVE ClickHouse hosts found";
            }

            endpoint = mdbEndpointGenerator->ToEndpoint(
                NYql::EDatabaseType::ClickHouse,
                aliveHosts[std::rand() % static_cast<int>(aliveHosts.size())],
                useTls
            );

            return TDatabaseDescription{"", endpoint.first, endpoint.second, "", useTls};
        };

        Parsers[NYql::EDatabaseType::PostgreSQL] = [](NJson::TJsonValue& databaseInfo, const NYql::IMdbEndpointGenerator::TPtr& mdbEndpointGenerator, bool useTls) {
            NYql::IMdbEndpointGenerator::TEndpoint endpoint;
            TVector<TString> aliveHosts;

            for (const auto& host : databaseInfo.GetMap().at("hosts").GetArraySafe()) {
                // all host services must be alive
                bool alive = true;
                for (const auto& service: host.GetMap().at("services").GetArraySafe()) {
                    if (service["health"].GetString() != "ALIVE") {
                        alive = false;
                        break;
                    }
                }

                if (alive) {
                    aliveHosts.push_back(host["name"].GetString());
                }
            }
            
            if (aliveHosts.empty()) {
                ythrow yexception() << "No ALIVE PostgreSQL hosts found";
            }

            endpoint = mdbEndpointGenerator->ToEndpoint(
                NYql::EDatabaseType::PostgreSQL,
                aliveHosts[std::rand() % static_cast<int>(aliveHosts.size())],
                useTls
            );

            return TDatabaseDescription{"", endpoint.first, endpoint.second, "", useTls};
        };
    }

    static constexpr char ActorName[] = "YQ_DATABASE_RESOLVER";

private:
    STRICT_STFUNC(State, {
        hFunc(TEvents::TEvEndpointRequest, Handle);
        cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
    });

    void SendResponse(
        const NActors::TActorId& recipient,
        TDatabaseResolverResponse::TDatabaseDescriptionMap&& ready,
        bool success = true,
        const TString& errorMessage = "")
    {
        LOG_D("ResponseProccessor::SendResponse: Success: " << success << ", Errors: " << (errorMessage ? errorMessage : "no"));
        NYql::TIssues issues;
        if (errorMessage)
            issues.AddIssue(errorMessage);
        Send(recipient,
            new TEvents::TEvEndpointResponse(NYql::TDatabaseResolverResponse{std::move(ready), success, issues}));
    }

    void Handle(TEvents::TEvEndpointRequest::TPtr ev)
    {
        TraceId = ev->Get()->TraceId;

        TStringBuilder initMsg;
        initMsg << "Handle endpoint request event for: ";
        for (const auto& item: ev->Get()->DatabaseIds)  {
            const auto& id = item.first.first;
            const auto& kind = item.first.second;
            initMsg << id << " (" << kind << ")" << ", ";
        }
        LOG_D("ResponseProccessor::Handle(EndpointRequest): " << initMsg);

        TRequestMap requests;
        TDatabaseResolverResponse::TDatabaseDescriptionMap ready;
        for (const auto& [p, databaseAuth] : ev->Get()->DatabaseIds) {
            const auto& [databaseId, databaseType] = p;
            TMaybe<std::variant<TDatabaseDescription, TString>> cacheVal;
            auto key = std::make_tuple(databaseId, databaseType, databaseAuth);
            if (Cache.Get(key, &cacheVal)) {
                switch(cacheVal->index()) {
                    case 0U: {
                        LOG_T("ResponseProccessor::Handle(EndpointRequest): obtained description from cache" 
                              << ": databaseId: " << std::get<0>(key)
                              << ", databaseType: " << std::get<1>(key)
                              << ", value: " << std::get<0>(*cacheVal).ToDebugString());
                        ready.insert(std::make_pair(p, std::get<0U>(*cacheVal)));
                        break;
                    }
                    case 1U: {
                        LOG_T("ResponseProccessor::Handle(EndpointRequest): obtained error from cache" 
                              << ": databaseId: " << std::get<0>(key)
                              << ", databaseType: " << std::get<1>(key)
                              << ", value: " << std::get<1>(*cacheVal));
                        SendResponse(ev->Sender, {}, false, std::get<1U>(*cacheVal));
                        return;
                    }
                    default: {
                        LOG_E("ResponseProccessor::Handle(EndpointRequest): unsupported cache value type");
                    }
                }
                continue;
            } else {
                LOG_T("ResponseProccessor::Handle(EndpointRequest): key is missing in cache" 
                       << ": databaseId: " << std::get<0>(key)
                       << ", databaseType: " << std::get<1>(key));
            }

            try {
                TString url;
                if (IsIn({NYql::EDatabaseType::Ydb, NYql::EDatabaseType::DataStreams }, databaseType)) {
                    url = TUrlBuilder(ev->Get()->YdbMvpEndpoint + "/database")
                            .AddUrlParam("databaseId", databaseId)
                            .Build();
                } else if (IsIn({NYql::EDatabaseType::ClickHouse, NYql::EDatabaseType::PostgreSQL }, databaseType)) {
                    YQL_ENSURE(ev->Get()->MdbGateway, "empty MDB Gateway");
                    url = TUrlBuilder(
                        ev->Get()->MdbGateway + "/managed-" + NYql::DatabaseTypeToMdbUrlPath(databaseType) + "/v1/clusters/")
                            .AddPathComponent(databaseId)
                            .AddPathComponent("hosts")
                            .Build();
                }
                LOG_D("ResponseProccessor::Handle(EndpointRequest): start GET request: " << url);

                NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestGet(url);

                auto credentialsProviderFactory = CreateCredentialsProviderFactoryForStructuredToken(CredentialsFactory, databaseAuth.StructuredToken, databaseAuth.AddBearerToToken);
                auto token = credentialsProviderFactory->CreateProvider()->GetAuthInfo();
                if (token) {
                    httpRequest->Set("Authorization", token);
                }

                requests[httpRequest] = TResolveParams{databaseId, databaseType, databaseAuth};
            } catch (const std::exception& e) {
                const TString msg = TStringBuilder() << "error while preparing to resolve database id: " << databaseId 
                                                     << ", details: " << e.what();
                LOG_E("ResponseProccessor::Handle(EndpointRequest): put error in cache: " << msg);
                Cache.Put(key, msg);
                SendResponse(ev->Sender, {}, /*success=*/false, msg);
                return;
            }
        }

        if (!requests.empty()) {
            auto helper = Register(
                    new TResponseProcessor(ev->Sender, Cache, ready, requests, TraceId, ev->Get()->MdbEndpointGenerator, Parsers));

            for (const auto& [request, _] : requests) {
                TActivationContext::Send(new IEventHandle(HttpProxy, helper, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(request)));
            }
        } else {
            SendResponse(ev->Sender, std::move(ready));
        }
    }

private:
    TParsers Parsers;
    const TActorId HttpProxy;
    const ISecuredServiceAccountCredentialsFactory::TPtr CredentialsFactory;
    TString TraceId;
    TCache Cache;
};

NActors::IActor* CreateDatabaseResolver(NActors::TActorId httpProxy, ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory) {
    return new TDatabaseResolver(httpProxy, credentialsFactory);
}

NActors::TActorId MakeDatabaseResolverActorId() {
    return NActors::TActorId(0, "DBRESOLVER");
}

} /* namespace NFq */
