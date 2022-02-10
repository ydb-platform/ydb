#include "database_resolver.h"

#include <ydb/core/yq/libs/events/events.h>
#include <ydb/core/yq/libs/common/cache.h>
 
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/http/http.h>
#include <library/cpp/actors/http/http_proxy.h>
#include <library/cpp/json/json_reader.h>
#include <ydb/core/protos/services.pb.h>

#define LOG_E(stream) \ 
    LOG_ERROR_S(*TlsActivationContext, NKikimrServices::YQL_PROXY, "DatabaseResolver - TraceId: " << TraceId << ": " << stream) 

#define LOG_D(stream) \ 
    LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::YQL_PROXY, "DatabaseResolver - TraceId: " << TraceId << ": " << stream) 

namespace NYq {

using namespace NActors;
using namespace NYql;

using TEndpoint = TEvents::TEvEndpointResponse::TEndpoint;

using TParsers = THashMap<DatabaseType, std::function<TEndpoint(NJson::TJsonValue& body, bool)>>;
using TCache = TTtlCache<std::tuple<TString, DatabaseType, TEvents::TDatabaseAuth>, std::variant<TEndpoint, TString>>;

TString TransformMdbHostToCorrectFormat(const TString& mdbHost) {
    return mdbHost.substr(0, mdbHost.find('.')) + ".db.yandex.net:8443";
}

class TResponseProcessor : public TActorBootstrapped<TResponseProcessor>
{
public:
    enum EWakeUp {
        WU_Die_On_Ttl
    };

    TResponseProcessor(
        const TActorId sender,
        TCache& cache,
        const THashMap<std::pair<TString, DatabaseType>, TEndpoint>& ready,
        const THashMap<NHttp::THttpOutgoingRequestPtr, std::tuple<TString, DatabaseType, TEvents::TDatabaseAuth>>& requests,
        const TString& traceId,
        bool mdbTransformHost,
        const TParsers& parsers)
        : Sender(sender)
        , Cache(cache)
        , Requests(requests)
        , TraceId(traceId)
        , MdbTransformHost(mdbTransformHost)
        , DatabaseId2Endpoint(ready)
        , Parsers(parsers)
    { }

    static constexpr char ActorName[] = "YQ_RESPONSE_PROCESSOR";

    void Bootstrap() {
        Schedule(ResolvingTtl, new NActors::TEvents::TEvWakeup(WU_Die_On_Ttl));
        Become(&TResponseProcessor::StateFunc);
    }

    STRICT_STFUNC(StateFunc, {
        hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
        hFunc(NActors::TEvents::TEvWakeup, HandleWakeup)
        cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
    });

private:
    void HandleWakeup(NActors::TEvents::TEvWakeup::TPtr& ev) {
        auto tag = ev->Get()->Tag;
        switch (tag) {
        case WU_Die_On_Ttl:
            DieOnTtl();
            break;
        }
    }

    void DieOnTtl() {
        Success = false;

        TString logMsg = "Could not resolve database ids: ";
        bool firstUnresolvedDbId = true;
        for (const auto& [_, info]: Requests) {
            const auto& dbId = std::get<0>(info);
            const auto& dbType = std::get<1>(info);
            if (const auto it = DatabaseId2Endpoint.find(std::make_pair(dbId, dbType)); it == DatabaseId2Endpoint.end()) {
                logMsg += (firstUnresolvedDbId ? TString{""} : TString{", "}) + dbId;
                if (firstUnresolvedDbId)
                    firstUnresolvedDbId = false;
            }
        }
        logMsg += TStringBuilder() << " in " << ResolvingTtl << " seconds.";
        LOG_E(logMsg);

        SendResolvedEndpointsAndDie();
    }

    void SendResolvedEndpointsAndDie() {
        Send(Sender, new TEvents::TEvEndpointResponse(std::move(DatabaseId2Endpoint), Success));
        PassAway();
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr& ev)
    {
        TString status;
        TString errorMessage;
        TString databaseId;
        DatabaseType databaseType = DatabaseType::Ydb;
        TEvents::TDatabaseAuth info;
        TMaybe<TEndpoint> result;
        HandledIds++;
        if (ev->Get()->Error.empty() && (ev->Get()->Response && ((status = ev->Get()->Response->Status) == "200"))) {
            NJson::TJsonReaderConfig jsonConfig;
            NJson::TJsonValue databaseInfo;
            auto it = Requests.find(ev->Get()->Request);

            LOG_D("Got databaseId response " << ev->Get()->Response->Body); 
            if (it == Requests.end()) {
                errorMessage = "Unknown databaseId";
            } else {
                std::tie(databaseId, databaseType, info) = it->second;
                const bool parseJsonOk = NJson::ReadJsonTree(ev->Get()->Response->Body, &jsonConfig, &databaseInfo);
                TParsers::const_iterator parserIt;
                if (parseJsonOk && (parserIt = Parsers.find(databaseType)) != Parsers.end()) {
                    try {
                        auto res = parserIt->second(databaseInfo, MdbTransformHost);
                        LOG_D("Got " << databaseId << " " << databaseType << " endpoint " << res.Endpoint << " " << res.Database); 
                        DatabaseId2Endpoint[std::make_pair(databaseId, databaseType)] = res;
                        result.ConstructInPlace(res);
                    } catch (...) {
                        errorMessage = TStringBuilder()
                            << " Couldn't resolve "
                            << databaseType << " Id: "
                            << databaseId << "\n"
                            << CurrentExceptionMessage();
                    }
                } else {
                    errorMessage = TStringBuilder() << "Unable to parse database information."
                        << "Database Id: " << databaseId
                        << ", Database Type: " << databaseType;
                }
            }
        } else {
            errorMessage = ev->Get()->Error;
            const TString error = "Cannot resolve databaseId (status = " + ToString(status) + ")";
            if (!errorMessage.empty()) {
                errorMessage += '\n';
            }
            errorMessage += error;
        }

        if (errorMessage) {
            LOG_E("Error on response parsing: " << errorMessage); 
            Success = false;
        }

        if (databaseId) {
            auto key = std::make_tuple(databaseId, databaseType, info);
            if (errorMessage) {
                Cache.Put(key, errorMessage);
            } else {
                Cache.Put(key, result);
            }
        }

        if (HandledIds == Requests.size()) {
            SendResolvedEndpointsAndDie();
        }

        LOG_D(DatabaseId2Endpoint.size() << " of " << Requests.size() << " done");
    }

private:
    const TActorId Sender;
    TCache& Cache;
    const THashMap<NHttp::THttpOutgoingRequestPtr, std::tuple<TString, DatabaseType, TEvents::TDatabaseAuth>> Requests;
    const TString TraceId;
    const bool MdbTransformHost;
    THashMap<std::pair<TString, DatabaseType>, TEvents::TEvEndpointResponse::TEndpoint> DatabaseId2Endpoint;
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
        auto ydbParser = [](NJson::TJsonValue& databaseInfo, bool) {
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
            return TEvents::TEvEndpointResponse::TEndpoint{endpoint, database, secure};
        };
        Parsers[DatabaseType::Ydb] = ydbParser;
        Parsers[DatabaseType::DataStreams] = [ydbParser](NJson::TJsonValue& databaseInfo, bool mdbTransformHost)
        {
            auto ret = ydbParser(databaseInfo, mdbTransformHost);
            // TODO: Take explicit field from MVP
            if (ret.Endpoint.StartsWith("ydb.")) {
                // Replace "ydb." -> "yds."
                ret.Endpoint[2] = 's';
            }
            return ret;
        };
        Parsers[DatabaseType::ClickHouse] = [](NJson::TJsonValue& databaseInfo, bool mdbTransformHost) {
            TString endpoint;
            TVector<TString> aliveHosts;
            for (const auto& host : databaseInfo.GetMap().at("hosts").GetArraySafe()) {
                if (host["health"].GetString() == "ALIVE" && host["type"].GetString() == "CLICKHOUSE") {
                    aliveHosts.push_back(host["name"].GetString());
                }
            }
            if (!aliveHosts.empty()) {
                endpoint = aliveHosts[std::rand() % static_cast<int>(aliveHosts.size())];
            }
            if (!endpoint) {
                ythrow yexception() << "No ALIVE ClickHouse hosts exist";
            }
            endpoint = mdbTransformHost ? TransformMdbHostToCorrectFormat(endpoint) : endpoint;
            return TEvents::TEvEndpointResponse::TEndpoint{endpoint, "", true};
        };
    }

    static constexpr char ActorName[] = "YQ_DATABASE_RESOLVER";

private:
    STRICT_STFUNC(State, {
        HFunc(TEvents::TEvEndpointRequest, Handle);
        cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
    });

    void Handle(TEvents::TEvEndpointRequest::TPtr ev, const TActorContext& ctx)
    {
        TraceId = ev->Get()->TraceId;
        LOG_D("Start databaseId resolver for " << ev->Get()->DatabaseIds.size() << " ids"); 
        THashMap<NHttp::THttpOutgoingRequestPtr, std::tuple<TString, DatabaseType, TEvents::TDatabaseAuth>> requests; // request, (dbId, type, info)
        THashMap<std::pair<TString, DatabaseType>, TEndpoint> ready;
        for (const auto& [p, info] : ev->Get()->DatabaseIds) {
            const auto& [databaseId, type] = p;
            TMaybe<std::variant<TEndpoint, TString>> endpoint;
            auto key = std::make_tuple(databaseId, type, info);
            if (Cache.Get(key, &endpoint)) {
                if (endpoint) {
                    ready.insert(std::make_pair(p,
                        (endpoint->index() == 0 ? std::get<0>(*endpoint) : TEndpoint{})
                    ));
                }
                continue;
            }

            try {
                TString url = IsIn({ DatabaseType::Ydb, DatabaseType::DataStreams }, type) ?
                    ev->Get()->YdbMvpEndpoint + "/database?databaseId=" + databaseId :
                    ev->Get()->MdbGateway + "/managed-clickhouse/v1/clusters/" + databaseId + "/hosts";
                LOG_D("Get '" << url << "'");

                NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestGet(url);

                auto credentialsProviderFactory = CreateCredentialsProviderFactoryForStructuredToken(CredentialsFactory, info.StructuredToken, info.AddBearerToToken);
                auto token = credentialsProviderFactory->CreateProvider()->GetAuthInfo();
                if (token) {
                    httpRequest->Set("Authorization", token);
                }

                requests[httpRequest] = key;
            } catch (const std::exception& e) {
                const TString msg = TStringBuilder() << " Error while preparing to resolve databaseId " << databaseId << ", details: " << e.what();
                LOG_E(msg);
                Cache.Put(key, endpoint);
            }
        }

        if (!requests.empty()) {
            auto helper = Register(
                    new TResponseProcessor(ev->Sender, Cache, ready, requests, TraceId, ev->Get()->MdbTransformHost, Parsers));

            for (const auto& [request, _] : requests) {
                ctx.Send(new IEventHandle(HttpProxy, helper, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(request)));
            }
        } else {
            Send(ev->Sender, new TEvents::TEvEndpointResponse(std::move(ready), /*success=*/true));
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

} /* namespace NYq */
