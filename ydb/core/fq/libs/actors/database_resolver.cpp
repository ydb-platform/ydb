#include "database_resolver.h"

#include <ydb/core/fq/libs/events/events.h>
#include <ydb/core/fq/libs/common/cache.h>
#include <ydb/library/yql/providers/common/db_id_async_resolver/db_async_resolver.h>
#include <ydb/library/yql/utils/url_builder.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/http/http.h>
#include <library/cpp/actors/http/http_proxy.h>
#include <library/cpp/json/json_reader.h>
#include <ydb/core/protos/services.pb.h>

#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::FQ_DATABASE_RESOLVER, "TraceId: " << TraceId << " " << stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::FQ_DATABASE_RESOLVER, "TraceId: " << TraceId << " " << stream)

namespace NFq {

using namespace NActors;
using namespace NYql;

using TEndpoint = NYql::TDbResolverResponse::TEndpoint;

using TParsers = THashMap<NYql::DatabaseType, std::function<TEndpoint(NJson::TJsonValue& body, bool)>>;
using TCache = TTtlCache<std::tuple<TString, NYql::DatabaseType, NYql::TDatabaseAuth>, std::variant<TEndpoint, TString>>;

TString TransformMdbHostToCorrectFormat(const TString& mdbHost) {
    return mdbHost.substr(0, mdbHost.find('.')) + ".db.yandex.net:8443";
}

class TResponseProcessor : public TActorBootstrapped<TResponseProcessor>
{
public:
    enum EWakeUp {
        WU_DIE_ON_TTL
    };

    TResponseProcessor(
        const TActorId sender,
        TCache& cache,
        const TDbResolverResponse::TDatabaseEndpointsMap& ready,
        const THashMap<NHttp::THttpOutgoingRequestPtr, std::tuple<TString, NYql::DatabaseType, NYql::TDatabaseAuth>>& requests,
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
        auto tag = ev->Get()->Tag;
        switch (tag) {
        case WU_DIE_ON_TTL:
            DieOnTtl();
            break;
        }
    }

    void DieOnTtl() {
        Success = false;

        TString errorMsg = "Could not resolve database ids: ";
        bool firstUnresolvedDbId = true;
        for (const auto& [_, info]: Requests) {
            const auto& dbId = std::get<0>(info);
            const auto& dbType = std::get<1>(info);
            if (const auto it = DatabaseId2Endpoint.find(std::make_pair(dbId, dbType)); it == DatabaseId2Endpoint.end()) {
                errorMsg += (firstUnresolvedDbId ? TString{""} : TString{", "}) + dbId;
                if (firstUnresolvedDbId)
                    firstUnresolvedDbId = false;
            }
        }
        errorMsg += TStringBuilder() << " in " << ResolvingTtl << " seconds.";
        LOG_E(errorMsg);

        SendResolvedEndpointsAndDie(errorMsg);
    }

    void SendResolvedEndpointsAndDie(const TString& errorMsg) {
        NYql::TIssues issues;
        if (errorMsg) {
            issues.AddIssue(errorMsg);
        }

        Send(Sender,
            new TEvents::TEvEndpointResponse(
                NYql::TDbResolverResponse(std::move(DatabaseId2Endpoint), Success, issues)));
        PassAway();
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr& ev)
    {
        TString status;
        TString errorMessage;
        TString databaseId;
        NYql::DatabaseType databaseType = DatabaseType::Ydb;
        NYql::TDatabaseAuth info;
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
                        LOG_D("Got db_id: " << databaseId
                            << ", db type: " << static_cast<std::underlying_type<NYql::DatabaseType>::type>(databaseType)
                            << ", endpoint: " << res.Endpoint
                            << ", database: " << res.Database);
                        DatabaseId2Endpoint[std::make_pair(databaseId, databaseType)] = res;
                        result.ConstructInPlace(res);
                    } catch (...) {
                        errorMessage = TStringBuilder()
                            << " Couldn't resolve "
                            << "databaseId: " << databaseId
                            << ", db type: " << static_cast<std::underlying_type<NYql::DatabaseType>::type>(databaseType) << "\n"
                            << CurrentExceptionMessage();
                    }
                } else {
                    errorMessage = TStringBuilder() << "Unable to parse database information."
                        << "Database Id: " << databaseId
                        << ", db type: " << static_cast<std::underlying_type<NYql::DatabaseType>::type>(databaseType);
                }
            }
        } else {
            errorMessage = ev->Get()->Error;
            const TString error = TStringBuilder()
                << "Cannot resolve databaseId (status = " + ToString(status) + "). "
                << "Response body from " << ev->Get()->Request->URL << ": " << (ev->Get()->Response ? ev->Get()->Response->Body : "empty");
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

        LOG_D(DatabaseId2Endpoint.size() << " of " << Requests.size() << " done");

        if (HandledIds == Requests.size()) {
            SendResolvedEndpointsAndDie(errorMessage);
        }
    }

private:
    const TActorId Sender;
    TCache& Cache;
    const THashMap<NHttp::THttpOutgoingRequestPtr, std::tuple<TString, NYql::DatabaseType, NYql::TDatabaseAuth>> Requests;
    const TString TraceId;
    const bool MdbTransformHost;
    TDbResolverResponse::TDatabaseEndpointsMap DatabaseId2Endpoint;
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
            return TEndpoint{endpoint, database, secure};
        };
        Parsers[NYql::DatabaseType::Ydb] = ydbParser;
        Parsers[NYql::DatabaseType::DataStreams] = [ydbParser](NJson::TJsonValue& databaseInfo, bool mdbTransformHost)
        {
            auto ret = ydbParser(databaseInfo, mdbTransformHost);
            // TODO: Take explicit field from MVP
            if (ret.Endpoint.StartsWith("ydb.")) {
                // Replace "ydb." -> "yds."
                ret.Endpoint[2] = 's';
            }
            return ret;
        };
        Parsers[NYql::DatabaseType::ClickHouse] = [](NJson::TJsonValue& databaseInfo, bool mdbTransformHost) {
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
            return TEndpoint{endpoint, "", true};
        };

        // TODO: https://st.yandex-team.ru/YQ-2171: support other data sources than ClickHouse
        Parsers[NYql::DatabaseType::Generic] = [](NJson::TJsonValue& databaseInfo, bool mdbTransformHost) {
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
                ythrow yexception() << "No ALIVE database host exists";
            }

            if (mdbTransformHost) {
                // TODO: https://st.yandex-team.ru/YQ-2170: support secure connections on 9440
                endpoint += ":9000";
            }

            return TEndpoint{endpoint, "", true};
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
        TDbResolverResponse::TDatabaseEndpointsMap&& ready,
        bool success = true,
        const TString& errorMessage = "")
    {
        LOG_D("Reply: Success: " << success << ", Errors: " << (errorMessage ? errorMessage : "no"));
        NYql::TIssues issues;
        if (errorMessage)
            issues.AddIssue(errorMessage);
        Send(recipient,
            new TEvents::TEvEndpointResponse(NYql::TDbResolverResponse{std::move(ready), success, issues}));
    }

    void Handle(TEvents::TEvEndpointRequest::TPtr ev)
    {
        TraceId = ev->Get()->TraceId;
        LOG_D("Start databaseId resolver for " << ev->Get()->DatabaseIds.size() << " ids");
        THashMap<NHttp::THttpOutgoingRequestPtr, std::tuple<TString, NYql::DatabaseType, NYql::TDatabaseAuth>> requests; // request, (dbId, type, info)
        TDbResolverResponse::TDatabaseEndpointsMap ready;
        for (const auto& [p, info] : ev->Get()->DatabaseIds) {
            const auto& [databaseId, type] = p;
            TMaybe<std::variant<TEndpoint, TString>> cacheVal;
            auto key = std::make_tuple(databaseId, type, info);
            if (Cache.Get(key, &cacheVal)) {
                switch(cacheVal->index()) {
                    case 0U: {
                        ready.insert(std::make_pair(p, std::get<0U>(*cacheVal)));
                        break;
                    }
                    case 1U: {
                        SendResponse(ev->Sender, {}, false, std::get<1U>(*cacheVal));
                        return;
                    }
                    default: {
                        LOG_E("Unsupported cache's value type");
                    }
                }
                continue;
            }

            try {
                TString url;
                if (IsIn({NYql::DatabaseType::Ydb, NYql::DatabaseType::DataStreams }, type)) {
                    url = TUrlBuilder(ev->Get()->YdbMvpEndpoint + "/database")
                            .AddUrlParam("databaseId", databaseId)
                            .Build();
                } else {
                    YQL_ENSURE(ev->Get()->MdbGateway, "empty MDB Gateway");
                    url = TUrlBuilder(ev->Get()->MdbGateway + "/managed-clickhouse/v1/clusters/")
                            .AddPathComponent(databaseId)
                            .AddPathComponent("hosts")
                            .Build();
                }
                LOG_D("Get '" << url << "'");

                NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestGet(url);

                auto credentialsProviderFactory = CreateCredentialsProviderFactoryForStructuredToken(CredentialsFactory, info.StructuredToken, info.AddBearerToToken);
                auto token = credentialsProviderFactory->CreateProvider()->GetAuthInfo();
                if (token) {
                    httpRequest->Set("Authorization", token);
                }

                requests[httpRequest] = key;
            } catch (const std::exception& e) {
                const TString msg = TStringBuilder() << " Error while preparing to resolve databaseId: " << databaseId << ", details: " << e.what();
                LOG_E(msg);
                Cache.Put(key, msg);
                SendResponse(ev->Sender, {}, /*success=*/false, msg);
                return;
            }
        }

        if (!requests.empty()) {
            auto helper = Register(
                    new TResponseProcessor(ev->Sender, Cache, ready, requests, TraceId, ev->Get()->MdbTransformHost, Parsers));

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

} /* namespace NFq */
