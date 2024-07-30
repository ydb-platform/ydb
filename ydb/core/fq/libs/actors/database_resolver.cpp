#include "database_resolver.h"

#include <util/string/split.h>
#include <ydb/core/fq/libs/common/cache.h>
#include <ydb/core/fq/libs/config/protos/issue_id.pb.h>
#include <ydb/core/fq/libs/events/events.h>
#include <ydb/core/fq/libs/exceptions/exceptions.h>
#include <ydb/core/util/tuples.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/yql/providers/common/db_id_async_resolver/db_async_resolver.h>
#include <ydb/library/yql/utils/url_builder.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <library/cpp/json/json_reader.h>

#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::FQ_DATABASE_RESOLVER, "TraceId: " << TraceId << " " << stream)
#define LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::FQ_DATABASE_RESOLVER, "TraceId: " << TraceId << " " << stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::FQ_DATABASE_RESOLVER, "TraceId: " << TraceId << " " << stream)
#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::FQ_DATABASE_RESOLVER, "TraceId: " << TraceId << " " << stream)

namespace NFq {

using namespace NActors;
using namespace NYql;

using TDatabaseDescription = NYql::TDatabaseResolverResponse::TDatabaseDescription;
using TParser = std::function<TDatabaseDescription(
        NJson::TJsonValue& body,
        const NYql::IMdbEndpointGenerator::TPtr& mdbEndpointGenerator,
        bool useTls,
        NConnector::NApi::EProtocol protocol
)>;
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
        Issues.AddIssue(errorMsg);
        SendResolvedEndpointsAndDie();
    }

    void SendResolvedEndpointsAndDie() {
        Send(Sender,
            new TEvents::TEvEndpointResponse(
                NYql::TDatabaseResolverResponse(std::move(DatabaseId2Description), Issues.Empty(), Issues)));
        PassAway();
        LOG_D("ResponseProcessor::SendResolvedEndpointsAndDie: passed away");
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr& ev)
    {
        TMaybe<TDatabaseDescription> result;
        const auto requestIter = Requests.find(ev->Get()->Request);
        HandledIds++;

        LOG_T("ResponseProcessor::Handle(HttpIncomingResponse): got API response: code=" << ev->Get()->Response->Status);

        try {
            HandleResponse(ev, requestIter, result);
        } catch (...) {
            const TString msg = TStringBuilder() << "error while response processing, params "
                << ((requestIter != Requests.end()) ? requestIter->second.ToDebugString() : TString{"unknown"})
                << ", details: " << CurrentExceptionMessage();
            LOG_E("ResponseProccessor::Handle(TEvHttpIncomingResponse): " << msg);
            Issues.AddIssue(msg);
        }

        LOG_T("ResponseProcessor::Handle(HttpIncomingResponse): progress: " 
              << DatabaseId2Description.size() << " of " << Requests.size() << " requests are done");

        if (HandledIds == Requests.size()) {
            SendResolvedEndpointsAndDie();
        }
    }

private:

    void HandleResponse(
        NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr& ev,
        const TRequestMap::const_iterator& requestIter,
        TMaybe<TDatabaseDescription>& result)
    { 
        TString errorMessage;

        if (requestIter == Requests.end()) {
            // Requests are guaranteed to be kept in within TResponseProcessor until the response arrives.
            // If there is no appropriate request, it's a fatal error.
            errorMessage = "Invariant violation: unknown request";
        } else {
            if (ev->Get()->Error.empty() && (ev->Get()->Response && ev->Get()->Response->Status == "200")) {
                errorMessage = HandleSuccessfulResponse(ev, *requestIter, result);
            } else {
                errorMessage = HandleFailedResponse(ev, *requestIter);
            }
        }

        if (errorMessage) {
            Issues.AddIssue(errorMessage);
            LOG_E("ResponseProcessor::Handle(HttpIncomingResponse): error=" << errorMessage);
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
    }

    TString HandleSuccessfulResponse(
        NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr& ev,
        const TRequestMap::value_type& requestWithParams,
        TMaybe<TDatabaseDescription>& result
    ) {
        NJson::TJsonReaderConfig jsonConfig;
        NJson::TJsonValue databaseInfo;

        const auto& params = requestWithParams.second;
        const bool parseJsonOk = NJson::ReadJsonTree(ev->Get()->Response->Body, &jsonConfig, &databaseInfo);
        TParsers::const_iterator parserIt;
        if (parseJsonOk && (parserIt = Parsers.find(params.DatabaseType)) != Parsers.end()) {
            try {
                auto description = parserIt->second(
                    databaseInfo,
                    MdbEndpointGenerator,
                    params.DatabaseAuth.UseTls,
                    params.DatabaseAuth.Protocol);
                LOG_D("ResponseProcessor::Handle(HttpIncomingResponse): got description" << ": params: " << params.ToDebugString()
                                                                                            << ", description: " << description.ToDebugString());
                DatabaseId2Description[std::make_pair(params.Id, params.DatabaseType)] = description;
                result.ConstructInPlace(description);
                return "";
            } catch (const TCodeLineException& ex) {
                return TStringBuilder()
                    << "response parser error: " << params.ToDebugString() << Endl
                    << ex.GetRawMessage();
            } catch (...) {
                return TStringBuilder()
                    << "response parser error: " << params.ToDebugString() << Endl
                    << CurrentExceptionMessage();
            }
        } else {
            return TStringBuilder() << "JSON parser error: " << params.ToDebugString();
        }
    }

    TString HandleFailedResponse(
        NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr& ev,
        const TRequestMap::value_type& requestWithParams
    ) const {
        auto sb = TStringBuilder() 
            << "Error while trying to resolve managed " << ToString(requestWithParams.second.DatabaseType)
            << " database with id " << requestWithParams.second.Id << " via HTTP request to"
            << ": endpoint '" << requestWithParams.first->Host << "'"
            << ", url '" << requestWithParams.first->URL << "'"
            << ": ";

        // Handle network error (when the response is empty)
        if (!ev->Get()->Response) {
            return sb << ev->Get()->Error;
        }

        // Handle unauthenticated error
        const auto& status = ev->Get()->Response->Status;
        if (status == "403") {
            return sb << "you have no permission to resolve database id into database endpoint." + DetailedPermissionsError(requestWithParams.second);
        }

        // Unexpected error. Add response body for debug
        return sb << Endl
                  << "Status: " << status << Endl
                  << "Response body: " << ev->Get()->Response->Body;
    }


    TString DetailedPermissionsError(const TResolveParams& params) const {
        if (params.DatabaseType == EDatabaseType::ClickHouse || params.DatabaseType == EDatabaseType::PostgreSQL) {
                auto mdbTypeStr = NYql::DatabaseTypeLowercase(params.DatabaseType);
                return TStringBuilder() << " Please check that your service account has role "  << 
                                       "`managed-" << mdbTypeStr << ".viewer`.";
        }
        return {};
    }

    const TActorId Sender;
    TCache& Cache;
    const TRequestMap Requests;
    const TString TraceId;
    const NYql::IMdbEndpointGenerator::TPtr MdbEndpointGenerator;
    TDatabaseResolverResponse::TDatabaseDescriptionMap DatabaseId2Description;
    size_t HandledIds = 0;
    NYql::TIssues Issues;
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
        auto ydbParser = [](NJson::TJsonValue& databaseInfo, const NYql::IMdbEndpointGenerator::TPtr&, bool, NConnector::NApi::EProtocol) {
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

            TVector<TString> split = StringSplitter(endpoint).Split(':');
            Y_ENSURE(split.size() == 2);

            TString host = std::move(split[0]);
            ui32 port = FromString(split[1]);

            // There are two kinds of managed YDBs: serverless and dedicated.
            // While working with dedicated databases, we have to use underlay network.
            // That's why we add `u-` prefix to database fqdn.
            if (databaseInfo.GetMap().contains("storageConfig")) {
                endpoint = "u-" + endpoint;
                host = "u-" + host;
            }

            return TDatabaseDescription{endpoint, std::move(host), port, database, secure};
        };
        Parsers[NYql::EDatabaseType::Ydb] = ydbParser;
        Parsers[NYql::EDatabaseType::DataStreams] = [ydbParser](
            NJson::TJsonValue& databaseInfo,
            const NYql::IMdbEndpointGenerator::TPtr& mdbEndpointGenerator,
            bool useTls,
            NConnector::NApi::EProtocol protocol)
        {
            auto ret = ydbParser(databaseInfo, mdbEndpointGenerator, useTls, protocol);
            // TODO: Take explicit field from MVP
            bool isDedicatedDb  = databaseInfo.GetMap().contains("storageConfig");
            if (!isDedicatedDb && ret.Endpoint.StartsWith("ydb.")) {
                // Replace "ydb." -> "yds."
                ret.Endpoint[2] = 's';
                ret.Host[2] = 's';
            }
            return ret;
        };
        Parsers[NYql::EDatabaseType::ClickHouse] = [](
            NJson::TJsonValue& databaseInfo,
            const NYql::IMdbEndpointGenerator::TPtr& mdbEndpointGenerator,
            bool useTls,
            NConnector::NApi::EProtocol protocol
            ) {
            NYql::IMdbEndpointGenerator::TEndpoint endpoint;
            TVector<TString> aliveHosts;

            for (const auto& host : databaseInfo.GetMap().at("hosts").GetArraySafe()) {
                if (host["health"].GetString() == "ALIVE" && host["type"].GetString() == "CLICKHOUSE") {
                    aliveHosts.push_back(host["name"].GetString());
                }
            }

            if (aliveHosts.empty()) {
                ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "No ALIVE ClickHouse hosts found";
            }

            NYql::IMdbEndpointGenerator::TParams params = {
                .DatabaseType = NYql::EDatabaseType::ClickHouse,
                .MdbHost = aliveHosts[std::rand() % static_cast<int>(aliveHosts.size())],
                .UseTls = useTls,
                .Protocol = protocol,
            };

            endpoint = mdbEndpointGenerator->ToEndpoint(params);

            return TDatabaseDescription{"", endpoint.first, endpoint.second, "", useTls};
        };

        Parsers[NYql::EDatabaseType::PostgreSQL] = [](
            NJson::TJsonValue& databaseInfo,
            const NYql::IMdbEndpointGenerator::TPtr& mdbEndpointGenerator,
            bool useTls,
            NConnector::NApi::EProtocol protocol
            ) {
            NYql::IMdbEndpointGenerator::TEndpoint endpoint;
            TVector<TString> aliveHosts;

            for (const auto& host : databaseInfo.GetMap().at("hosts").GetArraySafe()) {
                const auto& hostMap = host.GetMap();

                if (!hostMap.contains("services")) {
                    // indicates that cluster is down
                    continue;
                }

                // all services of a particular host must be alive
                bool alive = true;

                for (const auto& service: hostMap.at("services").GetArraySafe()) {
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
                ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "No ALIVE PostgreSQL hosts found";
            }

            NYql::IMdbEndpointGenerator::TParams params = {
                .DatabaseType = NYql::EDatabaseType::PostgreSQL,
                .MdbHost = aliveHosts[std::rand() % static_cast<int>(aliveHosts.size())],
                .UseTls = useTls,
                .Protocol = protocol,
            };

            endpoint = mdbEndpointGenerator->ToEndpoint(params);

            return TDatabaseDescription{"", endpoint.first, endpoint.second, "", useTls};
        };
        Parsers[NYql::EDatabaseType::Greenplum] = [](
            NJson::TJsonValue& databaseInfo,
            const NYql::IMdbEndpointGenerator::TPtr& mdbEndpointGenerator,
            bool useTls,
            NConnector::NApi::EProtocol protocol
            ) {
            NYql::IMdbEndpointGenerator::TEndpoint endpoint;
            TString aliveHost;

            for (const auto& host : databaseInfo.GetMap().at("hosts").GetArraySafe()) {
                const auto& hostMap = host.GetMap();

                if (hostMap.at("health").GetString() != "ALIVE"){
                    // Host is not alive, skip it
                    continue;

                }

                // If the host is alive, add it to the list of alive hosts
                aliveHost = hostMap.at("name").GetString();
                break;
            }
    
            if (aliveHost == "") {
                ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "No ALIVE Greenplum hosts found";
            }

            NYql::IMdbEndpointGenerator::TParams params = {
                .DatabaseType = NYql::EDatabaseType::Greenplum,
                .MdbHost = aliveHost,
                .UseTls = useTls,
                .Protocol = protocol,
            };

            endpoint = mdbEndpointGenerator->ToEndpoint(params);

            return TDatabaseDescription{"", endpoint.first, endpoint.second, "", useTls};
        };
        Parsers[NYql::EDatabaseType::MySQL] = [](
            NJson::TJsonValue& databaseInfo,
            const NYql::IMdbEndpointGenerator::TPtr& mdbEndpointGenerator,
            bool useTls,
            NConnector::NApi::EProtocol protocol
            ) {
            NYql::IMdbEndpointGenerator::TEndpoint endpoint;
            TVector<TString> aliveHosts;

            const auto& hostsArray = databaseInfo.GetMap().at("hosts").GetArraySafe();

            for (const auto& host : hostsArray) {
                const auto& hostMap = host.GetMap();

                if (!hostMap.contains("services")) {
                    // indicates that cluster is down
                    continue;
                }

                const auto& servicesArray = hostMap.at("services").GetArraySafe();

                // check if all services of a particular host are alive
                const bool alive = std::all_of(
                    servicesArray.begin(),
                    servicesArray.end(),
                    [](const auto& service) {
                        return service["health"].GetString() == "ALIVE";
                    }
                );

                if (alive) {
                    aliveHosts.push_back(host["name"].GetString());
                }
            }

            if (aliveHosts.empty()) {
                ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "No ALIVE MySQL hosts found";
            }

            NYql::IMdbEndpointGenerator::TParams params = {
                .DatabaseType = NYql::EDatabaseType::MySQL,
                .MdbHost = aliveHosts[std::rand() % static_cast<int>(aliveHosts.size())],
                .UseTls = useTls,
                .Protocol = protocol,
            };

            endpoint = mdbEndpointGenerator->ToEndpoint(params);

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
                    YQL_ENSURE(ev->Get()->YdbMvpEndpoint.Size() > 0, "empty YDB MVP Endpoint");
                    url = TUrlBuilder(ev->Get()->YdbMvpEndpoint + "/database")
                            .AddUrlParam("databaseId", databaseId)
                            .Build();
                } else if (IsIn({NYql::EDatabaseType::ClickHouse, NYql::EDatabaseType::PostgreSQL, NYql::EDatabaseType::MySQL}, databaseType)) {
                    YQL_ENSURE(ev->Get()->MdbGateway, "empty MDB Gateway");
                    url = TUrlBuilder(
                        ev->Get()->MdbGateway + "/managed-" + NYql::DatabaseTypeLowercase(databaseType) + "/v1/clusters/")
                            .AddPathComponent(databaseId)
                            .AddPathComponent("hosts")
                            .Build();
                } else if (NYql::EDatabaseType::Greenplum == databaseType) {
                    YQL_ENSURE(ev->Get()->MdbGateway, "empty MDB Gateway");
                    url = TUrlBuilder(
                        ev->Get()->MdbGateway + "/managed-" + NYql::DatabaseTypeLowercase(databaseType) + "/v1/clusters/")
                            .AddPathComponent(databaseId)
                            .AddPathComponent("master-hosts")
                            .Build();
                }

                NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestGet(url);

                auto credentialsProviderFactory = CreateCredentialsProviderFactoryForStructuredToken(CredentialsFactory, databaseAuth.StructuredToken, databaseAuth.AddBearerToToken);
                auto token = credentialsProviderFactory->CreateProvider()->GetAuthInfo();
                if (token) {
                    httpRequest->Set("Authorization", token);
                }

                LOG_D("ResponseProccessor::Handle(EndpointRequest): start GET request: " << "url: "  << httpRequest->URL);

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
