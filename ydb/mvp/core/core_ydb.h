#pragma once

#include <ydb/mvp/security/simple/security.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_datastreams/datastreams.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/persqueue.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>
#include <ydb/public/sdk/cpp/client/draft/ydb_scripting.h>
#include <ydb/core/viewer/json/json.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/library/grpc/client/grpc_client_low.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_value.h>
#include <util/generic/strbuf.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include "grpc_log.h"
#include "mvp_tokens.h"

template <typename T>
class TAtomicSingleton {
private:
    mutable TAtomic Pointer;

public:
    TAtomicSingleton()
        : Pointer(0)
    {}

    ~TAtomicSingleton() {
        delete reinterpret_cast<T*>(Pointer);
    }

    T& GetRef(std::function<T*()> init = [](){ return new T(); }) const {
        if (Pointer == 0) {
            T* newValue = init();
            if (!AtomicCas(&Pointer, reinterpret_cast<TAtomic>(newValue), 0)) {
                delete newValue;
            }
        }
        return *reinterpret_cast<T*>(Pointer);
    }
};

/*
template <typename T>
class TAtomicSingleton {
private:
    mutable std::atomic<T*> Pointer;

public:
    TAtomicSingleton()
        : Pointer(nullptr)
    {}

    ~TAtomicSingleton() {
        delete static_cast<T*>(Pointer);
    }

    T& GetRef(std::function<T*()> init = []{ return new T(); }) const {
        if (Pointer == nullptr) {
            T* oldValue = nullptr;
            T* newValue = init();
            if (!Pointer.compare_exchange_strong(oldValue, newValue)) {
                delete newValue;
            }
        }
        return *static_cast<T*>(Pointer);
    }
};
*/

struct TParameters {
    bool Success;
    NHttp::TUrlParameters UrlParameters;
    NJson::TJsonValue PostData;

    TParameters(const NHttp::THttpIncomingRequestPtr& request);
    TString GetContentParameter(TStringBuf name) const;
    TString GetUrlParameter(TStringBuf name) const;
    TString operator [](TStringBuf name) const;
    void ParamsToProto(google::protobuf::Message& proto, TJsonSettings::TNameGenerator nameGenerator = {}) const;
};

struct TRequest {
    NActors::TActorId Sender;
    NHttp::THttpIncomingRequestPtr Request;
    TParameters Parameters;

    TRequest(const NActors::TActorId& sender, const NHttp::THttpIncomingRequestPtr& request)
        : Sender(sender)
        , Request(request)
        , Parameters(request)
    {}

    static inline TString BlackBoxTokenFromSessionId(TStringBuf sessionId, TStringBuf userIp = NKikimr::NSecurity::DefaultUserIp()) {
        return NKikimr::NSecurity::BlackBoxTokenFromSessionId(sessionId, userIp);
    }

    TString GetAuthToken() const;
    TString GetAuthToken(const NHttp::THeaders& headers) const;
    TString GetAuthTokenForIAM() const;
    TString GetAuthTokenForIAM(const NHttp::THeaders& headers) const;
    static void SetHeader(NYdbGrpc::TCallMeta& meta, const TString& name, const TString& value);
    void ForwardHeaders(NYdbGrpc::TCallMeta& meta) const;
    void ForwardHeadersOnlyForIAM(NYdbGrpc::TCallMeta& meta) const;
    void ForwardHeader(const NHttp::THeaders& header, NYdbGrpc::TCallMeta& meta, TStringBuf name) const;
    void ForwardHeader(const NHttp::THeaders& header, NHttp::THttpOutgoingRequestPtr& request, TStringBuf name) const;
    void ForwardHeaders(NHttp::THttpOutgoingRequestPtr& request) const;
    void ForwardHeadersOnlyForIAM(NHttp::THttpOutgoingRequestPtr& request) const;
};

struct TYdbUnitResources {
    double Cpu = 0.0;
    ui64 Memory = 0;
    ui64 Storage = 0;

    TYdbUnitResources operator *(ui64 mul) const {
        TYdbUnitResources result = *this;
        result.Cpu *= mul;
        result.Memory *= mul;
        result.Storage *= mul;
        return result;
    }

    TYdbUnitResources operator +(const TYdbUnitResources& add) const {
        TYdbUnitResources result = *this;
        result.Cpu += add.Cpu;
        result.Memory += add.Memory;
        result.Storage += add.Storage;
        return result;
    }

    TYdbUnitResources& operator +=(const TYdbUnitResources& add) {
        Cpu += add.Cpu;
        Memory += add.Memory;
        Storage += add.Storage;
        return *this;
    }
};

extern TMap<std::pair<TStringBuf, TStringBuf>, TYdbUnitResources> DefaultUnitResources;

struct TYdbLocation {
    TString Name;
    TString Environment;
    TVector<std::pair<TString, TString>> Endpoints;
    TString RootDomain;
    TVector<TStringBuf> DataCenters;
    const TMap<std::pair<TStringBuf, TStringBuf>, TYdbUnitResources>& UnitResources;
    ui32 NotificationsEnvironmentId;
    bool Disabled = false;
    TAtomicSingleton<NYdb::TDriver> Driver;
    TAtomicSingleton<NYdbGrpc::TGRpcClientLow> GRpcClientLow;
    static TString UserToken;
    static TString CaCertificate;
    static TString SslCertificate;
    TString ServerlessDocumentProxyEndpoint;
    TString ServerlessYdbProxyEndpoint;

    TYdbLocation(const TString& name,
                 const TString& environment,
                 const TVector<std::pair<TString, TString>>& endpoints,
                 const TString& rootDomain)
        : Name(name)
        , Environment(environment)
        , Endpoints(endpoints)
        , RootDomain(rootDomain)
        , UnitResources(DefaultUnitResources)
    {}

    TYdbLocation(const TString& name,
                 const TString& environment,
                 const TVector<std::pair<TString, TString>>& endpoints,
                 const TString& rootDomain,
                 const TVector<TStringBuf>& dataCenters,
                 const TMap<std::pair<TStringBuf, TStringBuf>, TYdbUnitResources>& unitResources,
                 ui32 notificationsEnvironmentId = 0)
        : Name(name)
        , Environment(environment)
        , Endpoints(endpoints)
        , RootDomain(rootDomain)
        , DataCenters(dataCenters)
        , UnitResources(unitResources)
        , NotificationsEnvironmentId(notificationsEnvironmentId)
    {}

    static TString GetUserToken() {
        return UserToken;
    }

    TString GetEndpoint(TStringBuf type, TStringBuf schema) const {
        for (const auto& pr : Endpoints) {
            if (pr.first == type) {
                TStringBuf endpoint(pr.second);
                if (endpoint.NextTok(':') == schema) {
                    return TString(endpoint.After('/').After('/'));
                }
            }
        }
        return TString();
    }

    TString GetEndpoint(TStringBuf type) const {
        for (const auto& pr : Endpoints) {
            if (pr.first == type) {
                return TString(pr.second);
            }
        }
        return TString();
    }

    void SaveEndpoints(NJson::TJsonValue& endpoints) const {
        endpoints.SetType(NJson::JSON_ARRAY);
        for (const auto& pr : Endpoints) {
            NJson::TJsonValue& endpoint = endpoints.AppendValue(NJson::TJsonValue());
            endpoint["type"] = pr.first;
            TStringBuf url(pr.second);
            endpoint["url"] = url;
            endpoint["scheme"] = url.NextTok(':');
            endpoint["endpoint"] = url.After('/').After('/');
        }
    }

    const TYdbUnitResources& GetUnitResources(TStringBuf type, TStringBuf kind) const {
        auto it = UnitResources.find({type, kind});
        if (it != UnitResources.end()) {
            return it->second;
        }
        return Default<TYdbUnitResources>();
    }

    NHttp::THttpOutgoingRequestPtr CreateHttpMonRequestGet(TStringBuf uri, const TRequest& request) const;
    NHttp::THttpOutgoingRequestPtr CreateHttpMonRequestGet(TStringBuf uri) const;

    template <typename TGRpcService>
    std::unique_ptr<NMVP::TLoggedGrpcServiceConnection<TGRpcService>> CreateGRpcServiceConnection(const NYdbGrpc::TGRpcClientConfig& config) const {
        return std::unique_ptr<NMVP::TLoggedGrpcServiceConnection<TGRpcService>>(new NMVP::TLoggedGrpcServiceConnection<TGRpcService>(config, GetGRpcClientLow().CreateGRpcServiceConnection<TGRpcService>(config)));
    }

    template <typename TGRpcService>
    std::unique_ptr<NMVP::TLoggedGrpcServiceConnection<TGRpcService>> CreateGRpcServiceConnection(TStringBuf type = "cluster-api") const {
        // TODO: cache service connections
        // TODO: optimize endpoints
        TString endpoint;
        TString certificate;
        bool ssl = false;
        endpoint = GetEndpoint(type, "grpc");
        if (endpoint.empty()) {
            endpoint = GetEndpoint(type, "grpcs");
            certificate = CaCertificate;
            ssl = true;
        }
        NYdbGrpc::TGRpcClientConfig config(endpoint);
        if (!certificate.empty()) {
            config.SslCredentials.pem_root_certs = certificate;
        }
        config.EnableSsl = ssl;
        return CreateGRpcServiceConnection<TGRpcService>(config);
    }

    template <typename TGRpcService>
    std::unique_ptr<NMVP::TLoggedGrpcServiceConnection<TGRpcService>> CreateGRpcServiceConnectionFromEndpoint(const TString& endpoint) const {
        TStringBuf scheme = "grpc";
        TStringBuf host;
        TStringBuf uri;
        NHttp::CrackURL(endpoint, scheme, host, uri);
        NYdbGrpc::TGRpcClientConfig config;
        config.Locator = host;
        config.EnableSsl = (scheme == "grpcs");
        if (config.EnableSsl && CaCertificate) {
            config.SslCredentials.pem_root_certs = CaCertificate;
        }
        return CreateGRpcServiceConnection<TGRpcService>(config);
    }

    NYdb::TDriverConfig GetDriverConfig(TStringBuf endpoint = TStringBuf(), TStringBuf scheme = TStringBuf()) const {
        NYdb::TDriverConfig config;
        TString endp(endpoint);
        if (endpoint.empty()) {
            endpoint = GetEndpoint("cluster-api");
            endpoint.TrySplit("://", scheme, endpoint);
            endp = TString(endpoint);
        } else if (endp.find(':') == TString::npos) {
            endp += ":2135"; // default grpc port
        }
        if (scheme == "grpcs") {
            config.UseSecureConnection(CaCertificate);
        }
        config.SetEndpoint(endp);
        return config;
    }

    NYdb::TDriver& GetDriver() const;
    NYdb::TDriver GetDriver(TStringBuf endpoint, TStringBuf scheme) const;

    NYdb::NScheme::TSchemeClient GetSchemeClient(const TRequest& request) const;
    NYdb::NScheme::TSchemeClient GetSchemeClient(const NYdb::TCommonClientSettings& settings = NYdb::TCommonClientSettings()) const;
    std::unique_ptr<NYdb::NScheme::TSchemeClient> GetSchemeClientPtr(TStringBuf endpoint, const NYdb::TCommonClientSettings& settings = NYdb::TCommonClientSettings()) const;
    std::unique_ptr<NYdb::NScheme::TSchemeClient> GetSchemeClientPtr(TStringBuf endpoint, TStringBuf scheme, const NYdb::TCommonClientSettings& settings = NYdb::TCommonClientSettings()) const;
    std::unique_ptr<NYdb::NTable::TTableClient> GetTableClientPtr(TStringBuf endpoint, const NYdb::NTable::TClientSettings& settings = NYdb::NTable::TClientSettings()) const;
    std::unique_ptr<NYdb::NTable::TTableClient> GetTableClientPtr(TStringBuf endpoint, TStringBuf scheme, const NYdb::NTable::TClientSettings& settings = NYdb::NTable::TClientSettings()) const;
    std::unique_ptr<NYdb::NTopic::TTopicClient> GetTopicClientPtr(TStringBuf endpoint, TStringBuf scheme, const NYdb::NTopic::TTopicClientSettings& settings = NYdb::NTopic::TTopicClientSettings()) const;

    std::unique_ptr<NYdb::NDataStreams::V1::TDataStreamsClient> GetDataStreamsClientPtr(TStringBuf endpoint, TStringBuf scheme, const NYdb::TCommonClientSettings& settings = NYdb::TCommonClientSettings()) const;

    NYdb::NTable::TTableClient GetTableClient(const TRequest& request, const NYdb::NTable::TClientSettings& defaultClientSettings, const TString& metaDatabaseTokenName = "") const;
    NYdb::NTable::TTableClient GetTableClient(const NYdb::NTable::TClientSettings& clientSettings = {}) const;

    NYdb::NScripting::TScriptingClient GetScriptingClient(const TRequest& request) const;
    std::unique_ptr<NYdb::NScripting::TScriptingClient> GetScriptingClientPtr(TStringBuf endpoint, TStringBuf scheme, const NYdb::TCommonClientSettings& settings = NYdb::TCommonClientSettings()) const;

    TString GetPath(const TRequest& request) const {
        TString path = request.Parameters["path"];
        if (!path.StartsWith('/')) {
            path.insert(path.begin(), '/');
        }
        TString database = request.Parameters["database"];
        if (!database.empty()) {
            path = RootDomain + '/' + database + path;
        } else {
            path = RootDomain + path;
        }
        if (path.EndsWith('/')) {
            path.resize(path.size() - 1);
        }
        if (path.find_first_of("]") != TString::npos) {
            return TString();
        }
        return path;
    }

    TString GetName(const TRequest& request) const {
        TString name = request.Parameters["name"];
        if (!name.StartsWith('/')) {
            name.insert(name.begin(), '/');
        }
        name = RootDomain + name;
        if (name.find_first_of("]") != TString::npos) {
            return TString();
        }
        return name;
    }

    TString GetDatabaseName(const TRequest& request) const;
    TString GetServerlessProxyUrl(const TString& database) const;

private:
    NYdbGrpc::TGRpcClientLow& GetGRpcClientLow() const {
        return GRpcClientLow.GetRef();
    }
};

TString GetAuthHeaderValue(const TString& tokenName);
