#pragma once

#include "events.h"

#include <ydb/services/datastreams/datastreams_codes.h>

#include <ydb/core/protos/serverless_proxy_config.pb.h>

#include <ydb/core/protos/serverless_proxy_config.pb.h>
#include <ydb/library/http_proxy/authorization/signature.h>
#include <ydb/public/api/grpc/draft/ydb_datastreams_v1.grpc.pb.h>

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/library/grpc/client/grpc_client_low.h>
#include <library/cpp/http/server/http.h>
#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_reader.h>

#include <util/stream/output.h>
#include <util/string/builder.h>


#define ISSUE_CODE_OK 0
#define ISSUE_CODE_GENERIC 500030
#define ISSUE_CODE_ERROR 500100


namespace NKikimr::NHttpProxy {

class TRetryCounter {
public:
    bool HasAttemps() const {
        return UsedRetries < MaximumRetries;
    }

    void Void() {
        UsedRetries = 0;
    }

    void Click() {
        ++UsedRetries;
    }

    auto AttempN() const {
        return UsedRetries;
    }
private:
    const ui32 MaximumRetries{3};
    ui32 UsedRetries{0};
};


struct THttpResponseData {
    bool IsYmq = false;
    NYdb::EStatus Status{NYdb::EStatus::SUCCESS};
    NJson::TJsonValue Body;
    TString ErrorText{"OK"};
    TString YmqStatusCode;
    ui32 YmqHttpCode;

    TString DumpBody(MimeTypes contentType);
};

struct THttpRequestContext {
    THttpRequestContext(const NKikimrConfig::TServerlessProxyConfig& config,
                        NHttp::THttpIncomingRequestPtr request,
                        NActors::TActorId sender,
                        NYdb::TDriver* driver,
                        std::shared_ptr<NYdb::ICredentialsProvider> serviceAccountCredentialsProvider);
    const NKikimrConfig::TServerlessProxyConfig& ServiceConfig;
    NHttp::THttpIncomingRequestPtr Request;
    NActors::TActorId Sender;
    NYdb::TDriver* Driver;
    std::shared_ptr<NYdb::ICredentialsProvider> ServiceAccountCredentialsProvider;

    THttpResponseData ResponseData;
    TString ServiceAccountId;
    TString RequestId;
    TString DiscoveryEndpoint;
    TString DatabasePath;
    TString DatabaseId; // not in context
    TString FolderId;   // not in context
    TString CloudId;    // not in context
    TString StreamName; // not in context
    TString SourceAddress;
    TString MethodName; // used once
    TString ApiVersion; // used once
    MimeTypes ContentType{MIME_UNKNOWN};
    TString IamToken;
    TString SecurityToken;
    TString SerializedUserToken;
    TString UserName;

    TStringBuilder LogPrefix() const {
        return TStringBuilder() << "http request [" << MethodName << "] requestId [" << RequestId << "]";
    }

    THolder<NKikimr::NSQS::TAwsRequestSignV4> GetSignature();
    void DoReply(const TActorContext& ctx, size_t issueCode = ISSUE_CODE_GENERIC);
    void ParseHeaders(TStringBuf headers);
    void RequestBodyToProto(NProtoBuf::Message* request);
};

class IHttpRequestProcessor {
public:
    virtual ~IHttpRequestProcessor() = default;

    virtual const TString& Name() const = 0;
    virtual void Execute(THttpRequestContext&& context,
                         THolder<NKikimr::NSQS::TAwsRequestSignV4> signature,
                         const TActorContext& ctx) = 0;
};

class THttpRequestProcessors {
public:
    using TService = Ydb::DataStreams::V1::DataStreamsService;
    using TServiceConnection = NYdbGrpc::TServiceConnection<TService>;

public:
    void Initialize();
    bool Execute(const TString& name, THttpRequestContext&& params,
                 THolder<NKikimr::NSQS::TAwsRequestSignV4> signature,
                 const TActorContext& ctx);

private:
    THashMap<TString, THolder<IHttpRequestProcessor>> Name2DataStreamsProcessor;
    THashMap<TString, THolder<IHttpRequestProcessor>> Name2YmqProcessor;
};

NActors::IActor* CreateAccessServiceActor(const NKikimrConfig::TServerlessProxyConfig& config);
NActors::IActor* CreateIamTokenServiceActor(const NKikimrConfig::TServerlessProxyConfig& config);
NActors::IActor* CreateIamAuthActor(const TActorId sender, THttpRequestContext& context, THolder<NKikimr::NSQS::TAwsRequestSignV4>&& signature);


} // namespace NKinesis::NHttpProxy


template <>
void Out<NKikimr::NHttpProxy::THttpResponseData>(IOutputStream& o, const NKikimr::NHttpProxy::THttpResponseData& p);
