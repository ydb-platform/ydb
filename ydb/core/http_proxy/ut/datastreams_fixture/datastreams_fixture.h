#pragma once

#include <ydb/library/actors/http/http_proxy.h>

#include <ydb/library/grpc/server/actors/logger.h>

#include <ydb/core/http_proxy/discovery_actor.h>
#include <ydb/core/http_proxy/events.h>
#include <ydb/core/http_proxy/grpc_service.h>
#include <ydb/core/http_proxy/http_req.h>
#include <ydb/core/http_proxy/http_service.h>
#include <ydb/core/http_proxy/metrics_actor.h>
#include <ydb/core/mon/mon.h>
#include <ydb/core/ymq/actor/auth_multi_factory.h>

#include <ydb/library/aclib/aclib.h>
#include <ydb/library/persqueue/tests/counters.h>
#include <ydb/library/testlib/service_mocks/access_service_mock.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/common_client/settings.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/datastreams/datastreams.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/discovery/discovery.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>

#include <ydb/services/ydb/ydb_common_ut.h>

#include <ydb/core/http_proxy/auth_factory.h>

#include <ydb/library/folder_service/folder_service.h>
#include <ydb/core/ymq/actor/serviceid.h>

#include <library/cpp/http/misc/parsed_request.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/logger/global/global.h>
#include <library/cpp/resource/resource.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/testing/unittest/registar.h>

using TJMap = NJson::TJsonValue::TMapType;
using TJVector = NJson::TJsonValue::TArray;


struct THttpResult {
    ui32 HttpCode;
    TString Description;
    TString Body;
};


template <typename T>
T GetByPath(const NJson::TJsonValue& msg, TStringBuf path) {
    NJson::TJsonValue ret;
    UNIT_ASSERT_C(msg.GetValueByPath(path, ret), path);
    if constexpr (std::is_same<T, TString>::value) {
        return ret.GetStringSafe();
    }
    if constexpr (std::is_same<T, TJMap>::value) {
        return ret.GetMapSafe();
    }
    if constexpr (std::is_same<T, i64>::value) {
        return ret.GetIntegerSafe();
    }
    if constexpr (std::is_same<T, TJVector>::value) {
        return ret.GetArraySafe();
    }
}

class THttpProxyTestMock : public NUnitTest::TBaseFixture {
public:
    THttpProxyTestMock();
    ~THttpProxyTestMock();

    void TearDown(NUnitTest::TTestContext&) override;

    void SetUp(NUnitTest::TTestContext&) override;

    struct TInitParameters {
        bool YandexCloudMode : 1 = true;
        bool EnableMetering : 1 = false;
        bool EnableSqsTopic : 1 = false;
        bool EnforceUserTokenRequirement : 1 = false;
    };

    void InitAll(const TInitParameters initParameters);

    TString FormAuthorizationStr(const TString& region) const;

    void EnableAuthorization();
    void DisableAuthorization();

    static NJson::TJsonValue CreateCreateStreamRequest();

    static NJson::TJsonValue CreateDeleteStreamRequest();

    static NJson::TJsonValue CreateDescribeStreamRequest();

    static NJson::TJsonValue CreateDescribeStreamSummaryRequest();

    static NJson::TJsonValue CreatePutRecordsRequest();

    static NJson::TJsonValue CreateRegisterStreamConsumerRequest();

    static NJson::TJsonValue CreateDeregisterStreamConsumerRequest();

    static NJson::TJsonValue CreateListStreamConsumersRequest();

    static NJson::TJsonValue CreateGetShardIteratorRequest();

    static NJson::TJsonValue CreateGetRecordsRequest();

    static NJson::TJsonValue CreateListShardsRequest();

    static NJson::TJsonValue CreateSqsGetQueueUrlRequest();

    static NJson::TJsonValue CreateSqsCreateQueueRequest();

    THttpResult SendHttpRequestRaw(const TString& handler, const TString& target,
                                   const IOutputStream::TPart& body, const TString& authorizationStr,
                                   const TString& contentType = "application/json");

    THttpResult SendHttpRequestRawSpecified(const TString& handler, const TString& target,
                                   const TString& host, const TString& date, const TString& userAgent,
                                   const TString& acceptEncoding,
                                   const IOutputStream::TPart& body, const TString& authorizationStr,
                                   const TString& contentType = "application/json");

    THttpResult SendHttpRequest(const TString& handler, const TString& target, NJson::TJsonValue value,
                                const TString& authorizationStr,
                                const TString& contentType = "application/json");

    THttpResult SendHttpRequestSpecified(const TString& handler, const TString& target, NJson::TJsonValue value,
                                const TString& host, const TString& date, const TString& userAgent,
                                const TString& acceptEncoding, const TString& authorizationStr,
                                const TString& contentType = "application/json");

    THttpResult SendPing();

    NJson::TJsonMap CreateQueue(NJson::TJsonMap request, ui32 expectedHttpCode = 200) {
        auto res = SendHttpRequest("/Root", "AmazonSQS.CreateQueue", request, FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, expectedHttpCode);
        NJson::TJsonMap json;
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json, true));
        if (expectedHttpCode == 200) {
            const TString url = GetByPath<TString>(json, "QueueUrl");
            const TString queue = GetByPath<TString>(request, "QueueName");
            if (SqsTopicMode) {
                TStringBuf topic, consumer;
                TStringBuf{queue}.RSplit('@', topic, consumer);
                UNIT_ASSERT_C(url.contains(topic), LabeledOutput(url, queue, topic));
                UNIT_ASSERT_C(url.contains(consumer), LabeledOutput(url, queue, consumer));
            } else {
                UNIT_ASSERT_C(url.EndsWith(queue), LabeledOutput(url, queue));
            }
        }
        return json;
    }

    NJson::TJsonMap SendJsonRequest(TString method, NJson::TJsonMap request, ui32 expectedHttpCode = 200);

    NJson::TJsonMap DeleteQueue(NJson::TJsonMap request, ui32 expectedHttpCode = 200) {
        return SendJsonRequest("DeleteQueue", request, expectedHttpCode);
    }

    NJson::TJsonMap GetQueueAttributes(NJson::TJsonMap request, ui32 expectedHttpCode = 200) {
        return SendJsonRequest("GetQueueAttributes", request, expectedHttpCode);
    }

    NJson::TJsonMap SendMessage(NJson::TJsonMap request, ui32 expectedHttpCode = 200) {
        auto json = SendJsonRequest("SendMessage", request, expectedHttpCode);
        if (expectedHttpCode == 200) {
            UNIT_ASSERT(!GetByPath<TString>(json, "MD5OfMessageBody").empty());
        }
        return json;
    }

    NJson::TJsonMap SendMessageBatch(NJson::TJsonMap request, ui32 expectedHttpCode = 200) {
        return SendJsonRequest("SendMessageBatch", request, expectedHttpCode);
    }

    NJson::TJsonMap ReceiveMessage(NJson::TJsonMap request, ui32 expectedHttpCode = 200) {
        return SendJsonRequest("ReceiveMessage", request, expectedHttpCode);
    }

    NJson::TJsonMap DeleteMessage(NJson::TJsonMap request, ui32 expectedHttpCode = 200) {
        return SendJsonRequest("DeleteMessage", request, expectedHttpCode);
    }

    NJson::TJsonMap DeleteMessageBatch(NJson::TJsonMap request, ui32 expectedHttpCode = 200) {
        return SendJsonRequest("DeleteMessageBatch", request, expectedHttpCode);
    }

    NJson::TJsonMap GetQueueUrl(NJson::TJsonMap request, ui32 expectedHttpCode = 200) {
        return SendJsonRequest("GetQueueUrl", request, expectedHttpCode);
    }

    NJson::TJsonMap ListQueues(NJson::TJsonMap request, ui32 expectedHttpCode = 200) {
        return SendJsonRequest("ListQueues", request, expectedHttpCode);
    }

    NJson::TJsonMap PurgeQueue(NJson::TJsonMap request, ui32 expectedHttpCode = 200) {
        return SendJsonRequest("PurgeQueue", request, expectedHttpCode);
    }

    NJson::TJsonMap SetQueueAttributes(NJson::TJsonMap request, ui32 expectedHttpCode = 200) {
        return SendJsonRequest("SetQueueAttributes", request, expectedHttpCode);
    }

    NJson::TJsonMap ListQueueTags(NJson::TJsonMap request, ui32 expectedHttpCode = 200) {
        return SendJsonRequest("ListQueueTags", request, expectedHttpCode);
    }

    NJson::TJsonMap TagQueue(NJson::TJsonMap request = {}, ui32 expectedHttpCode = 200) {
        return SendJsonRequest("TagQueue", request, expectedHttpCode);
    }

    NJson::TJsonMap UntagQueue(NJson::TJsonMap request = {}, ui32 expectedHttpCode = 200) {
        return SendJsonRequest("UntagQueue", request, expectedHttpCode);
    }

    void WaitQueueAttributes(TString queueUrl, size_t retries, NJson::TJsonMap attributes);

    void WaitQueueAttributes(TString queueUrl, size_t retries, std::function<bool (NJson::TJsonMap json)> predicate);

    NJson::TJsonMap ChangeMessageVisibility(NJson::TJsonMap request, ui32 expectedHttpCode = 200) {
        return SendJsonRequest("ChangeMessageVisibility", request, expectedHttpCode);
    }

    NJson::TJsonMap ChangeMessageVisibilityBatch(NJson::TJsonMap request, ui32 expectedHttpCode = 200) {
        return SendJsonRequest("ChangeMessageVisibilityBatch", request, expectedHttpCode);
    }

private:
    TMaybe<NYdb::TResultSet> RunYqlDataQuery(TString query);

    void InitKikimr(bool yandexCloudMode, bool enableMetering, bool enforceUserTokenRequirement);

    void InitAccessServiceService();

    void InitHttpServer(bool yandexCloudMode, bool enableSqsTopic);

public:
    std::shared_ptr<NKikimr::NHttpProxy::IAuthFactory> AuthFactory;
    THolder<NYdb::TKikimrWithGrpcAndRootSchema> KikimrServer;
    TPortManager PortManager;
    TTestActorRuntime* ActorRuntime = nullptr;
    TAccessServiceMock AccessServiceMock;
    TString AccessServiceEndpoint;
    std::unique_ptr<grpc::Server> AccessServiceServer;
    std::unique_ptr<grpc::Server> IamTokenServer;
    std::unique_ptr<grpc::Server> DatabaseServiceServer;
    std::unique_ptr<NKikimr::NSQS::TMultiAuthFactory> MultiAuthFactory;
    TAutoPtr<TMon> Monitoring;
    TIntrusivePtr<NMonitoring::TDynamicCounters> Counters = {};
    THolder<NYdbGrpc::TGRpcServer> GRpcServer;
    ui16 GRpcServerPort = 0;
    ui16 HttpServicePort = 0;
    ui16 AccessServicePort = 0;
    ui16 IamTokenServicePort = 0;
    ui16 DatabaseServicePort = 0;
    ui16 MonPort = 0;
    ui16 KikimrGrpcPort = 0;
    bool SqsTopicMode = false;
    bool SendAuthorizationStr = true;
};

class THttpProxyTestMockForSQS : public THttpProxyTestMock {
    public:
    void SetUp(NUnitTest::TTestContext&) override {
        InitAll(TInitParameters{
            .YandexCloudMode = false,
        });
    }
};

class THttpProxyTestMockWithMetering : public THttpProxyTestMock {
    public:
    void SetUp(NUnitTest::TTestContext&) override {
        InitAll(TInitParameters{
            .EnableMetering = true,
        });
    }
};

class THttpProxyTestMockForSQSTopic : public THttpProxyTestMock {
    public:
    void SetUp(NUnitTest::TTestContext&) override {
        InitAll(TInitParameters{
            .EnableSqsTopic = true,
        });
    }
};
