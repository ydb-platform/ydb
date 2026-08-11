#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/yql/utils/actors/http_sender_actor.h>
#include <ydb/library/yql/utils/actors/http_sender.h>

namespace NYql {

using namespace NActors;

namespace {

struct TTestBootstrap {
    TTestActorRuntimeBase Runtime;
    TActorId SelfActorId;
    TActorId HttpSenderActorId;
    TActorId HttpProxyActorId;

    NYql::NDq::THttpSenderRetryPolicy::TPtr RetryPolicy;

    TTestBootstrap(NYql::NDq::THttpSenderRetryPolicy::TPtr retryPolicy)
        : Runtime(1, true)
        , SelfActorId(0, "SELF")
        , HttpSenderActorId(0, "SENDER")
        , HttpProxyActorId(0, "PROXY")
        , RetryPolicy(retryPolicy)
    {
        Runtime.Initialize();

        SelfActorId = Runtime.AllocateEdgeActor();
        HttpProxyActorId = Runtime.AllocateEdgeActor();

        auto httpSender = NYql::NDq::CreateHttpSenderActor(
            SelfActorId,
            HttpProxyActorId,
            RetryPolicy
        );
        HttpSenderActorId = Runtime.Register(httpSender);
        Runtime.EnableScheduleForActor(HttpSenderActorId, true);
    }

    void SendHttpOutgoingRequest()
    {
        auto sender = Runtime.AllocateEdgeActor();
        auto req = NHttp::THttpOutgoingRequest::CreateRequestGet("124");
        auto request = std::make_unique<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(req);
        Runtime.Send(new IEventHandle(HttpSenderActorId, sender, request.release()));
    }

    template<typename T>
    std::pair<TAutoPtr<IEventHandle>, T*> Grab()
    {
        TAutoPtr<IEventHandle> handle;
        T* event = Runtime.GrabEdgeEvent<T>(handle, TDuration::Seconds(10));
        return {handle, event};
    }

    void HandleHttpProxyOutgoing(std::unique_ptr<NHttp::THttpIncomingResponse>&& response) {
        auto [_, event] = Grab<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>();

        Runtime.Send(new IEventHandle(
            HttpSenderActorId,
            HttpProxyActorId,
            new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(event->Request, response.release())));
    }

    void RaiseHttpProxySuccessResponse() {
        auto response = std::make_unique<NHttp::THttpIncomingResponse>(nullptr);
        response->Status = "200";
        HandleHttpProxyOutgoing(std::move(response));
    }

    void RaiseHttpProxyErrorResponse() {
        auto response = std::make_unique<NHttp::THttpIncomingResponse>(nullptr);
        response->Status = "500";
        HandleHttpProxyOutgoing(std::move(response));
    }
};

} // namespace

Y_UNIT_TEST_SUITE(THttpSenderTests) {
    Y_UNIT_TEST(SuccessResponse)
    {
        auto retryPolicy = NYql::NDq::THttpSenderRetryPolicy::GetNoRetryPolicy();
        TTestBootstrap bootstrap(retryPolicy);
        bootstrap.SendHttpOutgoingRequest();

        auto response = std::make_unique<NHttp::THttpIncomingResponse>(nullptr);
        response->Status = "200";
        bootstrap.HandleHttpProxyOutgoing(std::move(response));

        auto [_, event] = bootstrap.Grab<NYql::NDq::TEvHttpBase::TEvSendResult>();
        UNIT_ASSERT(event->IsTerminal);
        UNIT_ASSERT_EQUAL(event->RetryCount, 0);
        UNIT_ASSERT(event->HttpIncomingResponse->Get()->GetError().empty());
    }

    Y_UNIT_TEST(FailResponse)
    {
        auto retryPolicy = NYql::NDq::THttpSenderRetryPolicy::GetNoRetryPolicy();
        TTestBootstrap bootstrap(retryPolicy);
        bootstrap.SendHttpOutgoingRequest();

        auto response = std::make_unique<NHttp::THttpIncomingResponse>(nullptr);
        response->Status = "500";
        bootstrap.HandleHttpProxyOutgoing(std::move(response));

        auto [_, event] = bootstrap.Grab<NYql::NDq::TEvHttpBase::TEvSendResult>();
        UNIT_ASSERT(event->IsTerminal);
        UNIT_ASSERT_EQUAL(event->RetryCount, 0);
        UNIT_ASSERT(!event->HttpIncomingResponse->Get()->GetError().empty());
    }

    Y_UNIT_TEST(RetrySuccess)
    {
        auto retryPolicy = NYql::NDq::THttpSenderRetryPolicy::GetExponentialBackoffPolicy(
            [](const NHttp::TEvHttpProxy::TEvHttpIncomingResponse*){
                return ERetryErrorClass::ShortRetry;
            });
        TTestBootstrap bootstrap(retryPolicy);

        bootstrap.SendHttpOutgoingRequest();

        bootstrap.RaiseHttpProxyErrorResponse();
        {
            auto [_, event] = bootstrap.Grab<NYql::NDq::TEvHttpBase::TEvSendResult>();
            UNIT_ASSERT(!event->IsTerminal);
            UNIT_ASSERT_EQUAL(event->RetryCount, 0);
            UNIT_ASSERT(!event->HttpIncomingResponse->Get()->GetError().empty());
        }

        bootstrap.RaiseHttpProxySuccessResponse();
        {
            auto [_, event] = bootstrap.Grab<NYql::NDq::TEvHttpBase::TEvSendResult>();
            UNIT_ASSERT(event->IsTerminal);
            UNIT_ASSERT_EQUAL(event->RetryCount, 1);
            UNIT_ASSERT(event->HttpIncomingResponse->Get()->GetError().empty());
        }
    }

    Y_UNIT_TEST(RetryUnsuccess)
    {
        auto retryPolicy = NYql::NDq::THttpSenderRetryPolicy::GetExponentialBackoffPolicy(
            [](const NHttp::TEvHttpProxy::TEvHttpIncomingResponse*){
                return ERetryErrorClass::ShortRetry;
            },
            TDuration::MilliSeconds(10),
            TDuration::MilliSeconds(200),
            TDuration::Seconds(30),
            1);
        TTestBootstrap bootstrap(retryPolicy);

        bootstrap.SendHttpOutgoingRequest();

        bootstrap.RaiseHttpProxyErrorResponse();
        {
            auto [_, event] = bootstrap.Grab<NYql::NDq::TEvHttpBase::TEvSendResult>();
            UNIT_ASSERT(!event->IsTerminal);
            UNIT_ASSERT_EQUAL(event->RetryCount, 0);
            UNIT_ASSERT(!event->HttpIncomingResponse->Get()->GetError().empty());
        }

        bootstrap.RaiseHttpProxyErrorResponse();
        {
            auto [_, event] = bootstrap.Grab<NYql::NDq::TEvHttpBase::TEvSendResult>();
            UNIT_ASSERT(event->IsTerminal);
            UNIT_ASSERT_EQUAL(event->RetryCount, 1);
            UNIT_ASSERT(!event->HttpIncomingResponse->Get()->GetError().empty());
        }
    }
};

} // namespace NYql
