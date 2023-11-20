#include <library/cpp/testing/unittest/registar.h>

#include "client.h"
#include "http_code_extractor.h"

#include <library/cpp/messagebus/rain_check/test/ut/test.h>

#include <library/cpp/messagebus/test/helper/fixed_port.h>

#include <library/cpp/http/io/stream.h>
#include <library/cpp/neh/rpc.h>

#include <util/generic/cast.h>
#include <util/generic/ptr.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/network/ip.h>
#include <util/stream/str.h>
#include <util/string/printf.h>
#include <util/system/defaults.h>
#include <util/system/yassert.h>

#include <cstdlib>
#include <utility>

using namespace NRainCheck;
using namespace NBus::NTest;

namespace {
    class THttpClientEnv: public TTestEnvTemplate<THttpClientEnv> {
    public:
        THttpClientService HttpClientService;
    };

    const TString TEST_SERVICE = "test-service";
    const TString TEST_GET_PARAMS = "p=GET";
    const TString TEST_POST_PARAMS = "p=POST";
    const TString TEST_POST_HEADERS = "Content-Type: application/json\r\n";
    const TString TEST_GET_RECV = "GET was ok.";
    const TString TEST_POST_RECV = "POST was ok.";

    TString BuildServiceLocation(ui32 port) {
        return Sprintf("http://*:%" PRIu32 "/%s", port, TEST_SERVICE.data());
    }

    TString BuildPostServiceLocation(ui32 port) {
        return Sprintf("post://*:%" PRIu32 "/%s", port + 1, TEST_SERVICE.data());
    }

    TString BuildGetTestRequest(ui32 port) {
        return BuildServiceLocation(port) + "?" + TEST_GET_PARAMS;
    }

    class TSimpleServer {
    public:
        inline void ServeRequest(const NNeh::IRequestRef& req) {
            NNeh::TData response;
            if (req->Data() == TEST_GET_PARAMS) {
                response.assign(TEST_GET_RECV.begin(), TEST_GET_RECV.end());
            } else {
                response.assign(TEST_POST_RECV.begin(), TEST_POST_RECV.end());
            }
            req->SendReply(response);
        }
    };

    NNeh::IServicesRef RunServer(ui32 port, TSimpleServer& server) {
        NNeh::IServicesRef runner = NNeh::CreateLoop();
        runner->Add(BuildServiceLocation(port), server);
        runner->Add(BuildPostServiceLocation(port), server);

        try {
            const int THR_POOL_SIZE = 2;
            runner->ForkLoop(THR_POOL_SIZE);
        } catch (...) {
            Y_ABORT("Can't run server: %s", CurrentExceptionMessage().data());
        }

        return runner;
    }
    enum ERequestType {
        RT_HTTP_GET = 0,
        RT_HTTP_POST = 1
    };

    using TTaskParam = std::pair<TIpPort, ERequestType>;

    class THttpClientTask: public ISimpleTask {
    public:
        THttpClientTask(THttpClientEnv* env, TTaskParam param)
            : Env(env)
            , ServerPort(param.first)
            , ReqType(param.second)
        {
        }

        TContinueFunc Start() override {
            switch (ReqType) {
                case RT_HTTP_GET: {
                    TString getRequest = BuildGetTestRequest(ServerPort);
                    for (size_t i = 0; i < 3; ++i) {
                        Requests.push_back(new THttpFuture());
                        Env->HttpClientService.Send(getRequest, Requests[i].Get());
                    }
                    break;
                }
                case RT_HTTP_POST: {
                    TString servicePath = BuildPostServiceLocation(ServerPort);
                    TStringInput headersText(TEST_POST_HEADERS);
                    THttpHeaders headers(&headersText);
                    for (size_t i = 0; i < 3; ++i) {
                        Requests.push_back(new THttpFuture());
                        Env->HttpClientService.SendPost(servicePath, TEST_POST_PARAMS, headers, Requests[i].Get());
                    }
                    break;
                }
            }

            return &THttpClientTask::GotReplies;
        }

        TContinueFunc GotReplies() {
            const TString& TEST_OK_RECV = (ReqType == RT_HTTP_GET) ? TEST_GET_RECV : TEST_POST_RECV;
            for (size_t i = 0; i < Requests.size(); ++i) {
                UNIT_ASSERT_EQUAL(Requests[i]->GetHttpCode(), 200);
                UNIT_ASSERT_EQUAL(Requests[i]->GetResponseBody(), TEST_OK_RECV);
            }

            Env->TestSync.CheckAndIncrement(0);

            return nullptr;
        }

        THttpClientEnv* const Env;
        const TIpPort ServerPort;
        const ERequestType ReqType;

        TVector<TSimpleSharedPtr<THttpFuture>> Requests;
    };

} // anonymous namespace

Y_UNIT_TEST_SUITE(RainCheckHttpClient) {
    static const TIpPort SERVER_PORT = 4000;

    Y_UNIT_TEST(Simple) {
        // TODO: randomize port
        if (!IsFixedPortTestAllowed()) {
            return;
        }

        TSimpleServer server;
        NNeh::IServicesRef runner = RunServer(SERVER_PORT, server);

        THttpClientEnv env;
        TIntrusivePtr<TSimpleTaskRunner> task = env.SpawnTask<THttpClientTask>(TTaskParam(SERVER_PORT, RT_HTTP_GET));

        env.TestSync.WaitForAndIncrement(1);
    }

    Y_UNIT_TEST(SimplePost) {
        // TODO: randomize port
        if (!IsFixedPortTestAllowed()) {
            return;
        }

        TSimpleServer server;
        NNeh::IServicesRef runner = RunServer(SERVER_PORT, server);

        THttpClientEnv env;
        TIntrusivePtr<TSimpleTaskRunner> task = env.SpawnTask<THttpClientTask>(TTaskParam(SERVER_PORT, RT_HTTP_POST));

        env.TestSync.WaitForAndIncrement(1);
    }

    Y_UNIT_TEST(HttpCodeExtraction) {
    // Find "request failed(" string, then copy len("HTTP/1.X NNN") chars and try to convert NNN to HTTP code.

#define CHECK_VALID_LINE(line, code)                                    \
    UNIT_ASSERT_NO_EXCEPTION(TryGetHttpCodeFromErrorDescription(line)); \
    UNIT_ASSERT(!!TryGetHttpCodeFromErrorDescription(line));            \
    UNIT_ASSERT_EQUAL(*TryGetHttpCodeFromErrorDescription(line), code)

        CHECK_VALID_LINE(TStringBuf("library/cpp/neh/http.cpp:<LINE>: request failed(HTTP/1.0 200 Some random message"), 200);
        CHECK_VALID_LINE(TStringBuf("library/cpp/neh/http.cpp:<LINE>: request failed(HTTP/1.0 404 Some random message"), 404);
        CHECK_VALID_LINE(TStringBuf("request failed(HTTP/1.0 100 Some random message"), 100);
        CHECK_VALID_LINE(TStringBuf("request failed(HTTP/1.0 105)"), 105);
        CHECK_VALID_LINE(TStringBuf("request failed(HTTP/1.1 2004 Some random message"), 200);
#undef CHECK_VALID_LINE

#define CHECK_INVALID_LINE(line)                                        \
    UNIT_ASSERT_NO_EXCEPTION(TryGetHttpCodeFromErrorDescription(line)); \
    UNIT_ASSERT(!TryGetHttpCodeFromErrorDescription(line))

        CHECK_INVALID_LINE(TStringBuf("library/cpp/neh/http.cpp:<LINE>: request failed(HTTP/1.1 1 Some random message"));
        CHECK_INVALID_LINE(TStringBuf("request failed(HTTP/1.0 asdf Some random message"));
        CHECK_INVALID_LINE(TStringBuf("HTTP/1.0 200 Some random message"));
        CHECK_INVALID_LINE(TStringBuf("request failed(HTTP/1.0 2x00 Some random message"));
        CHECK_INVALID_LINE(TStringBuf("HTTP/1.0 200 Some random message"));
        CHECK_INVALID_LINE(TStringBuf("HTTP/1.0 200"));
        CHECK_INVALID_LINE(TStringBuf("request failed(HTTP/1.1  3334 Some random message"));
#undef CHECK_INVALID_LINE
    }
}
