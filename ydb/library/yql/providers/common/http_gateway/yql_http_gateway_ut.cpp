#include "yql_http_default_retry_policy.h"
#include "yql_http_gateway.h"

#include <library/cpp/http/server/http.h>
#include <library/cpp/testing/common/network.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/core/future.h>
#include <util/generic/size_literals.h>
#include <util/string/builder.h>
#include <yql/essentials/utils/log/log.h>

#include <mutex>
#include <vector>

namespace NYql {
namespace {

class TResetUploadServer: public THttpServer::ICallBack {
public:
    struct TRequestData {
        TString FirstLine;
        TString Body;
    };

    TClientRequest* CreateClient() override {
        return new TRequest(*this);
    }

    std::vector<TRequestData> GetRequests() const {
        const std::lock_guard lock(Mutex);
        return Requests;
    }

private:
    class TRequest: public TClientRequest {
    public:
        explicit TRequest(TResetUploadServer& owner)
            : Owner(owner)
        {
        }

        bool Reply(void*) override {
            TRequestData request{ Input().FirstLine(), Input().ReadAll() };
            size_t count;
            {
                const std::lock_guard lock(Owner.Mutex);
                Owner.Requests.push_back(std::move(request));
                count = Owner.Requests.size();
            }

            if (count == 2) {
                // The first request primes a keep-alive connection. Reset it after
                // consuming the upload, before sending any response: libcurl then
                // tries to rewind the body to retry on a fresh connection.
                ResetConnection();
            } else {
                Output() << "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
                Output().Finish();
            }
            return true;
        }

    private:
        TResetUploadServer& Owner;
    };

    mutable std::mutex Mutex;
    std::vector<TRequestData> Requests;
};

void CheckUploadRetry(bool put) {
    NLog::YqlLoggerScope logger(&Cerr);
    auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
    const auto gateway = IHTTPGateway::Make(nullptr, counters);
    const auto port = NTesting::GetFreePort();
    TResetUploadServer serverImpl;
    THttpServer server(&serverImpl, THttpServer::TOptions(port).EnableKeepAlive(true));
    UNIT_ASSERT(server.Start());

    const TString url = TStringBuilder() << "http://localhost:" << port << "/object?uploadId=test";
    auto warmup = NThreading::NewPromise<IHTTPGateway::TResult>();
    gateway->Download(url, {}, 0, 0, [warmup](IHTTPGateway::TResult&& result) mutable {
        warmup.SetValue(std::move(result));
    });
    UNIT_ASSERT(warmup.GetFuture().Wait(TDuration::Seconds(10)));
    UNIT_ASSERT_VALUES_EQUAL(warmup.GetFuture().GetValue().Content.HttpResponseCode, 200);

    // A binary payload larger than curl's read buffer catches missing or partial rewinds.
    TString body(256_KB, '\0');
    for (size_t i = 0; i < body.size(); ++i) {
        body[i] = static_cast<char>(i % 251);
    }
    IHTTPGateway::THeaders headers;
    // An interim 100 response would prevent libcurl's reused-connection retry.
    headers.Fields.push_back("Expect:");

    auto upload = NThreading::NewPromise<IHTTPGateway::TResult>();
    gateway->Upload(url, headers, body, [upload](IHTTPGateway::TResult&& result) mutable {
        upload.SetValue(std::move(result));
    }, put, GetFqHTTPRetryPolicy());
    UNIT_ASSERT(upload.GetFuture().Wait(TDuration::Seconds(10)));
    const auto& result = upload.GetFuture().GetValue();
    UNIT_ASSERT_C(result.Issues.Empty(), result.Issues.ToOneLineString());
    UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(result.CurlResponseCode), static_cast<int>(CURLE_OK));
    UNIT_ASSERT_VALUES_EQUAL(result.Content.HttpResponseCode, 200);

    server.Stop();
    const auto requests = serverImpl.GetRequests();
    UNIT_ASSERT_VALUES_EQUAL(requests.size(), 3);
    UNIT_ASSERT_VALUES_EQUAL(requests[1].FirstLine, requests[2].FirstLine);
    UNIT_ASSERT_VALUES_EQUAL(requests[1].Body, body);
    UNIT_ASSERT_VALUES_EQUAL(requests[2].Body, body);
    const auto rewindErrors = counters->GetSubgroup("method", put ? "PUT" : "POST")
                                  ->GetSubgroup("curl_code", ToString(static_cast<int>(CURLE_SEND_FAIL_REWIND)))
                                  ->GetCounter("count", true);
    UNIT_ASSERT_VALUES_EQUAL(rewindErrors->Val(), 1);
}

}   // namespace

Y_UNIT_TEST_SUITE(THttpGatewayRetryTest) {
    Y_UNIT_TEST(PutRetriesRewindFailureWithFullBody) {
        CheckUploadRetry(true);
    }

    Y_UNIT_TEST(PostRetriesRewindFailureWithFullBody) {
        CheckUploadRetry(false);
    }
}

}   // namespace NYql
