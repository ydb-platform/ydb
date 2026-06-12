#include <ydb/core/cms/http_dump.h>

#include <library/cpp/monlib/service/mon_service_http_request.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/null.h>

namespace NKikimr::NCmsTest {

namespace {

class TFakeMonHttpRequest : public NMonitoring::IMonHttpRequest {
public:
    TFakeMonHttpRequest(HTTP_METHOD method, TStringBuf uri, THttpHeaders headers, TStringBuf body)
        : Method(method)
        , Uri(uri)
        , Headers(std::move(headers))
        , Body(body)
    {
    }

    IOutputStream& Output() override {
        return Cnull;
    }

    HTTP_METHOD GetMethod() const override {
        return Method;
    }

    TStringBuf GetPath() const override {
        return {};
    }

    TStringBuf GetPathInfo() const override {
        return {};
    }

    TStringBuf GetUri() const override {
        return Uri;
    }

    const TCgiParameters& GetParams() const override {
        return Params;
    }

    const TCgiParameters& GetPostParams() const override {
        return Params;
    }

    TStringBuf GetPostContent() const override {
        return Body;
    }

    const THttpHeaders& GetHeaders() const override {
        return Headers;
    }

    TStringBuf GetHeader(TStringBuf) const override {
        return {};
    }

    TStringBuf GetCookie(TStringBuf) const override {
        return {};
    }

    TString GetRemoteAddr() const override {
        return {};
    }

    TString GetServiceTitle() const override {
        return {};
    }

    NMonitoring::IMonPage* GetPage() const override {
        return nullptr;
    }

    NMonitoring::IMonHttpRequest* MakeChild(NMonitoring::IMonPage*, const TString&) const override {
        return nullptr;
    }

private:
    HTTP_METHOD Method;
    TStringBuf Uri;
    THttpHeaders Headers;
    TStringBuf Body;
    TCgiParameters Params;
};

} // anonymous namespace

Y_UNIT_TEST_SUITE(THttpDumpTest) {
    Y_UNIT_TEST(DumpRequest) {
        THttpHeaders headers;
        headers.AddHeader({"Host", "example.com"});
        headers.AddHeader({"Content-Type", "application/json"});
        headers.AddHeader({"Authorization", "Bearer secret-token"});
        headers.AddHeader({"X-Ya-Service-Ticket", "ticket-value"});
        headers.AddHeader({"Session_id", "session-value"});

        TFakeMonHttpRequest request(HTTP_METHOD_POST, "/api/some/path?x=1",
            std::move(headers), "request-body-payload");
        const TString dump = NCms::DumpRequest(request);

        UNIT_ASSERT_C(dump.Contains("Uri: /api/some/path?x=1"), dump);
        UNIT_ASSERT_C(dump.Contains("Body: request-body-payload"), dump);
        UNIT_ASSERT_C(dump.Contains("Host: example.com"), dump);
        UNIT_ASSERT_C(dump.Contains("Content-Type: application/json"), dump);

        UNIT_ASSERT_C(!dump.Contains("Authorization"), dump);
        UNIT_ASSERT_C(!dump.Contains("Bearer secret-token"), dump);
        UNIT_ASSERT_C(!dump.Contains("X-Ya-Service-Ticket"), dump);
        UNIT_ASSERT_C(!dump.Contains("ticket-value"), dump);
        UNIT_ASSERT_C(!dump.Contains("Session_id"), dump);
        UNIT_ASSERT_C(!dump.Contains("session-value"), dump);
    }
}

} // namespace NKikimr::NCmsTest
