#include <ydb/core/cms/json_proxy_toggle_config_validator.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/testlib/actors/test_runtime.h>

#include <library/cpp/monlib/service/mon_service_http_request.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/null.h>

namespace NKikimr::NCmsTest {

namespace {

class TFakeMonHttpRequest: public NMonitoring::IMonHttpRequest {
public:
    TFakeMonHttpRequest(HTTP_METHOD method, TString uri, THttpHeaders headers, TString body)
        : Method(method)
        , Uri(std::move(uri))
        , Headers(std::move(headers))
        , Body(std::move(body))
        , Params(TStringBuf(Uri).After('?'))
        , PostParams(Body)
    {
    }

    IOutputStream& Output() override {
        return Cnull;
    }

    HTTP_METHOD GetMethod() const override {
        return Method;
    }

    TStringBuf GetPath() const override {
        return TStringBuf(Uri).Before('?');
    }

    TStringBuf GetPathInfo() const override {
        return GetPath();
    }

    TStringBuf GetUri() const override {
        return Uri;
    }

    const TCgiParameters& GetParams() const override {
        return Params;
    }

    const TCgiParameters& GetPostParams() const override {
        return PostParams;
    }

    TStringBuf GetPostContent() const override {
        return Body;
    }

    const THttpHeaders& GetHeaders() const override {
        return Headers;
    }

    TStringBuf GetHeader(TStringBuf name) const override {
        if (const auto* header = Headers.FindHeader(name)) {
            return header->Value();
        }
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
    const HTTP_METHOD Method;
    const TString Uri;
    const THttpHeaders Headers;
    const TString Body;
    const TCgiParameters Params;
    const TCgiParameters PostParams;
};

struct TEvPrivate {
    enum EEv {
        EvToggleRequestPrepared = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
    };

    struct TEvToggleRequestPrepared
        : NActors::TEventLocal<TEvToggleRequestPrepared, EvToggleRequestPrepared>
    {
        TEvToggleRequestPrepared(TString name, bool disable)
            : Name(std::move(name))
            , Disable(disable)
        {
        }

        TString Name;
        bool Disable;
    };
};

class TToggleRequestProbe: public NActors::TActor<TToggleRequestProbe> {
public:
    explicit TToggleRequestProbe(NActors::TActorId replyTo)
        : TActor(&TThis::StateWork)
        , ReplyTo(replyTo)
    {
    }

private:
    void Handle(NActors::NMon::TEvHttpInfo::TPtr& ev, const NActors::TActorContext& ctx) {
        NCms::TJsonProxyToggleConfigValidator handler(ev);
        auto request = handler.PrepareRequest(ctx);

        Y_ABORT_UNLESS(request);
        ctx.Send(ReplyTo, new TEvPrivate::TEvToggleRequestPrepared(
            request->Record.GetName(), request->Record.GetDisable()));
        PassAway();
    }

    STRICT_STFUNC(StateWork,
        HFunc(NActors::NMon::TEvHttpInfo, Handle);
    )

    const NActors::TActorId ReplyTo;
};

NActors::TTestActorRuntime::TEgg MakeTestEgg() {
    return {
        new TAppData(0, 0, 0, 0, {}, nullptr, nullptr, nullptr, nullptr),
        nullptr,
        nullptr,
        {},
        {},
    };
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TJsonProxyToggleConfigValidatorTest) {
    Y_UNIT_TEST(PostWithQueryParametersIsParsed) {
        NActors::TTestActorRuntime runtime;
        runtime.Initialize(MakeTestEgg());
        const auto edge = runtime.AllocateEdgeActor();
        const auto probe = runtime.Register(new TToggleRequestProbe(edge));

        THttpHeaders headers;
        headers.AddHeader({"X-CSRF-Token", "csrf-token"});
        TFakeMonHttpRequest request(
            HTTP_METHOD_POST,
            "/api/json/toggleconfigvalidator?name=validator%20name&enable=1",
            std::move(headers),
            "");

        runtime.Send(new NActors::IEventHandle(probe, edge, new NActors::NMon::TEvHttpInfo(request)));

        TAutoPtr<NActors::IEventHandle> handle;
        const auto response = runtime.GrabEdgeEventRethrow<TEvPrivate::TEvToggleRequestPrepared>(handle);
        UNIT_ASSERT_VALUES_EQUAL(response->Name, "validator name");
        UNIT_ASSERT_VALUES_EQUAL(response->Disable, false);
    }

    Y_UNIT_TEST(LegacyGetWithQueryParametersIsParsed) {
        NActors::TTestActorRuntime runtime;
        runtime.Initialize(MakeTestEgg());
        const auto edge = runtime.AllocateEdgeActor();
        const auto probe = runtime.Register(new TToggleRequestProbe(edge));

        THttpHeaders headers;
        TFakeMonHttpRequest request(
            HTTP_METHOD_GET,
            "/api/json/toggleconfigvalidator?name=legacy-validator&enable=0",
            std::move(headers),
            "");

        runtime.Send(new NActors::IEventHandle(probe, edge, new NActors::NMon::TEvHttpInfo(request)));

        TAutoPtr<NActors::IEventHandle> handle;
        const auto response = runtime.GrabEdgeEventRethrow<TEvPrivate::TEvToggleRequestPrepared>(handle);
        UNIT_ASSERT_VALUES_EQUAL(response->Name, "legacy-validator");
        UNIT_ASSERT_VALUES_EQUAL(response->Disable, true);
    }
}

} // namespace NKikimr::NCmsTest
