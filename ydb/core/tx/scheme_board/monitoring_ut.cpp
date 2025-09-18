#include "mon_events.h"
#include "monitoring.h"
#include "ut_helpers.h"

#include <ydb/core/testlib/basics/appdata.h>

#include <ydb/library/actors/core/mon.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/string/printf.h>

namespace NKikimr {
namespace NSchemeBoard {

Y_UNIT_TEST_SUITE(TMonitoringTests) {
    static constexpr char HTTPUNAVAILABLE[] = "HTTP/1.1 503 Service Unavailable\r\nConnection: Close\r\n\r\nService Unavailable\r\n";

    struct TMockMonRequest: public NMonitoring::IMonHttpRequest {
        explicit TMockMonRequest(const TString& pathInfo = {}, const TCgiParameters& params = {})
            : PathInfo(pathInfo)
            , Params(params)
        {
        }

        TStringBuf GetPathInfo() const override {
            return PathInfo;
        }

        const TCgiParameters& GetParams() const override {
            return Params;
        }

        IOutputStream& Output() override { Y_ABORT("Not implemented"); }
        HTTP_METHOD GetMethod() const override { Y_ABORT("Not implemented"); }
        TStringBuf GetPath() const override { Y_ABORT("Not implemented"); }
        TStringBuf GetUri() const override { Y_ABORT("Not implemented"); }
        const TCgiParameters& GetPostParams() const override { Y_ABORT("Not implemented"); }
        TStringBuf GetPostContent() const override { Y_ABORT("Not implemented"); }
        const THttpHeaders& GetHeaders() const override { Y_ABORT("Not implemented"); }
        TStringBuf GetHeader(TStringBuf) const override { Y_ABORT("Not implemented"); }
        TStringBuf GetCookie(TStringBuf) const override { Y_ABORT("Not implemented"); }
        TString GetRemoteAddr() const override { Y_ABORT("Not implemented"); }
        TString GetServiceTitle() const override { Y_ABORT("Not implemented"); }
        NMonitoring::IMonPage* GetPage() const override { Y_ABORT("Not implemented"); }
        NMonitoring::IMonHttpRequest* MakeChild(NMonitoring::IMonPage*, const TString&) const override { Y_ABORT("Not implemented"); }

    private:
        const TString PathInfo;
        const TCgiParameters Params;
    };

    TActorId CreateMonitoring(TTestActorRuntimeBase& runtime) {
        const auto actorId = runtime.Register(CreateSchemeBoardMonitoring());

        // wait until actor is ready
        TDispatchOptions opts;
        opts.FinalEvents.emplace_back([&actorId](IEventHandle& ev) {
            return ev.Recipient == actorId && ev.GetTypeRewrite() == TEvents::TSystem::Bootstrap;
        });
        runtime.DispatchEvents(opts);

        return actorId;
    }

    void TestActorId(TTestContext& ctx, const TActorId& monitoring, const TActorId& sender,
            const TString& path, const TCgiParameters& params, const TString& expectedAnswer)
    {
        auto request = MakeHolder<TMockMonRequest>(path, params);
        ctx.Send(monitoring, sender, new NMon::TEvHttpInfo(*request.Get()));

        auto ev = ctx.GrabEdgeEvent<NMon::TEvHttpInfoRes>(sender);
        UNIT_ASSERT(ev->Get());

        auto response = static_cast<NMon::TEvHttpInfoRes*>(ev->Get());
        UNIT_ASSERT_VALUES_EQUAL(response->Answer, expectedAnswer);
    }

    void TestValidActorId(TTestContext& ctx, const TActorId& monitoring, const TActorId& sender,
            const TString& path, const TCgiParameters& params = {})
    {
        TestActorId(ctx, monitoring, sender, path, params, HTTPUNAVAILABLE);
    }

    Y_UNIT_TEST(ValidActorId) {
        auto context = MakeHolder<TTestContext>();
        context->Initialize(TAppPrepare().Unwrap());

        const auto monitoring = CreateMonitoring(*context);
        const auto sender = context->AllocateEdgeActor();

        for (const TString& actorId : {"7007306538023635091:201863463214"}) {
            TestValidActorId(*context, monitoring, sender, Sprintf("/populator/%s", actorId.data()));
            TestValidActorId(*context, monitoring, sender, Sprintf("/replica_populator/%s", actorId.data()));
            TestValidActorId(*context, monitoring, sender, Sprintf("/replica/%s", actorId.data()));
            TestValidActorId(*context, monitoring, sender, Sprintf("/subscriber/%s", actorId.data()));
            TestValidActorId(*context, monitoring, sender, Sprintf("/subscriber_proxy/%s", actorId.data()));
            TestValidActorId(*context, monitoring, sender, Sprintf("/replica_subscriber/%s", actorId.data()));
            TestValidActorId(*context, monitoring, sender, Sprintf("/cache/%s", actorId.data()));
            TestActorId(*context, monitoring, sender, "/describe", {{"actorId", actorId}}, NMonitoring::HTTPNOTFOUND);
        }
    }

    void TestInvalidActorId(TTestContext& ctx, const TActorId& monitoring, const TActorId& sender,
            const TString& path, const TCgiParameters& params = {})
    {
        TestActorId(ctx, monitoring, sender, path, params, NMonitoring::HTTPNOTFOUND);
    }

    Y_UNIT_TEST(InvalidActorId) {
        auto context = MakeHolder<TTestContext>();
        context->Initialize(TAppPrepare().Unwrap());

        const auto monitoring = CreateMonitoring(*context);
        const auto sender = context->AllocateEdgeActor();

        for (const TString& actorId : {"Invalid", "Invalid:1", "1:Invalid"}) {
            TestInvalidActorId(*context, monitoring, sender, Sprintf("/populator/%s", actorId.data()));
            TestInvalidActorId(*context, monitoring, sender, Sprintf("/replica_populator/%s", actorId.data()));
            TestInvalidActorId(*context, monitoring, sender, Sprintf("/replica/%s", actorId.data()));
            TestInvalidActorId(*context, monitoring, sender, Sprintf("/subscriber/%s", actorId.data()));
            TestInvalidActorId(*context, monitoring, sender, Sprintf("/subscriber_proxy/%s", actorId.data()));
            TestInvalidActorId(*context, monitoring, sender, Sprintf("/replica_subscriber/%s", actorId.data()));
            TestInvalidActorId(*context, monitoring, sender, Sprintf("/cache/%s", actorId.data()));
            TestInvalidActorId(*context, monitoring, sender, "/describe", {{"actorId", actorId}});
        }
    }
}

} // NSchemeBoard
} // NKikimr
