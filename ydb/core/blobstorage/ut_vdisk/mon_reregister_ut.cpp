#include "defs.h"
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/executor_pool_io.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/core/mon/mon.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/actors/protos/services_common.pb.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/scheme_types/scheme_type_registry.h>
#include <ydb/core/scheme/scheme_type_registry.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <util/system/event.h>
#include <ydb/core/blobstorage/ut_vdisk/lib/astest.h>
#include <ydb/core/blobstorage/ut_vdisk/lib/http_client.h>
#include <util/stream/null.h>

#define STR Cnull

using namespace NKikimr;


Y_UNIT_TEST_SUITE(TMonitoring) {

    ////////////////////////////////////////////////////////////////////
    // TMyWorker
    ////////////////////////////////////////////////////////////////////
    class TMyWorker : public TActorBootstrapped<TMyWorker> {
    private:
        const unsigned Incarnation;
        const TActorId ParentId;

        friend class TActorBootstrapped<TMyWorker>;

        void Bootstrap(const TActorContext &ctx) {
            TAppData *appData = AppData(ctx);
            Y_ABORT_UNLESS(appData);
            auto mon = appData->Mon;
            Y_ABORT_UNLESS(mon);

            NMonitoring::TIndexMonPage *actorsMonPage = mon->RegisterIndexPage("actors", "Actors");

            TString path = "myworker";
            TString name = "myworker";
            mon->RegisterActorPage(actorsMonPage, path, name, false, ctx.ExecutorThread.ActorSystem, ctx.SelfID);

            STR << "MyWorker Bootstrap: Incarnation: " << Incarnation << "\n";

            Become(&TThis::StateFunc);
            ctx.Send(ParentId, new TEvents::TEvCompleted()); // notify parent about successfull start
        }

        void HandlePoison(const TActorContext &ctx) {
            STR << "MyWorker HandlePoison: Incarnation: " << Incarnation << "\n";
            Die(ctx);
        }

        void Handle(NMon::TEvHttpInfo::TPtr &ev, const TActorContext &ctx) {
            TStringStream str;
            HTML(str) {
                DIV_CLASS("alert alert-error") {str << "Incarnation: " << Incarnation;}
            }

            ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str()));

        }

        STRICT_STFUNC(StateFunc,
            HFunc(NMon::TEvHttpInfo, Handle)
            CFunc(NActors::TEvents::TSystem::PoisonPill, HandlePoison)
        )

    public:
        TMyWorker(unsigned incarnation, const TActorId &parentId)
            : TActorBootstrapped<TThis>()
            , Incarnation(incarnation)
            , ParentId(parentId)
        {}
    };



    ////////////////////////////////////////////////////////////////////
    // TMyTest
    ////////////////////////////////////////////////////////////////////
    class TMyTest : public TActorBootstrapped<TMyTest> {
        std::shared_ptr<TTestWithActorSystem> Env;
        TActorId WorkerId;

        friend class TActorBootstrapped<TMyTest>;

        void Bootstrap(const TActorContext &ctx) {
            STR << "MyTest: Run Worker with Incarnation 1\n";
            WorkerId = ctx.Register(new TMyWorker(1, ctx.SelfID));
            Become(&TThis::StateFunc1);
        }

        STRICT_STFUNC(StateFunc1,
            CFunc(NActors::TEvents::TSystem::Completed, HandleInc1Ready)
        )

        void HandleInc1Ready(const TActorContext &ctx) {
            ctx.Send(WorkerId, new TEvents::TEvPoisonPill());
            STR << "MyTest: Run Worker with Incarnation 2\n";
            WorkerId = ctx.Register(new TMyWorker(2, ctx.SelfID));
            Become(&TThis::StateFunc2);
        }

        STRICT_STFUNC(StateFunc2,
            CFunc(NActors::TEvents::TSystem::Completed, HandleInc2Ready)
        )

        void HandleInc2Ready(const TActorContext &ctx) {
            THttpClient client("localhost", Env->MonPort);
            TStringStream str;

            client.SendHttpRequest("/actors/myworker", "", "GET", &str);
            STR << str.Str() << "\n";

            const TString resp = str.Str();
            // we MUST get response from "Incarnation: 2"
            Y_ABORT_UNLESS(resp.find("Incarnation: 2") != TString::npos);

            Env->Signal();
            Die(ctx);
        }

    public:
        TMyTest(std::shared_ptr<TTestWithActorSystem> env)
            : TActorBootstrapped<TThis>()
            , Env(env)
        {}
    };

    ////////////////////////////////////////////////////////////////////
    // Unit test
    ////////////////////////////////////////////////////////////////////
    Y_UNIT_TEST(ReregisterTest) {
        std::shared_ptr<TTestWithActorSystem> test(new TTestWithActorSystem(0));
        test->Run(new TMyTest(test));
    }
}
