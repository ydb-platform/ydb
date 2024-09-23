#include "defs.h"
#include "immediate_control_board_actor.h"
#include "immediate_control_board_wrapper.h"

#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/core/mon/mon.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/base/tablet.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/executor_pool_io.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/generic/string.h>
#include <util/generic/yexception.h>

namespace NKikimr {

constexpr ui32 TEST_TIMEOUT = NSan::PlainOrUnderSanitizer(300000, 1200000);


#define ASSERT_YTHROW(expr, str) \
do { \
    if (!(expr)) { \
        ythrow TWithBackTrace<yexception>() << str; \
    } \
} while(false)


#define VERBOSE_COUT(str) \
do { \
    if (IsVerbose) { \
        Cerr << str << Endl; \
    } \
} while(false)


static bool IsVerbose = false;

static THolder<TActorSystem> ActorSystem;

static TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
static THolder<NActors::TMon> Monitoring;

static TAtomic DoneCounter = 0;
static TSystemEvent DoneEvent(TSystemEvent::rAuto);
static yexception LastException;
static volatile bool IsLastExceptionSet = false;


static void SignalDoneEvent() {
    AtomicIncrement(DoneCounter);
    DoneEvent.Signal();
}

struct TTestConfig {
    TActorId IcbActorId;
    TControlBoard *Icb;

    TTestConfig(TActorId icbActorId, TControlBoard *icb)
        : IcbActorId(icbActorId)
        , Icb(icb)
    {}
};

template <class T>
static void Run(i64 instances = 1) {
    TVector<TActorId> testIds;
    TAppData appData(0, 0, 0, 0, TMap<TString, ui32>(),
                     nullptr, nullptr, nullptr, nullptr);

    try {
        Counters = TIntrusivePtr<::NMonitoring::TDynamicCounters>(new ::NMonitoring::TDynamicCounters());

        testIds.resize(instances);

        TIntrusivePtr<TTableNameserverSetup> nameserverTable(new TTableNameserverSetup());
        TPortManager pm;
        nameserverTable->StaticNodeTable[1] = std::pair<TString, ui32>("127.0.0.1", pm.GetPort(12001));
        nameserverTable->StaticNodeTable[2] = std::pair<TString, ui32>("127.0.0.1", pm.GetPort(12002));

        THolder<TActorSystemSetup> setup(new TActorSystemSetup());
        setup->NodeId = 1;
        setup->ExecutorsCount = 3;
        setup->Executors.Reset(new TAutoPtr<IExecutorPool>[3]);
        setup->Executors[0].Reset(new TBasicExecutorPool(0, 2, 20));
        setup->Executors[1].Reset(new TBasicExecutorPool(1, 2, 20));
        setup->Executors[2].Reset(new TIOExecutorPool(2, 10));
        setup->Scheduler.Reset(new TBasicSchedulerThread(TSchedulerConfig(512, 100)));

        const TActorId nameserviceId = GetNameserviceActorId();
        TActorSetupCmd nameserviceSetup(CreateNameserverTable(nameserverTable), TMailboxType::Simple, 0);
        setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(nameserviceId, std::move(nameserviceSetup)));

        // ICB Actor creation
        TActorId IcbActorId = MakeIcbId(setup->NodeId);
        TActorSetupCmd testSetup(CreateImmediateControlActor(appData.Icb, Counters), TMailboxType::Revolving, 0);
        setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(IcbActorId, std::move(testSetup)));


        THolder<TTestConfig> testConfig(new TTestConfig(IcbActorId, appData.Icb.Get()));
        for (ui32 i = 0; i < instances; ++i) {
            testIds[i] = MakeBlobStorageProxyID(1 + i);
            TActorSetupCmd testSetup(new T(testConfig.Get()), TMailboxType::Revolving, 0);
            setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(testIds[i], std::move(testSetup)));
        }

        AtomicSet(DoneCounter, 0);


        /////////////////////// LOGGER ///////////////////////////////////////////////

        NActors::TActorId loggerActorId = NActors::TActorId(1, "logger");
        TIntrusivePtr<NActors::NLog::TSettings> logSettings(
            new NActors::NLog::TSettings(loggerActorId, NActorsServices::LOGGER, NActors::NLog::PRI_ERROR, NActors::NLog::PRI_ERROR, 0));
        logSettings->Append(
            NActorsServices::EServiceCommon_MIN,
            NActorsServices::EServiceCommon_MAX,
            NActorsServices::EServiceCommon_Name
        );
        logSettings->Append(
            NKikimrServices::EServiceKikimr_MIN,
            NKikimrServices::EServiceKikimr_MAX,
            NKikimrServices::EServiceKikimr_Name
        );

        TString explanation;
        logSettings->SetLevel(NLog::PRI_EMERG, NKikimrServices::BS_PDISK, explanation);

        NActors::TLoggerActor *loggerActor = new NActors::TLoggerActor(logSettings, NActors::CreateStderrBackend(),
            GetServiceCounters(Counters, "utils"));
        NActors::TActorSetupCmd loggerActorCmd(loggerActor, NActors::TMailboxType::Simple, 2);
        std::pair<NActors::TActorId, NActors::TActorSetupCmd> loggerActorPair(loggerActorId, std::move(loggerActorCmd));
        setup->LocalServices.push_back(std::move(loggerActorPair));
        //////////////////////////////////////////////////////////////////////////////

        ActorSystem.Reset(new TActorSystem(setup, &appData, logSettings));

        ActorSystem->Start();

        VERBOSE_COUT("Sending TEvBoot to test");
        for (ui32 i = 0; i < instances; ++i) {
            ActorSystem->Send(testIds[i], new TEvTablet::TEvBoot(MakeTabletID(false, 1), 0, nullptr, TActorId(), nullptr));
        }

        TAtomicBase doneCount = 0;
        bool isOk = true;
        TInstant startTime = Now();
        while (doneCount < instances && isOk) {
            ui32 msRemaining = TEST_TIMEOUT - (ui32)(Now() - startTime).MilliSeconds();
            isOk = DoneEvent.Wait(msRemaining);
            doneCount = AtomicGet(DoneCounter);
        }
        UNIT_ASSERT_VALUES_EQUAL_C(doneCount, instances, "test timeout ");
    } catch (yexception ex) {
        LastException = ex;
        IsLastExceptionSet = true;
        VERBOSE_COUT(ex.what());
    }

    Monitoring.Destroy();
    if (ActorSystem.Get()) {
        ActorSystem->Stop();
        ActorSystem.Destroy();
    }
    DoneEvent.Reset();
    if (IsLastExceptionSet) {
        IsLastExceptionSet = false;
        ythrow LastException;
    }
}

class TBaseTest : public TActor<TBaseTest> {
protected:
    struct TResponseData {

        void *Cookie;
        NKikimrProto::EReplyStatus Status;
        NMon::TEvHttpInfoRes *HttpResult;

        TResponseData() {
            Clear();
        }

        void Clear() {
            Cookie = (void*)((ui64)-1);
            Status = NKikimrProto::OK;
            HttpResult = nullptr;
        }

        void Check() {
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&Cookie, sizeof(Cookie));
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&Status, sizeof(Status));
        }
    };

    TResponseData LastResponse;

    const TActorId IcbActor;
    TControlBoard *Icb;
    int TestStep;

    virtual void TestFSM(const TActorContext &ctx) = 0;

    void ActTestFSM(const TActorContext &ctx) {
        LastResponse.Check();
        try {
            TestFSM(ctx);
            LastResponse.Clear();
        }
        catch (yexception ex) {
            LastException = ex;
            IsLastExceptionSet = true;
            SignalDoneEvent();
        }
    }
    void HandleBoot(TEvTablet::TEvBoot::TPtr &ev, const TActorContext &ctx) {
        ActTestFSM(ctx);
        Y_UNUSED(ev);
    }

    void Handle(NMon::TEvHttpInfoRes::TPtr &ev, const TActorContext &ctx) {
        LastResponse.HttpResult = static_cast<NMon::TEvHttpInfoRes*>(ev->Get());
        ActTestFSM(ctx);
    }

public:
    TBaseTest(TTestConfig *cfg)
        : TActor(&TThis::StateRegister)
        , IcbActor(cfg->IcbActorId)
        , Icb(cfg->Icb)
        , TestStep(0)
    {}

    STFUNC(StateRegister) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NMon::TEvHttpInfoRes, Handle);
            //HFunc(NNodeWhiteboard::TEvWhiteboard::, Handle);
            HFunc(TEvTablet::TEvBoot, HandleBoot);
        }
    }
};

struct THttpRequest : NMonitoring::IHttpRequest {
    HTTP_METHOD Method;
    TCgiParameters CgiParameters;
    THttpHeaders HttpHeaders;

    THttpRequest(HTTP_METHOD method)
        : Method(method)
    {}

    ~THttpRequest() {}

    const char* GetURI() const override {
        return "";
    }

    const char* GetPath() const override {
        return "";
    }

    const TCgiParameters& GetParams() const override {
        return CgiParameters;
    }

    const TCgiParameters& GetPostParams() const override {
        return CgiParameters;
    }

    TStringBuf GetPostContent() const override {
        return TString();
    }

    HTTP_METHOD GetMethod() const override {
        return Method;
    }

    const THttpHeaders& GetHeaders() const override {
        return HttpHeaders;
    }

    TString GetRemoteAddr() const override {
        return TString();
    }
};

class TTestHttpGetResponse : public TBaseTest {
    TAutoPtr<THttpRequest> HttpRequest;
    NMonitoring::TMonService2HttpRequest MonService2HttpRequest;

    void TestFSM(const TActorContext &ctx) {
        Y_UNUSED(ctx);
        VERBOSE_COUT("Test step " << TestStep);
        switch (TestStep) {
            case 0:
                VERBOSE_COUT("Sending TEvHttpInfo");
                ctx.Send(IcbActor, new NMon::TEvHttpInfo(MonService2HttpRequest));
                break;
            case 10:
                ASSERT_YTHROW(LastResponse.HttpResult && LastResponse.HttpResult->Type() == NActors::NMon::HttpInfoRes,
                        "Unexpected response message type, expected HttpInfoRes");
                ASSERT_YTHROW(LastResponse.HttpResult->Answer.size() > 0, "Html page cannot have zero size");
                VERBOSE_COUT("Done");
                SignalDoneEvent();
                break;
            default:
                ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
                break;
        }
        TestStep += 10;
    }
public:
    TTestHttpGetResponse(TTestConfig *cfg)
        : TBaseTest(cfg)
        , HttpRequest(new THttpRequest(HTTP_METHOD_GET))
        , MonService2HttpRequest(nullptr, HttpRequest.Get(), nullptr, nullptr, "", nullptr)
    {}
};

class TTestHttpPostReaction : public TBaseTest {
    TAutoPtr<THttpRequest> HttpRequest;
    NMonitoring::TMonService2HttpRequest MonService2HttpRequest;

    void TestFSM(const TActorContext &ctx) {
        Y_UNUSED(ctx);
        VERBOSE_COUT("Test step " << TestStep);
        switch (TestStep) {
            case 0:
                VERBOSE_COUT("Testing POST request with an unexistentParameter");
                HttpRequest->CgiParameters.emplace("unexistentParameter", "10");
                ctx.Send(IcbActor, new NMon::TEvHttpInfo(MonService2HttpRequest));
                break;
            case 10:
            {
                ASSERT_YTHROW(LastResponse.HttpResult && LastResponse.HttpResult->Type() == NActors::NMon::HttpInfoRes,
                        "Unexpected response message type, expected is HttpInfoRes");
                bool isControlExists;
                TAtomicBase value;
                Icb->GetValue("unexistentParameter", value, isControlExists);
                ASSERT_YTHROW(!isControlExists, "Parameter mustn't be created by POST request");
                VERBOSE_COUT("Testing POST request with an existentParameter");
                TControlWrapper control(10);
                Icb->RegisterSharedControl(control, "existentParameter");
                Icb->GetValue("existentParameter", value, isControlExists);
                ASSERT_YTHROW(isControlExists, "Error in control creation and registration");
                ASSERT_YTHROW(value == 10, "Error in control creation and registration");
                HttpRequest->CgiParameters.clear();
                HttpRequest->CgiParameters.emplace("existentParameter", "15");
                ctx.Send(IcbActor, new NMon::TEvHttpInfo(MonService2HttpRequest));
                break;
            }
            case 20:
            {
                ASSERT_YTHROW(LastResponse.HttpResult && LastResponse.HttpResult->Type() == NActors::NMon::HttpInfoRes,
                        "Unexpected response message type, expected is HttpInfoRes");
                bool isControlExists;
                TAtomicBase value;
                Icb->GetValue("existentParameter", value, isControlExists);
                ASSERT_YTHROW(isControlExists, "Error in control creation and registration");
                ASSERT_YTHROW(value == 15, "Parameter haven't changed by POST request");
                VERBOSE_COUT("Test of restoreDefaults POST request");
                HttpRequest->CgiParameters.clear();
                HttpRequest->CgiParameters.emplace("restoreDefaults", "");
                ctx.Send(IcbActor, new NMon::TEvHttpInfo(MonService2HttpRequest));
                break;
            }
            case 30:
            {
                ASSERT_YTHROW(LastResponse.HttpResult && LastResponse.HttpResult->Type() == NActors::NMon::HttpInfoRes,
                        "Unexpected response message type, expected is HttpInfoRes");
                bool isControlExists;
                TAtomicBase value;
                Icb->GetValue("existentParameter", value, isControlExists);
                ASSERT_YTHROW(isControlExists, "Error in control creation and registration");
                ASSERT_YTHROW(value == 10,  "Parameter haven't restored default value");
                VERBOSE_COUT("Test is bounds pulling wokrs");
                TControlWrapper control1(10, 5, 15);
                TControlWrapper control2(10, 5, 15);
                Icb->RegisterSharedControl(control1, "existentParameterWithBoundsLower");
                Icb->RegisterSharedControl(control2, "existentParameterWithBoundsUpper");
                Icb->GetValue("existentParameterWithBoundsLower", value, isControlExists);
                ASSERT_YTHROW(isControlExists, "Error in control creation and registration");
                ASSERT_YTHROW(value == 10, "Error in control creation and registration");
                Icb->GetValue("existentParameterWithBoundsUpper", value, isControlExists);
                ASSERT_YTHROW(isControlExists, "Error in control creation and registration");
                ASSERT_YTHROW(value == 10, "Error in control creation and registration");
                HttpRequest->CgiParameters.clear();
                HttpRequest->CgiParameters.emplace("existentParameterWithBoundsLower", "1");
                HttpRequest->CgiParameters.emplace("existentParameterWithBoundsUpper", "99999");
                ctx.Send(IcbActor, new NMon::TEvHttpInfo(MonService2HttpRequest));
                break;
            }
            case 40:
            {
                ASSERT_YTHROW(LastResponse.HttpResult && LastResponse.HttpResult->Type() == NActors::NMon::HttpInfoRes,
                        "Unexpected response message type, expected is HttpInfoRes");
                bool isControlExists;
                TAtomicBase value;

                Icb->GetValue("existentParameterWithBoundsLower", value, isControlExists);
                ASSERT_YTHROW(isControlExists, "Error in control creation and registration");
                ASSERT_YTHROW(value == 5, "Pulling value to bounds doesn't work");

                Icb->GetValue("existentParameterWithBoundsUpper", value, isControlExists);
                ASSERT_YTHROW(isControlExists, "Error in control creation and registration");
                ASSERT_YTHROW(value == 15, "Pulling value to bounds doesn't work");

                VERBOSE_COUT("Done");
                SignalDoneEvent();
                break;
            }
            default:
                ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
                break;
        }
        TestStep += 10;
    }
public:
    TTestHttpPostReaction(TTestConfig *cfg)
        : TBaseTest(cfg)
        , HttpRequest(new THttpRequest(HTTP_METHOD_POST))
        , MonService2HttpRequest(nullptr, HttpRequest.Get(), nullptr, nullptr, "", nullptr)
    {}
};

Y_UNIT_TEST_SUITE(IcbAsActorTests) {
    Y_UNIT_TEST(TestHttpGetResponse) {
        Run<TTestHttpGetResponse>();
    }

    Y_UNIT_TEST(TestHttpPostReaction) {
        Run<TTestHttpPostReaction>();
    }
};

} // namespace NKikimr
