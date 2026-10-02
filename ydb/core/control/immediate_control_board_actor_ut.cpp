#include "defs.h"
#include "immediate_control_board_actor.h"

#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/core/mon/mon.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/base/tablet.h>
#include <ydb/core/control/lib/immediate_control_board_wrapper.h>

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
    TControlBoard* Icb;
    TDynamicControlBoard *Dcb;

    TTestConfig(TActorId icbActorId, TControlBoard *icb, TDynamicControlBoard *dcb)
        : IcbActorId(icbActorId)
        , Icb(icb)
        , Dcb(dcb)
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
        TActorSetupCmd testSetup(CreateImmediateControlActor(appData.Icb, appData.Dcb, Counters), TMailboxType::Revolving, 0);
        setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(IcbActorId, std::move(testSetup)));


        THolder<TTestConfig> testConfig(new TTestConfig(IcbActorId, appData.Icb.Get(), appData.Dcb.Get()));
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
    TDynamicControlBoard* Dcb;
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
        , Dcb(cfg->Dcb)
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
        return TStringBuf();
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
    // Shared dynamic control used for named restore and duplicate-name checks.
    TControlWrapper Control{10};
    TAutoPtr<THttpRequest> HttpRequest;
    NMonitoring::TMonService2HttpRequest MonService2HttpRequest;

    void TestFSM(const TActorContext &ctx) {
        Y_UNUSED(ctx);
        VERBOSE_COUT("Test step " << TestStep);
        switch (TestStep) {
            case 0:
                // Submit an unknown parameter.
                VERBOSE_COUT("Testing POST request with an unexistentParameter");
                HttpRequest->CgiParameters.emplace("unexistentParameter", "10");
                ctx.Send(IcbActor, new NMon::TEvHttpInfo(MonService2HttpRequest));
                break;
            case 10:
            {
                // Verify that POST did not create the unknown parameter.
                ASSERT_YTHROW(LastResponse.HttpResult && LastResponse.HttpResult->Type() == NActors::NMon::HttpInfoRes,
                        "Unexpected response message type, expected is HttpInfoRes");
                bool isControlExists;
                TAtomicBase value;
                Dcb->GetValue("unexistentParameter", value, isControlExists);
                ASSERT_YTHROW(!isControlExists, "Parameter mustn't be created by POST request");
                TestStep += 10;
                [[fallthrough]];
            }
            case 20:
            {
                // Register a dynamic control and submit a value for it.
                VERBOSE_COUT("Testing POST request with an existentParameter");
                TControlWrapper control(10);
                Dcb->RegisterSharedControl(control, "existentParameter");
                bool isControlExists;
                TAtomicBase value;
                Dcb->GetValue("existentParameter", value, isControlExists);
                ASSERT_YTHROW(isControlExists, "Error in control creation and registration");
                ASSERT_YTHROW(value == 10, "Error in control creation and registration");
                HttpRequest->CgiParameters.clear();
                HttpRequest->CgiParameters.emplace("existentParameter", "15");
                ctx.Send(IcbActor, new NMon::TEvHttpInfo(MonService2HttpRequest));
                break;
            }
            case 30:
            {
                // Verify that POST changed the registered control.
                ASSERT_YTHROW(LastResponse.HttpResult && LastResponse.HttpResult->Type() == NActors::NMon::HttpInfoRes,
                        "Unexpected response message type, expected is HttpInfoRes");
                bool isControlExists;
                TAtomicBase value;
                Dcb->GetValue("existentParameter", value, isControlExists);
                ASSERT_YTHROW(isControlExists, "Error in control creation and registration");
                ASSERT_YTHROW(value == 15, "Parameter haven't changed by POST request");
                TestStep += 10;
                [[fallthrough]];
            }
            case 40:
                // Submit bulk restore with a value that must not override it.
                VERBOSE_COUT("Test of restoreDefaults POST request");
                HttpRequest->CgiParameters.clear();
                HttpRequest->CgiParameters.emplace("restoreDefaults", "");
                HttpRequest->CgiParameters.emplace("existentParameter", "15");
                ctx.Send(IcbActor, new NMon::TEvHttpInfo(MonService2HttpRequest));
                break;
            case 50:
            {
                // Verify that bulk restore returned the control to its default and was recorded.
                ASSERT_YTHROW(LastResponse.HttpResult && LastResponse.HttpResult->Type() == NActors::NMon::HttpInfoRes,
                        "Unexpected response message type, expected is HttpInfoRes");
                bool isControlExists;
                TAtomicBase value;
                Dcb->GetValue("existentParameter", value, isControlExists);
                ASSERT_YTHROW(isControlExists, "Error in control creation and registration");
                ASSERT_YTHROW(value == 10, "Parameter haven't restored default value");
                ASSERT_YTHROW(LastResponse.HttpResult->Answer.find("<td>RestoreDefaults</td><td>0</td><td>0</td><td>Restore defaults</td>") != TString::npos,
                        "Bulk restore was not recorded with its action");
                TestStep += 10;
                [[fallthrough]];
            }
            case 60:
            {
                // Submit values outside the bounds of two dynamic controls.
                VERBOSE_COUT("Test is bounds pulling wokrs");
                TControlWrapper control1(10, 5, 15);
                TControlWrapper control2(10, 5, 15);
                Dcb->RegisterSharedControl(control1, "existentParameterWithBoundsLower");
                Dcb->RegisterSharedControl(control2, "existentParameterWithBoundsUpper");
                bool isControlExists;
                TAtomicBase value;
                Dcb->GetValue("existentParameterWithBoundsLower", value, isControlExists);
                ASSERT_YTHROW(isControlExists, "Error in control creation and registration");
                ASSERT_YTHROW(value == 10, "Error in control creation and registration");
                Dcb->GetValue("existentParameterWithBoundsUpper", value, isControlExists);
                ASSERT_YTHROW(isControlExists, "Error in control creation and registration");
                ASSERT_YTHROW(value == 10, "Error in control creation and registration");
                HttpRequest->CgiParameters.clear();
                HttpRequest->CgiParameters.emplace("existentParameterWithBoundsLower", "1");
                HttpRequest->CgiParameters.emplace("existentParameterWithBoundsUpper", "99999");
                ctx.Send(IcbActor, new NMon::TEvHttpInfo(MonService2HttpRequest));
                break;
            }
            case 70:
            {
                // Verify that both submitted values were clamped to their bounds.
                ASSERT_YTHROW(LastResponse.HttpResult && LastResponse.HttpResult->Type() == NActors::NMon::HttpInfoRes,
                        "Unexpected response message type, expected is HttpInfoRes");
                bool isControlExists;
                TAtomicBase value;
                Dcb->GetValue("existentParameterWithBoundsLower", value, isControlExists);
                ASSERT_YTHROW(isControlExists, "Error in control creation and registration");
                ASSERT_YTHROW(value == 5, "Pulling value to bounds doesn't work");
                Dcb->GetValue("existentParameterWithBoundsUpper", value, isControlExists);
                ASSERT_YTHROW(isControlExists, "Error in control creation and registration");
                ASSERT_YTHROW(value == 15, "Pulling value to bounds doesn't work");
                TestStep += 10;
                [[fallthrough]];
            }
            case 80:
                // Clear state left by the bounds checks before named restore.
                Dcb->RestoreDefaults();
                TestStep += 10;
                [[fallthrough]];
            case 90:
                // Change one dynamic control through the monitoring actor.
                Dcb->RegisterSharedControl(Control, "restoreParameter");
                HttpRequest->CgiParameters.clear();
                HttpRequest->CgiParameters.emplace("restoreParameter", "15");
                ctx.Send(IcbActor, new NMon::TEvHttpInfo(MonService2HttpRequest));
                break;
            case 100:
            {
                // Verify the changed value, its history action, and the named button.
                ASSERT_YTHROW(LastResponse.HttpResult && LastResponse.HttpResult->Type() == NActors::NMon::HttpInfoRes,
                        "Unexpected response message type, expected is HttpInfoRes");
                ASSERT_YTHROW(static_cast<i64>(Control) == 15, "POST did not change the control");
                ASSERT_YTHROW(LastResponse.HttpResult->Answer.find("name='restoreDefault' value='restoreParameter'") != TString::npos,
                        "Named restore button is missing");
                ASSERT_YTHROW(LastResponse.HttpResult->Answer.find("<td>restoreParameter</td><td>10</td><td>15</td><td>Set value</td>") != TString::npos,
                        "Value assignment was not recorded with its action");
                TestStep += 10;
                [[fallthrough]];
            }
            case 110:
                // Restore by name while ignoring the text input from the same form.
                HttpRequest->CgiParameters.clear();
                HttpRequest->CgiParameters.emplace("restoreParameter", "15");
                HttpRequest->CgiParameters.emplace("restoreDefault", "restoreParameter");
                ctx.Send(IcbActor, new NMon::TEvHttpInfo(MonService2HttpRequest));
                break;
            case 120:
            {
                // Verify that named restore clears the gauges and records its action.
                ASSERT_YTHROW(static_cast<i64>(Control) == 10, "Named restore did not recover the default");
                auto counters = GetServiceCounters(Counters, "utils");
                ASSERT_YTHROW(counters->GetCounter("Icb/ChangedControlsCount")->Val() == 0,
                        "Named restore did not clear the changed-control count");
                ASSERT_YTHROW(counters->GetCounter("Icb/HasChangedContol")->Val() == 0,
                        "Named restore did not clear the changed-control indicator");

                const TString& answer = LastResponse.HttpResult->Answer;
                const size_t historyPos = answer.find("<h3>History</h3>");
                ASSERT_YTHROW(historyPos != TString::npos, "History section is missing");
                ASSERT_YTHROW(answer.find("<th>Action</th>", historyPos) != TString::npos,
                        "History action column is missing");
                ASSERT_YTHROW(answer.find("<td>restoreParameter</td><td>15</td><td>10</td><td>Restore default</td>", historyPos) != TString::npos,
                        "Dynamic restore was not recorded with its values and action");
                TestStep += 10;
                [[fallthrough]];
            }
            case 130:
            {
                // Submit a change for a name present in both control boards.
                TControlWrapper staticControl(10, 0, 20);
                TControlBoard::RegisterLocalControl(staticControl, Icb->DataShardControls.MaxTxInFly);
                TAtomic previous = 0;
                Dcb->RegisterSharedControl(Control, "DataShardControls.MaxTxInFly");
                Dcb->SetValue("DataShardControls.MaxTxInFly", 15, previous);
                HttpRequest->CgiParameters.clear();
                HttpRequest->CgiParameters.emplace("DataShardControls.MaxTxInFly", "12");
                ctx.Send(IcbActor, new NMon::TEvHttpInfo(MonService2HttpRequest));
                break;
            }
            case 140:
            {
                // Verify that change selected the static control first.
                const auto control = Icb->DataShardControls.MaxTxInFly.AtomicLoad();
                ASSERT_YTHROW(control->Get() == 12, "Change did not select the static control");
                ASSERT_YTHROW(static_cast<i64>(Control) == 15, "Change modified the dynamic control");
                TestStep += 10;
                [[fallthrough]];
            }
            case 150:
                // Restore the name present in both control boards.
                HttpRequest->CgiParameters.clear();
                HttpRequest->CgiParameters.emplace("restoreDefault", "DataShardControls.MaxTxInFly");
                ctx.Send(IcbActor, new NMon::TEvHttpInfo(MonService2HttpRequest));
                break;
            case 160:
            {
                // Verify that named restore selected and recorded the static control.
                const auto control = Icb->DataShardControls.MaxTxInFly.AtomicLoad();
                ASSERT_YTHROW(control->Get() == 10, "Restore did not select the static control");
                ASSERT_YTHROW(static_cast<i64>(Control) == 15, "Restore modified the dynamic control");
                const TString& answer = LastResponse.HttpResult->Answer;
                const size_t historyPos = answer.find("<h3>History</h3>");
                ASSERT_YTHROW(answer.find("<td>DataShardControls.MaxTxInFly</td><td>12</td><td>10</td><td>Restore default</td>", historyPos) != TString::npos,
                        "Static restore was not recorded with its values and action");
                TestStep += 10;
                [[fallthrough]];
            }
            case 170:
                // Restore an unknown name to check that it is ignored.
                HttpRequest->CgiParameters.clear();
                HttpRequest->CgiParameters.emplace("restoreDefault", "unknown");
                ctx.Send(IcbActor, new NMon::TEvHttpInfo(MonService2HttpRequest));
                break;
            case 180:
                // Verify that an unknown restore leaves controls and history intact.
                ASSERT_YTHROW(static_cast<i64>(Control) == 15,
                        "Restore of an unknown name modified another control");
                ASSERT_YTHROW(LastResponse.HttpResult->Answer.find("<td>unknown</td>") == TString::npos,
                        "Restore of an unknown name was recorded in history");
                TestStep += 10;
                [[fallthrough]];
            case 190:
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
    TTestHttpPostReaction(TTestConfig *cfg)
        : TBaseTest(cfg)
        , HttpRequest(new THttpRequest(HTTP_METHOD_POST))
        , MonService2HttpRequest(nullptr, HttpRequest.Get(), nullptr, nullptr, "", nullptr)
    {}
};

// Actor test for changed-control gauges across repeated POST assignments.
// The class owns one registered control and its HTTP request for the test lifetime.
class TTestRepeatedOverridesCount : public TBaseTest {
public:
    // Create the test actor with a POST request and a default-valued control.
    TTestRepeatedOverridesCount(TTestConfig* cfg);

private:
    // Control counted once while its value differs from the default of 200.
    TControlWrapper Control{200};

    // Request data reused after each response from the monitoring actor.
    TAutoPtr<THttpRequest> HttpRequest;

    // Monitoring request view over HttpRequest for this actor's lifetime.
    NMonitoring::TMonService2HttpRequest MonService2HttpRequest;

    // Apply two changed values and the default, checking the gauges after each POST.
    void TestFSM(const TActorContext& ctx) override;
};

TTestRepeatedOverridesCount::TTestRepeatedOverridesCount(TTestConfig* cfg)
    : TBaseTest(cfg)
    , HttpRequest(new THttpRequest(HTTP_METHOD_POST))
    , MonService2HttpRequest(nullptr, HttpRequest.Get(), nullptr, nullptr, "", nullptr)
{}

// Check that every completed POST reports the number of changed controls.
// Reuse one control so a second non-default value must not increment the count.
void TTestRepeatedOverridesCount::TestFSM(const TActorContext& ctx) {
    auto counters = GetServiceCounters(Counters, "utils");
    switch (TestStep) {
        case 0:
            // Register one control and submit its first non-default value.
            Dcb->RegisterSharedControl(Control, "countedControl");
            HttpRequest->CgiParameters.emplace("countedControl", "500");
            ctx.Send(IcbActor, new NMon::TEvHttpInfo(MonService2HttpRequest));
            break;
        case 10:
            // Verify the first change and submit another non-default value.
            ASSERT_YTHROW(static_cast<i64>(Control) == 500, "First POST did not change the control");
            ASSERT_YTHROW(counters->GetCounter("Icb/ChangedControlsCount")->Val() == 1,
                    "First override was not counted");
            HttpRequest->CgiParameters.clear();
            HttpRequest->CgiParameters.emplace("countedControl", "600");
            ctx.Send(IcbActor, new NMon::TEvHttpInfo(MonService2HttpRequest));
            break;
        case 20:
            // Verify that the second value does not count the same control twice.
            ASSERT_YTHROW(static_cast<i64>(Control) == 600, "Second POST did not change the control");
            ASSERT_YTHROW(counters->GetCounter("Icb/ChangedControlsCount")->Val() == 1,
                    "Repeated override increased the changed-control count");
            HttpRequest->CgiParameters.clear();
            HttpRequest->CgiParameters.emplace("countedControl", "200");
            ctx.Send(IcbActor, new NMon::TEvHttpInfo(MonService2HttpRequest));
            break;
        case 30:
            // Verify that returning to default clears both gauges.
            ASSERT_YTHROW(static_cast<i64>(Control) == 200, "Third POST did not restore the default value");
            ASSERT_YTHROW(counters->GetCounter("Icb/ChangedControlsCount")->Val() == 0,
                    "Returning to default did not clear the changed-control count");
            ASSERT_YTHROW(counters->GetCounter("Icb/HasChangedContol")->Val() == 0,
                    "Returning to default did not clear the changed-control indicator");
            SignalDoneEvent();
            break;
        default:
            ythrow TWithBackTrace<yexception>() << "Unexpected TestStep " << TestStep << Endl;
    }
    TestStep += 10;
}

Y_UNIT_TEST_SUITE(IcbAsActorTests) {
    Y_UNIT_TEST(TestHttpGetResponse) {
        Run<TTestHttpGetResponse>();
    }

    Y_UNIT_TEST(TestHttpPostReaction) {
        Run<TTestHttpPostReaction>();
    }

    // Verify that repeated overrides count one control and returning to default clears both gauges.
    Y_UNIT_TEST(TestRepeatedOverridesCount) {
        Run<TTestRepeatedOverridesCount>();
    }
};

} // namespace NKikimr
