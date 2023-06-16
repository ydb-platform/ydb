#include "kqp_spilling_file.h"
#include "kqp_spilling.h"

#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/fs.h>

namespace NKikimr::NKqp {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NMiniKQL;
using namespace NKikimr::NKqp;
using namespace NYql;

namespace {

TString GetSpillingPrefix() {
    static TString str = Sprintf("%s_%d/", "kqp_spilling", (int)getpid());
    return str;
}

TBuffer CreateBlob(ui32 size, char symbol) {
    TBuffer blob(size);
    blob.Fill(symbol, size);
    return blob;
}

TRope CreateRope(ui32 size, char symbol, ui32 chunkSize = 7) {
    TRope result;
    while (size) {
        size_t count = std::min(size, chunkSize);
        TString str(count, symbol);
        result.Insert(result.End(), TRope{str});
        size -= count;
    }
    return result;
}

void AssertEquals(const TBuffer& lhs, const TBuffer& rhs) {
    TStringBuf l{lhs.data(), lhs.size()};
    TStringBuf r{rhs.data(), rhs.size()};
    UNIT_ASSERT_STRINGS_EQUAL(l, r);
}

TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters() {
    static auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
    return counters;
}

TActorId StartSpillingService(TTestBasicRuntime& runtime, ui64 maxTotalSize = 1000, ui64 maxFileSize = 500,
    ui64 maxFilePartSize = 100, const TString& root = "./" + GetSpillingPrefix())
{
    Cerr << "cwd: " << NFs::CurrentWorkingDirectory() << Endl;

    NKikimrConfig::TTableServiceConfig::TSpillingServiceConfig::TLocalFileConfig config;
    config.SetEnable(true);
    config.SetRoot(root);
    config.SetMaxTotalSize(maxTotalSize);
    config.SetMaxFileSize(maxFileSize);
    config.SetMaxFilePartSize(maxFilePartSize);

    auto counters = Counters();
    counters->ResetCounters();
    auto kqpCounters = MakeIntrusive<TKqpCounters>(counters);

    auto* spillingService = CreateKqpLocalFileSpillingService(config, kqpCounters);
    auto spillingServiceActorId = runtime.Register(spillingService);
    runtime.EnableScheduleForActor(spillingServiceActorId);
    runtime.RegisterService(MakeKqpLocalFileSpillingServiceID(runtime.GetNodeId()), spillingServiceActorId);

    return spillingServiceActorId;
}

TActorId StartSpillingActor(TTestBasicRuntime& runtime, const TActorId& client, bool removeBlobsAfterRead = true) {
    auto *spillingActor = CreateKqpLocalFileSpillingActor(1, "test", client, removeBlobsAfterRead);
    auto spillingActorId = runtime.Register(spillingActor);
    runtime.EnableScheduleForActor(spillingActorId);

    return spillingActorId;
}

void WaitBootstrap(TTestBasicRuntime& runtime) {
    TDispatchOptions options;
    options.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
    UNIT_ASSERT(runtime.DispatchEvents(options));
}

void SetupLogs(TTestBasicRuntime& runtime) {
    runtime.SetLogPriority(NKikimrServices::KQP_BLOBS_STORAGE, NActors::NLog::PRI_ERROR);
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(KqpSpillingFileTests) {

Y_UNIT_TEST(Simple) {
    TTestBasicRuntime runtime{1, false};
    runtime.Initialize(TAppPrepare().Unwrap());
    SetupLogs(runtime);

    auto spillingService = StartSpillingService(runtime);
    auto tester = runtime.AllocateEdgeActor();
    auto spillingActor = StartSpillingActor(runtime, tester);

    WaitBootstrap(runtime);

    // put blob 1
    {
        auto ev = new TEvKqpSpilling::TEvWrite(1, CreateRope(10, 'a'));
        runtime.Send(new IEventHandle(spillingActor, tester, ev));

        auto resp = runtime.GrabEdgeEvent<TEvKqpSpilling::TEvWriteResult>(tester, TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(1, resp->Get()->BlobId);
    }

    // put blob 2
    {
        auto ev = new TEvKqpSpilling::TEvWrite(2, CreateRope(11, 'z'));
        runtime.Send(new IEventHandle(spillingActor, tester, ev));

        auto resp = runtime.GrabEdgeEvent<TEvKqpSpilling::TEvWriteResult>(tester, TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(2, resp->Get()->BlobId);
    }

    // get blob 1
    {
        auto ev = new TEvKqpSpilling::TEvRead(1);
        runtime.Send(new IEventHandle(spillingActor, tester, ev));

        auto resp = runtime.GrabEdgeEvent<TEvKqpSpilling::TEvReadResult>(tester, TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(1, resp->Get()->BlobId);

        TBuffer expected = CreateBlob(10, 'a');
        AssertEquals(expected, resp->Get()->Blob);
    }

    // get blob 2
    {
        auto ev = new TEvKqpSpilling::TEvRead(2);
        runtime.Send(new IEventHandle(spillingActor, tester, ev));

        auto resp = runtime.GrabEdgeEvent<TEvKqpSpilling::TEvReadResult>(tester, TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(2, resp->Get()->BlobId);

        TBuffer expected = CreateBlob(11, 'z');
        AssertEquals(expected, resp->Get()->Blob);
    }

    // terminate
    {
        runtime.Send(new IEventHandle(spillingActor, tester, new TEvents::TEvPoison));

        std::atomic<bool> done = false;
        runtime.SetObserverFunc([&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event) {
            if (event->GetRecipientRewrite() == spillingService) {
                if (event->GetTypeRewrite() == 2146435074 /* EvCloseFileResponse */ ) {
                    done = true;
                }
            }
            return TTestActorRuntimeBase::EEventAction::PROCESS;
        });

        TDispatchOptions options;
        options.CustomFinalCondition = [&]() {
            return (bool) done;
        };

        runtime.DispatchEvents(options, TDuration::Seconds(1));
    }
}

Y_UNIT_TEST(Write_TotalSizeLimitExceeded) {
    TTestBasicRuntime runtime{1, false};
    runtime.Initialize(TAppPrepare().Unwrap());
    SetupLogs(runtime);

    StartSpillingService(runtime, 100, 1000, 1000);
    auto tester = runtime.AllocateEdgeActor();
    auto spillingActor = StartSpillingActor(runtime, tester);

    WaitBootstrap(runtime);

    {
        auto ev = new TEvKqpSpilling::TEvWrite(1, CreateRope(51, 'a'));
        runtime.Send(new IEventHandle(spillingActor, tester, ev));

        auto resp = runtime.GrabEdgeEvent<TEvKqpSpilling::TEvWriteResult>(tester);
        UNIT_ASSERT_VALUES_EQUAL(1, resp->Get()->BlobId);
    }

    {
        auto ev = new TEvKqpSpilling::TEvWrite(2, CreateRope(50, 'b'));
        runtime.Send(new IEventHandle(spillingActor, tester, ev));

        auto resp = runtime.GrabEdgeEvent<TEvKqpSpilling::TEvError>(tester);
        UNIT_ASSERT_STRINGS_EQUAL("Total size limit exceeded", resp->Get()->Message);
    }
}

Y_UNIT_TEST(Write_FileSizeLimitExceeded) {
    TTestBasicRuntime runtime{1, false};
    runtime.Initialize(TAppPrepare().Unwrap());
    SetupLogs(runtime);

    StartSpillingService(runtime, 1000, 100, 1000);
    auto tester = runtime.AllocateEdgeActor();
    auto spillingActor = StartSpillingActor(runtime, tester);

    WaitBootstrap(runtime);

    {
        auto ev = new TEvKqpSpilling::TEvWrite(1, CreateRope(51, 'a'));
        runtime.Send(new IEventHandle(spillingActor, tester, ev));

        auto resp = runtime.GrabEdgeEvent<TEvKqpSpilling::TEvWriteResult>(tester);
        UNIT_ASSERT_VALUES_EQUAL(1, resp->Get()->BlobId);
    }

    {
        auto ev = new TEvKqpSpilling::TEvWrite(2, CreateRope(50, 'b'));
        runtime.Send(new IEventHandle(spillingActor, tester, ev));

        auto resp = runtime.GrabEdgeEvent<TEvKqpSpilling::TEvError>(tester);
        UNIT_ASSERT_STRINGS_EQUAL("File size limit exceeded", resp->Get()->Message);
    }
}

Y_UNIT_TEST(MultipleFileParts) {
    TTestBasicRuntime runtime{1, false};
    runtime.Initialize(TAppPrepare().Unwrap());
    SetupLogs(runtime);

    StartSpillingService(runtime, 1000, 100, 25);
    auto tester = runtime.AllocateEdgeActor();
    auto spillingActor = StartSpillingActor(runtime, tester);

    WaitBootstrap(runtime);

    const TString filePrefix = TStringBuilder() << NFs::CurrentWorkingDirectory() << "/" << GetSpillingPrefix() << "node_" << runtime.GetNodeId() << "/1_test_";

    for (ui32 i = 0; i < 5; ++i) {
        // Cerr << "---- store blob #" << i << Endl;
        auto ev = new TEvKqpSpilling::TEvWrite(i, CreateRope(20, 'a' + i));
        runtime.Send(new IEventHandle(spillingActor, tester, ev));

        auto resp = runtime.GrabEdgeEvent<TEvKqpSpilling::TEvWriteResult>(tester);
        UNIT_ASSERT_VALUES_EQUAL(i, resp->Get()->BlobId);

        UNIT_ASSERT(NFs::Exists(TStringBuilder() << filePrefix << i));
    }

    for (i32 i = 4; i >= 0; --i) {
        // Cerr << "---- load blob #" << i << Endl;
        auto ev = new TEvKqpSpilling::TEvRead(i, true);
        runtime.Send(new IEventHandle(spillingActor, tester, ev));

        auto resp = runtime.GrabEdgeEvent<TEvKqpSpilling::TEvReadResult>(tester);
        UNIT_ASSERT_VALUES_EQUAL(i, resp->Get()->BlobId);
        TBuffer expected = CreateBlob(20, 'a' + i);
        AssertEquals(expected, resp->Get()->Blob);

        if (i == 4) {
            // do not remove last file
            UNIT_ASSERT(NFs::Exists(TStringBuilder() << filePrefix << i));
        } else {
            UNIT_ASSERT(!NFs::Exists(TStringBuilder() << filePrefix << i));
        }
    }
}

Y_UNIT_TEST(SingleFilePart) {
    TTestBasicRuntime runtime{1, false};
    runtime.Initialize(TAppPrepare().Unwrap());
    SetupLogs(runtime);

    StartSpillingService(runtime, 1000, 100, 25);
    auto tester = runtime.AllocateEdgeActor();
    auto spillingActor = StartSpillingActor(runtime, tester, false);

    WaitBootstrap(runtime);

    const TString filePrefix = TStringBuilder() << NFs::CurrentWorkingDirectory() << "/" << GetSpillingPrefix() << "node_" << runtime.GetNodeId() << "/1_test_";

    for (ui32 i = 0; i < 5; ++i) {
        // Cerr << "---- store blob #" << i << Endl;
        auto ev = new TEvKqpSpilling::TEvWrite(i, CreateRope(20, 'a' + i));
        runtime.Send(new IEventHandle(spillingActor, tester, ev));

        auto resp = runtime.GrabEdgeEvent<TEvKqpSpilling::TEvWriteResult>(tester);
        UNIT_ASSERT_VALUES_EQUAL(i, resp->Get()->BlobId);

        UNIT_ASSERT(NFs::Exists(TStringBuilder() << filePrefix << 0));
        if (i > 0) {
            UNIT_ASSERT(!NFs::Exists(TStringBuilder() << filePrefix << i));
        }
    }

    for (i32 i = 4; i >= 0; --i) {
        // Cerr << "---- load blob #" << i << Endl;
        auto ev = new TEvKqpSpilling::TEvRead(i, true);
        runtime.Send(new IEventHandle(spillingActor, tester, ev));

        auto resp = runtime.GrabEdgeEvent<TEvKqpSpilling::TEvReadResult>(tester);
        UNIT_ASSERT_VALUES_EQUAL(i, resp->Get()->BlobId);
        TBuffer expected = CreateBlob(20, 'a' + i);
        AssertEquals(expected, resp->Get()->Blob);

        UNIT_ASSERT(NFs::Exists(TStringBuilder() << filePrefix << 0));
    }
}

Y_UNIT_TEST(ReadError) {
    return;

    TTestBasicRuntime runtime{1, false};
    runtime.Initialize(TAppPrepare().Unwrap());
    SetupLogs(runtime);

    StartSpillingService(runtime);
    auto tester = runtime.AllocateEdgeActor();
    auto spillingActor = StartSpillingActor(runtime, tester);

    WaitBootstrap(runtime);

    {
        auto ev = new TEvKqpSpilling::TEvWrite(0, CreateRope(20, 'a'));
        runtime.Send(new IEventHandle(spillingActor, tester, ev));

        auto resp = runtime.GrabEdgeEvent<TEvKqpSpilling::TEvWriteResult>(tester);
        UNIT_ASSERT_VALUES_EQUAL(0, resp->Get()->BlobId);
    }

    ::unlink((NFs::CurrentWorkingDirectory() + GetSpillingPrefix() + "node_1/1_test_0").c_str());

    {
        auto ev = new TEvKqpSpilling::TEvRead(0, true);
        runtime.Send(new IEventHandle(spillingActor, tester, ev));

        auto resp = runtime.GrabEdgeEvent<TEvKqpSpilling::TEvError>(tester);
        auto& err = resp->Get()->Message;
        auto expected = "can't open \"" + GetSpillingPrefix() + "node_1/1_test_0\" with mode RdOnly";
        UNIT_ASSERT_C(err.Contains("No such file or directory"), err);
        UNIT_ASSERT_C(err.Contains(expected), err);
    }
}


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

Y_UNIT_TEST(StartError) {
    TTestBasicRuntime runtime{1, false};
    runtime.Initialize(TAppPrepare().Unwrap());
    SetupLogs(runtime);

    auto spillingService = StartSpillingService(runtime, 100, 500, 100, "/nonexistent/" + GetSpillingPrefix());
    auto tester = runtime.AllocateEdgeActor();
    auto spillingActor = StartSpillingActor(runtime, tester);

    WaitBootstrap(runtime);

    // put blob 1
    {
        auto ev = new TEvKqpSpilling::TEvWrite(1, CreateRope(10, 'a'));
        runtime.Send(new IEventHandle(spillingActor, tester, ev));

        auto resp = runtime.GrabEdgeEvent<TEvKqpSpilling::TEvError>(tester, TDuration::Seconds(1));
        UNIT_ASSERT_EQUAL("Service not started", resp->Get()->Message);
    }

    // get blob 1
    {
        auto ev = new TEvKqpSpilling::TEvRead(1);
        runtime.Send(new IEventHandle(spillingActor, tester, ev));

        auto resp = runtime.GrabEdgeEvent<TEvKqpSpilling::TEvError>(tester, TDuration::Seconds(1));
        UNIT_ASSERT_EQUAL("Service not started", resp->Get()->Message);
    }

    // mon
    {
        THttpRequest httpReq(HTTP_METHOD_GET);
        NMonitoring::TMonService2HttpRequest monReq(nullptr, &httpReq, nullptr, nullptr, "", nullptr);

        runtime.Send(new IEventHandle(spillingService, tester, new NMon::TEvHttpInfo(monReq)));

        auto resp = runtime.GrabEdgeEvent<NMon::TEvHttpInfoRes>(tester, TDuration::Seconds(1));
        UNIT_ASSERT_EQUAL("<html><h2>Service is not started due to IO error</h2></html>",
                          ((NMon::TEvHttpInfoRes*) resp->Get())->Answer);
    }
}

} // suite

} // namespace NKikimr::NKqp
