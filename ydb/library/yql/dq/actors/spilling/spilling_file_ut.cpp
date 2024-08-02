#include "spilling_file.h"
#include "spilling.h"

#include <ydb/library/services/services.pb.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/actors/testlib/test_runtime.h>

#include <util/system/fs.h>
#include <util/generic/string.h>
#include <util/folder/path.h>

namespace NYql::NDq {

using namespace NActors;

namespace {

class TTestActorRuntime: public TTestActorRuntimeBase {
public:
    void InitNodeImpl(TNodeDataBase* node, size_t nodeIndex) override {
        node->LogSettings->Append(
            NKikimrServices::EServiceKikimr_MIN,
            NKikimrServices::EServiceKikimr_MAX,
            NKikimrServices::EServiceKikimr_Name
        );
        TTestActorRuntimeBase::InitNodeImpl(node, nodeIndex);
    }

    ~TTestActorRuntime() {
        if (SpillingRoot_ && SpillingRoot_.Exists()) {
            SpillingRoot_.ForceDelete();
        }
    }

    void Initialize() override {
        TTestActorRuntimeBase::Initialize();
        SetLogPriority(NKikimrServices::KQP_COMPUTE, NActors::NLog::PRI_ERROR);
    }

    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters() {
        static auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        return counters;
    }

    static TString GetSpillingPrefix() {
        static TString str = Sprintf("%s_%d", "dq_spilling", (int)getpid());
        return str;
    }

    const TString& GetSpillingSessionId() const {
        return SpillingSessionId_;
    }

    TActorId StartSpillingService(ui64 maxTotalSize = 1000, ui64 maxFileSize = 500,
        ui64 maxFilePartSize = 100, const TFsPath& root = TFsPath::Cwd() / GetSpillingPrefix())
    {
        SpillingRoot_ = root;
        SpillingSessionId_ = CreateGuidAsString();

        auto config = TFileSpillingServiceConfig{
            .Root = root.GetPath(),
            .SpillingSessionId = SpillingSessionId_,
            .MaxTotalSize = maxTotalSize,
            .MaxFileSize = maxFileSize,
            .MaxFilePartSize = maxFilePartSize
        };

        auto counters = Counters();
        counters->ResetCounters();

        auto spillingService = CreateDqLocalFileSpillingService(config, MakeIntrusive<TSpillingCounters>(counters));
        auto spillingServiceActorId = Register(spillingService);
        EnableScheduleForActor(spillingServiceActorId);
        RegisterService(MakeDqLocalFileSpillingServiceID(GetNodeId()), spillingServiceActorId);

        return spillingServiceActorId;
    }

    TActorId StartSpillingActor(const TActorId& client, bool removeBlobsAfterRead = true) {
        auto spillingActor = CreateDqLocalFileSpillingActor(1ul, "test", client, removeBlobsAfterRead);
        auto spillingActorId = Register(spillingActor);
        EnableScheduleForActor(spillingActorId);

        return spillingActorId;
    }

    void WaitBootstrap() {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        UNIT_ASSERT(DispatchEvents(options));
    }

    const TFsPath& GetSpillingRoot() const {
        return SpillingRoot_;
    }

private:
    TFsPath SpillingRoot_;
    TString SpillingSessionId_;
};

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

} // anonymous namespace

Y_UNIT_TEST_SUITE(DqSpillingFileTests) {

    Y_UNIT_TEST(Simple) {
        TTestActorRuntime runtime;
        runtime.Initialize();

        auto spillingService = runtime.StartSpillingService();
        auto tester = runtime.AllocateEdgeActor();
        auto spillingActor = runtime.StartSpillingActor(tester);

        runtime.WaitBootstrap();

        // put blob 1
        {
            auto ev = new TEvDqSpilling::TEvWrite(1, CreateRope(10, 'a'));
            runtime.Send(new IEventHandle(spillingActor, tester, ev));

            auto resp = runtime.GrabEdgeEvent<TEvDqSpilling::TEvWriteResult>(tester, TDuration::Seconds(1));
            UNIT_ASSERT_VALUES_EQUAL(1, resp->Get()->BlobId);
        }

        // put blob 2
        {
            auto ev = new TEvDqSpilling::TEvWrite(2, CreateRope(11, 'z'));
            runtime.Send(new IEventHandle(spillingActor, tester, ev));

            auto resp = runtime.GrabEdgeEvent<TEvDqSpilling::TEvWriteResult>(tester, TDuration::Seconds(1));
            UNIT_ASSERT_VALUES_EQUAL(2, resp->Get()->BlobId);
        }

        // get blob 1
        {
            auto ev = new TEvDqSpilling::TEvRead(1);
            runtime.Send(new IEventHandle(spillingActor, tester, ev));

            auto resp = runtime.GrabEdgeEvent<TEvDqSpilling::TEvReadResult>(tester, TDuration::Seconds(1));
            UNIT_ASSERT_VALUES_EQUAL(1, resp->Get()->BlobId);

            TBuffer expected = CreateBlob(10, 'a');
            AssertEquals(expected, resp->Get()->Blob);
        }

        // get blob 2
        {
            auto ev = new TEvDqSpilling::TEvRead(2);
            runtime.Send(new IEventHandle(spillingActor, tester, ev));

            auto resp = runtime.GrabEdgeEvent<TEvDqSpilling::TEvReadResult>(tester, TDuration::Seconds(1));
            UNIT_ASSERT_VALUES_EQUAL(2, resp->Get()->BlobId);

            TBuffer expected = CreateBlob(11, 'z');
            AssertEquals(expected, resp->Get()->Blob);
        }

        // terminate
        {
            runtime.Send(new IEventHandle(spillingActor, tester, new TEvents::TEvPoison));

            std::atomic<bool> done = false;
            runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
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
        TTestActorRuntime runtime;
        runtime.Initialize();

        runtime.StartSpillingService(100, 1000, 1000);
        auto tester = runtime.AllocateEdgeActor();
        auto spillingActor = runtime.StartSpillingActor(tester);

        runtime.WaitBootstrap();

        {
            auto ev = new TEvDqSpilling::TEvWrite(1, CreateRope(51, 'a'));
            runtime.Send(new IEventHandle(spillingActor, tester, ev));

            auto resp = runtime.GrabEdgeEvent<TEvDqSpilling::TEvWriteResult>(tester);
            UNIT_ASSERT_VALUES_EQUAL(1, resp->Get()->BlobId);
        }

        {
            auto ev = new TEvDqSpilling::TEvWrite(2, CreateRope(50, 'b'));
            runtime.Send(new IEventHandle(spillingActor, tester, ev));

            auto resp = runtime.GrabEdgeEvent<TEvDqSpilling::TEvError>(tester);
            UNIT_ASSERT_STRINGS_EQUAL("Total size limit exceeded", resp->Get()->Message);
        }
    }

    Y_UNIT_TEST(Write_FileSizeLimitExceeded) {
        TTestActorRuntime runtime;
        runtime.Initialize();

        runtime.StartSpillingService(1000, 100, 1000);
        auto tester = runtime.AllocateEdgeActor();
        auto spillingActor = runtime.StartSpillingActor(tester);

        runtime.WaitBootstrap();

        {
            auto ev = new TEvDqSpilling::TEvWrite(1, CreateRope(51, 'a'));
            runtime.Send(new IEventHandle(spillingActor, tester, ev));

            auto resp = runtime.GrabEdgeEvent<TEvDqSpilling::TEvWriteResult>(tester);
            UNIT_ASSERT_VALUES_EQUAL(1, resp->Get()->BlobId);
        }

        {
            auto ev = new TEvDqSpilling::TEvWrite(2, CreateRope(50, 'b'));
            runtime.Send(new IEventHandle(spillingActor, tester, ev));

            auto resp = runtime.GrabEdgeEvent<TEvDqSpilling::TEvError>(tester);
            UNIT_ASSERT_STRINGS_EQUAL("File size limit exceeded", resp->Get()->Message);
        }
    }

    Y_UNIT_TEST(MultipleFileParts) {
        TTestActorRuntime runtime;
        runtime.Initialize();

        runtime.StartSpillingService(1000, 100, 25);
        auto tester = runtime.AllocateEdgeActor();
        auto spillingActor = runtime.StartSpillingActor(tester);

        runtime.WaitBootstrap();
        const TString filePrefix = TStringBuilder() << runtime.GetSpillingRoot().GetPath() << "/node_" << runtime.GetNodeId() << "_" << runtime.GetSpillingSessionId() << "/1_test_";

        for (ui32 i = 0; i < 5; ++i) {
            // Cerr << "---- store blob #" << i << Endl;
            auto ev = new TEvDqSpilling::TEvWrite(i, CreateRope(20, 'a' + i));
            runtime.Send(new IEventHandle(spillingActor, tester, ev));

            auto resp = runtime.GrabEdgeEvent<TEvDqSpilling::TEvWriteResult>(tester);
            UNIT_ASSERT_VALUES_EQUAL(i, resp->Get()->BlobId);

            UNIT_ASSERT(NFs::Exists(TStringBuilder() << filePrefix << i));
        }

        for (i32 i = 4; i >= 0; --i) {
            // Cerr << "---- load blob #" << i << Endl;
            auto ev = new TEvDqSpilling::TEvRead(i, true);
            runtime.Send(new IEventHandle(spillingActor, tester, ev));

            auto resp = runtime.GrabEdgeEvent<TEvDqSpilling::TEvReadResult>(tester);
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
        TTestActorRuntime runtime;
        runtime.Initialize();

        runtime.StartSpillingService(1000, 100, 25);
        auto tester = runtime.AllocateEdgeActor();
        auto spillingActor = runtime.StartSpillingActor(tester, false);

        runtime.WaitBootstrap();

        const TString filePrefix = TStringBuilder() << runtime.GetSpillingRoot().GetPath() << "/node_" << runtime.GetNodeId() << "_" << runtime.GetSpillingSessionId() << "/1_test_";

        for (ui32 i = 0; i < 5; ++i) {
            // Cerr << "---- store blob #" << i << Endl;
            auto ev = new TEvDqSpilling::TEvWrite(i, CreateRope(20, 'a' + i));
            runtime.Send(new IEventHandle(spillingActor, tester, ev));

            auto resp = runtime.GrabEdgeEvent<TEvDqSpilling::TEvWriteResult>(tester);
            UNIT_ASSERT_VALUES_EQUAL(i, resp->Get()->BlobId);

            UNIT_ASSERT(NFs::Exists(TStringBuilder() << filePrefix << 0));
            if (i > 0) {
                UNIT_ASSERT(!NFs::Exists(TStringBuilder() << filePrefix << i));
            }
        }

        for (i32 i = 4; i >= 0; --i) {
            // Cerr << "---- load blob #" << i << Endl;
            auto ev = new TEvDqSpilling::TEvRead(i, true);
            runtime.Send(new IEventHandle(spillingActor, tester, ev));

            auto resp = runtime.GrabEdgeEvent<TEvDqSpilling::TEvReadResult>(tester);
            UNIT_ASSERT_VALUES_EQUAL(i, resp->Get()->BlobId);
            TBuffer expected = CreateBlob(20, 'a' + i);
            AssertEquals(expected, resp->Get()->Blob);

            UNIT_ASSERT(NFs::Exists(TStringBuilder() << filePrefix << 0));
        }
    }

    Y_UNIT_TEST(ReadError) {
        TTestActorRuntime runtime;
        runtime.Initialize();

        auto spillingSvc = runtime.StartSpillingService();
        auto tester = runtime.AllocateEdgeActor();
        auto spillingActor = runtime.StartSpillingActor(tester);

        runtime.WaitBootstrap();

        {
            auto ev = new TEvDqSpilling::TEvWrite(0, CreateRope(20, 'a'));
            runtime.Send(new IEventHandle(spillingActor, tester, ev));

            auto resp = runtime.GrabEdgeEvent<TEvDqSpilling::TEvWriteResult>(tester);
            UNIT_ASSERT_VALUES_EQUAL(0, resp->Get()->BlobId);
        }
        auto nodePath = TFsPath("node_" + std::to_string(spillingSvc.NodeId()) + "_" + runtime.GetSpillingSessionId());
        (runtime.GetSpillingRoot() / nodePath / "1_test_0").ForceDelete();

        {
            auto ev = new TEvDqSpilling::TEvRead(0, true);
            runtime.Send(new IEventHandle(spillingActor, tester, ev));

            auto resp = runtime.GrabEdgeEvent<TEvDqSpilling::TEvError>(tester);
            auto err = resp->Get()->Message;
            auto expected = "can't open \"" + runtime.GetSpillingRoot().GetPath() + "/" + nodePath.GetPath() +"/1_test_0\" with mode RdOnly";
            UNIT_ASSERT_C(err.Contains("No such file or directory"), err);
            UNIT_ASSERT_C(err.Contains(expected), err);
        }
    }

    Y_UNIT_TEST(StartError) {
        TTestActorRuntime runtime;
        runtime.Initialize();

        auto spillingService = runtime.StartSpillingService(100, 500, 100, TFsPath("/nonexistent") / runtime.GetSpillingPrefix());
        auto tester = runtime.AllocateEdgeActor();
        auto spillingActor = runtime.StartSpillingActor(tester);

        runtime.WaitBootstrap();

        // put blob 1
        {
            auto ev = new TEvDqSpilling::TEvWrite(1, CreateRope(10, 'a'));
            runtime.Send(new IEventHandle(spillingActor, tester, ev));

            auto resp = runtime.GrabEdgeEvent<TEvDqSpilling::TEvError>(tester, TDuration::Seconds(1));
            UNIT_ASSERT_EQUAL("Service not started", resp->Get()->Message);
        }

        // get blob 1
        {
            auto ev = new TEvDqSpilling::TEvRead(1);
            runtime.Send(new IEventHandle(spillingActor, tester, ev));

            auto resp = runtime.GrabEdgeEvent<TEvDqSpilling::TEvError>(tester, TDuration::Seconds(1));
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

    Y_UNIT_TEST(NoSpillingService) {
        TTestActorRuntime runtime;
        runtime.Initialize();

        auto tester = runtime.AllocateEdgeActor();
        auto spillingActor = runtime.StartSpillingActor(tester);

        runtime.WaitBootstrap();

        // put blob 1
        {
            auto ev = new TEvDqSpilling::TEvWrite(1, CreateRope(10, 'a'));
            runtime.Send(new IEventHandle(spillingActor, tester, ev));

            auto resp = runtime.GrabEdgeEvent<TEvDqSpilling::TEvError>(tester, TDuration::Seconds(1));
            UNIT_ASSERT_EQUAL("Spilling Service not started", resp->Get()->Message);
        }

        // get blob 1
        {
            auto ev = new TEvDqSpilling::TEvRead(1);
            runtime.Send(new IEventHandle(spillingActor, tester, ev));

            auto resp = runtime.GrabEdgeEvent<TEvDqSpilling::TEvError>(tester, TDuration::Seconds(1));
            UNIT_ASSERT_EQUAL("Spilling Service not started", resp->Get()->Message);
        }
    }

} // suite

} // namespace NYql::NDq
