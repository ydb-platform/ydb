#include "spilling_file.h"
#include "spilling.h"

#include <ydb/library/services/services.pb.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/actors/testlib/test_runtime.h>

#include <util/system/fs.h>
#include <util/system/user.h>
#include <util/generic/string.h>
#include <util/folder/path.h>
#include <util/stream/file.h>

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
        if (SpillingRoot_ && SpillingRoot_.Exists() && SpillingRoot_ != GetDefaultSpillingRoot()) {
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
        ui64 maxFilePartSize = 100, ui32 ioThreadPoolQueueSize = 1000, const TFsPath& root = TFsPath::Cwd() / GetSpillingPrefix())
    {
        SpillingRoot_ = root;
        SpillingRoot_.MkDir();
        SpillingSessionId_ = CreateGuidAsString();

        auto config = TFileSpillingServiceConfig{
            .Root = root.GetPath(),
            .SpillingSessionId = SpillingSessionId_,
            .MaxTotalSize = maxTotalSize,
            .MaxFileSize = maxFileSize,
            .MaxFilePartSize = maxFilePartSize,
            .IoThreadPoolQueueSize = ioThreadPoolQueueSize
        };

        auto counters = Counters();
        counters->ResetCounters();

        auto spillingService = CreateDqLocalFileSpillingService(config, MakeIntrusive<TSpillingCounters>(counters));
        auto spillingServiceActorId = Register(spillingService);
        EnableScheduleForActor(spillingServiceActorId);
        RegisterService(MakeDqLocalFileSpillingServiceID(GetNodeId()), spillingServiceActorId);

        return spillingServiceActorId;
    }

    TActorId StartSpillingActor(const TActorId& client, bool removeBlobsAfterRead = true,
        ESpillingType spillingType = ESpillingType::Compute, ui64 txId = 1)
    {
        auto spillingActor = CreateDqLocalFileSpillingActor(txId, "test", client, removeBlobsAfterRead, spillingType);
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

    TFsPath GetSpillingNodeDir() const {
        return SpillingRoot_ / MakeSpillingNodeDirName(GetNodeId(), GetUsername(), SpillingSessionId_);
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

TChunkedBuffer CreateRope(ui32 size, char symbol, ui32 chunkSize = 7) {
    TChunkedBuffer result;
    while (size) {
        size_t count = std::min(size, chunkSize);
        auto str = std::make_shared<TString>(count, symbol);
        result.Append(*str, str);
        size -= count;
    }
    return result;
}

TFsPath MakeDirWithContent(const TFsPath& path) {
    path.MkDirs();
    (path / "nested").MkDir();
    TFileOutput((path / "nested" / "blob").GetPath()).Write("data");
    return path;
}

void AssertEquals(const TBuffer& lhs, const TBuffer& rhs) {
    TStringBuf l{lhs.data(), lhs.size()};
    TStringBuf r{rhs.data(), rhs.size()};
    UNIT_ASSERT_STRINGS_EQUAL(l, r);
}

ui64 CounterVal(TTestActorRuntime& runtime, const TString& name, bool derivative = true) {
    return runtime.Counters()->GetCounter(name, derivative)->Val();
}

TStringBuf TypePrefix(ESpillingType type) {
    return type == ESpillingType::Compute ? "Spilling/Compute/" : "Spilling/Channel/";
}

ui64 CounterVal(TTestActorRuntime& runtime, ESpillingType type, TStringBuf name) {
    return CounterVal(runtime, TString(TypePrefix(type)) + name);
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
            UNIT_ASSERT_STRINGS_EQUAL("Total size limit exceeded: 0/0Mb", resp->Get()->Message);
            UNIT_ASSERT_VALUES_EQUAL(CounterVal(runtime, "Spilling/Compute/NoSpaceErrors"), 1);
            UNIT_ASSERT_VALUES_EQUAL(CounterVal(runtime, "Spilling/Compute/Errors"), 1);
            UNIT_ASSERT_VALUES_EQUAL(CounterVal(runtime, "Spilling/Channel/NoSpaceErrors"), 0);
            UNIT_ASSERT_VALUES_EQUAL(CounterVal(runtime, "Spilling/Channel/Errors"), 0);
        }
    }

    Y_UNIT_TEST(Write_FileSizeLimitExceeded) {
        TTestActorRuntime runtime;
        runtime.Initialize();

        runtime.StartSpillingService(1000, 100, 1000);
        auto tester = runtime.AllocateEdgeActor();
        auto spillingActor = runtime.StartSpillingActor(tester, true, ESpillingType::Channel);

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
            UNIT_ASSERT_STRINGS_EQUAL("File size limit exceeded: 0/0Mb", resp->Get()->Message);
            UNIT_ASSERT_VALUES_EQUAL(CounterVal(runtime, "Spilling/Channel/TooBigFileErrors"), 1);
            UNIT_ASSERT_VALUES_EQUAL(CounterVal(runtime, "Spilling/Channel/Errors"), 1);
            UNIT_ASSERT_VALUES_EQUAL(CounterVal(runtime, "Spilling/Compute/TooBigFileErrors"), 0);
            UNIT_ASSERT_VALUES_EQUAL(CounterVal(runtime, "Spilling/Compute/Errors"), 0);
        }
    }

    void WriteOk(TTestActorRuntime& runtime, const TActorId& actor, const TActorId& tester, ui64 blobId, ui32 size) {
        runtime.Send(new IEventHandle(actor, tester, new TEvDqSpilling::TEvWrite(blobId, CreateRope(size, 'a'))));
        auto resp = runtime.GrabEdgeEvent<TEvDqSpilling::TEvWriteResult>(tester);
        UNIT_ASSERT_VALUES_EQUAL(blobId, resp->Get()->BlobId);
    }

    void WriteFails(TTestActorRuntime& runtime, const TActorId& actor, const TActorId& tester, ui64 blobId, ui32 size) {
        runtime.Send(new IEventHandle(actor, tester, new TEvDqSpilling::TEvWrite(blobId, CreateRope(size, 'b'))));
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvDqSpilling::TEvError>(tester));
    }

    Y_UNIT_TEST(ErrorCounters) {
        TTestActorRuntime runtime;
        runtime.Initialize();

        runtime.StartSpillingService(100, 1000, 1000);
        auto tester = runtime.AllocateEdgeActor();
        auto compute = runtime.StartSpillingActor(tester, true, ESpillingType::Compute, 1);
        auto channel = runtime.StartSpillingActor(tester, true, ESpillingType::Channel, 2);
        runtime.WaitBootstrap();

        UNIT_ASSERT_VALUES_EQUAL(CounterVal(runtime, ESpillingType::Compute, "Errors"), 0);
        UNIT_ASSERT_VALUES_EQUAL(CounterVal(runtime, ESpillingType::Channel, "Errors"), 0);
        UNIT_ASSERT_VALUES_EQUAL(CounterVal(runtime, ESpillingType::Compute, "NoSpaceErrors"), 0);
        UNIT_ASSERT_VALUES_EQUAL(CounterVal(runtime, ESpillingType::Channel, "NoSpaceErrors"), 0);
        UNIT_ASSERT_VALUES_EQUAL(CounterVal(runtime, ESpillingType::Compute, "IoErrors"), 0);

        WriteOk(runtime, compute, tester, 1, 40);
        WriteOk(runtime, channel, tester, 1, 40);

        WriteFails(runtime, compute, tester, 2, 30);
        UNIT_ASSERT_VALUES_EQUAL(CounterVal(runtime, ESpillingType::Compute, "NoSpaceErrors"), 1);
        UNIT_ASSERT_VALUES_EQUAL(CounterVal(runtime, ESpillingType::Compute, "Errors"), 1);
        UNIT_ASSERT_VALUES_EQUAL(CounterVal(runtime, ESpillingType::Channel, "NoSpaceErrors"), 0);
        UNIT_ASSERT_VALUES_EQUAL(CounterVal(runtime, ESpillingType::Channel, "Errors"), 0);

        WriteFails(runtime, channel, tester, 2, 30);
        UNIT_ASSERT_VALUES_EQUAL(CounterVal(runtime, ESpillingType::Channel, "NoSpaceErrors"), 1);
        UNIT_ASSERT_VALUES_EQUAL(CounterVal(runtime, ESpillingType::Channel, "Errors"), 1);
        UNIT_ASSERT_VALUES_EQUAL(CounterVal(runtime, ESpillingType::Compute, "NoSpaceErrors"), 1);
        UNIT_ASSERT_VALUES_EQUAL(CounterVal(runtime, ESpillingType::Compute, "Errors"), 1);

        const TFsPath computeFile = runtime.GetSpillingNodeDir() / "1_test_0";
        UNIT_ASSERT(NFs::Exists(computeFile.GetPath()));
        computeFile.ForceDelete();

        runtime.Send(new IEventHandle(compute, tester, new TEvDqSpilling::TEvRead(1)));
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvDqSpilling::TEvError>(tester));
        UNIT_ASSERT_VALUES_EQUAL(CounterVal(runtime, ESpillingType::Compute, "IoErrors"), 1);
        UNIT_ASSERT_VALUES_EQUAL(CounterVal(runtime, ESpillingType::Compute, "Errors"), 2);
        UNIT_ASSERT_VALUES_EQUAL(CounterVal(runtime, ESpillingType::Channel, "IoErrors"), 0);
        UNIT_ASSERT_VALUES_EQUAL(CounterVal(runtime, ESpillingType::Channel, "Errors"), 1);
    }

    Y_UNIT_TEST(MultipleFileParts) {
        TTestActorRuntime runtime;
        runtime.Initialize();

        runtime.StartSpillingService(1000, 100, 25);
        auto tester = runtime.AllocateEdgeActor();
        auto spillingActor = runtime.StartSpillingActor(tester);

        runtime.WaitBootstrap();
        const TString filePrefix = TStringBuilder() << runtime.GetSpillingNodeDir().GetPath() << "/1_test_";

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

        const TString filePrefix = TStringBuilder() << runtime.GetSpillingNodeDir().GetPath() << "/1_test_";

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

    template<bool MultiPart>
    void DoFdCounterTest()
    {
        TTestActorRuntime runtime;
        runtime.Initialize();

        auto spillingService = runtime.StartSpillingService(1000, 100, 25);
        auto tester = runtime.AllocateEdgeActor();
        auto spillingActor = runtime.StartSpillingActor(tester, MultiPart);

        runtime.WaitBootstrap();

        const TString filePrefix = TStringBuilder() << runtime.GetSpillingNodeDir().GetPath() << "/1_test_";

        constexpr const size_t numBlobs = 5;
        constexpr const size_t numFiles = MultiPart ? numBlobs : 1;

        auto assertFdCounter = [&](const size_t expected) {
            THttpRequest httpReq(HTTP_METHOD_GET);
            NMonitoring::TMonService2HttpRequest monReq(nullptr, &httpReq, nullptr, nullptr, "", nullptr);

            runtime.Send(new IEventHandle(spillingService, tester, new NMon::TEvHttpInfo(monReq)));
            auto resp = runtime.GrabEdgeEvent<NMon::TEvHttpInfoRes>(tester, TDuration::Seconds(1));
            UNIT_ASSERT(((NMon::TEvHttpInfoRes*) resp->Get())->Answer.Contains(TStringBuilder() << "Used file descriptors (compute): " << expected));
        };

        // write some blobs; one file per blob is created when MultiPart is true, a file per client otherwise
        for (ui32 i = 0; i < numBlobs; ++i) {
            auto ev = new TEvDqSpilling::TEvWrite(i, CreateRope(20, 'a' + i));
            runtime.Send(new IEventHandle(spillingActor, tester, ev));

            auto resp = runtime.GrabEdgeEvent<TEvDqSpilling::TEvWriteResult>(tester);
        }

        assertFdCounter(numFiles);

        // read back a single blob
        {
            const size_t blobIdx = 0;
            auto ev = new TEvDqSpilling::TEvRead(blobIdx);
            runtime.Send(new IEventHandle(spillingActor, tester, ev));
            auto resp = runtime.GrabEdgeEvent<TEvDqSpilling::TEvReadResult>(tester);
        }

        if (MultiPart) {
            assertFdCounter(numFiles - 1);
        } else {
            assertFdCounter(numFiles);
        }

        // close everything
        {
            runtime.Send(new IEventHandle(spillingActor, tester, new TEvents::TEvPoison));

            std::atomic<bool> done = false;
            runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
                if (event->GetRecipientRewrite() == spillingService) {
                    if (event->GetTypeRewrite() == 2146435074 /* EvCloseFileResponse */) {
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

        assertFdCounter(0);
    }

    Y_UNIT_TEST(FdCounterSingleFile) {
        DoFdCounterTest<false>();
    }

    Y_UNIT_TEST(FdCounterMultiFile) {
        DoFdCounterTest<true>();
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
        const TFsPath blobFile = runtime.GetSpillingNodeDir() / "1_test_0";
        blobFile.ForceDelete();

        {
            auto ev = new TEvDqSpilling::TEvRead(0, true);
            runtime.Send(new IEventHandle(spillingActor, tester, ev));

            auto resp = runtime.GrabEdgeEvent<TEvDqSpilling::TEvError>(tester);
            auto err = resp->Get()->Message;
            auto expected = "can't open \"" + blobFile.GetPath() + "\" with mode RdOnly";
            UNIT_ASSERT_C(err.Contains("No such file or directory"), err);
            UNIT_ASSERT_C(err.Contains(expected), err);
            UNIT_ASSERT_VALUES_EQUAL(CounterVal(runtime, "Spilling/Compute/IoErrors"), 1);
            UNIT_ASSERT_VALUES_EQUAL(CounterVal(runtime, "Spilling/Compute/Errors"), 1);
            UNIT_ASSERT_VALUES_EQUAL(CounterVal(runtime, "Spilling/Channel/IoErrors"), 0);
            UNIT_ASSERT_VALUES_EQUAL(CounterVal(runtime, "Spilling/Channel/Errors"), 0);
        }

        // The read error must trigger a proper cleanup: the file has to be closed and its
        // descriptor released. If the close operation is not dispatched (e.g. because the
        // active-op flag was left set), EvCloseFileResponse never arrives and the file stays
        // stuck in the active list, keeping the file descriptor counter above zero.
        {
            std::atomic<bool> closed = false;
            runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
                if (event->GetRecipientRewrite() == spillingSvc) {
                    if (event->GetTypeRewrite() == 2146435074 /* EvCloseFileResponse */) {
                        closed = true;
                    }
                }
                return TTestActorRuntimeBase::EEventAction::PROCESS;
            });

            TDispatchOptions options;
            options.CustomFinalCondition = [&]() {
                return (bool) closed;
            };
            runtime.DispatchEvents(options, TDuration::Seconds(1));
            UNIT_ASSERT_C(closed, "file was not closed after read error");
        }

        {
            THttpRequest httpReq(HTTP_METHOD_GET);
            NMonitoring::TMonService2HttpRequest monReq(nullptr, &httpReq, nullptr, nullptr, "", nullptr);

            runtime.Send(new IEventHandle(spillingSvc, tester, new NMon::TEvHttpInfo(monReq)));
            auto resp = runtime.GrabEdgeEvent<NMon::TEvHttpInfoRes>(tester, TDuration::Seconds(1));
            UNIT_ASSERT(((NMon::TEvHttpInfoRes*) resp->Get())->Answer.Contains("Used file descriptors (compute): 0"));
        }
    }

    Y_UNIT_TEST(WriteError) {
        TTestActorRuntime runtime;
        runtime.Initialize();

        auto spillingSvc = runtime.StartSpillingService(1000, 100, 25);
        auto tester = runtime.AllocateEdgeActor();
        auto spillingActor = runtime.StartSpillingActor(tester);

        runtime.WaitBootstrap();

        const TFsPath spillingDir = runtime.GetSpillingNodeDir();
        const TFsPath firstFile = spillingDir / "1_test_0";
        const TFsPath secondFile = spillingDir / "1_test_1";

        // Write the first blob. Because MaxFilePartSize is small and RemoveBlobsAfterRead is on,
        // this blob lands in its own file part "1_test_0".
        {
            auto ev = new TEvDqSpilling::TEvWrite(0, CreateRope(20, 'a'));
            runtime.Send(new IEventHandle(spillingActor, tester, ev));

            auto resp = runtime.GrabEdgeEvent<TEvDqSpilling::TEvWriteResult>(tester);
            UNIT_ASSERT_VALUES_EQUAL(0, resp->Get()->BlobId);
        }
        UNIT_ASSERT(NFs::Exists(firstFile.GetPath()));

        // File part names are predictable, so occupy the name of the would-be next file part
        // ("1_test_1") with a directory. Opening it for writing will fail and produce an IO error.
        secondFile.MkDirs();

        // Write the second blob. It requires a fresh file part ("1_test_1"), whose creation now
        // fails with an IO error.
        {
            auto ev = new TEvDqSpilling::TEvWrite(1, CreateRope(20, 'b'));
            runtime.Send(new IEventHandle(spillingActor, tester, ev));

            auto resp = runtime.GrabEdgeEvent<TEvDqSpilling::TEvError>(tester);
            UNIT_ASSERT_C(resp->Get()->Message.Contains("1_test_1"), resp->Get()->Message);
            UNIT_ASSERT_VALUES_EQUAL(CounterVal(runtime, "Spilling/Compute/IoErrors"), 1);
            UNIT_ASSERT_VALUES_EQUAL(CounterVal(runtime, "Spilling/Compute/Errors"), 1);
            UNIT_ASSERT_VALUES_EQUAL(CounterVal(runtime, "Spilling/Channel/IoErrors"), 0);
        }

        // The write error must trigger cleanup of the whole file, including the already-written
        // "1_test_0" part. Wait for the close operation to complete.
        {
            std::atomic<bool> closed = false;
            runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
                if (event->GetRecipientRewrite() == spillingSvc) {
                    if (event->GetTypeRewrite() == 2146435074 /* EvCloseFileResponse */) {
                        closed = true;
                    }
                }
                return TTestActorRuntimeBase::EEventAction::PROCESS;
            });

            TDispatchOptions options;
            options.CustomFinalCondition = [&]() {
                return (bool) closed;
            };
            runtime.DispatchEvents(options, TDuration::Seconds(1));
            UNIT_ASSERT_C(closed, "file was not closed after write error");
        }

        // The successfully written part must have been removed from disk during cleanup.
        UNIT_ASSERT_C(!NFs::Exists(firstFile.GetPath()), "partially written file was not cleaned up");
    }

    Y_UNIT_TEST(ThreadPoolQueueOverflow) {
        TTestActorRuntime runtime;
        runtime.Initialize();

        auto spillingService = runtime.StartSpillingService(1000, 500, 10, 1);
        ui32 iters = 100;
        TActorId tester;
        std::vector<TActorId> spillingActors;
        for (ui32 i = 0; i < iters; ++i) {
            spillingActors.emplace_back(runtime.StartSpillingActor(tester));
        }

        runtime.WaitBootstrap();

        std::atomic_uint writeResultEventsCount = 0;
        std::atomic_uint errorEventsCount = 0;

        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvDqSpilling::TEvError::EventType && event->Sender == spillingService) {
                auto error = event.Get()->Get<TEvDqSpilling::TEvError>();
                Cerr << error->Message << Endl;
                UNIT_ASSERT_EQUAL("[Write] Can not run operation", error->Message);
                ++writeResultEventsCount;
            }
            if (event->GetTypeRewrite() == TEvDqSpilling::TEvWriteResult::EventType && event->Sender == spillingService) {
                ++errorEventsCount;
            }
            return TTestActorRuntimeBase::EEventAction::PROCESS;
        });

        TDispatchOptions options;
        options.CustomFinalCondition = [&]() {
            return errorEventsCount.load() + writeResultEventsCount.load() == iters;
        };

        for (ui32 i = 0; i < iters; ++i) {
            auto ev = new TEvDqSpilling::TEvWrite(i, CreateRope(10, 'a'));
            runtime.Send(new IEventHandle(spillingActors[i], tester, ev));
        }

        runtime.DispatchEvents(options);

        UNIT_ASSERT(CounterVal(runtime, "Spilling/Compute/QueueOverflowErrors") > 0);
        UNIT_ASSERT(CounterVal(runtime, "Spilling/Compute/Errors") > 0);
        UNIT_ASSERT_VALUES_EQUAL(CounterVal(runtime, "Spilling/Channel/QueueOverflowErrors"), 0);
    }

    Y_UNIT_TEST(RecoverAfterStartError) {
        TTestActorRuntime runtime;
        runtime.SetScheduledEventFilter([](
                TTestActorRuntimeBase& runtime,
                TAutoPtr<IEventHandle>& event,
                TDuration delay,
                TInstant& deadline)
        {
            if (runtime.IsScheduleForActorEnabled(event->GetRecipientRewrite())) {
                deadline = runtime.GetTimeProvider()->Now() + delay;
                return false;
            }
            return true;
        });
        runtime.Initialize();

        const TFsPath root = TFsPath::Cwd() / (runtime.GetSpillingPrefix() + "_recover");
        root.ForceDelete();
        {
            TFileOutput output(root.GetPath());
        }

        runtime.StartSpillingService(1000, 500, 100, 1000, root);
        runtime.WaitBootstrap();

        // While the root is unusable, requests are rejected instead of killing the service.
        {
            auto tester = runtime.AllocateEdgeActor();
            runtime.StartSpillingActor(tester);
            runtime.WaitBootstrap();

            auto resp = runtime.GrabEdgeEvent<TEvDqSpilling::TEvError>(tester, TDuration::Seconds(1));
            UNIT_ASSERT_VALUES_EQUAL("Spilling service is not started", resp->Get()->Message);
            UNIT_ASSERT(CounterVal(runtime, "Spilling/StartupErrors") >= 1);
            UNIT_ASSERT_VALUES_EQUAL(CounterVal(runtime, "Spilling/ServiceNotStarted"), 1);
        }

        // Unblock the root: the service creates only its own directory inside an existing root.
        root.ForceDelete();
        root.MkDir();

        const TFsPath nodeDir = runtime.GetSpillingNodeDir();

        TDispatchOptions retryOptions;
        retryOptions.CustomFinalCondition = [&nodeDir]() { return nodeDir.IsDirectory(); };
        UNIT_ASSERT(runtime.DispatchEvents(retryOptions, TDuration::Seconds(5)));

        {
            auto tester = runtime.AllocateEdgeActor();
            auto spillingActor = runtime.StartSpillingActor(tester);
            runtime.WaitBootstrap();

            runtime.Send(new IEventHandle(
                spillingActor,
                tester,
                new TEvDqSpilling::TEvWrite(1, CreateRope(10, 'a'))));

            auto resp = runtime.GrabEdgeEvent<TEvDqSpilling::TEvWriteResult>(tester, TDuration::Seconds(1));
            UNIT_ASSERT_VALUES_EQUAL(resp->Get()->BlobId, 1);
        }
    }

    // The service spills into <root>/spilling-tmp-<nodeId>-<sessionId>-<user>/, where <root> is the
    // configured Root, or $TMP when Root is empty.
    //
    // Directories left by the previous version of the service are named node_<nodeId>_<sessionId> and live
    // either directly in <root>, or in $TMP/spilling-tmp-<user>/, which was the root of the old default
    // layout. Both places are cleaned up, no matter where the service spills now.
    //
    // Before the service starts:
    //
    //   <root>/
    //   ├── node_<thisNode>_<old-session>/                       delete: this node, old format
    //   ├── node_<thisNode>_backup/                              keep:   old format has a session id here
    //   ├── node_<otherNode>_<old-session>/                      keep:   other node may still be running
    //   ├── spilling-tmp-<thisNode>-<old-session>-<user>/        delete: this node, previous run
    //   ├── spilling-tmp-<thisNode>-<old-session>-<otherUser>/   keep:   same node id, another OS user
    //   ├── spilling-tmp-<thisNode>-<old-session>-<user>-other/  keep:   OS user named <user>-other
    //   ├── spilling-tmp-<thisNode>-<old-session>-other-<user>/  keep:   OS user named other-<user>
    //   ├── spilling-tmp-<otherNode>-<old-session>-<user>/       keep:   other node may still be running
    //   └── unrelated-<old-session>/                             keep:   not a spilling directory
    //
    //   $TMP/spilling-tmp-<user>/                                keep:   root of the old default layout
    //   ├── node_<thisNode>_<old-session>/                       delete: this node, old format
    //   └── node_<otherNode>_<old-session>/                      keep:   other node may still be running
    //
    // After the service starts:
    //
    //   <root>/
    //   ├── node_<thisNode>_backup/
    //   ├── node_<otherNode>_<old-session>/
    //   ├── spilling-tmp-<thisNode>-<current-session>-<user>/    created by the service for this run
    //   ├── spilling-tmp-<thisNode>-<old-session>-<otherUser>/
    //   ├── spilling-tmp-<thisNode>-<old-session>-<user>-other/
    //   ├── spilling-tmp-<thisNode>-<old-session>-other-<user>/
    //   ├── spilling-tmp-<otherNode>-<old-session>-<user>/
    //   └── unrelated-<old-session>/
    //
    //   $TMP/spilling-tmp-<user>/
    //   └── node_<otherNode>_<old-session>/
    Y_UNIT_TEST(RemoveOldTmpDirectories) {
        TTestActorRuntime runtime;
        runtime.Initialize();

        const ui32 thisNodeId = runtime.GetNodeId();
        const ui32 otherNodeId = thisNodeId + 1;
        const TString username = GetUsername();
        const TString oldSessionId = CreateGuidAsString();

        const TFsPath root = TFsPath::Cwd() / (runtime.GetSpillingPrefix() + "_cleanup");
        root.ForceDelete();
        root.MkDir();

        // The old default layout lives in $TMP, which is shared with the rest of the system, so the test
        // removes what it created instead of deleting whole directories.
        struct TCreatedPaths {
            TVector<TFsPath> Paths;

            TFsPath Add(const TFsPath& path) {
                Track(path);
                return MakeDirWithContent(path);
            }

            TFsPath Track(const TFsPath& path) {
                Paths.push_back(path);
                return path;
            }

            ~TCreatedPaths() {
                for (const auto& path : Paths) {
                    if (path.Exists()) {
                        path.ForceDelete();
                    }
                }
            }
        } created;

        const auto oldFormatDirName = [&](ui32 nodeId) {
            return TStringBuilder() << "node_" << nodeId << "_" << oldSessionId;
        };

        const TFsPath oldFormatThisNode = created.Add(root / oldFormatDirName(thisNodeId));
        const TFsPath oldFormatNoSession = created.Add(root / (TStringBuilder() << "node_" << thisNodeId << "_backup"));
        const TFsPath oldFormatOtherNode = created.Add(root / oldFormatDirName(otherNodeId));
        const TFsPath thisNodePreviousRun = created.Add(root / MakeSpillingNodeDirName(thisNodeId, username, oldSessionId));
        const TFsPath otherUserSameNode = created.Add(root / MakeSpillingNodeDirName(thisNodeId, "other-user", oldSessionId));
        const TFsPath userWithOurNameAtStart = created.Add(root / MakeSpillingNodeDirName(thisNodeId, username + "-other", oldSessionId));
        const TFsPath userWithOurNameAtEnd = created.Add(root / MakeSpillingNodeDirName(thisNodeId, "other-" + username, oldSessionId));
        const TFsPath otherNode = created.Add(root / MakeSpillingNodeDirName(otherNodeId, username, oldSessionId));
        const TFsPath unrelated = created.Add(root / (TStringBuilder() << "unrelated-" << oldSessionId));

        const TFsPath oldFormatRoot = created.Track(GetDefaultSpillingRoot() / (TStringBuilder() << SpillingDirPrefix << username));
        const TFsPath oldFormatRootThisNode = created.Add(oldFormatRoot / oldFormatDirName(thisNodeId));
        const TFsPath oldFormatRootOtherNode = created.Add(oldFormatRoot / oldFormatDirName(otherNodeId));

        runtime.StartSpillingService(1000, 500, 100, 1000, root);
        runtime.WaitBootstrap();

        // Cleanup is asynchronous, so wait until the service actually spills something.
        auto tester = runtime.AllocateEdgeActor();
        auto spillingActor = runtime.StartSpillingActor(tester);
        runtime.WaitBootstrap();
        runtime.Send(new IEventHandle(spillingActor, tester, new TEvDqSpilling::TEvWrite(1, CreateRope(10, 'a'))));
        auto resp = runtime.GrabEdgeEvent<TEvDqSpilling::TEvWriteResult>(tester, TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(resp->Get()->BlobId, 1);

        const TFsPath currentDir = created.Track(runtime.GetSpillingNodeDir());
        UNIT_ASSERT_C(currentDir.IsDirectory(), currentDir);

        for (const auto& path : {oldFormatThisNode, thisNodePreviousRun, oldFormatRootThisNode}) {
            UNIT_ASSERT_C(!path.Exists(), TStringBuilder() << "must be removed: " << path);
        }

        for (const auto& path : {oldFormatNoSession, oldFormatOtherNode, otherUserSameNode, userWithOurNameAtStart,
                                 userWithOurNameAtEnd, otherNode, unrelated, oldFormatRoot, oldFormatRootOtherNode}) {
            UNIT_ASSERT_C(path.IsDirectory(), TStringBuilder() << "must be kept: " << path);
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
