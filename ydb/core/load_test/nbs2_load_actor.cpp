#include "service_actor.h"

#include <ydb/core/nbs/cloud/blockstore/tools/testing/loadtest/lib/app_context.h>
#include <ydb/core/nbs/cloud/blockstore/tools/testing/loadtest/lib/helpers.h>
#include <ydb/core/nbs/cloud/blockstore/tools/testing/loadtest/lib/request_generator.h>
#include <ydb/core/nbs/cloud/blockstore/tools/testing/loadtest/lib/test_runner.h>

#include <ydb/core/base/counters.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/request.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/api/service.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>
#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/histogram.h>
#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/logging.h>

#include <ydb/library/actors/core/event.h>
#include <ydb/library/workload/abstract/workload_factory.h>
#include <ydb/library/workload/stock/stock.h>
#include <ydb/library/workload/kv/kv.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/histogram/hdr/histogram.h>
#include <library/cpp/protobuf/json/proto2json.h>
#include <library/cpp/time_provider/time_provider.h>

#include <util/generic/fwd.h>
#include <util/generic/queue.h>
#include <util/random/fast.h>
#include <util/random/shuffle.h>

namespace {

void FillLatency(
    const NCloud::TLatencyHistogram& hist,
    NCloud::NBlockStore::NProto::TLatency& latency)
{
    latency.SetP50(hist.GetValueAtPercentile(50));
    latency.SetP90(hist.GetValueAtPercentile(90));
    latency.SetP95(hist.GetValueAtPercentile(95));
    latency.SetP99(hist.GetValueAtPercentile(99));
    latency.SetP999(hist.GetValueAtPercentile(99.9));
    latency.SetMin(hist.GetMin());
    latency.SetMax(hist.GetMax());
    latency.SetMean(hist.GetMean());
    latency.SetStdDeviation(hist.GetStdDeviation());
}

// TODO вынести в nbs core lib
using IEventBasePtr = std::unique_ptr<NActors::IEventBase>;
inline void SendWithUndeliveryTracking(
    const NActors::TActorContext& ctx,
    const NActors::TActorId& recipient,
    IEventBasePtr event,
    ui64 cookie = 0)
{
    auto ev = std::make_unique<NActors::IEventHandle>(
        recipient,
        ctx.SelfID,
        event.release(),
        NActors::IEventHandle::FlagForwardOnNondelivery,    // flags
        cookie,  // cookie
        &ctx.SelfID    // forwardOnNondelivery
    );

    ctx.Send(ev.release());
}

} // namespace

namespace NKikimr {

enum {
    EvNBS2WorkerResponse
};

class TNBS2LoadActor : public TActorBootstrapped<TNBS2LoadActor> {
public:
    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::NBS2_TEST_WORKLOAD;
    }

    TNBS2LoadActor(const NKikimr::TEvLoadTestRequest::TNBS2Load& cmd, const TActorId& parent,
            const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters, ui64 index, ui64 tag)
        : Parent(parent)
        , Tag(tag)
        , DurationSeconds(cmd.GetDurationSeconds())
        , Name(cmd.GetName())
        , RangeTest(cmd.GetRangeTest())
    {
        Y_UNUSED(index);
        Y_UNUSED(counters);
        VERIFY_PARAM(DurationSeconds);
        VERIFY_PARAM(RangeTest);

        DirectPartitionId.Parse(cmd.GetDirectPartitionId().data(), cmd.GetDirectPartitionId().size());
        google::protobuf::TextFormat::PrintToString(cmd, &ConfigString);

        if (RangeTest.GetStart() >= RangeTest.GetEnd() || RangeTest.GetEnd() > 32767) {
            ythrow NKikimr::TLoadActorException() << "Range must be in [0, 32767]";
        }
        if (RangeTest.GetZeroRate() > 0) {
            ythrow NKikimr::TLoadActorException() << "ZeroRate is unsupported";
        }
        if (RangeTest.GetReadRate() + RangeTest.GetWriteRate() != 100) {
            ythrow NKikimr::TLoadActorException() << "Overall request rate must be 100";
        }
        if (RangeTest.GetMinRequestSize() || RangeTest.GetMaxRequestSize()) {
            ythrow NKikimr::TLoadActorException() << "Request size is strictly 1 block for now";
        }
        RangeTest.SetMinRequestSize(1);
        RangeTest.SetMaxRequestSize(1);
    }

    ~TNBS2LoadActor() {
    }

    void Bootstrap(const TActorContext& ctx) {
        LOG_WARN_S(ctx, NKikimrServices::NBS2_LOAD_TEST, "Tag# " << Tag << " TNBS2LoadActor Bootstrap called");

        Become(&TNBS2LoadActor::StateStart);
        auto TestInitializeingStart = Now();
        RunTest(ctx);
        TestInitializingDuration = Now() - TestInitializeingStart;
        LOG_WARN_S(
            ctx,
            NKikimrServices::NBS2_LOAD_TEST,
            "Tag# " << Tag << " Test has been initialized in "
                << TestInitializingDuration << " sec");

        ctx.Schedule(TDuration::Seconds(DurationSeconds + 1), new TEvents::TEvPoisonPill);
        LOG_WARN_S(ctx, NKikimrServices::NBS2_LOAD_TEST, "Tag# " << Tag << " Schedule PoisonPill");

    }

    void PrepareTestResult(const TActorContext& ctx)
    {
        using namespace NCloud::NBlockStore;
        LOG_WARN_S(ctx, NKikimrServices::NBS2_LOAD_TEST, " test has been completed " << Name);

        auto stopped = TInstant::Now();
        NJson::TJsonValue result;

        const auto& suiteResults = TestRunner->GetResults();
        NProto::TTestResults proto;
        proto.SetName(Name);
        proto.SetResult(suiteResults.Status);
        proto.SetStartTime(TestRunner->GetStartTime().MicroSeconds());
        proto.SetEndTime(stopped.MicroSeconds());
        proto.SetRequestsCompleted(suiteResults.RequestsCompleted);

        if (suiteResults.BlocksRead) {
            proto.SetBlocksRead(suiteResults.BlocksRead);
            FillLatency(suiteResults.ReadHist, *proto.MutableReadLatency());
        }

        if (suiteResults.BlocksWritten) {
            proto.SetBlocksWritten(suiteResults.BlocksWritten);
            FillLatency(suiteResults.WriteHist, *proto.MutableWriteLatency());
        }

        if (suiteResults.BlocksZeroed) {
            proto.SetBlocksZeroed(suiteResults.BlocksZeroed);
            FillLatency(suiteResults.ZeroHist, *proto.MutableZeroLatency());
        }

        NProtobufJson::Proto2Json(proto, result["TestResults"], {});
        TestContext.Result = NJson::WriteJson(result, false, false, false);
    }

    void SendIORequest(
        const TActorContext& ctx,
        IEventBasePtr request,
        NCloud::NBlockStore::NLoadTest::LoadTestSendRequestFunctionCB cb
    )
    {
        ui64 cookie = ++LastUsedCookie;
        CookieToRequestCB[cookie] = std::move(cb);
        SendWithUndeliveryTracking(ctx, DirectPartitionId, std::move(request), cookie);
    }

    NCloud::NBlockStore::NLoadTest::TLoadTestRequestCallbacks GetLoadTestCallbacks()
    {
        using namespace NCloud::NBlockStore;
        auto sendReadRequest =
            [this]
            (TBlockRange64 range, NCloud::NBlockStore::NLoadTest::LoadTestSendRequestFunctionCB cb, const void *udata) mutable {
            auto request = std::make_unique<NYdb::NBS::TEvService::TEvReadBlocksRequest>();
            request->Record.SetDiskId("TempDiskID");
            request->Record.SetStartIndex(range.Start);
            request->Record.SetBlocksCount(range.Size());

            const TActorContext *actorContext = reinterpret_cast<const TActorContext*>(udata);
            SendIORequest(*actorContext, std::move(request), cb);
        };

        auto sendWriteRequest =
            [this]
            (
                ui64 blockIndexWriteTo,
                const void* data,
                size_t dataSize,
                NCloud::NBlockStore::NLoadTest::LoadTestSendRequestFunctionCB cb,
                const void *udata
            ) mutable {
                auto request = std::make_unique<NYdb::NBS::TEvService::TEvWriteBlocksRequest>();
                request->Record.SetDiskId("TempDiskID");
                request->Record.SetStartIndex(blockIndexWriteTo);
                auto* dstBlocks = request->Record.MutableBlocks();
                dstBlocks->AddBuffers(data, dataSize);

                const TActorContext *actorContext = reinterpret_cast<const TActorContext*>(udata);
                SendIORequest(*actorContext, std::move(request), cb);
        };

        auto notifyTestCompleted = [this] (const void *udata) {
            const TActorContext *actorContext = reinterpret_cast<const TActorContext*>(udata);
            TestContext.Finished = true;
            ReasonOfFinishing = "Test completed";

            if (!ResultSent) {
                PrepareTestResult(*actorContext);
                SendTestResult(*actorContext);
            }
        };

        NCloud::NBlockStore::NLoadTest::TLoadTestRequestCallbacks requestCallbacks;
        requestCallbacks.Read = sendReadRequest;
        requestCallbacks.NotifyCompleted = notifyTestCompleted;
        requestCallbacks.Write = sendWriteRequest;

        return requestCallbacks;
    }

    void RunTest(const TActorContext& ctx) {
        using namespace NCloud::NBlockStore;
        LOG_DEBUG_S(ctx, NKikimrServices::NBS2_LOAD_TEST, "Tag# " << Tag << " RunTest called");


        //-------------

        NCloud::TLogSettings logSettings;
        logSettings.FiltrationLevel = ELogPriority::TLOG_INFO;
        auto logging = CreateLoggingService("console", logSettings);

        NCloud::NBlockStore::NLoadTest::TLoadTestRequestCallbacks
            requestCallbacks = GetLoadTestCallbacks();

        TestRunner = CreateTestRunner(
            logging,
            NCloud::NBlockStore::NLoadTest::MakeLoggingTag(Name),
            NCloud::NBlockStore::NLoadTest::CreateArtificialRequestGenerator(logging, RangeTest),
            RangeTest.GetIoDepth(),
            TestContext.ShouldStop,
            requestCallbacks,
            reinterpret_cast<const void*>(&ctx));
        TestRunner->Start();
    }

    template <typename Record>
    void HandleReadWriteResponse(
        const TActorContext& ctx,
        const Record& record,
        ui64 cookie
    )
    {
        using namespace NYdb::NBS;
        auto it = CookieToRequestCB.find(cookie);
        if (it == CookieToRequestCB.end()) {
            LOG_ERROR_S(ctx, NKikimrServices::NBS2_LOAD_TEST, "Tag# " << Tag << "Could not find delivered cookie request " << cookie);
            return;
        }

        auto error = HasError(record)
            ? record.GetError()
            : MakeError(S_OK);
        it->second(error, &ctx);
        CookieToRequestCB.erase(it);
    }

    void HandleReadBlocksResponse(
        const NYdb::NBS::TEvService::TEvReadBlocksResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        LOG_DEBUG_S(ctx, NKikimrServices::NBS2_LOAD_TEST, "Tag# " << Tag << "HandleReadBlocksResponse " << ev->Cookie);
        HandleReadWriteResponse(ctx, ev->Get()->Record, ev->Cookie);

    }

    void HandleWriteBlocksResponse(
        const NYdb::NBS::TEvService::TEvWriteBlocksResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        LOG_DEBUG_S(ctx, NKikimrServices::NBS2_LOAD_TEST, "Tag# " << Tag << "HandleWriteBlocksResponse " << ev->Cookie);
        HandleReadWriteResponse(ctx, ev->Get()->Record, ev->Cookie);
    }

    template <typename Ev>
    void HandleUndelivery(
        const Ev& ev,
        const TActorContext& ctx)
    {
        using namespace NYdb::NBS;
        LOG_ERROR_S(ctx, NKikimrServices::NBS2_LOAD_TEST, "Tag# " << Tag << "Could not deliver request " << ev->Cookie);

        auto it = CookieToRequestCB.find(ev->Cookie);
        if (it == CookieToRequestCB.end()) {
            LOG_ERROR_S(ctx, NKikimrServices::NBS2_LOAD_TEST, "Tag# " << Tag << "Could not find undelivered cookie request " << ev->Cookie);
            return;
        }

        auto error = MakeError(E_TRANSPORT_ERROR, "Request is undelivered");
        it->second(error, &ctx);
        CookieToRequestCB.erase(it);
    }

    STRICT_STFUNC(StateStart,
        CFunc(TEvents::TSystem::PoisonPill, HandlePoisonPill)
        HFunc(NMon::TEvHttpInfo, HandleHTML)
        HFunc(NYdb::NBS::TEvService::TEvReadBlocksResponse, HandleReadBlocksResponse)
        HFunc(NYdb::NBS::TEvService::TEvWriteBlocksResponse, HandleWriteBlocksResponse)
        HFunc(NYdb::NBS::TEvService::TEvReadBlocksRequest, HandleUndelivery)
    )


private:

    // death
    void HandlePoisonPill(const TActorContext& ctx) {
        LOG_WARN_S(ctx, NKikimrServices::NBS2_LOAD_TEST, "Tag# " << Tag << " HandlePoisonPill called");

        // TODO add waiting of active requests
        TestRunner->Stop();
        if (ReasonOfFinishing.empty()) {
            ReasonOfFinishing = TestRunner->IsFinished() ? "Test completed" : "HandlePoisonPill called";
        }

        if (!ResultSent) {
            PrepareTestResult(ctx);
            SendTestResult(ctx);
        }

        LOG_DEBUG_S(ctx, NKikimrServices::NBS2_LOAD_TEST, "Tag# " << Tag << " loadActor has been finifshed");
        Die(ctx);
    }

    void SendTestResult(const TActorContext& ctx) {
        LOG_WARN_S(ctx, NKikimrServices::NBS2_LOAD_TEST, "Tag# " << Tag << " sending result");
        ResultSent = true;
        TIntrusivePtr<TEvLoad::TLoadReport> report = nullptr;
        report.Reset(new TEvLoad::TLoadReport());
        report->Duration = TDuration::Seconds(DurationSeconds);

        auto* finishEv = new TEvLoad::TEvLoadTestFinished(Tag, report, ReasonOfFinishing);
        finishEv->LastHtmlPage = RenderHTML();
        //finishEv->JsonResult = GetJsonResult();
        ctx.Send(Parent, finishEv);
    }

private:

    TString RenderHTML() {
        TStringStream str;
        HTML(str) {
            TABLE_CLASS("table table-condensed") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() {
                            str << "DurationSeconds";
                        }
                        TABLEH() {
                            str << "DirectPartitionId";
                        }
                        TABLEH() {
                            str << "Name";
                        }
                        TABLEH() {
                            str << "TestInitializingDuration";
                        }
                        TABLEH() {
                            str << "TestResults";
                        }
                    }
                }
                TABLEBODY() {
                    TABLER() {
                        TABLED() {
                            str << DurationSeconds;
                        };
                        TABLED() {
                            str << DirectPartitionId;
                        };
                        TABLED() {
                            str << Name;
                        };
                        TABLED() {
                            str << TestInitializingDuration;
                        };
                        TABLED() {
                            str << TestContext.Result.Str();
                        };
                    }
                }
            }
            COLLAPSED_BUTTON_CONTENT(Sprintf("configProtobuf%" PRIu64, Tag), "Config") {
                str << "<pre>" << ConfigString << "</pre>";
            }
        }
        return str.Str();
    }

    void HandleHTML(NMon::TEvHttpInfo::TPtr& ev, const TActorContext& ctx) {
        ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(RenderHTML(), ev->Get()->SubRequestId));
    }

    // common

    const TActorId Parent;
    ui64 Tag;
    ui32 DurationSeconds;
    NActors::TActorId DirectPartitionId;
    TString Name;

    NCloud::NBlockStore::NProto::TRangeTest RangeTest;
    NCloud::NBlockStore::NLoadTest::TTestContext TestContext; // TODO remove me

    TMap<ui64, NCloud::NBlockStore::NLoadTest::LoadTestSendRequestFunctionCB> CookieToRequestCB;
    std::atomic<uint64_t> LastUsedCookie = 0;

    NCloud::NBlockStore::NLoadTest::ITestRunnerPtr TestRunner;
    TString ReasonOfFinishing;
    bool ResultSent = false;
    TDuration TestInitializingDuration;
    // ---
    TString ConfigString;
};

IActor * CreateNBS2LoadActor(const NKikimr::TEvLoadTestRequest::TNBS2Load& cmd,
        const TActorId& parent, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters, ui64 index, ui64 tag) {
    return new TNBS2LoadActor(cmd, parent, counters, index, tag);
}

} // NKikimr
