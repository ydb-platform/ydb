#include "service_actor.h"

#include "nbs2_lib/app_context.h"
#include "nbs2_lib/helpers.h"
#include "nbs2_lib/suite_runner.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/histogram.h>

#include <ydb/library/workload/abstract/workload_factory.h>
#include <ydb/library/workload/stock/stock.h>
#include <ydb/library/workload/kv/kv.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/histogram/hdr/histogram.h>
#include <library/cpp/protobuf/json/proto2json.h>
#include <library/cpp/time_provider/time_provider.h>

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
        , TestParam(cmd.GetTestParam())
        , Name(cmd.GetName())
        , RangeTest(cmd.GetRangeTest())
    {
        Y_UNUSED(index);
        Y_UNUSED(counters);
        VERIFY_PARAM(DurationSeconds);
        VERIFY_PARAM(RangeTest);
        google::protobuf::TextFormat::PrintToString(cmd, &ConfigString);
    }

    ~TNBS2LoadActor() {
    }

    void Bootstrap(const TActorContext& ctx) {
        // TODO delete all 'maks_ololo' there and in nbs_lib
        LOG_ERROR_S(ctx, NKikimrServices::NBS2_LOAD_TEST, "Tag# " << Tag << " maks_ololo TNBS2LoadActor Bootstrap called");

        ctx.Schedule(TDuration::Seconds(DurationSeconds + 3), new TEvents::TEvPoisonPill);
        LOG_ERROR_S(ctx, NKikimrServices::NBS2_LOAD_TEST, "Tag# " << Tag << " maks_ololo Schedule PoisonPill");

        Become(&TNBS2LoadActor::StateStart);
        RunTest(ctx);
    }

    void LoadTestCallback(const TActorContext& ctx)
    {
        using namespace NCloud::NBlockStore;
        LOG_ERROR_S(ctx, NKikimrServices::NBS2_LOAD_TEST, " maks_ololo Completed test " << Name);

        auto stopped = TInstant::Now();
        NJson::TJsonValue result;

        const auto& suiteResults = SuiteRunner->GetResults();
        NProto::TTestResults proto;
        proto.SetName(Name);
        proto.SetResult(suiteResults.Status);
        proto.SetStartTime(SuiteRunner->GetStartTime().MicroSeconds());
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

        SendTestResult(ctx);
    }

     void RunTest(
        const TActorContext& ctx
    ) {
        using namespace NCloud::NBlockStore;
        LOG_ERROR_S(ctx, NKikimrServices::NBS2_LOAD_TEST, "Tag# " << Tag << " maks_ololo RunTest called");


        //auto callback = [this, actorContext = &ctx ] () {
        //    LoadTestCallback(*actorContext);
        //};
        auto callback = [] () {
        };

        SuiteRunner.Reset(
            new NCloud::NBlockStore::NLoadTest::TSuiteRunner(
                AppContext,
                Name,
                TestContext,
                std::move(callback)
            )
        );

        SuiteRunner->StartSubtest(RangeTest);
     }

    STRICT_STFUNC(StateStart,
        CFunc(TEvents::TSystem::PoisonPill, HandlePoisonPill)
        HFunc(NMon::TEvHttpInfo, HandleHTML)
    )

private:

    // death
    void HandlePoisonPill(const TActorContext& ctx) {
        LOG_ERROR_S(ctx, NKikimrServices::NBS2_LOAD_TEST, "Tag# " << Tag << " maks_ololo HandlePoisonPill called");

        // todo if !test.finished
        NCloud::NBlockStore::NLoadTest::StopTest(TestContext);
        SuiteRunner->Wait(1);
        PoisonPillRecieved = true;

        LOG_ERROR_S(ctx, NKikimrServices::NBS2_LOAD_TEST, "Tag# " << Tag << " maks_ololo HandlePoisonPill after wait");
        //DieIfNeed(ctx);
        LoadTestCallback(ctx);
    }

    void SendTestResult(const TActorContext& ctx) {
        TestResultSent = true;

        TIntrusivePtr<TEvLoad::TLoadReport> report = nullptr;
        report.Reset(new TEvLoad::TLoadReport());
        report->Duration = TDuration::Seconds(DurationSeconds);

        // todo добавить и нормальный результат в reason
        auto* finishEv = new TEvLoad::TEvLoadTestFinished(Tag, report, "Poison pill");
        finishEv->LastHtmlPage = RenderHTML();
        //finishEv->JsonResult = GetJsonResult();
        ctx.Send(Parent, finishEv);
        DieIfNeed(ctx);
    }

    void DieIfNeed(const TActorContext& ctx) {
        if (PoisonPillRecieved && TestResultSent) {
            LOG_ERROR_S(ctx, NKikimrServices::NBS2_LOAD_TEST, "Tag# " << Tag << " maks_ololo loadActor has been finifshed");
            Die(ctx);
        }
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
                            str << "TestParam";
                        }
                        TABLEH() {
                            str << "Name";
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
                            str << TestParam;
                        };
                        TABLED() {
                            str << Name;
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
    TString TestParam;
    TString Name;

    NCloud::NBlockStore::NProto::TRangeTest RangeTest;
    NCloud::NBlockStore::NLoadTest::TAppContext AppContext; // TODO try to del me
    NCloud::NBlockStore::NLoadTest::TTestContext TestContext;
    TIntrusivePtr<NCloud::NBlockStore::NLoadTest::TSuiteRunner> SuiteRunner = nullptr;
    bool PoisonPillRecieved = false;
    bool TestResultSent = false;
    // ---
    TString ConfigString;
};

IActor * CreateNBS2LoadActor(const NKikimr::TEvLoadTestRequest::TNBS2Load& cmd,
        const TActorId& parent, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters, ui64 index, ui64 tag) {
    return new TNBS2LoadActor(cmd, parent, counters, index, tag);
}

} // NKikimr
