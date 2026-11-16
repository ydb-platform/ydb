#include <ydb/core/tx/cdc_aggregator/public/events.h>
#include <ydb/core/tx/cdc_aggregator/service/service.h>
#include <ydb/core/tx/cdc_aggregator/tablet/tablet.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/set.h>

namespace NKikimr::NCdcAggregator {

namespace {

constexpr ui64 Cookie = 42;
constexpr ui64 TabletId = MakeTabletID(false, 3);

using TBatch = TEvCdcAggregator::TEvWriteHeartbeats;
using TBatchResult = TEvCdcAggregator::TEvWriteHeartbeatsResult;
using TReply = TEvCdcAggregatorService::TEvWriteHeartbeatResult;
using TProtoResult = NKikimrCdcAggregator::TEvWriteHeartbeatsResult;

class TTestEnv {
public:
    TTestEnv() {
        SetupTabletServices(Runtime);
        const auto bootstrapper = CreateTestBootstrapper(Runtime,
            CreateTestTabletInfo(TabletId, TTabletTypes::CdcAggregator), &CreateCdcAggregator);
        Runtime.EnableScheduleForActor(bootstrapper);
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvTablet::EvBoot);
        Runtime.DispatchEvents(options);

        Service = Runtime.Register(CreateCdcAggregatorService());
        Runtime.EnableScheduleForActor(Service);
        Runtime.RegisterService(MakeCdcAggregatorServiceId(Runtime.GetNodeId(0)), Service);
        ReplyEdge = Runtime.AllocateEdgeActor();
        BatchEdge = Runtime.AllocateEdgeActor();

        BatchObserver = Runtime.AddObserver<TBatch>([this](TBatch::TPtr& ev) {
            if (ev->GetRecipientRewrite() != BatchEdge) {
                auto batch = std::move(ev);
                batch->Rewrite(TBatch::EventType, BatchEdge);
                Runtime.Send(batch.Release());
            }
        });
        PipeObserver = Runtime.AddObserver<TEvTabletPipe::TEvServerConnected>([this](TEvTabletPipe::TEvServerConnected::TPtr& ev) {
            if (ev->Get()->TabletId == TabletId) {
                PipeClient = ev->Get()->ClientId;
            }
        });
    }

    void SendHeartbeat(ui64 sourceDataShardId, ui64 step, ui64 cookie = Cookie) {
        auto request = MakeHolder<TEvCdcAggregatorService::TEvWriteHeartbeat>();
        request->RecipientTabletId = TabletId;
        request->SourceDataShardId = sourceDataShardId;
        request->Heartbeat = NPQ::THeartbeat{.Version = TRowVersion(step, 0)};
        Runtime.Send(new IEventHandle(Service, ReplyEdge, request.Release(), 0, cookie));
    }

    TBatch::TPtr GrabBatch(TDuration timeout = TDuration::Seconds(5)) {
        return Runtime.GrabEdgeEvent<TBatch>(BatchEdge, timeout);
    }

    TReply::TPtr GrabReply(TDuration timeout = TDuration::Seconds(5)) {
        return Runtime.GrabEdgeEvent<TReply>(ReplyEdge, timeout);
    }

    void ReplyToBatch(const TBatch::TPtr& batch, TProtoResult::EStatus status = TProtoResult::SUCCESS) {
        auto result = MakeHolder<TBatchResult>();
        result->Record.SetOrigin(TabletId);
        result->Record.SetStatus(status);
        if (status == TProtoResult::SUCCESS) {
            for (const auto& heartbeat : batch->Get()->Record.GetHeartbeats()) {
                auto& written = *result->Record.AddResults();
                written.SetSourceDataShardId(heartbeat.GetSourceDataShardId());
                written.SetStatus(TProtoResult::THeartbeatResult::SUCCESS);
                TRowVersion(heartbeat.GetHeartbeat().GetStep(), heartbeat.GetHeartbeat().GetTxId())
                    .ToProto(written.MutableWrittenVersion());
            }
        }
        Runtime.Send(new IEventHandle(Service, BatchEdge, result.Release(), 0, batch->Cookie));
    }

    void DisconnectPipe() {
        Runtime.Send(new IEventHandle(PipeClient, ReplyEdge, new TEvents::TEvPoisonPill()));
    }

    TTestBasicRuntime Runtime{1, false};
    TActorId Service;
    TActorId ReplyEdge;
    TActorId BatchEdge;
    TActorId PipeClient;

private:
    TTestActorRuntimeBase::TEventObserverHolder BatchObserver;
    TTestActorRuntimeBase::TEventObserverHolder PipeObserver;
};

} // namespace

Y_UNIT_TEST_SUITE(TCdcAggregatorServiceTest) {
    Y_UNIT_TEST(ReturnsRetryToWriteHeartbeat) {
        TTestBasicRuntime runtime(1, false);
        SetupTabletServices(runtime);

        const auto serviceId = MakeCdcAggregatorServiceId(runtime.GetNodeId(0));
        const auto actorId = runtime.Register(CreateCdcAggregatorService());
        runtime.EnableScheduleForActor(actorId);
        runtime.RegisterService(serviceId, actorId);

        auto request = MakeHolder<TEvCdcAggregatorService::TEvWriteHeartbeat>();
        request->RecipientTabletId = MakeTabletID(false, 1);
        request->SourceDataShardId = MakeTabletID(false, 2);
        request->Heartbeat = NPQ::THeartbeat{
            .Version = TRowVersion(100, 0),
            .Data = "{\"resolved\":[100,0]}",
        };

        const auto edge = runtime.AllocateEdgeActor();
        runtime.Send(new IEventHandle(serviceId, edge, request.Release(), 0, Cookie));

        auto result = runtime.GrabEdgeEvent<TEvCdcAggregatorService::TEvWriteHeartbeatResult>(edge);
        UNIT_ASSERT_C(result->Get()->Status == TEvCdcAggregatorService::TEvWriteHeartbeatResult::EStatus::Retry, result->Get()->ToString());
        UNIT_ASSERT_VALUES_EQUAL(result->Cookie, Cookie);
    }

    Y_UNIT_TEST(RejectsHeartbeatWithoutRecipientTablet) {
        TTestBasicRuntime runtime(1, false);
        SetupTabletServices(runtime);

        const auto serviceId = MakeCdcAggregatorServiceId(runtime.GetNodeId(0));
        const auto actorId = runtime.Register(CreateCdcAggregatorService());
        runtime.EnableScheduleForActor(actorId);
        runtime.RegisterService(serviceId, actorId);

        auto request = MakeHolder<TEvCdcAggregatorService::TEvWriteHeartbeat>();
        request->SourceDataShardId = MakeTabletID(false, 2);
        request->Heartbeat = NPQ::THeartbeat{
            .Version = TRowVersion(100, 0),
            .Data = "{\"resolved\":[100,0]}",
        };

        const auto edge = runtime.AllocateEdgeActor();
        runtime.Send(new IEventHandle(serviceId, edge, request.Release(), 0, Cookie));

        auto result = runtime.GrabEdgeEvent<TEvCdcAggregatorService::TEvWriteHeartbeatResult>(edge);
        UNIT_ASSERT_C(result->Get()->Status == TEvCdcAggregatorService::TEvWriteHeartbeatResult::EStatus::Rejected, result->Get()->ToString());
        UNIT_ASSERT_VALUES_EQUAL(result->Cookie, Cookie);
    }

    Y_UNIT_TEST(IgnoresReplyFromPreviousPipe) {
        for (const auto status : {TProtoResult::SUCCESS, TProtoResult::NOT_READY}) {
            TTestEnv env;
            env.SendHeartbeat(1, 100);
            const auto firstBatch = env.GrabBatch();
            UNIT_ASSERT(firstBatch);

            env.DisconnectPipe();
            const auto retry = env.GrabReply();
            UNIT_ASSERT(retry);
            UNIT_ASSERT(retry->Get()->Status == TReply::EStatus::Retry);

            env.SendHeartbeat(2, 200, Cookie + 1);
            const auto secondBatch = env.GrabBatch();
            UNIT_ASSERT(secondBatch);
            env.ReplyToBatch(firstBatch, status);
            UNIT_ASSERT(!env.GrabReply(TDuration::MilliSeconds(200)));

            env.SendHeartbeat(3, 300, Cookie + 2);
            UNIT_ASSERT(!env.GrabBatch(TDuration::MilliSeconds(200)));

            env.ReplyToBatch(secondBatch);
            const auto written = env.GrabReply();
            UNIT_ASSERT(written);
            UNIT_ASSERT(written->Get()->Status == TReply::EStatus::Written);
            UNIT_ASSERT_VALUES_EQUAL(written->Cookie, Cookie + 1);
            UNIT_ASSERT_VALUES_EQUAL(written->Get()->WrittenVersion, TRowVersion(200, 0));

            const auto thirdBatch = env.GrabBatch();
            UNIT_ASSERT(thirdBatch);
            UNIT_ASSERT_VALUES_EQUAL(thirdBatch->Get()->Record.GetHeartbeats(0).GetSourceDataShardId(), 3);
        }
    }

    Y_UNIT_TEST(IgnoresReplyFromPreviousBatch) {
        TTestEnv env;
        env.SendHeartbeat(1, 100);
        const auto firstBatch = env.GrabBatch();
        UNIT_ASSERT(firstBatch);
        env.ReplyToBatch(firstBatch);
        UNIT_ASSERT(env.GrabReply());

        env.SendHeartbeat(1, 200, Cookie + 1);
        const auto secondBatch = env.GrabBatch();
        UNIT_ASSERT(secondBatch);
        env.ReplyToBatch(firstBatch);
        UNIT_ASSERT(!env.GrabReply(TDuration::MilliSeconds(200)));

        env.ReplyToBatch(secondBatch);
        const auto written = env.GrabReply();
        UNIT_ASSERT(written);
        UNIT_ASSERT(written->Get()->Status == TReply::EStatus::Written);
        UNIT_ASSERT_VALUES_EQUAL(written->Get()->WrittenVersion, TRowVersion(200, 0));
    }

    Y_UNIT_TEST(ClosesIdleConnectionAndReconnects) {
        TTestEnv env;
        env.SendHeartbeat(1, 100);
        const auto firstBatch = env.GrabBatch();
        UNIT_ASSERT(firstBatch);
        const auto firstPipe = env.PipeClient;
        env.ReplyToBatch(firstBatch);
        UNIT_ASSERT(env.GrabReply());

        env.Runtime.SimulateSleep(TDuration::Seconds(61));
        UNIT_ASSERT(!env.Runtime.FindActor(firstPipe));

        env.SendHeartbeat(2, 200);
        const auto secondBatch = env.GrabBatch();
        UNIT_ASSERT(secondBatch);
        UNIT_ASSERT(env.PipeClient != firstPipe);
        env.ReplyToBatch(secondBatch);
        const auto written = env.GrabReply();
        UNIT_ASSERT(written);
        UNIT_ASSERT(written->Get()->Status == TReply::EStatus::Written);
    }

    Y_UNIT_TEST(CountsIdleTimeAfterBatchCompletes) {
        TTestEnv env;
        env.SendHeartbeat(1, 100);
        const auto batch = env.GrabBatch();
        UNIT_ASSERT(batch);
        const auto pipe = env.PipeClient;

        env.Runtime.SimulateSleep(TDuration::Seconds(20));
        UNIT_ASSERT(env.Runtime.FindActor(pipe));
        env.ReplyToBatch(batch);
        UNIT_ASSERT(env.GrabReply());

        env.Runtime.SimulateSleep(TDuration::Seconds(45));
        UNIT_ASSERT(env.Runtime.FindActor(pipe));
        env.Runtime.SimulateSleep(TDuration::Seconds(16));
        UNIT_ASSERT(!env.Runtime.FindActor(pipe));
    }

    Y_UNIT_TEST(RetriesUnansweredBatchAndIgnoresLateReply) {
        TTestEnv env;
        env.SendHeartbeat(1, 100);
        const auto firstBatch = env.GrabBatch();
        UNIT_ASSERT(firstBatch);
        const auto firstPipe = env.PipeClient;

        env.SendHeartbeat(2, 200, Cookie + 1);
        env.Runtime.SimulateSleep(TDuration::Seconds(31));
        TSet<ui64> retryCookies{Cookie, Cookie + 1};
        while (!retryCookies.empty()) {
            const auto retry = env.GrabReply();
            UNIT_ASSERT(retry);
            UNIT_ASSERT(retry->Get()->Status == TReply::EStatus::Retry);
            UNIT_ASSERT_VALUES_EQUAL(retryCookies.erase(retry->Cookie), 1);
        }
        UNIT_ASSERT(!env.Runtime.FindActor(firstPipe));

        env.SendHeartbeat(3, 300, Cookie + 2);
        const auto secondBatch = env.GrabBatch();
        UNIT_ASSERT(secondBatch);
        env.ReplyToBatch(firstBatch);
        UNIT_ASSERT(!env.GrabReply(TDuration::MilliSeconds(200)));

        env.ReplyToBatch(secondBatch);
        const auto written = env.GrabReply();
        UNIT_ASSERT(written);
        UNIT_ASSERT(written->Get()->Status == TReply::EStatus::Written);
        UNIT_ASSERT_VALUES_EQUAL(written->Cookie, Cookie + 2);
    }
}

} // namespace NKikimr::NCdcAggregator
