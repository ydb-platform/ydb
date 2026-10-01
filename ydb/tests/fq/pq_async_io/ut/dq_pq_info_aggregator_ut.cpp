#include <ydb/library/testlib/common/test_with_actor_system.h>
#include <ydb/library/yql/providers/pq/async_io/dq_pq_info_aggregation_actor.h>
#include <ydb/library/yql/providers/pq/common/events.h>

#include <library/cpp/protobuf/interop/cast.h>

namespace NYql::NDq {

using namespace NActors;
using namespace NTestUtils;

namespace {

class TPqComputeActorTestFixture : public TTestWithActorSystemFixture {
    using TBase = TTestWithActorSystemFixture;

    struct TResponse {
        i64 Scalar;
        ui64 SeqNo;
    };

public:
    using TBase::TBase;

    void SetUp(NUnitTest::TTestContext& ctx) override {
        Settings.LogSettings.AddLogPriority(NKikimrServices::EServiceKikimr::KQP_COMPUTE, NLog::PRI_TRACE);
        TBase::SetUp(ctx);

        InfoAggregationActorId = Runtime.Register(CreateDqPqInfoAggregationActor("test_tx_id"));
    }

protected:
    void SendValue(const TActorId& clientId, const NPq::NProto::TEvDqPqUpdateCounterValue& value) {
        Runtime.Send(InfoAggregationActorId, clientId, new TPqInfoAggregationActorEvents::TEvUpdateCounter(value));
    }

    NPq::NProto::TEvDqPqUpdateCounterValue MakeSumValue(const TString& counterId, ui64 seqNo, i64 sum,
        TDuration reportPeriod = TDuration::Days(1), i64 deltaThreshold = 0)
    {
        NPq::NProto::TEvDqPqUpdateCounterValue value;
        *value.MutableSettings()->MutableReportPeriod() = NProtoInterop::CastToProto(reportPeriod);
        value.MutableSettings()->SetScalarAggDeltaThreshold(deltaThreshold);
        value.SetCounterId(counterId);
        value.SetSeqNo(seqNo);
        value.SetAggSum(sum);
        return value;
    }

    NPq::NProto::TEvDqPqUpdateCounterValue MakeMinValue(const TString& counterId, ui64 seqNo, i64 minVal,
        TDuration reportPeriod = TDuration::Days(1), i64 deltaThreshold = 0)
    {
        NPq::NProto::TEvDqPqUpdateCounterValue value;
        *value.MutableSettings()->MutableReportPeriod() = NProtoInterop::CastToProto(reportPeriod);
        value.MutableSettings()->SetScalarAggDeltaThreshold(deltaThreshold);
        value.SetCounterId(counterId);
        value.SetSeqNo(seqNo);
        value.SetAggMin(minVal);
        return value;
    }

    NPq::NProto::TEvDqPqUpdateCounterValue MakeActionNotSet(const TString& counterId) {
        NPq::NProto::TEvDqPqUpdateCounterValue value;
        value.SetCounterId(counterId);
        return value;
    }

    void CheckResponse(const TActorId& clientId, std::unordered_map<TString, TResponse>&& responses, TDuration timeout = TDuration::Seconds(1)) {
        const auto timeoutAt = TInstant::Now() + timeout;
        while (!responses.empty() && TInstant::Now() < timeoutAt) {
            const auto responseEvent = Runtime.GrabEdgeEvent<TPqInfoAggregationActorEvents::TEvOnAggregateUpdated>(clientId);
            UNIT_ASSERT(responseEvent);

            const auto& record = responseEvent->Get()->Record;
            if (const auto it = responses.find(record.GetCounterId()); it != responses.end()) {
                if (record.GetScalar() == it->second.Scalar && record.GetSeqNo() == it->second.SeqNo) {
                    responses.erase(it);
                }
            }
        }
        UNIT_ASSERT_C(responses.empty(), "Expected responses are not received, remaining: " << responses.size());
    }

    void CheckResponse(const TActorId& clientId, const TString& counterId, i64 scalar, ui64 seqNo) {
        CheckResponse(clientId, {{counterId, {scalar, seqNo}}});
    }

    TActorId InfoAggregationActorId;
};

} // anonymous namespace

Y_UNIT_TEST_SUITE(TDqPqInfoAggregatorTest) {
    Y_UNIT_TEST_F(TestSumAggregationBasic, TPqComputeActorTestFixture) {
        const auto firstClientId = Runtime.AllocateEdgeActor();
        const auto secondClientId = Runtime.AllocateEdgeActor();
        const TString counterId("test_counter_id");

        SendValue(firstClientId, MakeSumValue(counterId, 1, 100));
        CheckResponse(firstClientId, counterId, 100, 1);

        SendValue(firstClientId, MakeSumValue(counterId, 2, 42));
        CheckResponse(firstClientId, counterId, 42, 2);

        SendValue(secondClientId, MakeSumValue(counterId, 2, 42));
        CheckResponse(secondClientId, counterId, 84, 3);
        CheckResponse(firstClientId, counterId, 84, 3);
    }

    Y_UNIT_TEST_F(TestMinAggregationBasic, TPqComputeActorTestFixture) {
        const auto firstClientId = Runtime.AllocateEdgeActor();
        const auto secondClientId = Runtime.AllocateEdgeActor();
        const TString counterId("min_counter");

        SendValue(firstClientId, MakeMinValue(counterId, 1, 20));
        CheckResponse(firstClientId, counterId, 20, 1);

        SendValue(secondClientId, MakeMinValue(counterId, 1, 10));
        CheckResponse(secondClientId, counterId, 10, 2);
        CheckResponse(firstClientId, counterId, 10, 2);

        SendValue(firstClientId, MakeMinValue(counterId, 2, 5));
        CheckResponse(firstClientId, counterId, 5, 3);
        CheckResponse(secondClientId, counterId, 5, 3);

        SendValue(firstClientId, MakeActionNotSet(counterId));
        CheckResponse(secondClientId, counterId, 10, 4);
    }

    Y_UNIT_TEST_F(TestSumRemoveSenderByActionNotSet, TPqComputeActorTestFixture) {
        const auto firstClientId = Runtime.AllocateEdgeActor();
        const auto secondClientId = Runtime.AllocateEdgeActor();
        const TString counterId("sum_counter");

        SendValue(firstClientId, MakeSumValue(counterId, 1, 100));
        CheckResponse(firstClientId, counterId, 100, 1);
        SendValue(secondClientId, MakeSumValue(counterId, 1, 50));
        CheckResponse(secondClientId, counterId, 150, 2);
        CheckResponse(firstClientId, counterId, 150, 2);

        SendValue(firstClientId, MakeActionNotSet(counterId));
        CheckResponse(secondClientId, counterId, 50, 3);
    }

    Y_UNIT_TEST_F(TestMinRemoveSenderByActionNotSet, TPqComputeActorTestFixture) {
        const auto firstClientId = Runtime.AllocateEdgeActor();
        const auto secondClientId = Runtime.AllocateEdgeActor();
        const TString counterId("min_counter");

        SendValue(firstClientId, MakeMinValue(counterId, 1, 5));
        CheckResponse(firstClientId, counterId, 5, 1);
        SendValue(secondClientId, MakeMinValue(counterId, 1, 30));
        CheckResponse(secondClientId, counterId, 5, 2);
        CheckResponse(firstClientId, counterId, 5, 2);

        SendValue(firstClientId, MakeActionNotSet(counterId));
        CheckResponse(secondClientId, counterId, 30, 3);
    }

    Y_UNIT_TEST_F(TestMultipleCountersDifferentAggTypes, TPqComputeActorTestFixture) {
        const auto clientId = Runtime.AllocateEdgeActor();
        const TString sumCounterId("sum_ctr");
        const TString minCounterId("min_ctr");

        SendValue(clientId, MakeSumValue(sumCounterId, 1, 100));
        SendValue(clientId, MakeMinValue(minCounterId, 1, 50));
        CheckResponse(clientId, {{sumCounterId, {100, 1}}, {minCounterId, {50, 1}}});

        SendValue(clientId, MakeSumValue(sumCounterId, 2, 25));
        SendValue(clientId, MakeMinValue(minCounterId, 2, 30));
        CheckResponse(clientId, {{sumCounterId, {25, 2}}, {minCounterId, {30, 2}}});
    }

    Y_UNIT_TEST_F(TestMultipleCountersDifferentAggTypesTwoClients, TPqComputeActorTestFixture) {
        const auto firstClientId = Runtime.AllocateEdgeActor();
        const auto secondClientId = Runtime.AllocateEdgeActor();
        const TString sumCounterId("sum_ctr");
        const TString minCounterId("min_ctr");

        SendValue(firstClientId, MakeSumValue(sumCounterId, 1, 100));
        CheckResponse(firstClientId, sumCounterId, 100, 1);
        SendValue(secondClientId, MakeMinValue(minCounterId, 1, 50));
        CheckResponse(secondClientId, minCounterId, 50, 1);

        SendValue(firstClientId, MakeSumValue(sumCounterId, 2, 40));
        CheckResponse(firstClientId, sumCounterId, 40, 2);
        SendValue(secondClientId, MakeMinValue(minCounterId, 2, 10));
        CheckResponse(secondClientId, minCounterId, 10, 2);
    }

    Y_UNIT_TEST_F(TestSeqNoOrdering, TPqComputeActorTestFixture) {
        const auto clientId = Runtime.AllocateEdgeActor();
        const TString counterId("seq_counter");

        SendValue(clientId, MakeSumValue(counterId, 2, 200));
        CheckResponse(clientId, counterId, 200, 1);

        SendValue(clientId, MakeSumValue(counterId, 1, 100));
        SendValue(clientId, MakeSumValue(counterId, 3, 300));
        CheckResponse(clientId, counterId, 300, 2);
    }

    Y_UNIT_TEST_F(TestDeltaThreshold, TPqComputeActorTestFixture) {
        const auto clientId = Runtime.AllocateEdgeActor();
        const TString counterId("delta_counter");
        const i64 deltaThreshold = 10;

        SendValue(clientId, MakeSumValue(counterId, 1, 100, TDuration::Days(1), deltaThreshold));
        CheckResponse(clientId, counterId, 100, 1);

        SendValue(clientId, MakeSumValue(counterId, 2, 116, TDuration::Days(1), deltaThreshold));
        CheckResponse(clientId, counterId, 116, 2);
    }

    Y_UNIT_TEST_F(TestPeriodicSend, TPqComputeActorTestFixture) {
        const auto clientId = Runtime.AllocateEdgeActor();
        const TString counterId("periodic_counter");
        const TDuration reportPeriod = TDuration::MilliSeconds(150);

        SendValue(clientId, MakeSumValue(counterId, 1, 1, reportPeriod));
        CheckResponse(clientId, counterId, 1, 1);

        Sleep(reportPeriod + TDuration::MilliSeconds(50));
        CheckResponse(clientId, counterId, 1, 2);
    }

    Y_UNIT_TEST_F(TestRemoveSenderByUndelivered, TPqComputeActorTestFixture) {
        const auto liveClientId = Runtime.AllocateEdgeActor();
        const TString counterId("undelivered_counter");
        const TActorId nonExistentSender(1, 0, 0, 0);

        SendValue(liveClientId, MakeSumValue(counterId, 1, 10));
        CheckResponse(liveClientId, counterId, 10, 1);

        Runtime.Send(InfoAggregationActorId, nonExistentSender, new TPqInfoAggregationActorEvents::TEvUpdateCounter(MakeSumValue(counterId, 1, 5)));
        Runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));

        Runtime.GrabEdgeEvent<TPqInfoAggregationActorEvents::TEvOnAggregateUpdated>(liveClientId);
        Runtime.GrabEdgeEvent<TPqInfoAggregationActorEvents::TEvOnAggregateUpdated>(liveClientId);

        SendValue(liveClientId, MakeSumValue(counterId, 2, 20));
        CheckResponse(liveClientId, counterId, 20, 4);
    }
}

} // namespace NYql::NDq
