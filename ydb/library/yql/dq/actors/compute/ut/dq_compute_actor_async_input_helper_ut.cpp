
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_log.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_metrics.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_watermarks.h>
#include <library/cpp/testing/unittest/registar.h>

#undef IS_CTX_LOG_PRIORITY_ENABLED
#define IS_CTX_LOG_PRIORITY_ENABLED(actorCtxOrSystem, priority, component, sampleBy) false
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_input_helper.h>

namespace NYql::NDq {

Y_UNIT_TEST_SUITE(TComputeActorAsyncInputHelperTest) {

    struct TDummyDqComputeActorAsyncInput: IDqComputeActorAsyncInput {
            TDummyDqComputeActorAsyncInput() {
                Batch.emplace_back(NUdf::TUnboxedValue{});
                Batch.emplace_back(NUdf::TUnboxedValue{});
            }
            ui64 GetInputIndex() const override {
                return 4;
            }

            const TDqAsyncStats& GetIngressStats() const override{
                static TDqAsyncStats stats;
                return stats;
            }

            i64 GetAsyncInputData(
                    NKikimr::NMiniKQL::TUnboxedValueBatch& batch,
                    TMaybe<TInstant>& watermark,
                    bool& finished,
                    i64 freeSpace) override
            {
                Y_ABORT_IF(Batch.empty());
                batch = Batch;
                Y_UNUSED(watermark);
                Y_UNUSED(finished);
                Y_UNUSED(freeSpace);
                return 2;
            }

            // Checkpointing.
            void SaveState(const NDqProto::TCheckpoint& checkpoint, TSourceState& state) override {
                Y_UNUSED(checkpoint);
                Y_UNUSED(state);
            }
            void CommitState(const NDqProto::TCheckpoint& checkpoint) override {
                Y_UNUSED(checkpoint);
            }
            void LoadState(const TSourceState& state) override {
                Y_UNUSED(state);
            }

            void PassAway() override {}
            NKikimr::NMiniKQL::TUnboxedValueBatch Batch;
        };

    struct TDummyAsyncInputHelper: TComputeActorAsyncInputHelper{
        using TComputeActorAsyncInputHelper::TComputeActorAsyncInputHelper;
        i64 GetFreeSpace() const override{
            return 10;
        }
        void AsyncInputPush(NKikimr::NMiniKQL::TUnboxedValueBatch&& batch, i64 space, bool finished) override{
            batch.clear();
            Y_UNUSED(space);
            Y_UNUSED(finished);
            return;
        }
    };

    Y_UNIT_TEST(PollAsyncInput) {
        NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__,  NKikimr::TAlignedPagePoolCounters(), true, true);
        TDummyDqComputeActorAsyncInput input;
        TDummyAsyncInputHelper helper("MyPrefix", 13, NDqProto::EWatermarksMode::WATERMARKS_MODE_DISABLED);
        helper.AsyncInput = &input;
        TDqComputeActorMetrics metrics{NMonitoring::TDynamicCounterPtr{}};
        TDqComputeActorWatermarks watermarks(NActors::TActorIdentity{NActors::TActorId{}}, TTxId{}, 7);
        auto result = helper.PollAsyncInput(metrics, watermarks, 20);
        UNIT_ASSERT(result && EResumeSource::CAPollAsync == *result);
    }
}

} //namespace NYql::NDq
