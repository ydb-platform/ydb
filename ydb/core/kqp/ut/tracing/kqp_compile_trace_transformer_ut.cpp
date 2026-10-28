#include <ydb/core/kqp/host/kqp_compile_trace_transformer.h>
#include <ydb/core/kqp/tracing/kqp_compile_trace.h>
#include <ydb/core/kqp/tracing/test_util/kqp_trace_test_helpers.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/testlib/helpers.h>

#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_function_registry.h>

#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/time_provider/time_provider.h>

namespace NKikimr::NKqp {

using namespace NYql;
using namespace NTest;

Y_UNIT_TEST_SUITE(TKqpCompileTraceTransformer) {
    Y_UNIT_TEST_TWIN(ExceptionsEndSpans, Async) {
        class TThrowingTransformer : public TGraphTransformerBase {
        public:
            TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext&) final {
                output = input;
                if constexpr (Async) {
                    return TStatus::Async;
                }
                ythrow yexception() << "optimizer failure";
            }

            NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode&) final {
                return NThreading::MakeFuture();
            }

            TStatus DoApplyAsyncChanges(TExprNode::TPtr, TExprNode::TPtr&, TExprContext&) final {
                ythrow yexception() << "optimizer failure after async wait";
            }

            void Rewind() final {}
        };

        NActors::TTestActorRuntimeBase runtime;
        runtime.Initialize();
        auto* uploader = new NWilson::TFakeWilsonUploader();
        runtime.RegisterService(NWilson::MakeWilsonUploaderId(), runtime.Register(uploader));
        const auto registry = NMiniKQL::CreateFunctionRegistry(NMiniKQL::CreateBuiltinRegistry());
        auto queryCtx = MakeIntrusive<TKikimrQueryContext>(
            registry.Get(), CreateDefaultTimeProvider(), CreateDefaultRandomProvider());
        queryCtx->CompileTrace = MakeIntrusive<TCompileTrace>(
            NWilson::TTraceId::NewTraceId(15, 4095), runtime.GetActorSystem(0));
        const auto parent = queryCtx->CompileTrace->GetCurrentParent();
        auto transformer = CreateCompileTraceTransformer(TAutoPtr<IGraphTransformer>(new TThrowingTransformer()),
            queryCtx, TCompileTraceLevels::Phase, "Failing optimizer");
        TExprContext ctx;
        auto input = ctx.NewAtom(TPositionHandle(), "input");
        TExprNode::TPtr output;
        if constexpr (Async) {
            UNIT_ASSERT_VALUES_EQUAL(transformer->Transform(input, output, ctx).Level, IGraphTransformer::TStatus::Async);
            transformer->GetAsyncFuture(*input).GetValueSync();
            UNIT_ASSERT_EXCEPTION(transformer->ApplyAsyncChanges(input, output, ctx), yexception);
        } else {
            UNIT_ASSERT_EXCEPTION(transformer->Transform(input, output, ctx), yexception);
        }
        queryCtx->CompileTrace->Finish(false);
        runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT(queryCtx->CompileTrace->GetCurrentParent() == parent);
        const auto spanCount = uploader->Spans.size();
        const auto* failing = FindSpan(*uploader, "Failing optimizer");
        UNIT_ASSERT(failing);
        const auto& span = *failing;
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(span.status().code()),
            static_cast<int>(NWilson::NTraceProto::Status::STATUS_CODE_ERROR));
        transformer->Rewind();
        transformer.Destroy();
        runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(uploader->Spans.size(), spanCount);
    }

    Y_UNIT_TEST(RewindCompletedFailedAndActiveSpans) {
        NActors::TTestActorRuntimeBase runtime;
        runtime.Initialize();
        auto* uploader = new NWilson::TFakeWilsonUploader();
        runtime.RegisterService(NWilson::MakeWilsonUploaderId(), runtime.Register(uploader));

        const auto registry = NMiniKQL::CreateFunctionRegistry(NMiniKQL::CreateBuiltinRegistry());
        auto queryCtx = MakeIntrusive<TKikimrQueryContext>(
            registry.Get(), CreateDefaultTimeProvider(), CreateDefaultRandomProvider());
        queryCtx->CompileTrace = MakeIntrusive<TCompileTrace>(
            NWilson::TTraceId::NewTraceId(15, 4095), runtime.GetActorSystem(0));

        using TStatus = IGraphTransformer::TStatus;
        TStatus status = TStatus::Ok;
        auto inner = CreateFunctorTransformer([&](TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext&) {
            output = input;
            return status;
        });
        auto transformer = CreateCompileTraceTransformer(TAutoPtr<IGraphTransformer>(inner.Release()),
            queryCtx, TCompileTraceLevels::Phase, "Test phase");
        TExprContext ctx;
        auto input = ctx.NewAtom(TPositionHandle(), "input");
        TExprNode::TPtr output;

        transformer->Rewind(); // No span has started yet.
        for (const TStatus nextStatus : {TStatus(TStatus::Ok), TStatus(TStatus::Error), TStatus(TStatus::Repeat),
                TStatus(TStatus::Ok), TStatus(TStatus::Error, true)}) {
            status = nextStatus;
            UNIT_ASSERT_VALUES_EQUAL(transformer->Transform(input, output, ctx).Level, status.Level);
            transformer->Rewind();
            transformer->Rewind();
            runtime.SimulateSleep(TDuration::MilliSeconds(1));

            UNIT_ASSERT_VALUES_EQUAL(uploader->Spans.size(), 1);
            const auto& span = uploader->Spans.front();
            const auto expectedStatus = status == TStatus::Ok
                ? NWilson::NTraceProto::Status::STATUS_CODE_OK : NWilson::NTraceProto::Status::STATUS_CODE_ERROR;
            UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(span.status().code()), static_cast<int>(expectedStatus));
            ClearUploader(*uploader);
        }
    }
}

} // namespace NKikimr::NKqp
