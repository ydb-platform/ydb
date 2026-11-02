#include <ydb/core/kqp/tracing/kqp_compile_trace_transformer.h>
#include <ydb/core/kqp/tracing/kqp_compile_trace.h>
#include <ydb/core/kqp/tracing/test_util/kqp_trace_test_helpers.h>
#include <ydb/library/actors/testlib/test_runtime.h>

namespace NKikimr::NKqp {

using namespace NYql;
using namespace NTest;

Y_UNIT_TEST_SUITE(TKqpCompileTraceTransformer) {
    Y_UNIT_TEST(PipelineReuseAndRewind) {
        NActors::TTestActorRuntimeBase runtime;
        runtime.Initialize();
        auto* uploader = new NWilson::TFakeWilsonUploader();
        runtime.RegisterService(NWilson::MakeWilsonUploaderId(), runtime.Register(uploader));
        TCompileTrace::TPtr trace;
        const auto getTrace = [&] { return trace.Get(); };
        const int group = 0;
        auto begin = CreateCompileTraceBegin(getTrace, TCompileTraceLevels::Phase, "Region", &group);
        auto end = CreateCompileTraceEnd(getTrace, "Region", &group);
        using TStatus = IGraphTransformer::TStatus;
        TStatus status = TStatus::Ok;
        auto inner = CreateFunctorTransformer([&](TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext&) {
            output = input;
            return status;
        });
        auto wrapped = CreateCompileTraceTransformer(TAutoPtr<IGraphTransformer>(inner.Release()), getTrace,
            TCompileTraceLevels::Step, "Work");
        TExprContext ctx;
        auto input = ctx.NewAtom(TPositionHandle(), "input");
        TExprNode::TPtr output;
        TString previousTrace;
        for (bool enabled : {false, true, false, true}) {
            trace = enabled ? MakeIntrusive<TCompileTrace>(NWilson::TTraceId::NewTraceId(15, 4095),
                runtime.GetActorSystem(0)) : nullptr;
            for (const TStatus result : {TStatus(TStatus::Ok), TStatus(TStatus::Error), TStatus(TStatus::Repeat), TStatus(TStatus::Error, true)}) {
                status = result;
                wrapped->Rewind();
                begin->Transform(input, output, ctx);
                UNIT_ASSERT_VALUES_EQUAL(wrapped->Transform(input, output, ctx).Level, status.Level);
                wrapped->Rewind();
                wrapped->Rewind();
                end->Transform(input, output, ctx);
                runtime.SimulateSleep(TDuration::MilliSeconds(1));
                UNIT_ASSERT_VALUES_EQUAL(uploader->Spans.size(), enabled ? 2 : 0);
                if (enabled) {
                    AssertDescendant(*uploader, "Work", "Region");
                    AssertStatus(*uploader, "Work", status == TStatus::Ok
                        ? NWilson::NTraceProto::Status::STATUS_CODE_OK : NWilson::NTraceProto::Status::STATUS_CODE_ERROR);
                    UNIT_ASSERT(FindSpan(*uploader, "Work")->trace_id() != previousTrace);
                }
                ClearUploader(*uploader);
            }
            if (trace) {
                const auto id = trace->GetCurrentParent();
                previousTrace = TString(static_cast<const char*>(id.GetTraceIdPtr()), id.GetTraceIdSize());
            }
        }
    }

    Y_UNIT_TEST(ExceptionsEndSpans) {
        class TThrowingTransformer : public TGraphTransformerBase {
        public:
            bool Async = false;
            TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext&) final {
                output = input;
                if (Async) {
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
        auto trace = MakeIntrusive<TCompileTrace>(NWilson::TTraceId::NewTraceId(15, 4095), runtime.GetActorSystem(0));
        const auto parent = trace->GetCurrentParent();
        auto inner = MakeHolder<TThrowingTransformer>();
        auto* throwing = inner.Get();
        auto transformer = CreateCompileTraceTransformer(TAutoPtr<IGraphTransformer>(inner.Release()),
            [trace] { return trace.Get(); }, TCompileTraceLevels::Phase, "Failing optimizer");
        TExprContext ctx;
        auto input = ctx.NewAtom(TPositionHandle(), "input");
        TExprNode::TPtr output;
        for (bool async : {false, true}) {
            throwing->Async = async;
            if (async) {
                UNIT_ASSERT_VALUES_EQUAL(transformer->Transform(input, output, ctx).Level, IGraphTransformer::TStatus::Async);
                transformer->GetAsyncFuture(*input).GetValueSync();
                UNIT_ASSERT_EXCEPTION(transformer->ApplyAsyncChanges(input, output, ctx), yexception);
            } else {
                UNIT_ASSERT_EXCEPTION(transformer->Transform(input, output, ctx), yexception);
            }
            UNIT_ASSERT(trace->GetCurrentParent() == parent);
            transformer->Rewind();
        }
        trace->Finish(false);
        transformer.Destroy();
        runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(uploader->Spans.size(), 2);
        for (const auto& span : uploader->Spans) {
            UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(span.status().code()),
                static_cast<int>(NWilson::NTraceProto::Status::STATUS_CODE_ERROR));
        }
    }
}

} // namespace NKikimr::NKqp
