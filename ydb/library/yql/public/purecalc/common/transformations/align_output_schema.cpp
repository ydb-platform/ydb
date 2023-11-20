#include "align_output_schema.h"

#include <ydb/library/yql/public/purecalc/common/type_from_schema.h>

#include <ydb/library/yql/core/yql_expr_type_annotation.h>

using namespace NYql;
using namespace NYql::NPureCalc;

namespace {
    class TOutputAligner : public TSyncTransformerBase {
    private:
        const TTypeAnnotationNode* OutputStruct_;
        EProcessorMode ProcessorMode_;

    public:
        explicit TOutputAligner(const TTypeAnnotationNode* outputStruct, EProcessorMode processorMode)
            : OutputStruct_(outputStruct)
            , ProcessorMode_(processorMode)
        {
        }

    public:
        TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
            output = input;

            const auto* expectedType = MakeExpectedType(ctx);
            const auto* expectedItemType = MakeExpectedItemType();
            const auto* actualType = MakeActualType(input);
            const auto* actualItemType = MakeActualItemType(input);

            if (!ValidateOutputType(actualItemType, expectedItemType, ctx)) {
                return TStatus::Error;
            }

            if (!expectedType) {
                return TStatus::Ok;
            }

            auto status = TryConvertTo(output, *actualType, *expectedType, ctx);

            if (status.Level == IGraphTransformer::TStatus::Repeat) {
                status = IGraphTransformer::TStatus(IGraphTransformer::TStatus::Repeat, true);
            }

            return status;
        }

        void Rewind() final {
        }

    private:
        const TTypeAnnotationNode* MakeExpectedType(TExprContext& ctx) {
            if (!OutputStruct_) {
                return nullptr;
            }

            switch (ProcessorMode_) {
                case EProcessorMode::PullList:
                    return ctx.MakeType<TListExprType>(OutputStruct_);
                case EProcessorMode::PullStream:
                case EProcessorMode::PushStream:
                    return ctx.MakeType<TStreamExprType>(OutputStruct_);
            }

            Y_ABORT("Unexpected");
        }

        const TTypeAnnotationNode* MakeExpectedItemType() {
            return OutputStruct_;
        }

        const TTypeAnnotationNode* MakeActualType(TExprNode::TPtr& input) {
            return input->GetTypeAnn();
        }

        const TTypeAnnotationNode* MakeActualItemType(TExprNode::TPtr& input) {
            auto actualType = MakeActualType(input);
            switch (actualType->GetKind()) {
                case ETypeAnnotationKind::Stream:
                    return actualType->Cast<TStreamExprType>()->GetItemType();
                case ETypeAnnotationKind::List:
                    return actualType->Cast<TListExprType>()->GetItemType();
                default:
                    Y_ABORT("unexpected return type");
            }
        }
    };
}

TAutoPtr<IGraphTransformer> NYql::NPureCalc::MakeOutputAligner(const TTypeAnnotationNode* outputStruct, EProcessorMode processorMode) {
    return new TOutputAligner(outputStruct, processorMode);
}
