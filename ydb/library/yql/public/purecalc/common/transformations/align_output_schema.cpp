#include "align_output_schema.h"

#include <ydb/library/yql/public/purecalc/common/names.h>
#include <ydb/library/yql/public/purecalc/common/type_from_schema.h>
#include <ydb/library/yql/public/purecalc/common/transformations/utils.h>

#include <ydb/library/yql/core/yql_expr_type_annotation.h>

using namespace NYql;
using namespace NYql::NPureCalc;

namespace {
    class TOutputAligner : public TSyncTransformerBase {
    private:
        const TTypeAnnotationNode* OutputStruct_;
        bool AcceptsBlocks_;
        EProcessorMode ProcessorMode_;

    public:
        explicit TOutputAligner(
            const TTypeAnnotationNode* outputStruct,
            bool acceptsBlocks,
            EProcessorMode processorMode
        )
            : OutputStruct_(outputStruct)
            , AcceptsBlocks_(acceptsBlocks)
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

            // XXX: Tweak the obtained expression type, is the spec supports blocks:
            // 1. Remove "_yql_block_length" attribute, since it's for internal usage.
            // 2. Strip block container from the type to store its internal type.
            if (AcceptsBlocks_) {
                Y_ENSURE(actualItemType->GetKind() == ETypeAnnotationKind::Struct);
                actualItemType = UnwrapBlockStruct(actualItemType->Cast<TStructExprType>(), ctx);
                if (ProcessorMode_ == EProcessorMode::PullList) {
                    actualType = ctx.MakeType<TListExprType>(actualItemType);
                } else {
                    actualType = ctx.MakeType<TStreamExprType>(actualItemType);
                }
            }

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
                    Y_ENSURE(ProcessorMode_ != EProcessorMode::PullList,
                             "processor mode mismatches the actual container type");
                    return actualType->Cast<TStreamExprType>()->GetItemType();
                case ETypeAnnotationKind::List:
                    Y_ENSURE(ProcessorMode_ == EProcessorMode::PullList,
                             "processor mode mismatches the actual container type");
                    return actualType->Cast<TListExprType>()->GetItemType();
                default:
                    Y_ABORT("unexpected return type");
            }
        }
    };
}

TAutoPtr<IGraphTransformer> NYql::NPureCalc::MakeOutputAligner(
    const TTypeAnnotationNode* outputStruct,
    bool acceptsBlocks,
    EProcessorMode processorMode
) {
    return new TOutputAligner(outputStruct, acceptsBlocks, processorMode);
}
