#include "root_to_blocks.h"

#include <ydb/library/yql/public/purecalc/common/transformations/utils.h>

#include <ydb/library/yql/core/yql_expr_type_annotation.h>

using namespace NYql;
using namespace NYql::NPureCalc;

namespace {

class TRootToBlocks: public TSyncTransformerBase {
private:
    bool AcceptsBlocks_;
    EProcessorMode ProcessorMode_;
    bool Wrapped_;

public:
    explicit TRootToBlocks(bool acceptsBlocks, EProcessorMode processorMode)
        : AcceptsBlocks_(acceptsBlocks)
        , ProcessorMode_(processorMode)
        , Wrapped_(false)
    {
    }

public:
    void Rewind() override {
        Wrapped_ = false;
    }

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        if (Wrapped_ || !AcceptsBlocks_) {
            return IGraphTransformer::TStatus::Ok;
        }

        const TTypeAnnotationNode* returnItemType;
        const TTypeAnnotationNode* returnType = input->GetTypeAnn();
        if (ProcessorMode_ == EProcessorMode::PullList) {
            Y_ENSURE(returnType->GetKind() == ETypeAnnotationKind::List);
            returnItemType = returnType->Cast<TListExprType>()->GetItemType();
        } else {
            Y_ENSURE(returnType->GetKind() == ETypeAnnotationKind::Stream);
            returnItemType = returnType->Cast<TStreamExprType>()->GetItemType();
        }

        Y_ENSURE(returnItemType->GetKind() == ETypeAnnotationKind::Struct);
        const TStructExprType* structType = returnItemType->Cast<TStructExprType>();
        const auto blocksLambda = NodeToBlocks(input->Pos(), structType, ctx);
        bool wrapLMap = ProcessorMode_ == EProcessorMode::PullList;
        output = ApplyToIterable(input->Pos(), input, blocksLambda, wrapLMap, ctx);

        Wrapped_ = true;

        return IGraphTransformer::TStatus(IGraphTransformer::TStatus::Repeat, true);
    }
};

} // namespace

TAutoPtr<IGraphTransformer> NYql::NPureCalc::MakeRootToBlocks(
    bool acceptsBlocks,
    EProcessorMode processorMode
) {
    return new TRootToBlocks(acceptsBlocks, processorMode);
}
