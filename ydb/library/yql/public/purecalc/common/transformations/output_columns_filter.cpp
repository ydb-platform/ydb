#include "output_columns_filter.h"

#include <ydb/library/yql/core/yql_expr_type_annotation.h>

using namespace NYql;
using namespace NYql::NPureCalc;

namespace {
    class TOutputColumnsFilter: public TSyncTransformerBase {
    private:
        TMaybe<THashSet<TString>> Filter_;
        bool Fired_;

    public:
        explicit TOutputColumnsFilter(TMaybe<THashSet<TString>> filter)
            : Filter_(std::move(filter))
            , Fired_(false)
        {
        }

    public:
        void Rewind() override {
            Fired_ = false;
        }

        TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
            output = input;

            if (Fired_ || Filter_.Empty()) {
                return IGraphTransformer::TStatus::Ok;
            }

            const TTypeAnnotationNode* returnType = output->GetTypeAnn();
            const TTypeAnnotationNode* returnItemType = nullptr;
            switch (returnType->GetKind()) {
                case ETypeAnnotationKind::Stream:
                    returnItemType = returnType->Cast<TStreamExprType>()->GetItemType();
                    break;
                case ETypeAnnotationKind::List:
                    returnItemType = returnType->Cast<TListExprType>()->GetItemType();
                    break;
                default:
                    Y_ABORT("unexpected return type");
            }

            if (returnItemType->GetKind() != ETypeAnnotationKind::Struct) {
                ctx.AddError(TIssue(ctx.GetPosition(output->Pos()), "columns filter only supported for single-output programs"));
            }

            const auto* returnItemStruct = returnItemType->Cast<TStructExprType>();

            auto arg = ctx.NewArgument(TPositionHandle(), "row");
            TExprNode::TListType asStructItems;
            for (const auto& x : returnItemStruct->GetItems()) {
                TExprNode::TPtr value;
                if (Filter_->contains(x->GetName())) {
                    value = ctx.Builder({})
                               .Callable("Member")
                               .Add(0, arg)
                               .Atom(1, x->GetName())
                               .Seal()
                               .Build();
                } else {
                    auto type = x->GetItemType();
                    value = ctx.Builder({})
                               .Callable(type->GetKind() == ETypeAnnotationKind::Optional ? "Nothing" : "Default")
                               .Add(0, ExpandType({}, *type, ctx))
                               .Seal()
                               .Build();
                }

                auto item = ctx.Builder({})
                               .List()
                               .Atom(0, x->GetName())
                               .Add(1, value)
                               .Seal()
                               .Build();

                asStructItems.push_back(item);
            }

            auto body = ctx.NewCallable(TPositionHandle(), "AsStruct", std::move(asStructItems));
            auto lambda = ctx.NewLambda(TPositionHandle(), ctx.NewArguments(TPositionHandle(), {arg}), std::move(body));
            output = ctx.Builder(TPositionHandle())
                      .Callable("Map")
                      .Add(0, output)
                      .Add(1, lambda)
                      .Seal()
                      .Build();

            Fired_ = true;

            return IGraphTransformer::TStatus(IGraphTransformer::TStatus::Repeat, true);
        }
    };
}

TAutoPtr<IGraphTransformer> NYql::NPureCalc::MakeOutputColumnsFilter(const TMaybe<THashSet<TString>>& columns) {
    return new TOutputColumnsFilter(columns);
}
