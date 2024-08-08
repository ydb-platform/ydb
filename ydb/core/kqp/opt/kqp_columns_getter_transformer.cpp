#include "kqp_columns_getter_transformer.h"

#include <ydb/library/yql/core/yql_expr_optimize.h>

namespace NKikimr::NKqp {

void TKqpColumnsGetterTransformer::PropagateTableToLambdaArgument(const TExprNode::TPtr& input) {
    if (input->ChildrenSize() < 2) {
        return;
    }

    auto callableInput = input->ChildRef(0);

   
    for (size_t i = 1; i < input->ChildrenSize(); ++i) {
        auto maybeLambda = TExprBase(input->ChildRef(i));
        if (!maybeLambda.Maybe<TCoLambda>()) {
            continue;
        }

        auto lambda = maybeLambda.Cast<TCoLambda>();
        if (!lambda.Args().Size()){
            continue;
        }

        if (callableInput->IsList()){
            for (size_t j = 0; j < callableInput->ChildrenSize(); ++j){
                TableByExprNode[lambda.Args().Arg(j).Ptr()] = TableByExprNode[callableInput->Child(j)];  
            }
        } else {
            TableByExprNode[lambda.Args().Arg(0).Ptr()] = TableByExprNode[callableInput.Get()];
        }
    }
}

IGraphTransformer::TStatus TKqpColumnsGetterTransformer::DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) {
    Y_UNUSED(ctx);
    
    output = input;
    
    // if (Config->CostBasedOptimizationLevel.Get().GetOrElse(TDqSettings::TDefault::CostBasedOptimizationLevel) == 0) {
    //     return IGraphTransformer::TStatus::Ok;
    // }

    VisitExprLambdasLast(
        input, 
        [&](const TExprNode::TPtr& input) {
            BeforeLambdas(input) || BeforeLambdasUnmatched(input);

            if (input->IsCallable()) {
                PropagateTableToLambdaArgument(input);
            }

            return true;
        },
        [&](const TExprNode::TPtr& input) {
            return AfterLambdas(input) || AfterLambdasUnmatched(input);
        }
    );
    
    return IGraphTransformer::TStatus::Ok;
}

bool TKqpColumnsGetterTransformer::BeforeLambdas(const TExprNode::TPtr& input) {
    bool matched = true;
    
    if (TKqpTable::Match(input.Get())) {
        TableByExprNode[input.Get()] = input.Get();
    } else {
        matched = false;
    }

    return matched;
}

bool TKqpColumnsGetterTransformer::BeforeLambdasUnmatched(const TExprNode::TPtr& input) {
    for (const auto& node: input->Children()) {
        if (TableByExprNode.contains(node)) {
            TableByExprNode[input.Get()] = TableByExprNode[node];
            return true;
        }
    }

    return true;
}

bool TKqpColumnsGetterTransformer::AfterLambdas(const TExprNode::TPtr& input) {
    bool matched = true;

    if (
        TCoFilterBase::Match(input.Get()) || 
        TCoFlatMapBase::Match(input.Get()) && IsPredicateFlatMap(TExprBase(input).Cast<TCoFlatMapBase>().Lambda().Body().Ref())
    ) {
        VisitExpr(
            input->Child(1), 
            [this](const TExprNode::TPtr& input) -> bool {
                if (TCoMember::Match(input.Get())) {
                    auto member = TExprBase(input).Cast<TCoMember>();

                    if (!TableByExprNode.contains(input.Get()) || TableByExprNode[input.Get()] == nullptr) {
                        return true;
                    }
                    auto table = TExprBase(TableByExprNode[input.Get()]).Cast<TKqpTable>().Path().StringValue();
                    auto column = member.Name().StringValue();
                    size_t pointPos = column.find('.'); // table.column
                    if (pointPos != TString::npos) {
                        column = column.substr(pointPos + 1);
                    }

                    ColumnsByTableName[table].insert(std::move(column));
                }

                return true;
            }
        );
    } else {
        matched = false;
    }

    return matched;
}

bool TKqpColumnsGetterTransformer::AfterLambdasUnmatched(const TExprNode::TPtr& input) {
    if (TableByExprNode.contains(input.Get())) {
        return true;
    }

    for (const auto& node: input->Children()) {
        if (TableByExprNode.contains(node)) {
            TableByExprNode[input.Get()] = TableByExprNode[node];
            return true;
        }
    }

    return true;
}

TAutoPtr<IGraphTransformer> CreateKqpColumnsGetterTransformer(
    const TKikimrConfiguration::TPtr& config
) {
    return THolder<IGraphTransformer>(new TKqpColumnsGetterTransformer(config));
}

} // end of NKikimr::NKqp
