#include "dq_opt_stat.h"

#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/yql_cost_function.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>


namespace NYql::NDq {

using namespace NNodes;

namespace {
    /***
     * We maintain a white list of callables that we consider part of constant expressions
     * All other callables will not be evaluated
     */
    THashSet<TString> constantFoldingWhiteList = {
        "Concat", "Just", "Optional", "SafeCast", "AsList",
        "+", "-", "*", "/", "%"};

    THashSet<TString> pgConstantFoldingWhiteList = {
        "PgResolvedOp", "PgResolvedCall", "PgCast", "PgConst", "PgArray", "PgType"};


    TString RemoveAliases(TString attributeName) {
        if (auto idx = attributeName.find_last_of('.'); idx != TString::npos) {
            return attributeName.substr(idx+1);
        }
        return attributeName;
    }

    TString ExtractAlias(TString attributeName) {
        if (auto idx = attributeName.find_last_of('.'); idx != TString::npos) {
            auto substr = attributeName.substr(0, idx);
            if (auto idx2 = substr.find_last_of('.'); idx != TString::npos) {
                substr = substr.substr(idx2+1);
            }
            return substr;
        }
        return TString();
    }

    TVector<TString> InferLabels(std::shared_ptr<TOptimizerStatistics>& stats, TCoAtomList joinColumns) {
        if(stats->Labels) {
            return *stats->Labels;
        }

        if (!joinColumns.Size()) {
            return TVector<TString>();
        }
        
        auto fullColumnName = joinColumns.Item(0).StringValue();
        for (size_t i = 0; i < fullColumnName.size(); i++) {
            if (fullColumnName[i]=='.') {
                fullColumnName = fullColumnName.substr(0, i);
            }
            else if (i == fullColumnName.size() - 1) {
                return TVector<TString>();
            }
        }

        auto res = TVector<TString>();
        res.push_back(fullColumnName);
        return res;
    }

    std::shared_ptr<TOptimizerStatistics> ApplyCardinalityHints(
        std::shared_ptr<TOptimizerStatistics>& inputStats, 
        TVector<TString>& labels, 
        TCardinalityHints hints) {

            if (labels.size() != 1) {
                return inputStats;
            }

            for (auto h : hints.Hints) {
                if (h.JoinLabels.size() == 1 && h.JoinLabels == labels) {
                    auto outputStats = std::make_shared<TOptimizerStatistics>(
                        inputStats->Type, 
                        h.ApplyHint(inputStats->Nrows), 
                        inputStats->Ncols, 
                        inputStats->ByteSize, 
                        inputStats->Cost, 
                        inputStats->KeyColumns,
                        inputStats->ColumnStatistics,
                        inputStats->StorageType);
                    outputStats->Labels = inputStats->Labels;
                    return outputStats;
                }
            }
            return inputStats;
    }

    TVector<TString> UnionLabels(TVector<TString>& leftLabels, TVector<TString>& rightLabels) {
        auto res = TVector<TString>();
        res.insert(res.begin(), leftLabels.begin(), leftLabels.end());
        res.insert(res.end(), rightLabels.begin(), rightLabels.end());
        return res;
    }

    TCardinalityHints::TCardinalityHint* FindCardHint(TVector<TString>& labels, TCardinalityHints& hints) {
        THashSet<TString> labelsSet;
        labelsSet.insert(labels.begin(), labels.end());

        for (auto & h: hints.Hints ) {
            THashSet<TString> hintLabels;
            hintLabels.insert(h.JoinLabels.begin(), h.JoinLabels.end());
            if (labelsSet == hintLabels) {
                return &h;
            }
        }

        return nullptr;
    }

}

bool NeedCalc(NNodes::TExprBase node) {
    auto type = node.Ref().GetTypeAnn();
    if (type->IsSingleton()) {
        return false;
    }

    if (type->GetKind() == ETypeAnnotationKind::Optional) {
        if (node.Maybe<TCoNothing>()) {
            return false;
        }
        if (auto maybeJust = node.Maybe<TCoJust>()) {
            return NeedCalc(maybeJust.Cast().Input());
        }
        return true;
    }

    if (type->GetKind() == ETypeAnnotationKind::Tuple) {
        if (auto maybeTuple = node.Maybe<TExprList>()) {
            return AnyOf(maybeTuple.Cast(), [](const auto& item) { return NeedCalc(item); });
        }
        return true;
    }

    if (type->GetKind() == ETypeAnnotationKind::List) {
        if (node.Maybe<TCoList>()) {
            YQL_ENSURE(node.Ref().ChildrenSize() == 1, "Should be rewritten to AsList");
            return false;
        }
        if (auto maybeAsList = node.Maybe<TCoAsList>()) {
            return AnyOf(maybeAsList.Cast().Args(), [](const auto& item) { return NeedCalc(NNodes::TExprBase(item)); });
        }
        return true;
    }

    YQL_ENSURE(type->GetKind() == ETypeAnnotationKind::Data,
                "Object of type " << *type << " should not be considered for calculation");

    return !node.Maybe<TCoDataCtor>();
}

bool IsConstantExprPg(const TExprNode::TPtr& input) {
    if (input->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Pg) {
        if (input->IsCallable("PgConst")) {
            return true;
        }
    }

    if (TMaybeNode<TCoAtom>(input)) {
        return true;
    }

    if (input->IsCallable(pgConstantFoldingWhiteList) || input->IsList()) {
        for (size_t i = 0; i < input->ChildrenSize(); i++) {
            auto callableInput = input->Child(i);
            if (callableInput->IsLambda() && !IsConstantExprPg(callableInput->Child(1))) {
                return false;
            }
            if (!callableInput->IsCallable("PgType") && !IsConstantExprPg(callableInput)) {
                return false;
            }
        }
        return true;
    }

    return false;
}

/***
 * Check if the expression is a constant expression
 * Its type annotation need to specify that its a data type, and then we check:
 *   - If its a literal, its a constant expression
 *   - If its a callable in the while list and all children are constant expressions, then its a constant expression
 *   - If one of the child is a type expression, it also passes the check
 */
bool IsConstantExpr(const TExprNode::TPtr& input) {
    if (input->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Pg) {
        return IsConstantExprPg(input);
    }

    if (!IsDataOrOptionalOfData(input->GetTypeAnn())) {
        return false;
    }

    if (!NeedCalc(TExprBase(input))) {
        return true;
    }

    else if (input->IsCallable(constantFoldingWhiteList)) {
        for (size_t i = 0; i < input->ChildrenSize(); i++) {
            auto callableInput = input->Child(i);
            if (callableInput->GetTypeAnn()->GetKind() != ETypeAnnotationKind::Type && !IsConstantExpr(callableInput)) {
                return false;
            }
        }
        return true;
    }

    return false;
}

bool IsConstantExprWithParams(const TExprNode::TPtr& input) {
    if (input->IsCallable("Parameter")) {
        return true;
    }

    if (input->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Pg) {
        return IsConstantExprPg(input);
    }

    if (!IsDataOrOptionalOfData(input->GetTypeAnn())) {
        return false;
    }

    if (!NeedCalc(TExprBase(input))) {
        return true;
    }

    else if (input->IsCallable(constantFoldingWhiteList)) {
        for (size_t i = 0; i < input->ChildrenSize(); i++) {
            auto callableInput = input->Child(i);
            if (callableInput->GetTypeAnn()->GetKind() != ETypeAnnotationKind::Type && !IsConstantExprWithParams(callableInput)) {
                return false;
            }
        }
        return true;
    }

    return false;
}


/**
 * Compute statistics for map join
 * FIX: Currently we treat all join the same from the cost perspective, need to refine cost function
 */
void InferStatisticsForMapJoin(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx, const IProviderContext& ctx, TCardinalityHints hints) {
    
    auto inputNode = TExprBase(input);
    auto join = inputNode.Cast<TCoMapJoinCore>();

    auto leftArg = join.LeftInput();
    auto rightArg = join.RightDict();

    auto leftStats = typeCtx->GetStats(leftArg.Raw());
    auto rightStats = typeCtx->GetStats(rightArg.Raw());

    if (!leftStats || !rightStats) {
        return;
    }

    auto leftLabels = InferLabels(leftStats, join.LeftKeysColumnNames());
    auto rightLabels = InferLabels(rightStats, join.RightKeysColumnNames());

    leftStats = ApplyCardinalityHints(leftStats, leftLabels, hints);
    rightStats = ApplyCardinalityHints(rightStats, rightLabels, hints);

    TVector<TJoinColumn> leftJoinKeys;
    TVector<TJoinColumn> rightJoinKeys;

    for (size_t i=0; i<join.LeftKeysColumnNames().Size(); i++) {
        auto alias = ExtractAlias(join.LeftKeysColumnNames().Item(i).StringValue());
        auto attrName = RemoveAliases(join.LeftKeysColumnNames().Item(i).StringValue());
        leftJoinKeys.push_back(TJoinColumn(alias, attrName));
    }
    for (size_t i=0; i<join.RightKeysColumnNames().Size(); i++) {
        auto alias = ExtractAlias(join.RightKeysColumnNames().Item(i).StringValue());
        auto attrName = RemoveAliases(join.RightKeysColumnNames().Item(i).StringValue());
        rightJoinKeys.push_back(TJoinColumn(alias, attrName));
    }

    auto unionOfLabels = UnionLabels(leftLabels, rightLabels);
    auto resStats = std::make_shared<TOptimizerStatistics>(           
        ctx.ComputeJoinStats(
            *leftStats, 
            *rightStats, 
            leftJoinKeys, 
            rightJoinKeys, 
            EJoinAlgoType::MapJoin, 
            ConvertToJoinKind(join.JoinKind().StringValue()),
            FindCardHint(unionOfLabels, hints))
        );
    resStats->Labels = std::make_shared<TVector<TString>>();
    resStats->Labels->insert(resStats->Labels->begin(), unionOfLabels.begin(), unionOfLabels.end());
    typeCtx->SetStats(join.Raw(), resStats);
    YQL_CLOG(TRACE, CoreDq) << "Infer statistics for MapJoin: " << resStats->ToString();
}

/**
 * Compute statistics for grace join
 * FIX: Currently we treat all join the same from the cost perspective, need to refine cost function
 */
void InferStatisticsForGraceJoin(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx, const IProviderContext& ctx, TCardinalityHints hints) {
    auto inputNode = TExprBase(input);
    auto join = inputNode.Cast<TCoGraceJoinCore>();

    auto leftArg = join.LeftInput();
    auto rightArg = join.RightInput();

    auto leftStats = typeCtx->GetStats(leftArg.Raw());
    auto rightStats = typeCtx->GetStats(rightArg.Raw());

    if (!leftStats || !rightStats) {
        return;
    }

    auto leftLabels = InferLabels(leftStats, join.LeftKeysColumnNames());
    auto rightLabels = InferLabels(rightStats, join.RightKeysColumnNames());

    leftStats = ApplyCardinalityHints(leftStats, leftLabels, hints);
    rightStats = ApplyCardinalityHints(rightStats, rightLabels, hints);

    TVector<TJoinColumn> leftJoinKeys;
    TVector<TJoinColumn> rightJoinKeys;

    for (size_t i=0; i<join.LeftKeysColumnNames().Size(); i++) {
        auto alias = ExtractAlias(join.LeftKeysColumnNames().Item(i).StringValue());
        auto attrName = RemoveAliases(join.LeftKeysColumnNames().Item(i).StringValue());
        leftJoinKeys.push_back(TJoinColumn(alias, attrName));
    }
    for (size_t i=0; i<join.RightKeysColumnNames().Size(); i++) {
        auto alias = ExtractAlias(join.RightKeysColumnNames().Item(i).StringValue());
        auto attrName = RemoveAliases(join.RightKeysColumnNames().Item(i).StringValue());
        rightJoinKeys.push_back(TJoinColumn(alias, attrName));
    }

    auto unionOfLabels = UnionLabels(leftLabels, rightLabels);

    auto joinAlgo = EJoinAlgoType::GraceJoin;
    for (size_t i=0; i<join.Flags().Size(); i++) {
        if (join.Flags().Item(i).StringValue() == "Broadcast") {
            joinAlgo = EJoinAlgoType::MapJoin;
            break;
        }
    }

    auto resStats = std::make_shared<TOptimizerStatistics>(
            ctx.ComputeJoinStats(
                *leftStats,
                *rightStats,
                leftJoinKeys,
                rightJoinKeys, 
                joinAlgo,
                ConvertToJoinKind(join.JoinKind().StringValue()),
                FindCardHint(unionOfLabels, hints)
            )
        );

    resStats->Labels = std::make_shared<TVector<TString>>();
    resStats->Labels->insert(resStats->Labels->begin(), unionOfLabels.begin(), unionOfLabels.end());
    typeCtx->SetStats(join.Raw(), resStats);
    YQL_CLOG(TRACE, CoreDq) << "Infer statistics for GraceJoin: " << resStats->ToString();
}

/**
 * Infer statistics for DqSource
 *
 * We just pass up the statistics from the Settings of the DqSource
 */
void InferStatisticsForDqSource(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {
    auto inputNode = TExprBase(input);
    auto dqSource = inputNode.Cast<TDqSource>();
    auto inputStats = typeCtx->GetStats(dqSource.Settings().Raw());
    if (!inputStats) {
        return;
    }

    typeCtx->SetStats(input.Get(), inputStats);
}

/**
 * For Flatmap we check the input and fetch the statistcs and cost from below
 * Then we analyze the filter predicate and compute it's selectivity and apply it
 * to the result.
 * 
 * If this flatmap's lambda is a join, we propagate the join result as the output of FlatMap
 */
void InferStatisticsForFlatMap(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {

    auto inputNode = TExprBase(input);
    auto flatmap = inputNode.Cast<TCoFlatMapBase>();
    auto flatmapInput = flatmap.Input();
    auto inputStats = typeCtx->GetStats(flatmapInput.Raw());

    if (! inputStats ) {
        return;
    }

    if (IsPredicateFlatMap(flatmap.Lambda().Body().Ref())) {
        // Selectivity is the fraction of tuples that are selected by this predicate
        // Currently we just set the number to 10% before we have statistics and parse
        // the predicate

        double selectivity = TPredicateSelectivityComputer(inputStats).Compute(flatmap.Lambda().Body());

        auto outputStats = TOptimizerStatistics(
            inputStats->Type, 
            inputStats->Nrows * selectivity, 
            inputStats->Ncols, 
            inputStats->ByteSize * selectivity, 
            inputStats->Cost, 
            inputStats->KeyColumns,
            inputStats->ColumnStatistics,
            inputStats->StorageType);

        outputStats.Labels = inputStats->Labels;
        outputStats.Selectivity *= (inputStats->Selectivity * selectivity);

        typeCtx->SetStats(input.Get(), std::make_shared<TOptimizerStatistics>(std::move(outputStats)) );
    }
    else if (flatmap.Lambda().Body().Maybe<TCoMapJoinCore>() || 
            flatmap.Lambda().Body().Maybe<TCoMap>().Input().Maybe<TCoMapJoinCore>() ||
            flatmap.Lambda().Body().Maybe<TCoJoinDict>() ||
            flatmap.Lambda().Body().Maybe<TCoMap>().Input().Maybe<TCoJoinDict>() ||
            flatmap.Lambda().Body().Maybe<TCoGraceJoinCore>() ||
            flatmap.Lambda().Body().Maybe<TCoMap>().Input().Maybe<TCoGraceJoinCore>()){

        typeCtx->SetStats(input.Get(), typeCtx->GetStats(flatmap.Lambda().Body().Raw()));
    }
    else {
        typeCtx->SetStats(input.Get(), typeCtx->GetStats(flatmapInput.Raw()));
    }
}

/**
 * For Filter we check the input and fetch the statistcs and cost from below
 * Then we analyze the filter predicate and compute it's selectivity and apply it
 * to the result, just like in FlatMap, except we check for a specific pattern:
 * If the filter's lambda is an Exists callable with a Member callable, we set the
 * selectivity to 1 to be consistent with SkipNullMembers in the logical plan
 */
void InferStatisticsForFilter(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {

    auto inputNode = TExprBase(input);
    auto filter = inputNode.Cast<TCoFilterBase>();
    auto filterInput = filter.Input();
    auto inputStats = typeCtx->GetStats(filterInput.Raw());

    if (!inputStats){
        return;
    }

    // Selectivity is the fraction of tuples that are selected by this predicate
    // Currently we just set the number to 10% before we have statistics and parse
    // the predicate
    auto filterBody = filter.Lambda().Body();
    double selectivity = TPredicateSelectivityComputer(inputStats).Compute(filterBody);

    auto outputStats = TOptimizerStatistics(
        inputStats->Type, 
        inputStats->Nrows * selectivity, 
        inputStats->Ncols, 
        inputStats->ByteSize * selectivity, 
        inputStats->Cost, 
        inputStats->KeyColumns,
        inputStats->ColumnStatistics,
        inputStats->StorageType);

    outputStats.Selectivity *= (selectivity * inputStats->Selectivity);
    outputStats.Labels = inputStats->Labels;

    typeCtx->SetStats(input.Get(), std::make_shared<TOptimizerStatistics>(std::move(outputStats)) );
}

/**
 * Infer statistics and costs for SkipNullMembers
 * We don't have a good idea at this time how many nulls will be discarded, so we just return the
 * input statistics.
 */
void InferStatisticsForSkipNullMembers(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {

    auto inputNode = TExprBase(input);
    auto skipNullMembers = inputNode.Cast<TCoSkipNullMembers>();
    auto skipNullMembersInput = skipNullMembers.Input();

    auto inputStats = typeCtx->GetStats(skipNullMembersInput.Raw());
    if (!inputStats) {
        return;
    }

    typeCtx->SetStats( input.Get(), inputStats );
}

/**
 * Infer statistics and costs for ExtractlMembers
 * We just return the input statistics.
*/
void InferStatisticsForExtractMembers(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {

    auto inputNode = TExprBase(input);
    auto extractMembers = inputNode.Cast<TCoExtractMembers>();
    auto extractMembersInput = extractMembers.Input();

    auto inputStats = typeCtx->GetStats(extractMembersInput.Raw() );
    if (!inputStats) {
        return;
    }

    typeCtx->SetStats( input.Get(), inputStats );
}

/**
 * Infer statistics and costs for AggregateCombine
 * We just return the input statistics.
*/
void InferStatisticsForAggregateCombine(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {

    auto inputNode = TExprBase(input);
    auto agg = inputNode.Cast<TCoAggregateCombine>();
    auto aggInput = agg.Input();

    auto inputStats = typeCtx->GetStats(aggInput.Raw());
    if (!inputStats) {
        return;
    }

    typeCtx->SetStats( input.Get(), inputStats );
}

/**
 * Infer statistics and costs for AggregateMergeFinalize
 * Just return input stats
*/
void InferStatisticsForAggregateMergeFinalize(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {

    auto inputNode = TExprBase(input);
    auto agg = inputNode.Cast<TCoAggregateMergeFinalize>();
    auto aggInput = agg.Input();

    auto inputStats = typeCtx->GetStats(aggInput.Raw() );
    if (!inputStats) {
        return;
    }

    typeCtx->SetStats( input.Get(), inputStats );
}

void InferStatisticsForAsList(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {
    double nRows = input->ChildrenSize();
    int nAttrs = 5;
    if (input->ChildrenSize() && input->Child(0)->IsCallable("AsStruct")) {
        nAttrs = input->Child(0)->ChildrenSize();
    }
    auto outputStats = std::make_shared<TOptimizerStatistics>(
        EStatisticsType::BaseTable, nRows, nAttrs, nRows*nAttrs, 0.0);
    outputStats->StorageType = EStorageType::RowStorage;
    typeCtx->SetStats(input.Get(), outputStats);
}

/***
 * Infer statistics for a list of structs
 */
bool InferStatisticsForListParam(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {
    auto param = TCoParameter(input);
    if (param.Name().Maybe<TCoAtom>()) {
        auto atom = param.Name().Cast<TCoAtom>();
        if (atom.Value().StartsWith("%kqp%tx_result_binding")) {
            return false;
        }
    }

    if (auto maybeListType = param.Type().Maybe<TCoListType>()) {
        auto itemType = maybeListType.Cast().ItemType();
        if (auto maybeStructType = itemType.Maybe<TCoStructType>()) {
            int nRows = 100;
            int nAttrs = maybeStructType.Cast().Ptr()->ChildrenSize();
            auto resStats = std::make_shared<TOptimizerStatistics>(EStatisticsType::BaseTable, nRows, nAttrs, nRows*nAttrs, 0.0);
            resStats->StorageType = EStorageType::RowStorage;
            typeCtx->SetStats(input.Get(), resStats);
        }
    }
    return true;
}

/***
 * For callables that include lambdas, we want to propagate the statistics from lambda's input to its argument, so
 * that the operators inside lambda receive the correct statistics
*/
void PropagateStatisticsToLambdaArgument(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {

    if (input->ChildrenSize()<2) {
        return;
    }

    auto callableInput = input->ChildRef(0);

    // Iterate over all children except for the input
    // Check if the child is a lambda and propagate the statistics into it
    for (size_t i=1; i<input->ChildrenSize(); i++) {
        auto maybeLambda = TExprBase(input->ChildRef(i));
        if (!maybeLambda.Maybe<TCoLambda>()) {
            continue;
        }

        auto lambda = maybeLambda.Cast<TCoLambda>();
        if (!lambda.Args().Size()){
            continue;
        }

        // If the input to the callable is a list, then lambda also takes a list of arguments
        // So we need to propagate corresponding arguments

        if (callableInput->IsList()){
            for(size_t j=0; j<callableInput->ChildrenSize(); j++){
                auto inputStats = typeCtx->GetStats(callableInput->Child(j) );
                if (inputStats){
                    typeCtx->SetStats( lambda.Args().Arg(j).Raw(), inputStats );
                }
            }
            
        }
        else {
            auto inputStats = typeCtx->GetStats(callableInput.Get());
            if (!inputStats) {
                return;
            }
            
            // We have a special case of Olap tables, where statistics is computed before lambda, but
            // is finalized after visiting labda (which may contain a filter)
            if (typeCtx->GetStats(input.Get())){
                inputStats = typeCtx->GetStats(input.Get());
            }
            
            typeCtx->SetStats( lambda.Args().Arg(0).Raw(), inputStats );
        }
    }
}

/**
 * After processing the lambda for the stage we set the stage output to the result of the lambda
*/
void InferStatisticsForStage(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {
    auto inputNode = TExprBase(input);
    auto stage = inputNode.Cast<TDqStageBase>();

    auto lambdaStats = typeCtx->GetStats( stage.Program().Body().Raw());
    if (lambdaStats){
        typeCtx->SetStats( stage.Raw(), lambdaStats );
    }
}

} // namespace NYql::NDq {
