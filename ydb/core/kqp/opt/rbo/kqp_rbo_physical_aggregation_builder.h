#pragma once
#include "kqp_rbo.h"
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/utils/log/log.h>

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

struct TPhysicalAggregationTraits {
    TPhysicalAggregationTraits(const TString& inputColName, const TTypeAnnotationNode* inputItemType, const TString& aggFieldName,
                               const TTypeAnnotationNode* outputItemType, const TString& aggFunc, const TString& stateFieldName)
        : InputColName(inputColName)
        , InputItemType(inputItemType)
        , AggFieldName(aggFieldName)
        , OutputItemType(outputItemType)
        , AggFunc(aggFunc)
        , StateFieldName(stateFieldName) {
    }

    TString InputColName;
    const TTypeAnnotationNode* InputItemType;
    TString AggFieldName;
    const TTypeAnnotationNode* OutputItemType;
    TString AggFunc;
    TString StateFieldName;
};

class TPhysicalAggregationBuilder {
public:
    TPhysicalAggregationBuilder(std::shared_ptr<TOpAggregate> aggregate, TExprContext& ctx)
        : Aggregate(aggregate)
        , Ctx(ctx) {
    }

    TPhysicalAggregationBuilder(const TPhysicalAggregationBuilder&) = delete;
    TPhysicalAggregationBuilder& operator=(const TPhysicalAggregationBuilder&) = delete;
    TExprNode::TPtr BuildPhysicalAggregation(TExprNode::TPtr input);

private:
    TExprNode::TPtr BuildCountAggregationInitialStateForOptionalType(TExprNode::TPtr asStruct, const TString& colName);
    TExprNode::TPtr BuildCountAggregationInitialState();
    TExprNode::TPtr BuildAvgAggregationInitialState(TExprNode::TPtr asStruct, const TString& colName);
    TExprNode::TPtr BuildAvgAggregationInitialStateForOptionalType(TExprNode::TPtr asStruct, const TString& colName);
    TString GetTypeToSafeCastForSumAggregation(const TTypeAnnotationNode* itemType);
    TExprNode::TPtr BuildSumAggregationInitialState(TExprNode::TPtr asStruct, const TString& colName, const TTypeAnnotationNode* itemType);
    TExprNode::TPtr BuildCountAggregationUpdateStateForOptionalType(TExprNode::TPtr asStructStateColumns, TExprNode::TPtr asStructInputColumns,
                                                                    const TString& stateColumn, const TString& columnName);
    TExprNode::TPtr BuildCountAggregationUpdateState(TExprNode::TPtr asStructStateColumns, const TString& stateColumn);
    TExprNode::TPtr BuildAvgAggregationUpdateStateForOptionalType(TExprNode::TPtr asStructStateColumns, TExprNode::TPtr asStructInputColumns,
                                                                  const TString& stateColumn, const TString& columnName);
    TExprNode::TPtr BuildAvgAggregationUpdateState(TExprNode::TPtr asStructStateColumns, TExprNode::TPtr asStrcutInputColumns, const TString& stateColumn,
                                                   const TString& columnName);
    TExprNode::TPtr BuildSumAggregationUpdateState(TExprNode::TPtr asStructStateColumns, TExprNode::TPtr asStrcutInputColumns, const TString& stateColumn,
                                                   const TString& columnName, const TTypeAnnotationNode* itemType);
    TExprNode::TPtr BuildAvgAggregationFinishStateForOptionalType(TExprNode::TPtr asStructStateColumns, const TString& stateName);
    TExprNode::TPtr BuildAvgAggregationFinishState(TExprNode::TPtr asStructStateColumns, const TString& stateName);
    TExprNode::TPtr BuildKeyExtractorLambda(const TVector<TString>& keyFields, const TVector<TString>& inputFields);
    TExprNode::TPtr BuildInitHandlerLambda(const TVector<TString>& keyFields, const TVector<TString>& inputFields,
                                           const TVector<TPhysicalAggregationTraits>& aggTraitsList);
    TExprNode::TPtr BuildUpdateHandlerLambda(const TVector<TString>& keyFields, const TVector<TString>& inputFields,
                                             const TVector<TPhysicalAggregationTraits>& aggTraitsList);
    TExprNode::TPtr BuildFinishHandlerLambda(const TVector<TString>& keyFields, const TVector<TPhysicalAggregationTraits>& aggTraitsList, bool distinctAll);
    TExprNode::TPtr BuildExpandMapForWideCombinerInput(TExprNode::TPtr input, const TVector<TString>& inputColumns);
    TExprNode::TPtr BuildNarrowMapForWideCombinerOutput(TExprNode::TPtr input, const TVector<TString>& keyFields,
                                                        const TVector<TPhysicalAggregationTraits>& aggTraitsList,
                                                        const THashMap<TString, TString>& projectionMap, bool distinctAll);
    TExprNode::TPtr BuildCondenseForAggregationOutputWithEmptyKeys(TExprNode::TPtr input, const TVector<TPhysicalAggregationTraits>& traits,
                                                                   const THashMap<TString, TString>& projectionMap, const TTypeAnnotationNode* type);

    TVector<TString> GetInputColumns(const TVector<TOpAggregationTraits>& aggregationTraitsList, const TVector<TInfoUnit>& keyColumns);
    void GetPhysicalAggregationTraits(const TVector<TString>& inputColumns, const TVector<TOpAggregationTraits>& aggregationTraitsList,
                                      TVector<TString>& inputFields, TVector<TPhysicalAggregationTraits>& aggTraits, THashMap<TString, TString>& projectionMap,
                                      const TTypeAnnotationNode* inputType, const TTypeAnnotationNode* outputType);
    TVector<TString> GetKeyFields(const TVector<TInfoUnit>& keyColumns);

    TExprNode::TPtr CreateNothingForEmptyInput(const TTypeAnnotationNode* aggType);
    TExprNode::TPtr MapCondenseOutput(TExprNode::TPtr input, const TVector<TPhysicalAggregationTraits>& traits,
                                      const THashMap<TString, TString>& projectionMap);

    std::shared_ptr<TOpAggregate> Aggregate;
    TPositionHandle Pos;
    TExprContext& Ctx;
    THashMap<TString, TString> AggregationFunctionToAggregationCallable{{"sum", "AggrAdd"}, {"min", "AggrMin"}, {"max", "AggrMax"}};
};
