#pragma once
#include "kqp_rbo.h"
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/utils/log/log.h>

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

// This class represents a physical builder for OpAggregate, it emits a physical aggreation based on a given OpAggregate.
class TPhysicalAggregationBuilder {
    // Internal representation of physical aggregation traits.
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

public:
    TPhysicalAggregationBuilder(std::shared_ptr<TOpAggregate> aggregate, TExprContext& ctx, TPositionHandle pos)
        : Aggregate(aggregate)
        , Ctx(ctx)
        , Pos(pos) {
    }
    TPhysicalAggregationBuilder() = delete;
    TPhysicalAggregationBuilder(const TPhysicalAggregationBuilder&) = delete;
    TPhysicalAggregationBuilder& operator=(const TPhysicalAggregationBuilder&) = delete;
    TPhysicalAggregationBuilder(const TPhysicalAggregationBuilder&&) = delete;
    TPhysicalAggregationBuilder& operator=(const TPhysicalAggregationBuilder&&) = delete;
    ~TPhysicalAggregationBuilder() = default;

    // This function builds a physical aggregation with given input.
    TExprNode::TPtr BuildPhysicalAggregation(TExprNode::TPtr input);

private:
    // Following functions creates a 4 lambdas for physical aggregation:
    // 1) Extracts aggreation keys from input.
    TExprNode::TPtr BuildKeyExtractorLambda(const TVector<TString>& keyFields, const TVector<TString>& inputFields);
    // 2) Initializes initial state for aggregation for each new key.
    TExprNode::TPtr BuildInitHandlerLambda(const TVector<TString>& keyFields, const TVector<TString>& inputFields,
                                           const TVector<TPhysicalAggregationTraits>& aggTraitsList);
    // 3) Performs an aggregation.
    TExprNode::TPtr BuildUpdateHandlerLambda(const TVector<TString>& keyFields, const TVector<TString>& inputFields,
                                             const TVector<TPhysicalAggregationTraits>& aggTraitsList);
    // 4) Returns result after aggregation.
    TExprNode::TPtr BuildFinishHandlerLambda(const TVector<TString>& keyFields, const TVector<TPhysicalAggregationTraits>& aggTraitsList, bool distinctAll);
    // Build a map to extend input from narrow to wide for aggregation.
    TExprNode::TPtr BuildExpandMapForPhysicalAggregationInput(TExprNode::TPtr input, const TVector<TString>& inputColumns);
    // Build a map to fuse output from wide to narrow for aggregation.
    TExprNode::TPtr BuildNarrowMapForPhysicalAggregationOutput(TExprNode::TPtr input, const TVector<TString>& keyFields,
                                                               const TVector<TPhysicalAggregationTraits>& aggTraitsList,
                                                               const THashMap<TString, TString>& projectionMap, bool distinctAll);
    // `Count` aggregation function.
    TExprNode::TPtr BuildCountAggregationInitialStateForOptionalType(TExprNode::TPtr asStruct, const TString& colName);
    TExprNode::TPtr BuildCountAggregationInitialState();
    TExprNode::TPtr BuildCountAggregationUpdateStateForOptionalType(TExprNode::TPtr asStructStateColumns, TExprNode::TPtr asStructInputColumns,
                                                                    const TString& stateColumn, const TString& columnName);
    TExprNode::TPtr BuildCountAggregationUpdateState(TExprNode::TPtr asStructStateColumns, const TString& stateColumn);
    // `Avg` aggregation function.
    TExprNode::TPtr BuildAvgAggregationInitialState(TExprNode::TPtr asStruct, const TString& colName);
    TExprNode::TPtr BuildAvgAggregationInitialStateForOptionalType(TExprNode::TPtr asStruct, const TString& colName);
    TExprNode::TPtr BuildAvgAggregationUpdateStateForOptionalType(TExprNode::TPtr asStructStateColumns, TExprNode::TPtr asStructInputColumns,
                                                                  const TString& stateColumn, const TString& columnName);
    TExprNode::TPtr BuildAvgAggregationUpdateState(TExprNode::TPtr asStructStateColumns, TExprNode::TPtr asStrcutInputColumns, const TString& stateColumn,
                                                   const TString& columnName);
    TExprNode::TPtr BuildAvgAggregationFinishStateForOptionalType(TExprNode::TPtr asStructStateColumns, const TString& stateName);
    TExprNode::TPtr BuildAvgAggregationFinishState(TExprNode::TPtr asStructStateColumns, const TString& stateName);
    TExprNode::TPtr BuildSumAggregationInitialState(TExprNode::TPtr asStruct, const TString& colName, const TTypeAnnotationNode* itemType);
    TExprNode::TPtr BuildCondenseForAggregationOutputWithEmptyKeys(TExprNode::TPtr input, const TVector<TPhysicalAggregationTraits>& traits,
                                                                   const THashMap<TString, TString>& projectionMap, const TTypeAnnotationNode* type);
    // `Sum` aggregation functions.
    TExprNode::TPtr BuildSumAggregationUpdateState(TExprNode::TPtr asStructStateColumns, TExprNode::TPtr asStrcutInputColumns, const TString& stateColumn,
                                                   const TString& columnName, const TTypeAnnotationNode* itemType);
    // Helpers.
    TString GetTypeToSafeCastForSumAggregation(const TTypeAnnotationNode* itemType);
    TVector<TString> GetInputColumns(const TVector<TOpAggregationTraits>& aggregationTraitsList, const TVector<TInfoUnit>& keyColumns);
    void GetPhysicalAggregationTraits(const TVector<TString>& inputColumns, const TVector<TOpAggregationTraits>& aggregationTraitsList,
                                      TVector<TString>& inputFields, TVector<TPhysicalAggregationTraits>& aggTraits, THashMap<TString, TString>& projectionMap,
                                      const TTypeAnnotationNode* inputType, const TTypeAnnotationNode* outputType);
    TVector<TString> GetKeyFields(const TVector<TInfoUnit>& keyColumns);

    // Helpers for scalar aggregation.
    TExprNode::TPtr CreateNothingForEmptyInput(const TTypeAnnotationNode* aggType);
    TExprNode::TPtr MapCondenseOutput(TExprNode::TPtr input, const TVector<TPhysicalAggregationTraits>& traits,
                                      const THashMap<TString, TString>& projectionMap);

    std::shared_ptr<TOpAggregate> Aggregate;
    TExprContext& Ctx;
    TPositionHandle Pos;
    // This Map represents a simple physical aggregation functions.
    const THashMap<TString, TString> AggregationFunctionToAggregationCallable{{"sum", "AggrAdd"}, {"min", "AggrMin"}, {"max", "AggrMax"}};
    // The name of the physical aggregation.
    static constexpr TStringBuf PhysicalAggregationName = "WideCombiner";
};
