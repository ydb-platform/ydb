#pragma once
#include "kqp_rbo_physical_op_builder.h"
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/utils/log/log.h>

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

// This class represents a physical builder for OpAggregate, it emits a physical aggreation based on a given OpAggregate.
class TPhysicalAggregationBuilder : public TPhysicalUnaryOpBuilder {
    // Internal representation of physical aggregation traits.
    struct TPhysicalAggregationTraits {
        TPhysicalAggregationTraits(const TString& aggFieldName, const TString stateFieldName, const TString& aggFunc, const TTypeAnnotationNode* inputItemType,
                                   const TTypeAnnotationNode* outputItemType)
            : AggFieldName(aggFieldName)
            , StateFieldName(stateFieldName)
            , AggFunc(aggFunc)
            , InputItemType(inputItemType)
            , OutputItemType(outputItemType) {
        }

        TString AggFieldName;
        TString StateFieldName;
        TString AggFunc;
        const TTypeAnnotationNode* InputItemType;
        const TTypeAnnotationNode* OutputItemType;
    };

public:
    TPhysicalAggregationBuilder(TIntrusivePtr<TOpAggregate> aggregate, TExprContext& ctx, TPositionHandle pos)
        : TPhysicalUnaryOpBuilder(ctx, pos)
        , Aggregate(aggregate) {
    }

    TExprNode::TPtr BuildPhysicalOp(TExprNode::TPtr input) override;

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
    // Physical aggregation lambdas - packed.
    TExprNode::TPtr BuildKeyExtractorLambdaPacked(const TVector<TString>& keyFields, const TVector<TString>& inputFields);
    TExprNode::TPtr BuildInitHandlerLambdaPacked(const TVector<TString>& keyFields, const TVector<TString>& inputFields,
                                                 const TVector<TPhysicalAggregationTraits>& aggTraitsList);
    TExprNode::TPtr BuildUpdateHandlerLambdaPacked(const TVector<TString>& keyFields, const TVector<TString>& inputFields,
                                                   const TVector<TPhysicalAggregationTraits>& aggTraitsList);
    TExprNode::TPtr BuildFinishHandlerLambdaPacked(const TVector<TString>& keyFields, const TVector<TPhysicalAggregationTraits>& aggTraitsList,
                                                   bool distinctAll);

    // Init state.
    TExprNode::TPtr BuildCountAggregationInitialStateForOptionalType(TExprNode::TPtr lambdaArg);
    TExprNode::TPtr BuildCountAggregationInitialStateForOptionalTypePacked(TExprNode::TPtr asStruct, const TString& colName);
    TExprNode::TPtr BuildCountAggregationInitialState();
    TExprNode::TPtr BuildAvgAggregationInitialState(TExprNode::TPtr lambdaArg);
    TExprNode::TPtr BuildAvgAggregationInitialStatePacked(TExprNode::TPtr asStruct, const TString& colName);
    TExprNode::TPtr BuildAvgAggregationInitialStateForOptionalType(TExprNode::TPtr lambdaArg);
    TExprNode::TPtr BuildAvgAggregationInitialStateForOptionalTypePacked(TExprNode::TPtr asStruct, const TString& colName);
    TExprNode::TPtr BuildSumAggregationInitialState(TExprNode::TPtr lambdaArg, const TTypeAnnotationNode* itemType);
    TExprNode::TPtr BuildSumAggregationInitialStatePacked(TExprNode::TPtr asStruct, const TString& colName, const TTypeAnnotationNode* itemType);


    // Update state.
    TExprNode::TPtr BuildSumAggregationUpdateState(TExprNode::TPtr lambdaArgState, TExprNode::TPtr lambdaArgField, const TTypeAnnotationNode* itemType);
    TExprNode::TPtr BuildSumAggregationUpdateStatePacked(TExprNode::TPtr asStructStateColumns, TExprNode::TPtr asStrcutInputColumns, const TString& stateColumn,
                                                         const TString& columnName, const TTypeAnnotationNode* itemType);
    TExprNode::TPtr BuildCountAggregationUpdateStateForOptionalType(TExprNode::TPtr lambdaArgState, TExprNode::TPtr lambdaArgField);
    TExprNode::TPtr BuildCountAggregationUpdateStateForOptionalTypePacked(TExprNode::TPtr asStructStateColumns, TExprNode::TPtr asStructInputColumns,
                                                                          const TString& stateColumn, const TString& columnName);
    TExprNode::TPtr BuildCountAggregationUpdateState(TExprNode::TPtr lambdaArgState);
    TExprNode::TPtr BuildCountAggregationUpdateStatePacked(TExprNode::TPtr asStructStateColumns, const TString& stateColumn);
    TExprNode::TPtr BuildAvgAggregationUpdateStateForOptionalType(TExprNode::TPtr lambdaArgState, TExprNode::TPtr lambdaArgField);
    TExprNode::TPtr BuildAvgAggregationUpdateStateForOptionalTypePacked(TExprNode::TPtr asStructStateColumns, TExprNode::TPtr asStructInputColumns,
                                                                        const TString& stateColumn, const TString& columnName);
    TExprNode::TPtr BuildAvgAggregationUpdateState(TExprNode::TPtr lambdaArgState, TExprNode::TPtr lambdaFieldState);
    TExprNode::TPtr BuildAvgAggregationUpdateStatePacked(TExprNode::TPtr asStructStateColumns, TExprNode::TPtr asStrcutInputColumns, const TString& stateColumn,
                                                         const TString& columnName);

    // Finish state.
    TExprNode::TPtr BuildAvgAggregationFinishStateForOptionalType(TExprNode::TPtr lambdaArgState);
    TExprNode::TPtr BuildAvgAggregationFinishStateForOptionalTypePacked(TExprNode::TPtr asStructStateColumns, const TString& stateName);
    TExprNode::TPtr BuildAvgAggregationFinishState(TExprNode::TPtr lambdaArgState);
    TExprNode::TPtr BuildAvgAggregationFinishStatePacked(TExprNode::TPtr asStructStateColumns, const TString& stateName);

    // Scalar aggregation wrapper.
    TExprNode::TPtr BuildCondenseForAggregationOutputWithEmptyKeys(TExprNode::TPtr input, const TVector<TPhysicalAggregationTraits>& traits,
                                                                   const THashMap<TString, TString>& projectionMap, const TTypeAnnotationNode* type);
    // Helpers.
    TString GetTypeToSafeCastForSumAggregation(const TTypeAnnotationNode* itemType);
    TVector<TString> GetInputColumns(const TVector<TOpAggregationTraits>& aggregationTraitsList, const TVector<TInfoUnit>& keyColumns);
    void BuildPhysicalAggregationTraits(const TVector<TString>& inputColumns, const TVector<TString>& keyColumns,
                                        const TVector<TOpAggregationTraits>& aggregationTraitsList, TVector<TString>& inputFields,
                                        TVector<TPhysicalAggregationTraits>& aggTraits, THashMap<TString, TString>& projectionMap,
                                        const TTypeAnnotationNode* inputType, const TTypeAnnotationNode* outputType);
    TVector<TString> GetKeyFields(const TVector<TInfoUnit>& keyColumns);

    // Helpers for scalar aggregation.
    TExprNode::TPtr CreateNothingForEmptyInput(const TTypeAnnotationNode* aggType);
    TExprNode::TPtr MapCondenseOutput(TExprNode::TPtr input, const TVector<TPhysicalAggregationTraits>& traits,
                                      const THashMap<TString, TString>& projectionMap);

    TIntrusivePtr<TOpAggregate> Aggregate;
    static constexpr bool DebugPackWideLambdasToStruct{false};

    // This Map represents a simple physical aggregation functions.
    const THashMap<TString, TString> AggregationFunctionToAggregationCallable{{"sum", "AggrAdd"}, {"min", "AggrMin"}, {"max", "AggrMax"}};
    // The name of the physical aggregation.
    static constexpr TStringBuf PhysicalAggregationName = "DqPhyHashCombine";
};
