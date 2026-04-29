#pragma once
#include "kqp_rbo_physical_op_builder.h"
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/utils/log/log.h>

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

// This class represents a physical builder for OpAggregate, it emits a physical aggreation based on a given OpAggregate.
class TPhysicalAggregationBuilder : public TPhysicalUnaryOpBuilderWithMemLimit {
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

    // Internal representation of the decimal type.
    struct TDecimalType {
        TString Precision;
        TString Scale;
    };

public:
    TPhysicalAggregationBuilder(TIntrusivePtr<TOpAggregate> aggregate, TExprContext& ctx, TPositionHandle pos)
        : TPhysicalUnaryOpBuilderWithMemLimit(ctx, pos)
        , Aggregate(aggregate) {
    }

    TExprNode::TPtr BuildPhysicalOp(TExprNode::TPtr input, std::optional<i64> memLimit) override;

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
    // Init state.
    TExprNode::TPtr BuildCountAggregationInitialStateForOptionalType(TExprNode::TPtr lambdaArg);
    TExprNode::TPtr BuildCountAggregationInitialState();
    TExprNode::TPtr BuildAvgAggregationInitialState(TExprNode::TPtr lambdaArg, const TTypeAnnotationNode* typeNode);
    TExprNode::TPtr BuildAvgAggregationInitialStateForOptionalType(TExprNode::TPtr lambdaArg, const TTypeAnnotationNode* typeNode);
    TExprNode::TPtr BuildSumAggregationInitialState(TExprNode::TPtr lambdaArg, const TTypeAnnotationNode* typeNode);

    // Update state.
    TExprNode::TPtr BuildSumAggregationUpdateState(TExprNode::TPtr lambdaArgState, TExprNode::TPtr lambdaArgField, const TTypeAnnotationNode* itemType);
    TExprNode::TPtr BuildCountAggregationUpdateStateForOptionalType(TExprNode::TPtr lambdaArgState, TExprNode::TPtr lambdaArgField);
    TExprNode::TPtr BuildCountAggregationUpdateState(TExprNode::TPtr lambdaArgState);
    TExprNode::TPtr BuildAvgAggregationUpdateStateForOptionalType(TExprNode::TPtr lambdaArgState, TExprNode::TPtr lambdaArgField, const TTypeAnnotationNode* typeNode);
    TExprNode::TPtr BuildAvgAggregationUpdateState(TExprNode::TPtr lambdaArgState, TExprNode::TPtr lambdaFieldState, const TTypeAnnotationNode* typeNode);

    // Finish state.
    TExprNode::TPtr BuildAvgAggregationFinishStateForOptionalType(TExprNode::TPtr lambdaArgState, const TTypeAnnotationNode* typeNode);
    TExprNode::TPtr BuildAvgAggregationFinishState(TExprNode::TPtr lambdaArgState, const TTypeAnnotationNode* typeNode);

    // Scalar aggregation wrapper.
    TExprNode::TPtr BuildCondenseForAggregationOutputWithEmptyKeys(TExprNode::TPtr input, const TVector<TPhysicalAggregationTraits>& traits,
                                                                   const THashMap<TString, TString>& projectionMap, const TTypeAnnotationNode* type);
    // Helpers.
    TExprNode::TPtr GetDataTypeForSumAggregation(const TTypeAnnotationNode* itemType) const;
    TVector<TString> GetInputColumns(const TVector<TOpAggregationTraits>& aggregationTraitsList, const TVector<TInfoUnit>& keyColumns) const;
    void BuildPhysicalAggregationTraits(const TVector<TString>& inputColumns, const TVector<TString>& keyColumns,
                                        const TVector<TOpAggregationTraits>& aggregationTraitsList, TVector<TString>& inputFields,
                                        TVector<TPhysicalAggregationTraits>& aggTraits, THashMap<TString, TString>& projectionMap,
                                        const TTypeAnnotationNode* inputType, const TTypeAnnotationNode* outputType);
    TVector<TString> GetKeyFields(const TVector<TInfoUnit>& keyColumns) const;

    // Helpers for scalar aggregation.
    TExprNode::TPtr CreateNothingForEmptyInput(const TTypeAnnotationNode* aggType);
    TExprNode::TPtr MapCondenseOutput(TExprNode::TPtr input, const TVector<TPhysicalAggregationTraits>& traits,
                                      const THashMap<TString, TString>& projectionMap);

    TExprNode::TPtr GetDataTypeForAccumulator(const TTypeAnnotationNode* typeNode, bool keepOriginalPrecision = false) const;
    bool IsDecimalType(const TTypeAnnotationNode* typeNode) const;
    TDecimalType GetDecimalType(const TTypeAnnotationNode* typeNode) const;
    TExprNode::TPtr GetDecimalDataType(const TTypeAnnotationNode* typeNode, bool keepOriginalPrecision = false) const;

    // Holds an aggregate operator.
    TIntrusivePtr<TOpAggregate> Aggregate;
    // This Map represents a simple physical aggregation functions.
    const THashMap<TString, TString> AggregationFunctionToAggregationCallable{{"sum", "AggrAdd"}, {"min", "AggrMin"}, {"max", "AggrMax"}};
    // The name of the physical aggregation.
    static constexpr TStringBuf PhysicalAggregationName = "DqPhyHashCombine";
};
