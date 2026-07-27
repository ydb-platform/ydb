#pragma once

#include "dq_block_hash_join_settings.h"

#include <yql/essentials/minikql/mkql_program_builder.h>

namespace NKikimr {
namespace NMiniKQL {

class TDqProgramBuilder : public TProgramBuilder {
  public:
    TDqProgramBuilder(const TTypeEnvironment& env, const IFunctionRegistry& functionRegistry);

    // Single-side join filter: receives the wide row (one runtime node per input column) and
    // must return a Bool (or Optional<Bool>) predicate. An empty callback means "no filter".
    using TJoinFilterLambda = std::function<TRuntimeNode(TRuntimeNode::TList)>;
    // Common (non-equi) join filter over both sides: receives (leftRow, rightRow) wide rows and
    // must return a Bool (or Optional<Bool>) predicate. An empty callback means "no filter".
    using TJoinCommonFilterLambda = std::function<TRuntimeNode(TRuntimeNode::TList, TRuntimeNode::TList)>;

    TRuntimeNode DqHashCombine(TRuntimeNode flow, ui64 memLimit, const TWideLambda& keyExtractor,
                               const TBinaryWideLambda& init, const TTernaryWideLambda& update,
                               const TBinaryWideLambda& finish);
    TRuntimeNode DqHashAggregate(TRuntimeNode flow, const bool spilling, const TWideLambda& keyExtractor,
                                 const TBinaryWideLambda& init, const TTernaryWideLambda& update,
                                 const TBinaryWideLambda& finish);

    TRuntimeNode DqBlockHashJoin(TRuntimeNode leftStream, TRuntimeNode rightStream, EJoinKind joinKind,
                                 const TArrayRef<const ui32>& leftKeyColumns,
                                 const TArrayRef<const ui32>& rightKeyColumns, const TArrayRef<const ui32>& leftRenames,
                                 const TArrayRef<const ui32>& rightRenames, TType* returnType,
                                 TBlockHashJoinSettings settings = {}, const TJoinFilterLambda& leftFilter = {},
                                 const TJoinFilterLambda& rightFilter = {},
                                 const TJoinCommonFilterLambda& commonFilter = {});

    TRuntimeNode DqScalarHashJoin(TRuntimeNode leftFlow, TRuntimeNode rightFlow, EJoinKind joinKind,
                                  const TArrayRef<const ui32>& leftKeyColumns,
                                  const TArrayRef<const ui32>& rightKeyColumns,
                                  const TArrayRef<const ui32>& leftRenames, const TArrayRef<const ui32>& rightRenames,
                                  TType* returnType, const TJoinFilterLambda& leftFilter = {},
                                  const TJoinFilterLambda& rightFilter = {},
                                  const TJoinCommonFilterLambda& commonFilter = {});

    TType* LastScalarIndexBlock();

    TRuntimeNode AsTuple(TArrayRef<const ui32> nums);

    TRuntimeNode DqWatermarkGenerator(
        TRuntimeNode input,
        const TUnaryLambda& watermarkExtractor,
        const TUnaryLambda& partitionKeyExtractor,
        const TUnaryLambda& writeTimeExtractor,
        TConstArrayRef<std::pair<std::string, std::string>> watermarkSettings,
        TRuntimeNode partitionKeys
    );

  protected:
    TCallableBuilder BuildCommonCombinerParams(const TStringBuf operatorName, const TRuntimeNode operatorParams,
                                               const TRuntimeNode flow,
                                               const TProgramBuilder::TWideLambda& keyExtractor,
                                               const TProgramBuilder::TBinaryWideLambda& init,
                                               const TProgramBuilder::TTernaryWideLambda& update,
                                               const TProgramBuilder::TBinaryWideLambda& finish);
};

} // namespace NMiniKQL
} // namespace NKikimr
