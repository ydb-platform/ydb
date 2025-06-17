#pragma once

#include <yql/essentials/minikql/mkql_program_builder.h>

namespace NKikimr {
namespace NMiniKQL {

class TDqProgramBuilder: public TProgramBuilder {
public:
    TDqProgramBuilder(const TTypeEnvironment& env, const IFunctionRegistry& functionRegistry);

    TRuntimeNode DqHashCombine(TRuntimeNode flow, ui64 memLimit, const TWideLambda& keyExtractor, const TBinaryWideLambda& init, const TTernaryWideLambda& update, const TBinaryWideLambda& finish);
    TRuntimeNode DqHashAggregate(TRuntimeNode flow, const bool spilling, const TWideLambda& keyExtractor, const TBinaryWideLambda& init, const TTernaryWideLambda& update, const TBinaryWideLambda& finish);

    TRuntimeNode DqBlockHashJoin(TRuntimeNode leftStream, TRuntimeNode rightStream, EJoinKind joinKind,
        const TArrayRef<const ui32>& leftKeyColumns, const TArrayRef<const ui32>& rightKeyColumns, TType* returnType);

protected:
    TCallableBuilder BuildCommonCombinerParams(
        const TStringBuf operatorName,
        const TRuntimeNode operatorParams,
        const TRuntimeNode flow,
        const TProgramBuilder::TWideLambda& keyExtractor,
        const TProgramBuilder::TBinaryWideLambda& init,
        const TProgramBuilder::TTernaryWideLambda& update,
        const TProgramBuilder::TBinaryWideLambda& finish);
};

} // namespace NMiniKQL
} // namespace NKikimr
