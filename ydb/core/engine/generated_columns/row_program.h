#pragma once

#include <ydb/core/protos/index_builder.pb.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <yql/essentials/minikql/mkql_function_registry.h>

namespace NKikimr::NGeneratedColumns {

// A prepared scalar program. Neither its input nor its result borrows scan memory.
class IRowProgram {
public:
    virtual ~IRowProgram() = default;
    virtual bool Evaluate(TConstArrayRef<TCell> inputs, TSerializedCellVec& result, TString& error) = 0;
};

THolder<IRowProgram> CreateRowProgram(const NKikimrIndexBuilder::TGeneratedColumnBuild& descriptor,
    const NMiniKQL::IFunctionRegistry& registry, ui64 memoryLimitBytes, TString& error);

} // namespace NKikimr::NGeneratedColumns
