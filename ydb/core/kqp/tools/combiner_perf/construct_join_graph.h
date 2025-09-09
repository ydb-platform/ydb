#pragma once
// #include <yql/essentials/minikql/mkql_program_builder.h>
#include <ydb/library/yql/dq/comp_nodes/dq_program_builder.h>
#include <ydb/library/yql/dq/comp_nodes/ut/dq_setup.h>

namespace NKikimr::NMiniKQL{
enum class ETestedJoinAlgo{
    kScalarGrace,
    kBlockMap,
    kBlockHash,
    kScalarHash
};

struct JoinSourceData{
    TArrayRef<TType* const> ColumnTypes;
    TArrayRef<const ui32> KeyColumnIndexes;
    NYql::NUdf::TUnboxedValue ValuesList;
    // TArrayRef<const ui32> Renames;//??
};

struct InnerJoinDescription{
    JoinSourceData LeftSource;
    JoinSourceData RightSource;
    // TType* ReturnType;
    TDqSetup<false>* Setup; 
    // EAnyJoinSettings AnyJoinSettings;
};

THolder<IComputationGraph> ConstructInnerJoinGraphStream(ETestedJoinAlgo algo, InnerJoinDescription descr);
i32 ResultColumnCount(ETestedJoinAlgo algo, InnerJoinDescription descr);
}