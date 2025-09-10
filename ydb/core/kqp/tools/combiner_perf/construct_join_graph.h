#pragma once
#include <ydb/library/yql/dq/comp_nodes/dq_program_builder.h>
#include <ydb/library/yql/dq/comp_nodes/ut/dq_setup.h>

namespace NKikimr::NMiniKQL{
enum class ETestedJoinAlgo{
    kScalarGrace,
    kBlockMap,
    kScalarMap,
    kBlockHash,
    kScalarHash
};

struct JoinSourceData{
    TArrayRef<TType* const> ColumnTypes;
    TArrayRef<const ui32> KeyColumnIndexes;
    NYql::NUdf::TUnboxedValue ValuesList;
};

struct InnerJoinDescription{
    JoinSourceData LeftSource;
    JoinSourceData RightSource;
    TDqSetup<false>* Setup; 
};

THolder<IComputationGraph> ConstructInnerJoinGraphStream(ETestedJoinAlgo algo, InnerJoinDescription descr);
i32 ResultColumnCount(ETestedJoinAlgo algo, InnerJoinDescription descr);
}