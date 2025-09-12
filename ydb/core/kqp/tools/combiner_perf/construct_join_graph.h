#pragma once
#include <ydb/library/yql/dq/comp_nodes/dq_program_builder.h>
#include <ydb/library/yql/dq/comp_nodes/ut/utils/dq_setup.h>

namespace NKikimr::NMiniKQL{
enum class ETestedJoinAlgo{
    kScalarGrace,
    kBlockMap,
    kBlockHash,
    kScalarHash
};

struct TJoinSourceData{
    TArrayRef<TType* const> ColumnTypes;
    TArrayRef<const ui32> KeyColumnIndexes;
    NYql::NUdf::TUnboxedValue ValuesList;
};

struct TInnerJoinDescription{
    TJoinSourceData LeftSource;
    TJoinSourceData RightSource;
    TDqSetup<false>* Setup; 
};

THolder<IComputationGraph> ConstructInnerJoinGraphStream(ETestedJoinAlgo algo, TInnerJoinDescription descr);
i32 ResultColumnCount(ETestedJoinAlgo algo, TInnerJoinDescription descr);
}