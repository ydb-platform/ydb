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
    TType* ItemType;
    TArrayRef<const ui32> KeyColumns;
    NYql::NUdf::TUnboxedValue Values;
    // TArrayRef<const ui32> Renames;//??
};

struct InnerJoinDescription{
    JoinSourceData LeftSource;
    JoinSourceData RightSource;
    // TType* ReturnType;
    TDqSetup<false>* Setup; 
    EAnyJoinSettings AnyJoinSettings;
};

NYql::NUdf::TUnboxedValue ConstructInnerJoinGraphStream(ETestedJoinAlgo algo, InnerJoinDescription descr);

}