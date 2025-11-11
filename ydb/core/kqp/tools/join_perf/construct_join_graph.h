#pragma once
#include "benchmark_settings.h"
#include <ydb/library/yql/dq/comp_nodes/dq_program_builder.h>
#include <ydb/library/yql/dq/comp_nodes/type_utils.h>
#include <ydb/library/yql/dq/comp_nodes/ut/utils/dq_setup.h>
#include <ydb/library/yql/dq/comp_nodes/ut/utils/utils.h>

namespace NKikimr::NMiniKQL {

struct TJoinSourceData {
    TArrayRef<TType* const> ColumnTypes;
    TArrayRef<const ui32> KeyColumnIndexes;
    NYql::NUdf::TUnboxedValue ValuesList;
};

struct TJoinDescription {
    TJoinSourceData LeftSource;
    TJoinSourceData RightSource;
    TDqSetup<false>* Setup;
    std::optional<TDqRenames> CustomRenames;
};

bool IsBlockJoin(ETestedJoinAlgo algo);

THolder<IComputationGraph> ConstructJoinGraphStream(EJoinKind joinKind, ETestedJoinAlgo algo, TJoinDescription descr);

i32 ResultColumnCount(ETestedJoinAlgo algo, TJoinDescription descr);
} // namespace NKikimr::NMiniKQL