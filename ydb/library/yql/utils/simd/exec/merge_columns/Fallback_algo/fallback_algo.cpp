#include <ydb/library/yql/utils/simd/exec/merge_columns/merge.h>

template<>
THolder<Perfomancer::Interface> Perfomancer::Create<FallbackTrait>() {
    Cerr << "FallbackTrait ";
    return MakeHolder<Algo<FallbackTrait>>();
}