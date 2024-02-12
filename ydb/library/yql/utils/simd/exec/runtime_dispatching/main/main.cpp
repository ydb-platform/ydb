#include <ydb/library/yql/utils/simd/exec/runtime_dispatching/algo.h>

int main() {
    
    std::vector<std::vector<ui64>> columns(10, std::vector<ui64>(64e6, 1e8));
    std::vector<ui64> result(64e6);

    Perfomancer perfomancer;

    auto worker = ChooseTrait(perfomancer);

    worker->Add(columns, result);
}