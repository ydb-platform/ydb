#include <ydb/library/yql/utils/simd/exec/runtime_dispatching/algo.h>

int main() {
    
    std::vector<std::vector<ui64>> columns(10, std::vector<ui64>(64e6, 1e12 + 7));
    std::vector<ui64> result(64e6);

    Perfomancer perfomancer;

    Cerr << "Best Trait is: ";
    auto worker = ChooseTrait(perfomancer);

    worker->Add(columns, result);
    result.assign(64e6, 0);

    if (NX86::HaveSSE42()) {
        auto SSE42worker = ChooseSSE42Trait(perfomancer);
        SSE42worker->Add(columns, result);
        result.assign(64e6, 0);
    }
    
    if (NX86::HaveAVX2()) {
        auto AVX2worker = ChooseAVX2Trait(perfomancer);
        AVX2worker->Add(columns, result);
        result.assign(64e6, 0);
    }
    
    auto Fallbackworker = ChooseFallbackTrait(perfomancer);
    Fallbackworker->Add(columns, result);
}