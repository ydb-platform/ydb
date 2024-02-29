#include <ydb/library/yql/utils/simd/exec/merge_columns/merge.h>

int main() {
    i8* data[4];
    i8* result = nullptr;
    size_t sizes[4];
    size_t length = 64e6;
    Perfomancer perfomancer;

    Cerr << "Best Trait is: ";
    auto worker = ChooseTrait(perfomancer);

    worker->MergeColumns(result, data, sizes, length);
}