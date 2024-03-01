#include <ydb/library/yql/utils/simd/simd.h>

int main() {
    i8* pt1 = new i8[1000];
    i8* pt2 = new i8[1000];
    i8* pt3 = new i8[1000];
    i8* pt4 = new i8[1000];
    
    i8* data[4]{pt1, pt2, pt3, pt4};
    i8* result = new i8[124897];
    size_t sizes[4]{1, 1, 1, 1};
    size_t length = 64;
    NSimd::Perfomancer perfomancer;

    Cerr << "Best Trait is: ";
    auto worker = ChooseTrait(perfomancer);

    worker->MergeColumns(result, data, sizes, length);
}