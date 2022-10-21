#include <util/datetime/cputimer.h>
#include <ydb/library/yql/public/udf/udf_allocator.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;
using namespace NKikimr::NUdf;

namespace {
    inline void MyFree(const void* p, size_t) { return free(const_cast<void*>(p)); }

    template <void*(*Alloc)(ui64), void (*Free)(const void*,ui64)>
    void Test(const TString& name) {
        TSimpleTimer timer;
        for (ui32 i = 0; i < 100000000; ++i) {
            std::array<void*, 10> strs;
            for (ui32 j = 0; j < strs.size(); ++j) {
                void* p = Alloc(32);
                *((volatile char*)p) = 0xff;
                strs[j] = p;
            }

            for (ui32 j = 0; j < strs.size(); ++j) {
                void* p = strs[j];
                Free(p, 32);
            }
        }

        Cerr << "[" << name << "] Elapsed: " << timer.Get() << "\n";
    }
}

int main(int, char**) {
    Test<&malloc, &MyFree>("malloc");
    {
        TScopedAlloc sopedAlloc(__LOCATION__);
        Test<&UdfAllocateWithSize, &UdfFreeWithSize>("mkql");
    }
    return 0;
}
