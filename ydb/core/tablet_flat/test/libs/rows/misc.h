#pragma once

#include <ydb/core/tablet_flat/util_basics.h>
#include <util/generic/vector.h>
#include <util/generic/utility.h>
#include <util/memory/pool.h>

namespace NKikimr {
namespace NTable {
namespace NTest{

    class TGrowHeap: public TAtomicRefCount<TGrowHeap> {
    public:
        explicit TGrowHeap(size_t bytes)
            : Pool(bytes, TMemoryPool::TLinearGrow::Instance())
        {

        }

        TGrowHeap(const TGrowHeap&) = delete;

        void* Alloc(size_t bytes)
        {
            return Pool.Allocate(bytes);
        }

        size_t Used() const noexcept
        {
            return Pool.MemoryAllocated();
        }

    private:
        TMemoryPool Pool;
    };

    template<typename TGen>
    struct TRandomString {
        TRandomString(TGen &gen): Gen(gen) { }

        TString Do(const size_t len) noexcept
        {
            TString line;

            line.reserve(len);

            for (size_t it = 0; it < len; it++) {
                line.push_back(char(Gen.Uniform(65, 90)));
            }

            return line;
        }

        TGen &Gen;
    };
}
}
}
