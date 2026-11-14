#include "sanitizer_utils/sanitizer_utils.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/ptr.h>

namespace NYql::NUdf {

Y_UNIT_TEST_SUITE(TUdfSanitizerUtils) {
Y_UNIT_TEST(AllocationRoundTrip) {
    for (size_t size : {0, 1, 15, 16, 17, 64}) {
        const size_t allocationSize = GetSizeToAlloc(size);
        TArrayHolder<char> allocation(new char[allocationSize]);
        char* data = static_cast<char*>(WrapPointerWithRedZones(allocation.Get(), allocationSize));
        UNIT_ASSERT_VALUES_EQUAL(GetOriginalAllocatedObject(data, size == 0), allocation.Get());
        for (size_t index = 0; index < size; ++index) {
            data[index] = static_cast<char>(index);
        }
        {
            YQL_MSAN_FREEZE_AND_SCOPED_UNPOISON(data, size);
            for (size_t index = 0; index < size; ++index) {
                UNIT_ASSERT_VALUES_EQUAL(data[index], static_cast<char>(index));
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(UnwrapPointerWithRedZones(data, size), allocation.Get());
        SanitizerMakeRegionAccessible(allocation.Get(), allocationSize);
    }
}

Y_UNIT_TEST(EmptyNullAllocation) {
    UNIT_ASSERT_VALUES_EQUAL(GetSizeToAlloc(0), 0);
    UNIT_ASSERT(WrapPointerWithRedZones(nullptr, 0) == nullptr);
    UNIT_ASSERT(UnwrapPointerWithRedZones(nullptr, 0) == nullptr);
    UNIT_ASSERT(GetOriginalAllocatedObject(nullptr, true) == nullptr);
    UNIT_ASSERT(SanitizerMakeRegionInaccessible(nullptr, 0) == nullptr);
    UNIT_ASSERT(SanitizerMakeRegionAccessible(nullptr, 0) == nullptr);
}
} // Y_UNIT_TEST_SUITE(TUdfSanitizerUtils)

} // namespace NYql::NUdf
