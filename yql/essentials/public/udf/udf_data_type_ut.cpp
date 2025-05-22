#include "udf_data_type.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NYql::NUdf;

Y_UNIT_TEST_SUITE(TCastsTests) {
    Y_UNIT_TEST(HasAllCasts) {
        for (ui32 source = 0; source < DataSlotCount; ++source) {
            for (ui32 target = 0; target < DataSlotCount; ++target) {
                UNIT_ASSERT_C(GetCastResult((EDataSlot)source, (EDataSlot)target),
                    "Missing cast from " << (EDataSlot)source << " to " << (EDataSlot)target);
            }
        }
    }
}
