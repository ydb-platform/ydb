#include "dsproxy_fault_tolerance_ut_run.h"
#include "dsproxy_fault_tolerance_ut_discover.h"
#include "dsproxy_fault_tolerance_ut_range.h"
#include "dsproxy_fault_tolerance_ut_put.h"

Y_UNIT_TEST_SUITE(TBsProxyFaultToleranceTest) {

#define FAULT_TOLERANCE_TEST(ERASURE, T) \
    Y_UNIT_TEST(Check##T##ERASURE) { \
        RunTest<T>(TBlobStorageGroupType::ERASURE); \
    }

#define ERASURE_TEST(ERASURE) \
    FAULT_TOLERANCE_TEST(ERASURE, TRangeFaultToleranceTest) \
    FAULT_TOLERANCE_TEST(ERASURE, TDiscoverFaultToleranceTest) \
    FAULT_TOLERANCE_TEST(ERASURE, TPutFaultToleranceTest)

    ERASURE_TEST(Erasure4Plus2Block)
    ERASURE_TEST(ErasureMirror3dc)
    ERASURE_TEST(ErasureMirror3of4)

}
