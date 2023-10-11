#include "mkql_computation_node_ut.h"
#include <util/system/cpu_id.h>

namespace NKikimr {
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLIsaDetection) {
    
    Y_UNIT_TEST_LLVM(TestAVX2) {
        UNIT_ASSERT_VALUES_EQUAL(NX86::HaveAVX2(), true);
    }

    Y_UNIT_TEST_LLVM(TestSSE42) {
        UNIT_ASSERT_VALUES_EQUAL(NX86::HaveSSE42(), true);
    }
}

}
}