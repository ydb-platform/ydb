#include "mkql_alloc.h" 
#include "mkql_string_util.h" 
 
#include <library/cpp/testing/unittest/registar.h> 
 
using namespace NYql; 
using namespace NKikimr::NMiniKQL; 
 
Y_UNIT_TEST_SUITE(TMiniKQLStringUtils) { 
    Y_UNIT_TEST(SubstringWithLargeOffset) { 
        TScopedAlloc alloc; 
        const auto big = MakeStringNotFilled(NUdf::TUnboxedValuePod::OffsetLimit << 1U); 
        const auto sub0 = SubString(big, 1U, 42U); 
        const auto sub1 = SubString(big, NUdf::TUnboxedValuePod::OffsetLimit - 1U, 42U); 
        const auto sub2 = SubString(big, NUdf::TUnboxedValuePod::OffsetLimit, 42U); 
 
        UNIT_ASSERT(sub0.AsStringValue().Data() == sub1.AsStringValue().Data()); 
        UNIT_ASSERT(sub1.AsStringValue().Data() != sub2.AsStringValue().Data()); 
    } 
} 
 
