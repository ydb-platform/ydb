#include <ydb/core/fq/libs/signer/signer.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NFq;

Y_UNIT_TEST_SUITE(Signer) {
    Y_UNIT_TEST(Basic) {
        TSigner s("abc");
        UNIT_ASSERT_VALUES_EQUAL("b+UDr24riPj5pb7ha2D2AERFvXw=", s.Sign(TSigner::SERVICE_ACCOUNT_ID, "xyz012345"));
        s.Verify(TSigner::SERVICE_ACCOUNT_ID, "xyz012345", "b+UDr24riPj5pb7ha2D2AERFvXw=");
        UNIT_ASSERT_EXCEPTION_CONTAINS(s.Verify(TSigner::UNKNOWN, "xyz012345", "b+UDr24riPj5pb7ha2D2AERFvXw="), yexception, "Incorrect signature for value: xyz012345, signature: b+UDr24riPj5pb7ha2D2AERFvXw=, expected signature: 8O3pYqWEJLvL0TvwQzbEn7dbqzs=");
        UNIT_ASSERT_EXCEPTION_CONTAINS(s.Verify(TSigner::SERVICE_ACCOUNT_ID, "xyz01234", "b+UDr24riPj5pb7ha2D2AERFvXw="), yexception, "Incorrect signature for value: xyz01234, signature: b+UDr24riPj5pb7ha2D2AERFvXw=, expected signature: N637rqjAbKHpvUPeBXRvGkdVKew=");
    }
}
