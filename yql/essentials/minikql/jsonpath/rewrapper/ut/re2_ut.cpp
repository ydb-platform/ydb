#include <yql/essentials/minikql/jsonpath/rewrapper/re.h>
#include <yql/essentials/minikql/jsonpath/rewrapper/proto/serialization.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NReWrapper::NDispatcher {

Y_UNIT_TEST_SUITE(ReWrapperDispatcherRe2) {
Y_UNIT_TEST(Serialization) {
    auto w1 = Compile("[0-9]+", 0, NReWrapper::TSerialization::kRe2);
    auto string = w1->Serialize();

    auto w2 = Deserialize(string);
    UNIT_ASSERT_VALUES_EQUAL(w1->Matches("123"), true);
    UNIT_ASSERT_VALUES_EQUAL(w1->Matches("abc"), false);
    UNIT_ASSERT_VALUES_EQUAL(w2->Matches("123"), true);
    UNIT_ASSERT_VALUES_EQUAL(w2->Matches("abc"), false);
}
} // Y_UNIT_TEST_SUITE(ReWrapperDispatcherRe2)

} // namespace NReWrapper::NDispatcher
