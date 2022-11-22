#include <ydb/library/rewrapper/re.h>
#include <ydb/library/rewrapper/proto/serialization.pb.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/regex/hyperscan/hyperscan.h>

namespace NReWrapper {
namespace NDispatcher {

Y_UNIT_TEST_SUITE(ReWrapperDispatcherTestHyperscan) {
    Y_UNIT_TEST(LegacySerialization) {
        unsigned int hyperscanFlags = 0;
        hyperscanFlags |= HS_FLAG_UTF8;
        if (NX86::HaveAVX2()) {
            hyperscanFlags |= HS_CPU_FEATURES_AVX2;
        }
        auto database = ::NHyperscan::Compile("[0-9]+", hyperscanFlags);
        auto string = ::NHyperscan::Serialize(database);

        auto wrapper = Deserialize(string);
        UNIT_ASSERT_VALUES_EQUAL(wrapper->Matches("123"), true);
        UNIT_ASSERT_VALUES_EQUAL(wrapper->Matches("abc"), false);
    }
    Y_UNIT_TEST(Serialization) {
        auto w1 = Compile("[0-9]+", 0, NReWrapper::TSerialization::kHyperscan);
        auto string = w1->Serialize();

        auto w2 = Deserialize(string);
        UNIT_ASSERT_VALUES_EQUAL(w1->Matches("123"), true);
        UNIT_ASSERT_VALUES_EQUAL(w1->Matches("abc"), false);
        UNIT_ASSERT_VALUES_EQUAL(w2->Matches("123"), true);
        UNIT_ASSERT_VALUES_EQUAL(w2->Matches("abc"), false);
    }
}

}
}
