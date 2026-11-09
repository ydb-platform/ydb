#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>

#include <ydb/public/api/protos/ydb_scheme.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::inline Dev::NScheme {

    Y_UNIT_TEST_SUITE(SchemeEntry) {
        Y_UNIT_TEST(UserAttributes) {
            Ydb::Scheme::Entry proto;
            (*proto.mutable_attributes())["__omit_backup"] = "true";
            (*proto.mutable_attributes())["application"] = "value";
            const TSchemeEntry entry(proto);
            UNIT_ASSERT_VALUES_EQUAL(entry.Attributes.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(entry.Attributes.at("__omit_backup"), "true");
            UNIT_ASSERT_VALUES_EQUAL(entry.Attributes.at("application"), "value");
        }

        Y_UNIT_TEST(OlderServerWithoutAttributes) {
            const TSchemeEntry entry{Ydb::Scheme::Entry{}};
            UNIT_ASSERT(entry.Attributes.empty());
        }
    } // Y_UNIT_TEST_SUITE(SchemeEntry)

} // namespace NYdb::inline Dev::NScheme
