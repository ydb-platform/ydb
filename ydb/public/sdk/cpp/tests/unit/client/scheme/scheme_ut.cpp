#include <ydb/library/testlib/helpers.h>
#include <ydb/public/api/protos/ydb_scheme.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NScheme {

Y_UNIT_TEST_SUITE(Scheme) {
    Y_UNIT_TEST_TWIN(SerializeInterruptInheritance, InterruptInheritance) {
        TSchemeEntry entry;
        entry.InterruptInheritance = InterruptInheritance;

        Ydb::Scheme::ModifyPermissionsRequest request;
        entry.SerializeTo(request);

        UNIT_ASSERT(request.has_interrupt_inheritance());
        UNIT_ASSERT_VALUES_EQUAL(request.interrupt_inheritance(), InterruptInheritance);
    }

    Y_UNIT_TEST(ParseAbsentInterruptInheritance) {
        const Ydb::Scheme::ModifyPermissionsRequest request;
        const TModifyPermissionsSettings settings(request);

        UNIT_ASSERT(!settings.SetInterruptInheritance_);
    }

    Y_UNIT_TEST_TWIN(ParseInterruptInheritance, InterruptInheritance) {
        Ydb::Scheme::ModifyPermissionsRequest request;
        request.set_interrupt_inheritance(InterruptInheritance);

        const TModifyPermissionsSettings settings(request);

        UNIT_ASSERT(settings.SetInterruptInheritance_);
        UNIT_ASSERT_VALUES_EQUAL(settings.InterruptInheritanceValue_, InterruptInheritance);
    }
}

} // namespace NYdb::NScheme
