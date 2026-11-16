#include <ydb/public/api/protos/ydb_scheme.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NScheme {

Y_UNIT_TEST_SUITE(Scheme) {
    Y_UNIT_TEST(SerializeInterruptInheritance) {
        for (const bool interruptInheritance : {false, true}) {
            TSchemeEntry entry;
            entry.InterruptInheritance = interruptInheritance;

            Ydb::Scheme::ModifyPermissionsRequest request;
            entry.SerializeTo(request);

            UNIT_ASSERT(request.has_interrupt_inheritance());
            UNIT_ASSERT_VALUES_EQUAL(request.interrupt_inheritance(), interruptInheritance);
        }
    }

    Y_UNIT_TEST(ParseInterruptInheritance) {
        {
            const Ydb::Scheme::ModifyPermissionsRequest request;
            const TModifyPermissionsSettings settings(request);

            UNIT_ASSERT(!settings.SetInterruptInheritance_);
        }

        for (const bool interruptInheritance : {false, true}) {
            Ydb::Scheme::ModifyPermissionsRequest request;
            request.set_interrupt_inheritance(interruptInheritance);

            const TModifyPermissionsSettings settings(request);

            UNIT_ASSERT(settings.SetInterruptInheritance_);
            UNIT_ASSERT_VALUES_EQUAL(settings.InterruptInheritanceValue_, interruptInheritance);
        }
    }
}

} // namespace NYdb::NScheme
