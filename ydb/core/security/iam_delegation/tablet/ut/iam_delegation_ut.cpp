#include "ut_helpers.h"

#include <util/generic/vector.h>

namespace NKikimr::NIamDelegation::NTests {
namespace {

namespace NProto = NKikimrIamDelegation;

NProto::TDatabaseIdentity DatabaseIdentity(
    const TString& incarnation = "database-incarnation-1",
    const TString& path = "/Root/database",
    const TString& databaseId = "database-id-1")
{
    NProto::TDatabaseIdentity identity;
    identity.SetIncarnation(incarnation);
    identity.SetPath(path);
    identity.SetDatabaseId(databaseId);
    return identity;
}

NProto::TRequest RegisterRequest(const NProto::TDatabaseIdentity& identity) {
    NProto::TRequest request;
    request.MutableRegisterDatabase()->MutableIdentity()->CopyFrom(identity);
    return request;
}

} // namespace

Y_UNIT_TEST_SUITE(IamDelegationTablet) {
    Y_UNIT_TEST(DatabaseRegistrationIsDurableAndImmutable) {
        TTestContext ctx;
        for (const TString path : {"/", "/Root/A/", "/Root//A", "/Root/../A"}) {
            ctx.Call(RegisterRequest(DatabaseIdentity("invalid", path, "")), NProto::INVALID_ARGUMENT);
        }
        const auto identity = DatabaseIdentity();
        const auto registered = ctx.Call(RegisterRequest(identity));
        UNIT_ASSERT_VALUES_EQUAL(registered.GetDatabase().GetIdentity().SerializeAsString(), identity.SerializeAsString());
        ctx.Reboot();
        const auto restored = ctx.Call(RegisterRequest(identity));
        UNIT_ASSERT_VALUES_EQUAL(restored.SerializeAsString(), registered.SerializeAsString());
        auto conflicting = identity;
        conflicting.SetDatabaseId("different-database-id");
        ctx.Call(RegisterRequest(conflicting), NProto::CONFLICT);
        const auto second = DatabaseIdentity("database-incarnation-2", identity.GetPath(), "");
        const auto replacement = ctx.Call(RegisterRequest(second));
        UNIT_ASSERT_VALUES_EQUAL(replacement.GetDatabase().GetIdentity().GetIncarnation(), second.GetIncarnation());
    }

}

} // namespace NKikimr::NIamDelegation::NTests
