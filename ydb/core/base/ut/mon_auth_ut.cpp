#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/base/mon_auth.h>

using namespace NKikimr;

Y_UNIT_TEST_SUITE(TabletDevUiMonAccess) {
    Y_UNIT_TEST(IsTabletDevUiSecurePath) {
        // true
        UNIT_ASSERT(IsTabletDevUiSecurePath("/app/secure"));
        UNIT_ASSERT(IsTabletDevUiSecurePath("/app/secure/"));
        UNIT_ASSERT(IsTabletDevUiSecurePath("/app/secure/some/endpoint"));

        // false
        UNIT_ASSERT(!IsTabletDevUiSecurePath("/app"));
        UNIT_ASSERT(!IsTabletDevUiSecurePath("/app/secured"));
        UNIT_ASSERT(!IsTabletDevUiSecurePath("/app/secure-ish"));
        UNIT_ASSERT(!IsTabletDevUiSecurePath("app/secure"));
        UNIT_ASSERT(!IsTabletDevUiSecurePath("/x/app/secure"));
    }

    Y_UNIT_TEST(UsesTabletDevUiSecurePath) {
        UNIT_ASSERT(UsesTabletDevUiSecurePath(TTabletTypes::DataShard));
        UNIT_ASSERT(UsesTabletDevUiSecurePath(TTabletTypes::Hive));
        UNIT_ASSERT(UsesTabletDevUiSecurePath(TTabletTypes::GraphShard));

        UNIT_ASSERT(!UsesTabletDevUiSecurePath(TTabletTypes::BSController));
        UNIT_ASSERT(!UsesTabletDevUiSecurePath(TTabletTypes::Coordinator));
    }
}
