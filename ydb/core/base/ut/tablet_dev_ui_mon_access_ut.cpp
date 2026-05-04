#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/base/tablet_dev_ui_mon_access.h>

using namespace NKikimr;

Y_UNIT_TEST_SUITE(TabletDevUiMonAccess) {
    Y_UNIT_TEST(IsTabletDevUiSecurePath) {
        // true
        UNIT_ASSERT(::NKikimr::IsTabletDevUiSecurePath("/app/secure"));
        UNIT_ASSERT(::NKikimr::IsTabletDevUiSecurePath("/app/secure/"));
        UNIT_ASSERT(::NKikimr::IsTabletDevUiSecurePath("/app/secure/some/endpoint"));

        // false
        UNIT_ASSERT(!::NKikimr::IsTabletDevUiSecurePath("/app"));
        UNIT_ASSERT(!::NKikimr::IsTabletDevUiSecurePath("/app/secured"));
        UNIT_ASSERT(!::NKikimr::IsTabletDevUiSecurePath("/app/secure-ish"));
        UNIT_ASSERT(!::NKikimr::IsTabletDevUiSecurePath("app/secure"));
        UNIT_ASSERT(!::NKikimr::IsTabletDevUiSecurePath("/x/app/secure"));
    }
}
