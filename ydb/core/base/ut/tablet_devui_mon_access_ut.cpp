#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/base/tablet_devui_mon_access.h>

using namespace NKikimr;

Y_UNIT_TEST_SUITE(TabletDevUiMonAccess) {
    Y_UNIT_TEST(RecognizesSecurePrefix) {
        UNIT_ASSERT(IsTabletDevUiSecurePathInfo("/app/secure"));
        UNIT_ASSERT(IsTabletDevUiSecurePathInfo("/app/secure/"));
        UNIT_ASSERT(IsTabletDevUiSecurePathInfo("/app/secure/some/endpoint"));
    }

    Y_UNIT_TEST(RejectsNonSecurePaths) {
        UNIT_ASSERT(!IsTabletDevUiSecurePathInfo("/app"));
        UNIT_ASSERT(!IsTabletDevUiSecurePathInfo("/app/secured"));
        UNIT_ASSERT(!IsTabletDevUiSecurePathInfo("/app/secure-ish"));
        UNIT_ASSERT(!IsTabletDevUiSecurePathInfo("app/secure"));
        UNIT_ASSERT(!IsTabletDevUiSecurePathInfo("/x/app/secure"));
    }
}
