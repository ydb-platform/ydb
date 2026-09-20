#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/mon_auth.h>

using namespace NKikimr;

namespace {

class TTabletDevUiMonAccessFixture {
public:
    TTabletDevUiMonAccessFixture()
        : AppData(0, 0, 0, 0, TMap<TString, ui32>{}, nullptr, nullptr, nullptr, nullptr)
    {}

    const TAppData* GetAppData() const {
        return &AppData;
    }

    void SetEnableTabletDevUiSecurePath(bool value) {
        AppData.FeatureFlags.SetEnableTabletDevUiSecurePath(value);
    }

private:
    TAppData AppData;
};

} // namespace

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

    Y_UNIT_TEST(HasTabletDevUiSecureSubtree) {
        TTabletDevUiMonAccessFixture fixture;

        for (const auto type : {
            TTabletTypes::DataShard,
            TTabletTypes::Hive,
            TTabletTypes::GraphShard,
            TTabletTypes::BSController,
            TTabletTypes::SchemeShard,
        }) {
            fixture.SetEnableTabletDevUiSecurePath(false);
            UNIT_ASSERT(!HasTabletDevUiSecureSubtree(fixture.GetAppData(), type));

            fixture.SetEnableTabletDevUiSecurePath(true);
            UNIT_ASSERT(HasTabletDevUiSecureSubtree(fixture.GetAppData(), type));
        }

        for (const auto type : {
            TTabletTypes::Coordinator,
        }) {
            fixture.SetEnableTabletDevUiSecurePath(false);
            UNIT_ASSERT(!HasTabletDevUiSecureSubtree(fixture.GetAppData(), type));

            fixture.SetEnableTabletDevUiSecurePath(true);
            UNIT_ASSERT(!HasTabletDevUiSecureSubtree(fixture.GetAppData(), type));
        }
    }

    Y_UNIT_TEST(IsTabletDevUiAppPageAdminOnly) {
        TTabletDevUiMonAccessFixture fixture;

        for (const auto type : {
            TTabletTypes::DataShard,
            TTabletTypes::BSController,
            TTabletTypes::Hive,
        }) {
            fixture.SetEnableTabletDevUiSecurePath(false);
            UNIT_ASSERT(!IsTabletDevUiAppPageAdminOnly(fixture.GetAppData(), type));

            fixture.SetEnableTabletDevUiSecurePath(true);
            UNIT_ASSERT(IsTabletDevUiAppPageAdminOnly(fixture.GetAppData(), type));
        }

        for (const auto type : {
            TTabletTypes::GraphShard,
            TTabletTypes::SchemeShard,
            TTabletTypes::Coordinator,
        }) {
            fixture.SetEnableTabletDevUiSecurePath(false);
            UNIT_ASSERT(!IsTabletDevUiAppPageAdminOnly(fixture.GetAppData(), type));

            fixture.SetEnableTabletDevUiSecurePath(true);
            UNIT_ASSERT(!IsTabletDevUiAppPageAdminOnly(fixture.GetAppData(), type));
        }
    }
}
