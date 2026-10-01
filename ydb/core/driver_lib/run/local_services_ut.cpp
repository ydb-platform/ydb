#include "kikimr_services_initializers.h"

#include <ydb/core/mind/local.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKikimrServicesInitializers {
Y_UNIT_TEST_SUITE(LocalTabletRegistration) {
    Y_UNIT_TEST(PreserveAvailabilityOverridesAndDefaults) {
        NKikimrConfig::TAppConfig config;
        TAppData appData(1, 2, 3, 4, {}, nullptr, nullptr, nullptr, nullptr);
        const auto make = [&]() {
            TKikimrRunConfig run(config, 17);
            return TLocalServiceInitializer(run).BuildLocalConfig(&appData);
        };
        auto defaults = make();
        UNIT_ASSERT(defaults->TabletClassInfo.contains(TTabletTypes::ColumnShard));
        UNIT_ASSERT(defaults->TabletClassInfo.contains(TTabletTypes::DataShard));
        UNIT_ASSERT(!defaults->TabletClassInfo.at(TTabletTypes::ColumnShard).MaxCount);
        for (ui64 limit : {0ull, 7ull}) {
            config.MutableDynamicNodeConfig()->ClearTabletAvailability();
            auto* availability = config.MutableDynamicNodeConfig()->AddTabletAvailability();
            availability->SetType(TTabletTypes::ColumnShard);
            availability->SetMaxCount(limit);
            availability->SetPriority(19);
            auto configured = make();
            const auto& column = configured->TabletClassInfo.at(TTabletTypes::ColumnShard);
            UNIT_ASSERT(column.SetupInfo);
            UNIT_ASSERT(column.MaxCount);
            UNIT_ASSERT_VALUES_EQUAL(*column.MaxCount, limit);
            UNIT_ASSERT_VALUES_EQUAL(column.Priority, 19);
            UNIT_ASSERT(!configured->TabletClassInfo.at(TTabletTypes::DataShard).MaxCount);
            UNIT_ASSERT_VALUES_EQUAL(configured->TabletClassInfo.size(), defaults->TabletClassInfo.size());
        }
        UNIT_ASSERT_VALUES_EQUAL(defaults->DrainNodeTimeout, TDuration::Seconds(30));
        for (ui32 seconds : {0u, 97u}) {
            config.MutableShutdownConfig()->SetDrainTimeoutSeconds(seconds);
            UNIT_ASSERT_VALUES_EQUAL(make()->DrainNodeTimeout, TDuration::Seconds(seconds));
        }
    }
}
}
