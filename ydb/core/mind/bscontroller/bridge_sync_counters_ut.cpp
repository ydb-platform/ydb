#include <library/cpp/testing/unittest/registar.h>

#include "bridge_sync_counters.h"

using namespace NKikimr::NBsController;

Y_UNIT_TEST_SUITE(BridgeSyncCounters) {

    Y_UNIT_TEST(DatabaseLabel) {
        auto root = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        TBridgeSyncCounters counters(root);
        counters.Update({{"pile_1", {}}}, {{{"pile_1", "/Root/db:ssd"}, {}}, {{"pile_1", "static"}, {}}});
        const auto pile = root->GetSubgroup("subsystem", "bsc")->FindSubgroup("pile", "pile_1");
        UNIT_ASSERT(pile);
        const auto database = pile->FindSubgroup("database", "/Root/db");
        UNIT_ASSERT(database && database->FindSubgroup("storagePool", "/Root/db:ssd"));
        const auto staticDatabase = pile->FindSubgroup("database", "static");
        UNIT_ASSERT(staticDatabase && staticDatabase->FindSubgroup("storagePool", "static"));
    }

    Y_UNIT_TEST(OldInstanceKeepsNewerCounters) {
        auto root = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        auto old = std::make_unique<TBridgeSyncCounters>(root);
        TBridgeSyncCounters current(root);
        current.Update({{"pile_1", {}}}, {});
        old.reset();
        const auto bsc = root->FindSubgroup("subsystem", "bsc");
        UNIT_ASSERT(bsc && bsc->FindSubgroup("pile", "pile_1"));
    }

}
