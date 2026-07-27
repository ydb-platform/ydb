#include <ydb/core/nbs/cloud/blockstore/libs/storage/dbs_controller/dbs_controller_actor.h>

#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NDbsController {

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDbsControllerTest)
{
    Y_UNIT_TEST(ShouldBoot)
    {
        TTestBasicRuntime runtime;
        SetupTabletServices(runtime);

        const ui64 tabletId = MakeTabletID(0, 0, 1);

        CreateTestBootstrapper(
            runtime,
            CreateTestTabletInfo(tabletId, TTabletTypes::DbsController),
            [](const TActorId& tablet, TTabletStorageInfo* info) -> IActor*
            { return new TDbsControllerActor(tablet, info); });

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvTablet::EvBoot, 1);
        runtime.DispatchEvents(options);
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NDbsController
