#include "ut_helpers.h"
#include "cms_ut_common.h"
#include "ydb/public/api/protos/draft/ydb_maintenance.pb.h"

#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/base/ticket_parser.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/svnversion/svnversion.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/system/hostname.h>

namespace NKikimr::NCmsTest {

using namespace NCms;
using namespace NNodeWhiteboard;
using namespace NKikimrWhiteboard;
using namespace NKikimrCms;
using namespace NKikimrBlobStorage;

Y_UNIT_TEST_SUITE(TMaintenanceApiCmsTest) {

    Y_UNIT_TEST(RequestDecomissionDevice) {
        TCmsTestEnv env(8);
        
        std::unique_ptr<NKikimr::NCms::TEvCms::TEvCreateMaintenanceTaskRequest> req = std::make_unique<NKikimr::NCms::TEvCms::TEvCreateMaintenanceTaskRequest>();

        Ydb::Maintenance::CreateMaintenanceTaskRequest* rec = req->Record.MutableRequest();

        req->Record.SetUserSID("user");

        auto* options = rec->mutable_task_options();
        options->set_availability_mode(::Ydb::Maintenance::AVAILABILITY_MODE_STRONG);

        auto* actionGroup = rec->add_action_groups();
        auto* action = actionGroup->Addactions();
        auto* lockAction = action->mutable_lock_action();

        auto* scope = lockAction->mutable_scope();

        auto* pdiskId = scope->mutable_pdisk_id();
        auto disk = env.PDiskId(0);
        pdiskId->Setnode_id(disk.NodeId);
        pdiskId->Setpdisk_id(disk.DiskId);
        lockAction->mutable_duration()->set_seconds(60);

        env.SendToCms(req.release());

        auto response = env.GrabEdgeEvent<TEvCms::TEvMaintenanceTaskResponse>();

        Cout << response->ToString() << Endl;
        
        env.CheckPermissionRequest
            ("user", false, false, false, true, TStatus::DISALLOW_TEMP,
             MakeAction(TAction::REPLACE_DEVICES, 1, 60000000, env.PDiskName(0)));
    }
}

}