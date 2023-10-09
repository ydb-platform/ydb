#include "task_emulator.h"
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/testlib/cs_helper.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/services/bg_tasks/abstract/activity.h>
#include <ydb/services/bg_tasks/abstract/task.h>
#include <ydb/services/bg_tasks/service.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <util/system/hostname.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(BGTaskTests) {
    Y_UNIT_TEST(DSRunTask) {
        TPortManager pm;

        ui32 grpcPort = pm.GetPort();
        ui32 msgbPort = pm.GetPort();

        Tests::TServerSettings serverSettings(msgbPort);
        serverSettings.Port = msgbPort;
        serverSettings.GrpcPort = grpcPort;
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableMetadataProvider(true)
            .SetEnableBackgroundTasks(true)
            .SetEnableOlapSchemaOperations(true);
        ;

        Tests::TServer::TPtr server = new Tests::TServer(serverSettings);
        server->EnableGRpc(grpcPort);
        Tests::TClient client(serverSettings);

        auto& runtime = *server->GetRuntime();

        auto sender = runtime.AllocateEdgeActor();
        server->SetupRootStoragePools(sender);

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_NOTICE);
        runtime.SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NLog::PRI_INFO);
        runtime.SetLogPriority(NKikimrServices::METADATA_PROVIDER, NLog::PRI_INFO);
        runtime.SetLogPriority(NKikimrServices::BG_TASKS, NLog::PRI_DEBUG);
        //        runtime.SetLogPriority(NKikimrServices::TX_PROXY_SCHEME_CACHE, NLog::PRI_DEBUG);
        runtime.SimulateSleep(TDuration::Seconds(1));
        Cerr << "Initialization finished" << Endl;

        TString activityId;
        {
            std::shared_ptr<TTestInsertTaskActivity> tActivity(new TTestInsertTaskActivity);
            activityId = tActivity->GetActivityTaskId();
            std::shared_ptr<TTestInsertTaskScheduler> tScheduler(new TTestInsertTaskScheduler);
            NBackgroundTasks::TTask newTask(tActivity, tScheduler);
            runtime.SendAsync(new IEventHandle(NBackgroundTasks::MakeServiceId(1), {}, new NBackgroundTasks::TEvAddTask(std::move(newTask))));
        }
        TDispatchOptions rmReady;
        rmReady.CustomFinalCondition = [activityId] {
            Y_ABORT_UNLESS(TTestInsertTaskActivity::GetCounterSum(activityId) <= 6);
            if (TTestInsertTaskActivity::IsFinished(activityId)) {
                Y_ABORT_UNLESS(TTestInsertTaskActivity::GetCounterSum(activityId) == 6);
                return true;
            } else {
                Cerr << "COUNTER_SUM:" << TTestInsertTaskActivity::GetCounterSum(activityId) << Endl;
            }
            return false;
        };
        Y_ABORT_UNLESS(runtime.DispatchEvents(rmReady, TDuration::Seconds(30)));
    }

    Y_UNIT_TEST(NoArtefactsBeforeUsing) {
        TPortManager pm;

        ui32 grpcPort = pm.GetPort();
        ui32 msgbPort = pm.GetPort();

        Tests::TServerSettings serverSettings(msgbPort);
        serverSettings.Port = msgbPort;
        serverSettings.GrpcPort = grpcPort;
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableMetadataProvider(true)
            .SetEnableBackgroundTasks(true)
            .SetEnableOlapSchemaOperations(true);
        ;

        Tests::TServer::TPtr server = new Tests::TServer(serverSettings);
        server->EnableGRpc(grpcPort);
        Tests::TClient client(serverSettings);

        auto& runtime = *server->GetRuntime();

        auto sender = runtime.AllocateEdgeActor();
        server->SetupRootStoragePools(sender);

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_NOTICE);
        runtime.SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NLog::PRI_INFO);
        runtime.SetLogPriority(NKikimrServices::METADATA_PROVIDER, NLog::PRI_INFO);
        runtime.SetLogPriority(NKikimrServices::BG_TASKS, NLog::PRI_DEBUG);
        //        runtime.SetLogPriority(NKikimrServices::TX_PROXY_SCHEME_CACHE, NLog::PRI_DEBUG);
        for (ui32 i = 0; i < 100; ++i) {
            runtime.SimulateSleep(TDuration::Seconds(1));
        }
        Cerr << "Initialization finished" << Endl;

        Tests::NCS::THelper lHelper(*server);
        lHelper.StartDataRequest("SELECT * FROM `/Root/.metadata/initialization/migrations`", false);
        lHelper.StartDataRequest("SELECT * FROM `/Root/.bg_tasks/tasks`", false);

        {
            TString activityId;
            {
                std::shared_ptr<TTestInsertTaskActivity> tActivity(new TTestInsertTaskActivity);
                activityId = tActivity->GetActivityTaskId();
                std::shared_ptr<TTestInsertTaskScheduler> tScheduler(new TTestInsertTaskScheduler);
                NBackgroundTasks::TTask newTask(tActivity, tScheduler);
                runtime.SendAsync(new IEventHandle(NBackgroundTasks::MakeServiceId(1), {}, new NBackgroundTasks::TEvAddTask(std::move(newTask))));
            }
            TDispatchOptions rmReady;
            rmReady.CustomFinalCondition = [activityId] {
                Y_ABORT_UNLESS(TTestInsertTaskActivity::GetCounterSum(activityId) <= 6);
                if (TTestInsertTaskActivity::IsFinished(activityId)) {
                    Y_ABORT_UNLESS(TTestInsertTaskActivity::GetCounterSum(activityId) == 6);
                    return true;
                } else {
                    Cerr << "COUNTER_SUM:" << TTestInsertTaskActivity::GetCounterSum(activityId) << Endl;
                }
                return false;
            };
            Y_ABORT_UNLESS(runtime.DispatchEvents(rmReady, TDuration::Seconds(30)));
        }
        lHelper.StartDataRequest("SELECT * FROM `/Root/.bg_tasks/tasks`", true);
        lHelper.StartDataRequest("SELECT * FROM `/Root/.metadata/initialization/migrations`", true);
    }
}
}
