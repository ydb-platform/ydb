#include <ydb/library/yql/providers/dq/actors/execution_helpers.h>
#include <ydb/library/yql/providers/dq/runtime/task_command_executor.h>

#include <ydb/library/yql/providers/yt/comp_nodes/dq/dq_yt_factory.h>
#include <ydb/library/yql/providers/yt/mkql_dq/yql_yt_dq_transform.h>

#include <ydb/library/yql/providers/common/comp_nodes/yql_factory.h>

#include <ydb/library/yql/dq/comp_nodes/yql_common_dq_factory.h>
#include <ydb/library/yql/dq/integration/transform/yql_dq_task_transform.h>
#include <ydb/library/yql/dq/transform/yql_common_dq_transform.h>

#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>
#include <ydb/library/yql/tools/dq/worker_job/dq_worker.h>
#include <ydb/library/yql/utils/backtrace/backtrace.h>

#include <library/cpp/svnversion/svnversion.h>

#include <library/cpp/yt/mlock/mlock.h>

using namespace NYql;

int main(int argc, const char* argv[]) {
    NBacktrace::RegisterKikimrFatalActions();
    NBacktrace::EnableKikimrSymbolize();

    if (!NYT::MlockFileMappings()) {
        Cerr << "mlockall failed, but that's fine" << Endl;
    }

    if (argc > 1) {
        if (!strcmp(argv[1], "-V")) {
            Cerr << ToString(GetProgramCommitId()) << Endl;
            return 0;
        } else if (!strcmp(argv[1], "tasks_runner_proxy")) {
            NKikimr::NMiniKQL::IStatsRegistryPtr statsRegistry = NKikimr::NMiniKQL::CreateDefaultStatsRegistry();

            auto dqCompFactory = NKikimr::NMiniKQL::GetCompositeWithBuiltinFactory({
                GetCommonDqFactory(),
                GetDqYtFactory(statsRegistry.Get()),
                NKikimr::NMiniKQL::GetYqlFactory(),
            });

            auto dqTaskTransformFactory = CreateCompositeTaskTransformFactory({
                CreateCommonDqTaskTransformFactory(),
                CreateYtDqTaskTransformFactory(),
            });

            return NTaskRunnerProxy::CreateTaskCommandExecutor(dqCompFactory, dqTaskTransformFactory, statsRegistry.Get(), true);
        }
    }

    try {
        NYT::Initialize(argc, argv);

        auto job = new NDq::NWorker::TWorkerJob();

        job->Do();
    } catch (...) {
        Cerr << CurrentExceptionMessage();
        return -1;
    }

    return 0;
}
