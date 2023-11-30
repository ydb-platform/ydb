#pragma once

#include <ydb/library/yql/dq/tasks/dq_connection_builder.h>
#include <ydb/library/yql/dq/tasks/dq_tasks_graph.h>

#include <ydb/library/actors/core/actorid.h>

namespace NYql::NDqs {
    struct TStageInfoMeta {
        NNodes::TDqPhyStage Stage;
    };

    struct TGraphMeta {};

    struct TTaskInputMeta {
    };

    struct TTaskOutputMeta {};

    struct TTaskMeta {
        THashMap<TString, TString> TaskParams;
        TString ClusterNameHint;
    };

    using TStageInfo = NYql::NDq::TStageInfo<TStageInfoMeta>;
    using TTaskOutput = NYql::NDq::TTaskOutput<TTaskOutputMeta>;
    using TTaskOutputType = NYql::NDq::TTaskOutputType;
    using TTaskInput = NYql::NDq::TTaskInput<TTaskInputMeta>;
    using TTask = NYql::NDq::TTask<TStageInfoMeta, TTaskMeta, TTaskInputMeta, TTaskOutputMeta>;
    using TDqsTasksGraph = NYql::NDq::TDqTasksGraph<TGraphMeta, TStageInfoMeta, TTaskMeta, TTaskInputMeta, TTaskOutputMeta>;
}
