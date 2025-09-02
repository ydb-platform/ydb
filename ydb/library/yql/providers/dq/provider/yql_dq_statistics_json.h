#pragma once

#include <yql/essentials/core/yql_execution.h>
#include <library/cpp/yson/writer.h>

namespace NYql {

void CollectTaskRunnerStatisticsByStage(NYson::TYsonWriter& writer, const TOperationStatistics& statistics, bool totalOnly);
void CollectTaskRunnerStatisticsByTask(NYson::TYsonWriter& writer, const TOperationStatistics& statistics);

} // namespace NYql
