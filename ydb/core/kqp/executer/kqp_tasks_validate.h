#pragma once

#include "kqp_executer_impl.h"
#include "kqp_tasks_graph.h"

namespace NKikimr {
namespace NKqp {

bool ValidateTasks(const TKqpTasksGraph& tasksGraph, const EExecType& execType, bool enableSpilling, NYql::TIssue& issue);

} // namespace NKqp
} // namespace NKikimr
