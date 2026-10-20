#pragma once

#include <ydb/core/protos/kqp.pb.h>
#include <ydb/library/yql/dq/task_scheduler/task_graph.h>

#include <memory>

namespace NKikimr::NKqp {

void SerializeSchedulingGraph(const NYql::NDq::TQueryTaskGraph& graph, NKikimrKqp::TTaskSchedulingGraph& result);
std::shared_ptr<const NYql::NDq::TQueryTaskGraph> DeserializeSchedulingGraph(const NKikimrKqp::TTaskSchedulingGraph& graph);

} // namespace NKikimr::NKqp
