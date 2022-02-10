#pragma once
#include <ydb/library/yql/public/issue/yql_issue.h>

#include <ydb/library/yql/dq/proto/dq_state_load_plan.pb.h>
#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>

#include <util/generic/hash.h>

namespace NYql::NDq {

// Make plan for loading streaming offsets from an old graph.
bool MakeContinueFromStreamingOffsetsPlan(const google::protobuf::RepeatedPtrField<NYql::NDqProto::TDqTask>& src, const google::protobuf::RepeatedPtrField<NYql::NDqProto::TDqTask>& dst, bool force, THashMap<ui64, NDqProto::NDqStateLoadPlan::TTaskPlan>& plan, TIssues& issues);

} // namespace NYql::NDq
