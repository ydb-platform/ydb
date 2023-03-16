#pragma once

#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>

namespace NFq {

bool IsIngress(const NYql::NDqProto::TDqTask& task);

bool IsEgress(const NYql::NDqProto::TDqTask& task);

bool HasState(const NYql::NDqProto::TDqTask& task);

} // namespace NFq
