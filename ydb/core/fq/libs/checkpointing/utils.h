#pragma once

#include <ydb/library/yql/dq/runtime/dq_tasks_runner.h>

namespace NFq {

bool IsIngress(const NYql::NDq::TDqTaskSettings& task);

bool IsEgress(const NYql::NDq::TDqTaskSettings& task);

bool HasState(const NYql::NDq::TDqTaskSettings& task);

} // namespace NFq
