#pragma once

#include "vector_workload_params.h"
#include "vector_recall_evaluator.h"

#include <ydb/library/workload/abstract/workload_query_generator.h>

#include <cctype>
#include <mutex>
#include <vector>
#include <string>
#include <atomic>

namespace NYdbWorkload {

enum class EWorkloadRunType {
    Upsert,
    Select
};

} // namespace NYdbWorkload
