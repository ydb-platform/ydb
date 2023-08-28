#pragma once

#include "workload_query_generator.h"

#include <memory>

namespace NYdbWorkload {

enum class EWorkload {
    STOCK,
    KV,
    TPCC,
};

class TWorkloadFactory {
public:
    std::shared_ptr<IWorkloadQueryGenerator> GetWorkloadQueryGenerator(const EWorkload& type, const TWorkloadParams* params);
};

} // namespace NYdbWorkload