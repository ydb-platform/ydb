#pragma once

#include "workload_query_gen.h"
#include "stock_workload.h"

#include <memory>

class TWorkloadFactory {
public:
    static std::unique_ptr<IWorkloadQueryGenerator> GetWorkloadQueryGenerator(const std::string& workloadName, const TWorkloadParams* params) {
        if (!params) {
            return nullptr;
        }

        if (workloadName == "stock") {
            return std::unique_ptr<TStockWorkloadGenerator>(TStockWorkloadGenerator::New(static_cast<const TStockWorkloadParams*>(params)));
        } else {
            return nullptr;
        }
    }
};
