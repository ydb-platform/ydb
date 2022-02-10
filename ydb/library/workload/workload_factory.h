#pragma once 
 
#include "workload_query_generator.h" 
 
#include <memory> 
 
namespace NYdbWorkload { 
 
class TWorkloadFactory { 
public: 
    std::shared_ptr<IWorkloadQueryGenerator> GetWorkloadQueryGenerator(const std::string& workloadName, const TWorkloadParams* params); 
}; 
 
} // namespace NYdbWorkload 