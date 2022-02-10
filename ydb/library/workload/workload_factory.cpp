#include "workload_factory.h" 
 
#include "stock_workload.h" 
 
namespace NYdbWorkload { 
 
    std::shared_ptr<IWorkloadQueryGenerator> TWorkloadFactory::GetWorkloadQueryGenerator(const std::string& workloadName,  
        const TWorkloadParams* params)  
    { 
        if (!params) { 
            return nullptr; 
        } 
 
        if (workloadName == "stock") { 
            return std::shared_ptr<TStockWorkloadGenerator>(TStockWorkloadGenerator::New(static_cast<const TStockWorkloadParams*>(params))); 
        } else { 
            return nullptr; 
        } 
    } 
 
} 