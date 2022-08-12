#include "workload_factory.h"

#include "stock_workload.h"

namespace NYdbWorkload {

    std::shared_ptr<IWorkloadQueryGenerator> TWorkloadFactory::GetWorkloadQueryGenerator(const EWorkload& type , const TWorkloadParams* params) 
    {
        if (!params) {
            throw yexception() << "Params not specified";
        }

        if (type == EWorkload::STOCK) {
            return std::shared_ptr<TStockWorkloadGenerator>(TStockWorkloadGenerator::New(static_cast<const TStockWorkloadParams*>(params)));
        }

        throw yexception() << "Unknown workload";
    }

}