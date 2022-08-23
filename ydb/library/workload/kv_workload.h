#pragma once

#include "workload_query_generator.h"

#include <cctype>
#include <random>

namespace NYdbWorkload {

struct TKvWorkloadParams : public TWorkloadParams {
    ui64 MinPartitions = 1;
    ui64 InitRowCount = 1000;
    ui64 MaxFirstKey = 5000;
    bool PartitionsByLoad = true;
};

class TKvWorkloadGenerator : public IWorkloadQueryGenerator {
public:

    static TKvWorkloadGenerator* New(const TKvWorkloadParams* params) {
        if (!validateDbPath(params->DbPath)) {
            throw yexception() << "Invalid path to database." << Endl;
        }
        return new TKvWorkloadGenerator(params);
    }

    virtual ~TKvWorkloadGenerator() {}

    std::string GetDDLQueries() const override;

    TQueryInfoList GetInitialData() override;

    std::string GetCleanDDLQueries() const override;

    TQueryInfoList GetWorkload(int type) override;

    TKvWorkloadParams* GetParams() override;

    enum class EType {
        UpsertRandom,
        SelectRandom
    };

private:
    TQueryInfoList UpsertRandom();
    TQueryInfoList SelectRandom();
    
    TKvWorkloadGenerator(const TKvWorkloadParams* params);

    TQueryInfo FillKvData() const;

    std::string DbPath;
    TKvWorkloadParams Params;

    std::random_device Rd;
    std::mt19937_64 Gen;
    std::uniform_int_distribution<ui64> UniformDistGen;
};

} // namespace NYdbWorkload