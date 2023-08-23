#pragma once

#include "workload_query_generator.h"

#include <cctype>
#include <random>

namespace NYdbWorkload {

enum KvWorkloadConstants : ui64 {
    MIN_PARTITIONS = 40,
    INIT_ROW_COUNT = 1000,
    MAX_FIRST_KEY = Max<ui64>(),
    STRING_LEN = 8,
    COLUMNS_CNT = 2,
    INT_COLUMNS_CNT = 1,
    KEY_COLUMNS_CNT = 1,
    ROWS_CNT = 1,
    PARTITIONS_BY_LOAD = true
};

struct TKvWorkloadParams : public TWorkloadParams {
    ui64 MinPartitions = KvWorkloadConstants::MIN_PARTITIONS;
    ui64 InitRowCount = KvWorkloadConstants::INIT_ROW_COUNT;
    ui64 MaxFirstKey = KvWorkloadConstants::MAX_FIRST_KEY;
    ui64 StringLen = KvWorkloadConstants::STRING_LEN;
    ui64 ColumnsCnt = KvWorkloadConstants::COLUMNS_CNT;
    ui64 IntColumnsCnt = KvWorkloadConstants::INT_COLUMNS_CNT;
    ui64 KeyColumnsCnt = KvWorkloadConstants::KEY_COLUMNS_CNT;
    ui64 RowsCnt = KvWorkloadConstants::ROWS_CNT;
    bool PartitionsByLoad = KvWorkloadConstants::PARTITIONS_BY_LOAD;
};

class TKvWorkloadGenerator : public IWorkloadQueryGenerator {
public:

    static TKvWorkloadGenerator* New(const TKvWorkloadParams* params) {
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
        InsertRandom,
        SelectRandom,
        ReadRowsRandom,
        MaxType
    };

private:
    TQueryInfoList UpsertRandom();
    TQueryInfoList InsertRandom();
    TQueryInfoList SelectRandom();
    TQueryInfoList ReadRowsRandom();

    TKvWorkloadGenerator(const TKvWorkloadParams* params);

    TQueryInfo FillKvData() const;
    TQueryInfoList AddOperation(TString operation);

    std::string DbPath;
    TKvWorkloadParams Params;
    TString BigString;
};

} // namespace NYdbWorkload
