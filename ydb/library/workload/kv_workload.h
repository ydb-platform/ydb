#pragma once

#include "workload_query_generator.h"

#include <cctype>
#include <random>
#include <sstream>

namespace NYdbWorkload {

enum KvWorkloadConstants : ui64 {
    MIN_PARTITIONS = 40,
    MAX_PARTITIONS = 1000,
    PARTITION_SIZE_MB = 2000,
    INIT_ROW_COUNT = 1000,
    MAX_FIRST_KEY = Max<ui64>(),
    STRING_LEN = 8,
    COLUMNS_CNT = 2,
    INT_COLUMNS_CNT = 1,
    KEY_COLUMNS_CNT = 1,
    ROWS_CNT = 1,
    PARTITIONS_BY_LOAD = true,

    MIXED_CHANGE_PARTITIONS_SIZE = false,
    MIXED_DO_READ_ROWS = true,
    MIXED_DO_SELECT = true,
};

struct TKvWorkloadParams : public TWorkloadParams {
    ui64 MinPartitions = KvWorkloadConstants::MIN_PARTITIONS;
    const ui64 MaxPartitions = KvWorkloadConstants::MAX_PARTITIONS;
    ui64 PartitionSizeMb = KvWorkloadConstants::PARTITION_SIZE_MB;
    ui64 InitRowCount = KvWorkloadConstants::INIT_ROW_COUNT;
    ui64 MaxFirstKey = KvWorkloadConstants::MAX_FIRST_KEY;
    ui64 StringLen = KvWorkloadConstants::STRING_LEN;
    ui64 ColumnsCnt = KvWorkloadConstants::COLUMNS_CNT;
    ui64 IntColumnsCnt = KvWorkloadConstants::INT_COLUMNS_CNT;
    ui64 KeyColumnsCnt = KvWorkloadConstants::KEY_COLUMNS_CNT;
    ui64 RowsCnt = KvWorkloadConstants::ROWS_CNT;
    bool PartitionsByLoad = KvWorkloadConstants::PARTITIONS_BY_LOAD;
    
    ui64 MixedChangePartitionsSize = KvWorkloadConstants::MIXED_CHANGE_PARTITIONS_SIZE;
    ui64 MixedDoReadRows = KvWorkloadConstants::MIXED_DO_READ_ROWS;
    ui64 MixedDoSelect = KvWorkloadConstants::MIXED_DO_SELECT;
    
    const std::string TableName = "kv_test";
};

class TKvWorkloadGenerator : public IWorkloadQueryGenerator {
public:
    struct TRow {
        TVector<ui64> Ints;
        TVector<TString> Strings;

        TString ToString() const {
            std::stringstream ss;
            ss << "( ";
            for (auto i : Ints) {
                ss << i << " ";
            }
            for (auto s : Strings) {
                ss << s << " ";
            }
            ss << ")";
            return ss.str();
        }

        bool operator == (const TRow &other) const {
            return Ints == other.Ints && Strings == other.Strings;
        }
    };

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
        Mixed,
        MaxType
    };

private:
    TQueryInfoList Upsert(TVector<TRow>&& rows);
    TQueryInfoList Insert(TVector<TRow>&& rows);
    TQueryInfoList WriteRows(TString operation, TVector<TRow>&& rows);
    TQueryInfoList Select(TVector<TRow>&& rows);
    TQueryInfoList ReadRows(TVector<TRow>&& rows);
    TQueryInfoList Mixed();

    TKvWorkloadGenerator(const TKvWorkloadParams* params);

    TQueryInfo FillKvData() const;
    TVector<TRow> GenerateRandomRows(bool randomValues = false);

    TKvWorkloadParams Params;
    TString BigString;

    std::atomic<TInstant> MixedNextChangePartitionsSize;
};

} // namespace NYdbWorkload
