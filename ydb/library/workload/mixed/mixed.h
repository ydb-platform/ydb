#pragma once

#include <ydb/library/workload/abstract/workload_query_generator.h>

#include <cctype>

namespace NYdbWorkload {

namespace NMixed {

enum LogWorkloadConstants : ui64 {
    MIN_PARTITIONS = 40,
    MAX_PARTITIONS = 1000,
    PARTITION_SIZE_MB = 2000,
    STRING_LEN = 8,
    STR_COLUMNS_CNT = 1,
    INT_COLUMNS_CNT = 1,
    KEY_COLUMNS_CNT = 1,
    ROWS_CNT = 1,
    PARTITIONS_BY_LOAD = true,

    TIMESTAMP_STANDARD_DEVIATION_MINUTES = 0,
    TIMESTAMP_TTL_MIN = 60,
};

class TMixedWorkloadParams : public TWorkloadParams {
public:
    enum class EStoreType {
        Row     /* "row"    */,
        Column  /* "column" */,
    };

    void ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) override;
    THolder<IWorkloadQueryGenerator> CreateGenerator() const override;
    TString GetWorkloadName() const override;
    ui64 MinPartitions = LogWorkloadConstants::MIN_PARTITIONS;
    ui64 MaxPartitions = LogWorkloadConstants::MAX_PARTITIONS;
    ui64 PartitionSizeMb = LogWorkloadConstants::PARTITION_SIZE_MB;
    ui64 StringLen = LogWorkloadConstants::STRING_LEN;
    ui64 StrColumnsCnt = LogWorkloadConstants::STR_COLUMNS_CNT;
    ui64 IntColumnsCnt = LogWorkloadConstants::INT_COLUMNS_CNT;
    ui64 KeyColumnsCnt = LogWorkloadConstants::KEY_COLUMNS_CNT;
    ui64 TimestampStandardDeviationMinutes = LogWorkloadConstants::TIMESTAMP_STANDARD_DEVIATION_MINUTES;
    ui64 TimestampTtlMinutes = LogWorkloadConstants::TIMESTAMP_STANDARD_DEVIATION_MINUTES;
    ui64 RowsCnt = LogWorkloadConstants::ROWS_CNT;
    bool PartitionsByLoad = LogWorkloadConstants::PARTITIONS_BY_LOAD;

    std::string TableName = "log_writer_test";

    YDB_READONLY(EStoreType, StoreType, EStoreType::Row);
};

class TLogGenerator final: public TWorkloadQueryGeneratorBase<TMixedWorkloadParams> {
public:
    using TBase = TWorkloadQueryGeneratorBase<TMixedWorkloadParams>;
    struct TRow {
        TInstant Ts;
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
            return Ts == other.Ts && Ints == other.Ints && Strings == other.Strings;
        }
    };
    TLogGenerator(const TMixedWorkloadParams* params);

    std::string GetDDLQueries() const override;

    TQueryInfoList GetInitialData() override;

    TVector<std::string> GetCleanPaths() const override;

    TQueryInfoList GetWorkload(int type) override;
    TVector<TWorkloadType> GetSupportedWorkloadTypes() const override;

    enum class EType {
        Insert,
        Upsert,
        BulkUpsert,
        Select
    };

private:
    TQueryInfoList WriteRows(TString operation, TVector<TRow>&& rows);
    TQueryInfoList Select(TVector<TRow>&& rows);
    TQueryInfoList Insert(TVector<TRow>&& rows);
    TQueryInfoList Upsert(TVector<TRow>&& rows);
    TQueryInfoList BulkUpsert(TVector<TRow>&& rows);
    TVector<TRow> GenerateRandomRows();

    const ui64 TotalColumnsCnt;
};

} // namespace NMixed

} // namespace NYdbWorkload
