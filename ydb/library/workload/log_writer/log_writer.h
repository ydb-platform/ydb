#pragma once

#include <ydb/library/workload/abstract/workload_query_generator.h>

#include <cctype>
#include <random>
#include <sstream>

namespace NYdbWorkload {

namespace NLogWriter {

enum LogWriterWorkloadConstants : ui64 {
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

class TLogWriterWorkloadParams : public TWorkloadParams {
public:
    enum class EStoreType {
        Row     /* "row"    */,
        Column  /* "column" */,
    };

    void ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) override;
    THolder<IWorkloadQueryGenerator> CreateGenerator() const override;
    TString GetWorkloadName() const override;
    ui64 MinPartitions = LogWriterWorkloadConstants::MIN_PARTITIONS;
    ui64 MaxPartitions = LogWriterWorkloadConstants::MAX_PARTITIONS;
    ui64 PartitionSizeMb = LogWriterWorkloadConstants::PARTITION_SIZE_MB;
    ui64 StringLen = LogWriterWorkloadConstants::STRING_LEN;
    ui64 StrColumnsCnt = LogWriterWorkloadConstants::STR_COLUMNS_CNT;
    ui64 IntColumnsCnt = LogWriterWorkloadConstants::INT_COLUMNS_CNT;
    ui64 KeyColumnsCnt = LogWriterWorkloadConstants::KEY_COLUMNS_CNT;
    ui64 TimestampStandardDeviationMinutes = LogWriterWorkloadConstants::TIMESTAMP_STANDARD_DEVIATION_MINUTES;
    ui64 TimestampTtlMinutes = LogWriterWorkloadConstants::TIMESTAMP_STANDARD_DEVIATION_MINUTES;
    ui64 RowsCnt = LogWriterWorkloadConstants::ROWS_CNT;
    bool PartitionsByLoad = LogWriterWorkloadConstants::PARTITIONS_BY_LOAD;

    std::string TableName = "log_writer_test";

    YDB_READONLY(EStoreType, StoreType, EStoreType::Row);
};

class TLogWriterWorkloadGenerator final: public TWorkloadQueryGeneratorBase<TLogWriterWorkloadParams> {
public:
    using TBase = TWorkloadQueryGeneratorBase<TLogWriterWorkloadParams>;
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
    TLogWriterWorkloadGenerator(const TLogWriterWorkloadParams* params);

    std::string GetDDLQueries() const override;

    TQueryInfoList GetInitialData() override;

    TVector<std::string> GetCleanPaths() const override;

    TQueryInfoList GetWorkload(int type) override;
    TVector<TWorkloadType> GetSupportedWorkloadTypes() const override;

    enum class EType {
        Insert,
        Upsert,
        BulkUpsert,
    };

private:
    TQueryInfoList WriteRows(TString operation, TVector<TRow>&& rows);
    TQueryInfoList Insert(TVector<TRow>&& rows);
    TQueryInfoList Upsert(TVector<TRow>&& rows);
    TQueryInfoList BulkUpsert(TVector<TRow>&& rows);
    TVector<TRow> GenerateRandomRows();

    const ui64 TotalColumnsCnt;

    std::random_device RandomDevice;
    std::mt19937 Mt19937;
};

} // namespace NLogWriter

} // namespace NYdbWorkload
