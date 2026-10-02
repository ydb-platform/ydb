#pragma once

#include <ydb/library/workload/abstract/workload_query_generator.h>

namespace NYdbWorkload {

namespace NLog {

class TLogWorkloadParams : public TWorkloadParams {
public:
    enum class EStoreType {
        Row     /* "row"    */,
        Column  /* "column" */,
    };

    void ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) override;
    THolder<IWorkloadQueryGenerator> CreateGenerator() const override;
    TString GetWorkloadName() const override;
    ui64 MinPartitions = 40;
    ui64 MaxPartitions = 1000;
    ui64 PartitionSizeMb = 2000;
    ui64 StringLen = 8;
    ui64 StrColumnsCnt = 0;
    ui64 IntColumnsCnt = 0;
    ui64 KeyColumnsCnt = 0;
    TMaybe<ui64> TimestampStandardDeviationMinutes;
    TMaybe<ui64> TimestampDateFrom;
    TMaybe<ui64> TimestampDateTo;
    ui64 TimestampTtlMinutes = 0;
    ui64 TimestampSubtract = 0;
    ui64 RowsCnt = 1;
    ui32 NullPercent = 10;
    bool PartitionsByLoad = true;

    std::string TableName = "log_writer_test";

    YDB_READONLY(EStoreType, StoreType, EStoreType::Row);
    TWorkloadDataInitializer::TList CreateDataInitializers() const override;

    void Validate(const ECommandType commandType, int workloadType) override;
private:
    void ConfigureOptsFillData(NLastGetopt::TOpts& opts);
    void ConfigureOptsColumns(NLastGetopt::TOpts& opts);
};

class TLogGenerator final: public TWorkloadQueryGeneratorBase<TLogWorkloadParams> {
public:
    using TBase = TWorkloadQueryGeneratorBase<TLogWorkloadParams>;
    struct TRow {
        std::string LogId;
        TInstant Ts;
        i32 Level;
        std::string ServiceName;
        std::string Component;
        std::string Message;
        std::string RequestId;
        std::string Metadata;
        TInstant IngestedAt;
        TVector<ui64> Ints;
        TVector<TString> Strings;
    };

    using TBase::TBase;

    std::string GetDDLQueries() const override;

    TQueryInfoList GetInitialData() override;

    TVector<std::string> GetCleanPaths() const override;

    TQueryInfoList GetWorkload(int type) override;
    TVector<TWorkloadType> GetSupportedWorkloadTypes() const override;

    enum class EType {
        Insert,
        Upsert,
        BulkUpsert,
        Select,
        Delete
    };

private:
    TQueryInfoList WriteRows(TString operation, TVector<TRow>&& rows) const;
    TQueryInfoList Insert(TVector<TRow>&& rows) const;
    TQueryInfoList Upsert(TVector<TRow>&& rows) const;
    TQueryInfoList BulkUpsert(TVector<TRow>&& rows) const;
    TQueryInfoList Select() const;
    TQueryInfoList Delete(TVector<TRow>&& rows) const;
};

} // namespace NLog

} // namespace NYdbWorkload
