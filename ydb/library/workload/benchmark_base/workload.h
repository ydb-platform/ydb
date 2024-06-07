#pragma once

#include "state.h"
#include <ydb/library/workload/abstract/workload_query_generator.h>
#include <ydb/library/accessor/accessor.h>
#include <util/generic/set.h>
#include <util/generic/deque.h>
#include <util/folder/path.h>
#include <util/system/tls.h>

namespace NYdbWorkload {

class TWorkloadBaseParams: public TWorkloadParams {
public:
    enum class EStoreType {
        Row     /* "row"    */,
        Column  /* "column" */,
        ExternalS3      /* "external-s3"     */
    };
    void ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) override;
    TString GetFullTableName(const char* table) const;
    YDB_ACCESSOR_DEF(TString, Path);
    YDB_READONLY(ui64, BulkSize, 10000);
    YDB_READONLY(EStoreType, StoreType, EStoreType::Row);
    YDB_READONLY_DEF(TString, S3Endpoint);
    YDB_READONLY_DEF(TString, S3Prefix);
    YDB_READONLY(TString, StringType, "Utf8");
    YDB_READONLY(TString, DateType, "Date");
    YDB_READONLY(TString, TimestampType, "Timestamp");
};

class TWorkloadGeneratorBase : public IWorkloadQueryGenerator {
public:
    explicit TWorkloadGeneratorBase(const TWorkloadBaseParams& params);
    std::string GetDDLQueries() const override final;
    TVector<std::string> GetCleanPaths() const override final;
    TBulkDataGeneratorList GetBulkInitialData() const override final;

    static const TString TsvDelimiter;
    static const TString TsvFormatString;
    static const TString CsvDelimiter;
    static const TString CsvFormatString;

protected:
    virtual TString DoGetDDLQueries() const = 0;

    THolder<TGeneratorStateProcessor> StateProcessor;
private:
    const TWorkloadBaseParams& Params;
};

} // namespace NYdbWorkload
