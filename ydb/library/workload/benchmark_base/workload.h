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
    YDB_READONLY(EStoreType, StoreType, EStoreType::Row);
    YDB_READONLY_DEF(TString, S3Endpoint);
    YDB_READONLY_DEF(TString, S3Prefix);
    YDB_READONLY(TString, StringType, "Utf8");
    YDB_READONLY(TString, DateType, "Date32");
    YDB_READONLY(TString, TimestampType, "Timestamp64");
};

class TWorkloadGeneratorBase : public IWorkloadQueryGenerator {
public:
    explicit TWorkloadGeneratorBase(const TWorkloadBaseParams& params);
    std::string GetDDLQueries() const override final;
    TVector<std::string> GetCleanPaths() const override final;

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

class TWorkloadDataInitializerBase: public TWorkloadDataInitializer {
public:
    TWorkloadDataInitializerBase(const TString& name, const TString& description, const TWorkloadBaseParams& params);
    void ConfigureOpts(NLastGetopt::TOpts& opts) override;
    TBulkDataGeneratorList GetBulkInitialData() override final;

protected:
    virtual TBulkDataGeneratorList DoGetBulkInitialData() = 0;
    THolder<TGeneratorStateProcessor> StateProcessor;
    const TWorkloadBaseParams& Params;
    bool Clear = false;
    TFsPath StatePath;
};

} // namespace NYdbWorkload
