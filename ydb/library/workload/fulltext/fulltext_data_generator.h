#pragma once

#include "fulltext_workload_params.h"
#include <ydb/library/workload/benchmark_base/workload.h>
#include <ydb/library/workload/benchmark_base/data_generator.h>

namespace NYdbWorkload {

    class TFulltextDataInitializerBase : public TWorkloadDataInitializerBase {
    protected:
        const TFulltextWorkloadParams& FulltextParams;

    public:
        TFulltextDataInitializerBase(const TString& name, const TString& description, const TFulltextWorkloadParams& params);
        int PostImport() override;
    };

    class TFulltextFilesDataInitializer: public TFulltextDataInitializerBase {
    private:
        TString DataFiles = "documents.tsv.gz";
        TString RelevanceFile = "query_relevances.tsv.gz";
        TString QueryFile = "queries.tsv.gz";
        TString UpsertQueryFile = "upsert_queries.tsv.gz";

        TString DocumentsTable;
        TString QueryTableName;
        TString QueryRelevanceTableName;
        TString UpsertQueryTableName;

    public:
        TFulltextFilesDataInitializer(const TFulltextWorkloadParams& params);

        void ConfigureOpts(NLastGetopt::TOpts& opts) override;
        TBulkDataGeneratorList DoGetBulkInitialData() override;
    };

    class TFulltextGeneratorDataInitializer: public TFulltextDataInitializerBase {
    private:
        ui64 RowCount = 100000;
        TString ModelPath;
        size_t MinSentenceLen = 100;
        size_t MaxSentenceLen = 1000;

    public:
        TFulltextGeneratorDataInitializer(const TFulltextWorkloadParams& params);

        void ConfigureOpts(NLastGetopt::TOpts& opts) override;
        TBulkDataGeneratorList DoGetBulkInitialData() override;
    };

} // namespace NYdbWorkload
