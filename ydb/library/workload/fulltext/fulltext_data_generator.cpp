#include "fulltext_data_generator.h"

#include <ydb/library/workload/abstract/colors.h>
#include <ydb/public/api/protos/ydb_table.pb.h>

#include <library/cpp/colorizer/colors.h>

#include <util/string/builder.h>

namespace NYdbWorkload {

    namespace {

        TString ExtractIndexParams(const TFulltextWorkloadParams& params) {
            THashMap<TStringBuf, TStringBuf> indexParams;

            for (const auto& param : params.IndexParams) {
                const size_t pos = param.find('=');
                if (pos == TString::npos) {
                    continue;
                }

                const TStringBuf name = TStringBuf(param).substr(0, pos);
                const TStringBuf value = TStringBuf(param).substr(pos + 1);
                indexParams[name] = value;
            }

            TStringBuilder builder;
            bool first = true;
            for (const auto& [name, value] : indexParams) {
                if (!first) {
                    builder << ",";
                } else {
                    first = false;
                }

                builder << name << "=" << value;
            }
            return builder;
        }

    } // namespace

    TFulltextWorkloadDataInitializer::TFulltextWorkloadDataInitializer(const TFulltextWorkloadParams& params)
        : TWorkloadDataInitializerBase("files", "Import fulltext data from files", params)
        , Params(params)
    {
    }

    void TFulltextWorkloadDataInitializer::ConfigureOpts(NLastGetopt::TOpts& opts) {
        NColorizer::TColors colors = GetColors(Cout);

        TStringBuilder inputDescription;
        inputDescription
            << "File or directory with the dataset to import. Only two columns are imported: "
            << colors.BoldColor() << "id" << colors.OldColor() << " and "
            << colors.BoldColor() << "text" << colors.OldColor() << ". "
            << "If a directory is set, all supported files inside will be used."
            << "\nSupported formats: CSV/TSV (zipped or unzipped) and Parquet.";

        opts.AddLongOption('i', "input", inputDescription)
            .RequiredArgument("PATH")
            .Required()
            .StoreResult(&DataFiles);
    }

    TBulkDataGeneratorList TFulltextWorkloadDataInitializer::DoGetBulkInitialData() {
        return {
            std::make_shared<TDataGenerator>(
                *this,
                Params.TableName,
                0,
                Params.TableName,
                DataFiles,
                TVector<TString>{"id", "text"},
                TDataGenerator::EPortionSizeUnit::Line)};
    }

    int TFulltextWorkloadDataInitializer::PostImport() {
        const TString ddlQuery = std::format(
            R"sql(
                ALTER TABLE `{0}`
                ADD INDEX `{1}`
                GLOBAL SYNC USING {2}
                ON (text) WITH (
                    {3}
                );
            )sql",
            Params.GetFullTableName(Params.TableName.c_str()).c_str(),
            Params.IndexName.c_str(),
            Params.IndexType.c_str(),
            ExtractIndexParams(Params).c_str());

        const auto result = Params.QueryClient->RetryQuerySync([&ddlQuery](NYdb::NQuery::TSession session) {
            return session.ExecuteQuery(ddlQuery, NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        });
        return result.IsSuccess() ? EXIT_SUCCESS : EXIT_FAILURE;
    }

} // namespace NYdbWorkload
