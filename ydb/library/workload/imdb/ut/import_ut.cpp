#include <ydb/library/workload/imdb/imdb.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/folder/tempdir.h>
#include <util/stream/file.h>
#include <util/stream/zlib.h>

using namespace NYdbWorkload;

namespace {

void WriteFile(const TFsPath& path, const TString& data) {
    TFileOutput output(path);
    if (path.GetName().EndsWith(".gz")) {
        TZLibCompress compressed(&output, ZLib::GZip);
        compressed << data;
        compressed.Finish();
    } else {
        output << data;
    }
}

void CreateEmptyTables(const TImdbWorkloadParams& params, const TFsPath& directory) {
    for (const auto& path: params.CreateGenerator()->GetCleanPaths()) {
        WriteFile(directory / (TFsPath(path).GetName() + ".csv"), "");
    }
}

auto MakeInitializer(const TImdbWorkloadParams& params, const TString& directory, const TString& state = {}) {
    auto initializer = params.CreateDataInitializers().front();
    NLastGetopt::TOpts opts;
    initializer->ConfigureOpts(opts);
    const char* argv[] = {"files", "--input", directory.c_str(), "--state", state.c_str()};
    NLastGetopt::TOptsParseResultException parsed(&opts, state.empty() ? 3 : 5, argv);
    return initializer;
}

} // namespace

Y_UNIT_TEST_SUITE(ImdbImport) {
    Y_UNIT_TEST(ImportsSortedPartsInPlainAndCompressedFormats) {
        for (const auto* extension: {".csv", ".csv.gz", ".tsv", ".tsv.gz"}) {
            TTempDir directory;
            TImdbWorkloadParams params;
            params.DbPath = "/Root";
            params.SetPath("custom/imdb");
            const bool tsv = TString(extension).StartsWith(".tsv");
            const TString delimiter = tsv ? "\t" : ",";
            for (const auto& path: params.CreateGenerator()->GetCleanPaths()) {
                const auto name = TFsPath(path).GetName();
                if (name == "company_type") {
                    const auto parts = TFsPath(directory.Name()) / name;
                    parts.MkDir();
                    WriteFile(parts / (TString("02") + extension), "2" + delimiter + "distributors\n");
                    WriteFile(parts / (TString("01") + extension), "1" + delimiter + "production companies\n");
                } else {
                    WriteFile(TFsPath(directory.Name()) / (name + extension), "");
                }
            }
            auto initializer = MakeInitializer(params, directory.Name());
            const auto generators = initializer->GetBulkInitialData();
            UNIT_ASSERT_VALUES_EQUAL(generators.size(), 22);
            TVector<TString> rows;
            for (const auto& generator: generators) {
                for (const auto& portion: generator->GenerateDataPortion()) {
                    UNIT_ASSERT_VALUES_EQUAL(portion->GetTable(), "/Root/custom/imdb/company_type");
                    const auto& csv = std::get<IBulkDataGenerator::TDataPortion::TCsv>(portion->MutableData());
                    rows.push_back(csv.Data);
                }
                UNIT_ASSERT(generator->GenerateDataPortion().empty());
            }
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(rows[0], "id" + delimiter + "kind\n1" + delimiter + "production companies\n");
            UNIT_ASSERT_VALUES_EQUAL(rows[1], "id" + delimiter + "kind\n2" + delimiter + "distributors\n");
        }
    }

    Y_UNIT_TEST(RejectsFilesAsInputAndNestedDirectoriesAsParts) {
        TTempDir directory;
        TImdbWorkloadParams params;
        const auto file = TFsPath(directory.Name()) / "data.csv";
        WriteFile(file, "");
        auto initializer = MakeInitializer(params, file.GetPath());
        UNIT_ASSERT_EXCEPTION_CONTAINS(initializer->GetBulkInitialData(), yexception, "existing directory");

        const auto parts = TFsPath(directory.Name()) / "aka_name";
        parts.MkDir();
        initializer = MakeInitializer(params, directory.Name());
        UNIT_ASSERT_EXCEPTION_CONTAINS(initializer->GetBulkInitialData(), yexception, "aka_name not found");
        (parts / "nested.csv").MkDir();
        UNIT_ASSERT_EXCEPTION_CONTAINS(initializer->GetBulkInitialData(), yexception, "Unsupported IMDb data file");
    }

    Y_UNIT_TEST(CsvPreservesDoubledQuotesAndLiteralBackslashes) {
        TTempDir directory;
        TImdbWorkloadParams params;
        params.BulkSize = 0;
        CreateEmptyTables(params, directory.Name());
        // A zero bulk size still emits one record. The final record has no newline.
        const TString row = "1,1,\"Name \"\"quoted\"\" \\q path\\\nnext line\",,,,,";
        WriteFile(TFsPath(directory.Name()) / "aka_name.csv", row);
        auto initializer = MakeInitializer(params, directory.Name());
        const auto generators = initializer->GetBulkInitialData();
        const auto portions = generators.front()->GenerateDataPortion();
        UNIT_ASSERT_VALUES_EQUAL(portions.size(), 1);
        const auto& csv = std::get<IBulkDataGenerator::TDataPortion::TCsv>(portions.front()->MutableData());
        UNIT_ASSERT_STRING_CONTAINS(csv.Data, row + "\n");
        UNIT_ASSERT(generators.front()->GenerateDataPortion().empty());
    }

    Y_UNIT_TEST(CsvRejectsUnterminatedQuotedRecord) {
        TTempDir directory;
        TImdbWorkloadParams params;
        CreateEmptyTables(params, directory.Name());
        WriteFile(TFsPath(directory.Name()) / "aka_name.csv", "1,1,\"unterminated\nsecond line");
        auto initializer = MakeInitializer(params, directory.Name());
        const auto generators = initializer->GetBulkInitialData();
        UNIT_ASSERT_EXCEPTION_CONTAINS(generators.front()->GenerateDataPortion(), yexception, "Unterminated quoted field");
    }

    Y_UNIT_TEST(CsvResumeRejectsTruncatedSource) {
        TTempDir directory;
        TImdbWorkloadParams params;
        CreateEmptyTables(params, directory.Name());
        const auto source = TFsPath(directory.Name()) / "aka_name.csv";
        WriteFile(source, "1,1,Name,,,,,\n");
        const auto state = (TFsPath(directory.Name()) / "state.json").GetPath();
        {
            auto initializer = MakeInitializer(params, directory.Name(), state);
            const auto generators = initializer->GetBulkInitialData();
            const auto portions = generators.front()->GenerateDataPortion();
            UNIT_ASSERT_VALUES_EQUAL(portions.size(), 1);
            portions.front()->SetSendResult(NYdb::TStatus(NYdb::EStatus::SUCCESS, NYdb::NIssue::TIssues()));
        }
        WriteFile(source, "");
        auto initializer = MakeInitializer(params, directory.Name(), state);
        UNIT_ASSERT_EXCEPTION_CONTAINS(initializer->GetBulkInitialData(), yexception, "shorter than its saved import position");
    }
}
