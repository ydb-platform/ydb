#include <yql/essentials/tools/purebench/gbenchmark/suites/suite.h>

#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/path.h>
#include <util/folder/tempdir.h>
#include <util/generic/hash_set.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/system/fs.h>

#include <limits>

namespace {

namespace NPureBench = NYql::NPureBench;
using NYql::EBlockEngineMode;

constexpr TStringBuf MeasuredQuery = "SELECT index FROM Input;\n";

TTempDir MakeTemporaryDirectory() {
    const TFsPath temporaryRoot = TFsPath(GetWorkPath()) / "tmp";
    temporaryRoot.MkDirs();
    return TTempDir::NewTempDir(temporaryRoot);
}

void WriteFile(const TFsPath& path, TStringBuf contents) {
    Y_ENSURE(NFs::MakeDirectoryRecursive(path.Parent().GetPath()), "Cannot create test directory: " << path.Parent());
    TFileOutput(path).Write(contents);
}

void CheckOptions(const NPureBench::TBenchmarkProgramOptions& actual, const NPureBench::TBenchmarkProgramOptions& expected) {
    UNIT_ASSERT_VALUES_EQUAL(actual.GeneratorInputRows, expected.GeneratorInputRows);
    UNIT_ASSERT_VALUES_EQUAL(actual.GenerationQuery, expected.GenerationQuery);
    UNIT_ASSERT_VALUES_EQUAL(actual.MeasuredQuery, expected.MeasuredQuery);
    UNIT_ASSERT(actual.GenerationSyntax == expected.GenerationSyntax);
    UNIT_ASSERT(actual.MeasuredQuerySyntax == expected.MeasuredQuerySyntax);
    UNIT_ASSERT_VALUES_EQUAL(actual.UdfsDirectory, expected.UdfsDirectory);
    UNIT_ASSERT_VALUES_EQUAL(actual.LLVMSettings, expected.LLVMSettings);
    UNIT_ASSERT(actual.BlockEngineMode == expected.BlockEngineMode);
    UNIT_ASSERT_VALUES_EQUAL(actual.LanguageVersion, expected.LanguageVersion);
    UNIT_ASSERT_VALUES_EQUAL(actual.EnableCalibration, expected.EnableCalibration);
}

TVector<NPureBench::TBenchmarkCase> ExpectedCases() {
    TVector<NPureBench::TBenchmarkCase> cases{
        {.Name = "empty/engine:disable", .Options = {}},
        {.Name = "group/deep/max/engine:auto", .Options = {.GeneratorInputRows = std::numeric_limits<ui64>::max(), .BlockEngineMode = EBlockEngineMode::Auto}},
        {.Name = "group/deep/max/engine:force", .Options = {.GeneratorInputRows = std::numeric_limits<ui64>::max(), .BlockEngineMode = EBlockEngineMode::Force}},
        {.Name = "group/first/engine:force", .Options = {.GeneratorInputRows = 0, .GenerationQuery = "SELECT index * 2 AS index FROM Input;\n", .BlockEngineMode = EBlockEngineMode::Force}},
        {.Name = "group/first/engine:disable", .Options = {.GeneratorInputRows = 0, .GenerationQuery = "SELECT index * 2 AS index FROM Input;\n"}},
        {.Name = "group/first/engine:auto", .Options = {.GeneratorInputRows = 0, .GenerationQuery = "SELECT index * 2 AS index FROM Input;\n", .BlockEngineMode = EBlockEngineMode::Auto}},
        {.Name = "group/second/engine:disable", .Options = {.GeneratorInputRows = 1, .GenerationQuery = "SELECT index + 1 AS index FROM Input;\n"}},
        {.Name = "plain/engine:disable", .Options = {}},
        {.Name = "sparse/engine:disable", .Options = {.GenerationSyntax = NPureBench::ESyntax::PG, .MeasuredQuerySyntax = NPureBench::ESyntax::PG,
                                                      .LLVMSettings = "OFF",
                                                      .LanguageVersion = NYql::MakeLangVersion(2026, 2)}},
    };
    for (auto& benchmarkCase : cases) {
        benchmarkCase.Name.prepend("window/");
        benchmarkCase.Options.MeasuredQuery = MeasuredQuery;
        benchmarkCase.Options.EnableCalibration = false;
    }
    return cases;
}

} // namespace

Y_UNIT_TEST_SUITE(TPureBenchSuites) {
Y_UNIT_TEST(LoadsTestSuite) {
    const TFsPath suiteDirectory(SRC_("test_suite"));
    const TString relativeDirectory = suiteDirectory.RelativePath(TFsPath::Cwd()).GetPath();
    const auto expected = ExpectedCases();
    for (const auto& directory : {suiteDirectory.GetPath(), relativeDirectory}) {
        const auto actual = NYql::NPureBench::LoadBenchmarkSuite("window", directory);
        UNIT_ASSERT_VALUES_EQUAL(actual.size(), expected.size());
        for (size_t index = 0; index < actual.size(); ++index) {
            UNIT_ASSERT_VALUES_EQUAL(actual[index].Name, expected[index].Name);
            CheckOptions(actual[index].Options, expected[index].Options);
        }
    }
}

Y_UNIT_TEST(UsesPortableQueryNames) {
    const TVector<std::pair<TStringBuf, TStringBuf>> directories{
        {"nested", "nested"},
        {"я", "%D1%8F"},
    };
    for (const auto& [directoryName, encodedDirectoryName] : directories) {
        auto directory = MakeTemporaryDirectory();
        WriteFile(directory.Path() / directoryName / "query.yql", MeasuredQuery);
        const auto cases = NYql::NPureBench::LoadBenchmarkSuite("window", directory.Name());
        UNIT_ASSERT_VALUES_EQUAL(cases.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(cases.front().Name, "window/" + TString(encodedDirectoryName) + "/query/engine:disable");
    }
}

Y_UNIT_TEST(RejectsInvalidConfiguration) {
    auto directory = MakeTemporaryDirectory();
    WriteFile(directory.Path() / "query.yql", MeasuredQuery);
    const TVector<std::pair<TStringBuf, TStringBuf>> configurations{
        {"unknown value\n", "Unknown setting"},
        {"gateway_cfg_patch patch.conf\n", "Gateway patch"},
        {"langver 2026.02\nlangver 2026.02\n", "Duplicate setting"},
        {"langver max\n", "Invalid fixed langver"},
        {"langver 2026.99\n", "Invalid fixed langver"},
        {"bench_generation_syntax invalid\n", "invalid"},
        {"bench_query_syntax sql pg\n", "exactly one"},
        {"bench_llvm_settings\n", "exactly one"},
        {"bench_input_rows\n", "exactly one"},
        {"bench_input_rows 0 1\n", "exactly one"},
        {"bench_udfs_dir\n", "exactly one"},
        {"bench_udfs_dir first second\n", "exactly one"},
        {"bench_udfs_dir first\nbench_udfs_dir second\n", "Duplicate setting"},
        {"bench_input_rows -1\n", "Unexpected symbol"},
        {"bench_input_rows 1.0\n", "Unexpected symbol"},
        {"bench_input_rows 18446744073709551616\n", "Integer overflow"},
        {"bench_block_engines\n", "must not be empty"},
        {"bench_block_engines invalid\n", "invalid"},
        {"bench_block_engines auto auto\n", "Duplicate block engine"},
    };
    for (const auto& [configuration, message] : configurations) {
        WriteFile(directory.Path() / "query.cfg", configuration);
        UNIT_ASSERT_EXCEPTION_CONTAINS(NYql::NPureBench::LoadBenchmarkSuite("window", directory.Name()), yexception, message);
    }
}

Y_UNIT_TEST(ParsesUdfsDirectory) {
    auto directory = MakeTemporaryDirectory();
    WriteFile(directory.Path() / "query.yql", MeasuredQuery);
    const TString udfSetting = "bench_udfs_dir yql/essentials/udfs/common/digest/impl\n";
    const TString engineSetting = "bench_block_engines auto force disable\n";
    for (const auto& configuration : {udfSetting + engineSetting, engineSetting + udfSetting}) {
        WriteFile(directory.Path() / "query.cfg", configuration);
        const auto cases = NYql::NPureBench::LoadBenchmarkSuite("window", directory.Name());
        UNIT_ASSERT_VALUES_EQUAL(cases.size(), 3);
        for (const auto& benchmarkCase : cases) {
            UNIT_ASSERT_VALUES_EQUAL(benchmarkCase.Options.UdfsDirectory, BinaryPath("yql/essentials/udfs/common/digest/impl"));
        }
    }
}

Y_UNIT_TEST(ParsesAllStandardRowCounts) {
    auto directory = MakeTemporaryDirectory();
    WriteFile(directory.Path() / "query.yql", MeasuredQuery);
    const TVector<std::pair<TStringBuf, ui64>> inputs{
        {"0", 0},
        {"+0", 0},
        {"000", 0},
        {"+1", 1},
        {"00017", 17},
        {"18446744073709551615", std::numeric_limits<ui64>::max()},
    };
    for (const auto& [value, expected] : inputs) {
        WriteFile(directory.Path() / "query.cfg", "bench_input_rows " + TString(value));
        const auto cases = NYql::NPureBench::LoadBenchmarkSuite("window", directory.Name());
        UNIT_ASSERT_VALUES_EQUAL(cases.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(cases.front().Options.GeneratorInputRows, expected);
    }
}

Y_UNIT_TEST(ParsesQueryDialects) {
    auto directory = MakeTemporaryDirectory();
    WriteFile(directory.Path() / "query.yql", MeasuredQuery);
    for (const auto& [value, expected] : {std::pair{TStringBuf("sql"), NPureBench::ESyntax::SQL},
                                          std::pair{TStringBuf("pg"), NPureBench::ESyntax::PG}}) {
        WriteFile(directory.Path() / "query.cfg", "bench_generation_syntax " + TString(value) + "\nbench_query_syntax " + TString(value));
        const auto cases = NYql::NPureBench::LoadBenchmarkSuite("window", directory.Name());
        UNIT_ASSERT(cases.front().Options.GenerationSyntax == expected);
        UNIT_ASSERT(cases.front().Options.MeasuredQuerySyntax == expected);
    }
}

Y_UNIT_TEST(RejectsInvalidSuiteFiles) {
    auto directory = MakeTemporaryDirectory();
    UNIT_ASSERT_EXCEPTION_CONTAINS(NYql::NPureBench::LoadBenchmarkSuite("window", directory.Name()), yexception, "no main .yql");
    UNIT_ASSERT_EXCEPTION_CONTAINS(NYql::NPureBench::LoadBenchmarkSuite("window", (directory.Path() / "missing").GetPath()),
                                   yexception, "not a directory");
    WriteFile(directory.Path() / "query.yql", " \t\r\n");
    UNIT_ASSERT_EXCEPTION_CONTAINS(NYql::NPureBench::LoadBenchmarkSuite("window", directory.Name()), yexception, "File is empty");
    WriteFile(directory.Path() / "query.yql", MeasuredQuery);
    WriteFile(directory.Path() / "query.prepare.yql", "");
    UNIT_ASSERT_EXCEPTION_CONTAINS(NYql::NPureBench::LoadBenchmarkSuite("window", directory.Name()), yexception, "File is empty");
    (directory.Path() / "query.prepare.yql").DeleteIfExists();
    (directory.Path() / "query.cfg").MkDir();
    UNIT_ASSERT_EXCEPTION_CONTAINS(NYql::NPureBench::LoadBenchmarkSuite("window", directory.Name()), yexception, "not a regular file");
    for (const TStringBuf suiteName : {"", "window/basic"}) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(NYql::NPureBench::LoadBenchmarkSuite(suiteName, directory.Name()), yexception, "Suite name");
    }
    UNIT_ASSERT_EXCEPTION_CONTAINS(NYql::NPureBench::LoadBenchmarkSuite("window", ""), yexception, "data directory");
}

Y_UNIT_TEST(EncodesNamesUnambiguously) {
    auto directory = MakeTemporaryDirectory();
    const TVector<std::pair<TStringBuf, TStringBuf>> queries{
        {"a b.yql", "a+b"},
        {"a+b.yql", "a%2Bb"},
        {"a%2Bb.yql", "a%252Bb"},
        {"group/query[x].yql", "group/query%5Bx%5D"},
#if !defined(_win_)
        // These filename characters are forbidden by Windows.
        {"group/query::[x]\"|\n.yql", "group/query%3A%3A%5Bx%5D%22%7C%0A"},
#endif
        {"запрос.yql", "%D0%B7%D0%B0%D0%BF%D1%80%D0%BE%D1%81"},
    };
    THashSet<TString> expectedNames;
    for (const auto& [path, encodedName] : queries) {
        WriteFile(directory.Path() / path, MeasuredQuery);
        expectedNames.insert("s%3A%3A%0A%25%22/" + TString(encodedName) + "/engine:disable");
    }
    const auto cases = NYql::NPureBench::LoadBenchmarkSuite("s::\n%\"", directory.Name());
    UNIT_ASSERT_VALUES_EQUAL(cases.size(), expectedNames.size());
    for (const auto& benchmarkCase : cases) {
        UNIT_ASSERT_VALUES_EQUAL(expectedNames.erase(benchmarkCase.Name), 1);
    }
    UNIT_ASSERT(expectedNames.empty());
}
} // Y_UNIT_TEST_SUITE(TPureBenchSuites)
