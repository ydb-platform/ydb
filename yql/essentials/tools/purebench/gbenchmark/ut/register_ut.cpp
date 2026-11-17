#include <yql/essentials/tools/purebench/gbenchmark/register.h>

#include <library/cpp/testing/gbenchmark/benchmark.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/path.h>
#include <util/folder/tempdir.h>
#include <util/generic/scope.h>
#include <util/stream/file.h>

namespace {

TTempDir MakeSuiteDirectory() {
    TTempDir directory;
    TFileOutput(directory.Path() / "selected.yql").Write("SELECT missing_column FROM Input");
    TFileOutput(directory.Path() / "selected.prepare.yql").Write("SELECT missing_column FROM Input");
    TFileOutput(directory.Path() / "unselected.yql").Write("SELECT missing_column FROM Input");
    return directory;
}

} // namespace

Y_UNIT_TEST_SUITE(TPureBenchGBenchmark) {
Y_UNIT_TEST(RegistersCasesWithoutCompilingSql) {
    benchmark::ClearRegisteredBenchmarks();
    Y_DEFER {
        benchmark::ClearRegisteredBenchmarks();
    };
    const auto directory = MakeSuiteDirectory();
    const auto benchmarks = NYql::NGBenchmark::RegisterSuite("window", directory.Name());
    UNIT_ASSERT_VALUES_EQUAL(benchmarks.size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(benchmarks[0]->GetName(), "window/selected/engine:disable");
    UNIT_ASSERT_VALUES_EQUAL(benchmarks[1]->GetName(), "window/unselected/engine:disable");
}
} // Y_UNIT_TEST_SUITE(TPureBenchGBenchmark)
