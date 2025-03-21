#include <library/cpp/testing/unittest/registar.h>

#include <yt/yql/providers/yt/fmr/utils/parse_records.h>
#include <yt/yql/providers/yt/fmr/yt_service/impl/yql_yt_yt_service_impl.h>
#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>
#include <yt/yql/providers/yt/fmr/yt_service/mock/yql_yt_yt_service_mock.h>

using namespace NYql::NFmr;

Y_UNIT_TEST_SUITE(ParseRecordTests) {
    Y_UNIT_TEST(MockParseRecord) {
        TString inputYsonContent = "{\"key\"=\"075\";\"subkey\"=\"1\";\"value\"=\"abc\"};\n"
                                   "{\"key\"=\"800\";\"subkey\"=\"2\";\"value\"=\"ddd\"};\n";
        TYtTableRef testYtTable = TYtTableRef{.Path = "test_path", .Cluster = "hahn"};
        std::unordered_map<TYtTableRef, TString> inputTables{{testYtTable, inputYsonContent}};
        std::unordered_map<TYtTableRef, TString> outputTables;

        auto ytService = MakeMockYtService(inputTables, outputTables);
        auto reader = ytService->MakeReader(testYtTable, TClusterConnection());
        auto writer = ytService->MakeWriter(testYtTable, TClusterConnection());
        ParseRecords(*reader, *writer, 1, 10);
        writer->Flush();
        UNIT_ASSERT_VALUES_EQUAL(outputTables.size(), 1);
        UNIT_ASSERT(outputTables.contains(testYtTable));
        UNIT_ASSERT_NO_DIFF(outputTables[testYtTable], inputYsonContent);
    }
}
