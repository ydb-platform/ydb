#include <library/cpp/testing/unittest/registar.h>

#include <yt/cpp/mapreduce/common/helpers.h>

#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>
#include <yt/yql/providers/yt/fmr/test_tools/yson/yql_yt_yson_helpers.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_parse_records.h>
#include <yt/yql/providers/yt/fmr/yt_job_service/mock/yql_yt_job_service_mock.h>

using namespace NYql::NFmr;

Y_UNIT_TEST_SUITE(UtilHelperTests) {
    Y_UNIT_TEST(MockParseRecords) {
        TString inputYsonContent = "{\"key\"=\"075\";\"subkey\"=\"1\";\"value\"=\"abc\"};\n"
                                   "{\"key\"=\"800\";\"subkey\"=\"2\";\"value\"=\"ddd\"};\n";
        auto richPath = NYT::TRichYPath("test_path").Cluster("test_cluster");
        TYtTableRef testYtTable{.RichPath = richPath};
        std::unordered_map<TString, TString> inputTables{{NYT::NodeToCanonicalYsonString(NYT::PathToNode(richPath)), inputYsonContent}};
        std::unordered_map<TString, TString> outputTables;

        auto ytJobService = MakeMockYtJobService(inputTables, outputTables);

        auto reader = ytJobService->MakeReader(testYtTable);
        auto writer = ytJobService->MakeWriter(testYtTable, TClusterConnection());
        auto cancelFlag = std::make_shared<std::atomic<bool>>(false);
        ParseRecords(reader, writer, 1, 10, cancelFlag);
        writer->Flush();
        UNIT_ASSERT_VALUES_EQUAL(outputTables.size(), 1);
        TString serializedRichPath = SerializeRichPath(richPath);
        UNIT_ASSERT(outputTables.contains(serializedRichPath));
        UNIT_ASSERT_NO_DIFF(outputTables[serializedRichPath], inputYsonContent);
    }
    Y_UNIT_TEST(SplitYsonByColumnGroups) {
        const TString ysonRowStr = "{\"key\"=\"075\";\"subkey\"=[\"1\"];\"fir_value\"=\"abc\";\"sec_value\"={\"a\"=1;\"b\"=2}};\n";
        const TString binaryYsonStr = GetBinaryYson(ysonRowStr);

        TString columnGroupsStr = "{\"a\"=[\"key\";\"fir_value\"];\"b\"=#}";
        auto parsedColumnGroupsSpec = GetColumnGroupsFromSpec(columnGroupsStr);

        auto gottenSplittedYson = SplitYsonByColumnGroups(binaryYsonStr, parsedColumnGroupsSpec);

        std::unordered_map<TString, TString> expected = {
            {"a", "{\"key\"=\"075\";\"fir_value\"=\"abc\"};\n"},
            {"b", "{\"subkey\"=[\"1\"];\"sec_value\"={\"a\"=1;\"b\"=2}};\n"}
        };
        for (auto& [key, val]: expected) {
            UNIT_ASSERT(gottenSplittedYson.contains(key));
            UNIT_ASSERT_NO_DIFF(val, GetTextYson(gottenSplittedYson[key]));
        }

    }
    Y_UNIT_TEST(SeveralYsonUnion) {
        TString firstYson = "{\"key\"=\"075\"};";
        TString secondYson = "{\"subkey\"=[\"1\"];\"fir_value\"=\"abc\"};";
        TString thirdYson = "{\"sec_value\" = {\"a\" = 1; \"b\" = 2 }};";
        std::vector<TString> ysonInputs{firstYson, secondYson, thirdYson};
        std::for_each(ysonInputs.begin(), ysonInputs.end(), [] (TString& yson) {
            yson = GetBinaryYson(yson);
        });
        auto gottenUnionBinaryYson = GetYsonUnion(ysonInputs);
        TString expected = "{\"key\"=\"075\";\"subkey\"=[\"1\"];\"fir_value\"=\"abc\";\"sec_value\"={\"a\"=1;\"b\"=2}};\n";
        UNIT_ASSERT_NO_DIFF(GetTextYson(gottenUnionBinaryYson), expected);
    }
}
