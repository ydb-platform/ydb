#include <library/cpp/testing/unittest/registar.h>

#include <yt/cpp/mapreduce/common/helpers.h>

#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>
#include <yt/yql/providers/yt/fmr/test_tools/yson/yql_yt_yson_helpers.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_parse_records.h>
#include <yt/yql/providers/yt/fmr/yt_job_service/file/yql_yt_file_yt_job_service.h>

#include <util/stream/file.h>

using namespace NYql::NFmr;

Y_UNIT_TEST_SUITE(UtilHelperTests) {
    Y_UNIT_TEST(MockParseRecords) {
        TString inputYsonContent = "{\"key\"=\"075\";\"subkey\"=\"1\";\"value\"=\"abc\"};\n"
                                   "{\"key\"=\"800\";\"subkey\"=\"2\";\"value\"=\"ddd\"};\n";
        auto richPath = NYT::TRichYPath("test_path").Cluster("test_cluster");
        TTempFileHandle inputFile{};
        {
            TFileOutput out(inputFile.Name());
            out.Write(inputYsonContent.data(), inputYsonContent.size());
        }
        TTempFileHandle outputFile{};

        TYtTableRef inputYtTable(richPath, inputFile.Name());
        TYtTableRef outputYtTable(richPath, outputFile.Name());

        auto ytJobService = MakeFileYtJobService();

        auto reader = ytJobService->MakeReader(inputYtTable);
        auto writer = ytJobService->MakeWriter(outputYtTable, TClusterConnection());
        auto cancelFlag = std::make_shared<std::atomic<bool>>(false);
        ParseRecords(reader, writer, 1, 10, cancelFlag);
        writer->Flush();

        const TString outputYsonContent = TFileInput(outputFile.Name()).ReadAll();
        UNIT_ASSERT_NO_DIFF(
            GetTextYson(GetBinaryYson(outputYsonContent)),
            GetTextYson(GetBinaryYson(inputYsonContent))
        );
    }
    Y_UNIT_TEST(SplitYsonByColumnGroups) {
        const TString ysonRowStr = "{\"key\"=\"075\";\"subkey\"=[\"1\"];\"fir_value\"=\"abc\";\"sec_value\"={\"a\"=1;\"b\"=2}};\n";
        const TString binaryYsonStr = GetBinaryYson(ysonRowStr);

        TString columnGroupsStr = "{\"a\"=[\"key\";\"fir_value\"];\"b\"=#}";
        auto parsedColumnGroupsSpec = GetColumnGroupsFromSpec(columnGroupsStr);

        auto splittedYsonByColumnGroups = SplitYsonByColumnGroups(binaryYsonStr, parsedColumnGroupsSpec);

        std::unordered_map<TString, TString> expected = {
            {"a", "{\"key\"=\"075\";\"fir_value\"=\"abc\"};\n"},
            {"b", "{\"subkey\"=[\"1\"];\"sec_value\"={\"a\"=1;\"b\"=2}};\n"}
        };
        auto gottenSplittedYson = splittedYsonByColumnGroups.SplittedYsonByColumnGroups;
        for (auto& [key, val]: expected) {
            UNIT_ASSERT(gottenSplittedYson.contains(key));
            UNIT_ASSERT_NO_DIFF(val, GetTextYson(gottenSplittedYson[key]));
        }
        UNIT_ASSERT_VALUES_EQUAL(splittedYsonByColumnGroups.RecordsCount, 1);
    }
    Y_UNIT_TEST(SplitYsonByColumnGroupsRaw_Basic) {
        const TString ysonRowStr = "{\"key\"=\"075\";\"subkey\"=[\"1\"];\"fir_value\"=\"abc\";\"sec_value\"={\"a\"=1;\"b\"=2}};\n";
        const TString binaryYsonStr = GetBinaryYson(ysonRowStr);

        TString columnGroupsStr = "{\"a\"=[\"key\";\"fir_value\"];\"b\"=#}";
        auto parsedColumnGroupsSpec = GetColumnGroupsFromSpec(columnGroupsStr);

        auto splittedYsonByColumnGroups = SplitYsonByColumnGroupsRaw(binaryYsonStr, parsedColumnGroupsSpec);

        std::unordered_map<TString, TString> expected = {
            {"a", "{\"key\"=\"075\";\"fir_value\"=\"abc\"};\n"},
            {"b", "{\"subkey\"=[\"1\"];\"sec_value\"={\"a\"=1;\"b\"=2}};\n"}
        };
        auto gottenSplittedYson = splittedYsonByColumnGroups.SplittedYsonByColumnGroups;
        for (auto& [key, val]: expected) {
            UNIT_ASSERT(gottenSplittedYson.contains(key));
            UNIT_ASSERT_NO_DIFF(val, GetTextYson(gottenSplittedYson[key]));
        }
        UNIT_ASSERT_VALUES_EQUAL(splittedYsonByColumnGroups.RecordsCount, 1);
    }
    Y_UNIT_TEST(SplitYsonByColumnGroupsRaw_MultipleRows) {
        const TString ysonRowStr =
            "{\"key\"=\"075\";\"subkey\"=\"1\";\"value\"=\"abc\"};\n"
            "{\"key\"=\"800\";\"subkey\"=\"2\";\"value\"=\"ddd\"};\n";
        const TString binaryYsonStr = GetBinaryYson(ysonRowStr);

        TString columnGroupsStr = "{\"keys\"=[\"key\";\"subkey\"];\"vals\"=#}";
        auto parsedColumnGroupsSpec = GetColumnGroupsFromSpec(columnGroupsStr);

        auto splittedYsonByColumnGroups = SplitYsonByColumnGroupsRaw(binaryYsonStr, parsedColumnGroupsSpec);

        auto gottenSplittedYson = splittedYsonByColumnGroups.SplittedYsonByColumnGroups;
        UNIT_ASSERT_VALUES_EQUAL(splittedYsonByColumnGroups.RecordsCount, 2);
        UNIT_ASSERT(gottenSplittedYson.contains("keys"));
        UNIT_ASSERT(gottenSplittedYson.contains("vals"));

        TString expectedKeys = "{\"key\"=\"075\";\"subkey\"=\"1\"};\n{\"key\"=\"800\";\"subkey\"=\"2\"};\n";
        TString expectedVals = "{\"value\"=\"abc\"};\n{\"value\"=\"ddd\"};\n";
        UNIT_ASSERT_NO_DIFF(expectedKeys, GetTextYson(gottenSplittedYson["keys"]));
        UNIT_ASSERT_NO_DIFF(expectedVals, GetTextYson(gottenSplittedYson["vals"]));
    }
    Y_UNIT_TEST(SplitYsonByColumnGroupsRaw_MatchesOriginal) {
        // Verify that the raw version produces identical output to the original
        const TString ysonRowStr = "{\"key\"=\"075\";\"subkey\"=[\"1\"];\"fir_value\"=\"abc\";\"sec_value\"={\"a\"=1;\"b\"=2}};\n";
        const TString binaryYsonStr = GetBinaryYson(ysonRowStr);

        TString columnGroupsStr = "{\"a\"=[\"key\";\"fir_value\"];\"b\"=#}";
        auto parsedColumnGroupsSpec = GetColumnGroupsFromSpec(columnGroupsStr);

        auto originalResult = SplitYsonByColumnGroups(binaryYsonStr, parsedColumnGroupsSpec);
        auto rawResult = SplitYsonByColumnGroupsRaw(binaryYsonStr, parsedColumnGroupsSpec);

        UNIT_ASSERT_VALUES_EQUAL(originalResult.RecordsCount, rawResult.RecordsCount);
        for (auto& [key, val]: originalResult.SplittedYsonByColumnGroups) {
            UNIT_ASSERT(rawResult.SplittedYsonByColumnGroups.contains(key));
            UNIT_ASSERT_NO_DIFF(
                GetTextYson(val),
                GetTextYson(rawResult.SplittedYsonByColumnGroups[key])
            );
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

        auto gottenFullUnionBinaryYson = GetYsonUnion(ysonInputs, {});
        auto gottenUnionBinaryYsonWithColumns = GetYsonUnion(ysonInputs, {"key", "sec_value"});

        TString expectedFullUnion = "{\"key\"=\"075\";\"subkey\"=[\"1\"];\"fir_value\"=\"abc\";\"sec_value\"={\"a\"=1;\"b\"=2}};\n";
        TString expectedNeededColsUnion = "{\"key\"=\"075\";\"sec_value\"={\"a\"=1;\"b\"=2}};\n";

        UNIT_ASSERT_NO_DIFF(GetTextYson(gottenFullUnionBinaryYson), expectedFullUnion);
        UNIT_ASSERT_NO_DIFF(GetTextYson(gottenUnionBinaryYsonWithColumns), expectedNeededColsUnion);
    }
    Y_UNIT_TEST(GetYsonUnionRaw_MatchesOriginal) {
        TString firstYson = "{\"key\"=\"075\"};";
        TString secondYson = "{\"subkey\"=[\"1\"];\"fir_value\"=\"abc\"};";
        TString thirdYson = "{\"sec_value\" = {\"a\" = 1; \"b\" = 2 }};";
        std::vector<TString> ysonInputs{firstYson, secondYson, thirdYson};
        std::for_each(ysonInputs.begin(), ysonInputs.end(), [] (TString& yson) {
            yson = GetBinaryYson(yson);
        });

        auto originalFull = GetYsonUnion(ysonInputs, {});
        auto rawFull = GetYsonUnionRaw(ysonInputs, {});
        UNIT_ASSERT_NO_DIFF(GetTextYson(originalFull), GetTextYson(rawFull));

        auto originalFiltered = GetYsonUnion(ysonInputs, {"key", "sec_value"});
        auto rawFiltered = GetYsonUnionRaw(ysonInputs, {"key", "sec_value"});
        UNIT_ASSERT_NO_DIFF(GetTextYson(originalFiltered), GetTextYson(rawFiltered));
    }
    Y_UNIT_TEST(GetYsonUnionRaw_MultipleRows) {
        TString firstYson = "{\"key\"=\"a\"};{\"key\"=\"b\"};";
        TString secondYson = "{\"val\"=1};{\"val\"=2};";
        std::vector<TString> ysonInputs{firstYson, secondYson};
        std::for_each(ysonInputs.begin(), ysonInputs.end(), [] (TString& yson) {
            yson = GetBinaryYson(yson);
        });

        auto originalResult = GetYsonUnion(ysonInputs, {});
        auto rawResult = GetYsonUnionRaw(ysonInputs, {});
        UNIT_ASSERT_NO_DIFF(GetTextYson(originalResult), GetTextYson(rawResult));
    }
    Y_UNIT_TEST(GetYsonUnionRaw_SingleInput) {
        TString yson = "{\"key\"=\"a\";\"val\"=1};";
        std::vector<TString> ysonInputs{GetBinaryYson(yson)};

        auto rawResult = GetYsonUnionRaw(ysonInputs, {});
        // Single input with no filtering should return same data
        UNIT_ASSERT_NO_DIFF(GetTextYson(ysonInputs[0]), GetTextYson(rawResult));
    }
}
