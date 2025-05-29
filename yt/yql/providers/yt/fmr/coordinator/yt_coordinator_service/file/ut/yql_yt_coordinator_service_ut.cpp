#include <library/cpp/testing/unittest/registar.h>
#include <util/stream/file.h>
#include <util/system/tempfile.h>
#include <yt/yql/providers/yt/fmr/coordinator/yt_coordinator_service/file/yql_yt_file_coordinator_service.h>

namespace NYql::NFmr {

Y_UNIT_TEST_SUITE(FileYtCoordinatorServiceTests) {
    Y_UNIT_TEST(PartitionFiles) {
        const i64 FileNums = 5;

        std::vector<THolder<TTempFileHandle>> fileHandles(FileNums);
        std::vector<TYtTableRef> ytTables(FileNums, TYtTableRef());
        std::vector<i64> fileLengths = {30, 10, 20, 5, 40};

        for (int i = 0; i < FileNums; ++i) {
            fileHandles[i] = MakeHolder<TTempFileHandle>();
            auto curFileName= fileHandles[i]->Name();
            ytTables[i].FilePath = curFileName;
            TFileOutput writer(curFileName);
            writer.Write(TString("1") * fileLengths[i]);
            writer.Flush();
        }

        auto fileService = MakeFileYtCoordinatorService();
        auto settings = TYtPartitionerSettings{.MaxDataWeightPerPart = 50, .MaxParts = 100};
        auto [gottenPartitions, status] = fileService->PartitionYtTables(ytTables, {}, settings);
        UNIT_ASSERT_VALUES_EQUAL(status, true);

        std::vector<std::vector<TString>> expectedFilePartitions = {
            {fileHandles[0]->Name(), fileHandles[1]->Name()},
            {fileHandles[2]->Name(), fileHandles[3]->Name()},
            {fileHandles[4]->Name()}
        };
        std::vector<std::vector<TString>> gottenFileParititons;
        for (auto& part: gottenPartitions) {
            gottenFileParititons.emplace_back(part.FilePaths);
        }
        UNIT_ASSERT_VALUES_EQUAL(gottenFileParititons, expectedFilePartitions);
    }
}

} // namespace NYql::NFmr
