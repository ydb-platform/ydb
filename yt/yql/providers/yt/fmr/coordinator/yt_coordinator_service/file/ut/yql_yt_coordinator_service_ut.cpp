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

    Y_UNIT_TEST(SplitLargeFileByByteRanges) {
        // Build a file with 6 rows of equal length (10 bytes each including ";\n"):
        //   {0};\n  {1};\n  {2};\n  {3};\n  {4};\n  {5};\n   → 60 bytes total.
        // maxDataWeightPerPart = 25 → expect 3 parts of ~20 bytes, each split on \n.
        TTempFileHandle dataFile;

        TString fileContent;
        const TString rowTemplate = "{\"k\"=%d};\n"; // exactly 10 bytes for single-digit k
        for (int i = 0; i < 6; ++i) {
            fileContent += Sprintf("{\"k\"=%d};\n", i);
        }
        {
            TFileOutput out(dataFile.Name());
            out << fileContent;
        }
        const i64 fileSize = static_cast<i64>(fileContent.size());

        TYtTableRef ytTable;
        ytTable.FilePath = dataFile.Name();

        auto fileService = MakeFileYtCoordinatorService();
        auto settings = TYtPartitionerSettings{.MaxDataWeightPerPart = 25, .MaxParts = 100};
        auto [partitions, ok] = fileService->PartitionYtTables({ytTable}, {}, settings);
        UNIT_ASSERT(ok);
        UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 3u);

        // Every partition must reference the same file.
        for (auto& part : partitions) {
            UNIT_ASSERT_VALUES_EQUAL(part.FilePaths.size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(part.FilePaths[0], dataFile.Name());
            UNIT_ASSERT_VALUES_EQUAL(part.RichPaths.size(), 1u);
        }

        // Byte ranges must be contiguous, start at 0, end at fileSize,
        // and every boundary must fall on a \n.
        i64 prevEnd = 0;
        for (auto& part : partitions) {
            const auto& ranges = part.RichPaths[0].GetRanges();
            UNIT_ASSERT(ranges.Defined() && ranges->size() == 1u);
            const auto& r = ranges->front();
            UNIT_ASSERT(r.LowerLimit_.RowIndex_.Defined());
            UNIT_ASSERT(r.UpperLimit_.RowIndex_.Defined());
            const i64 lo = *r.LowerLimit_.RowIndex_;
            const i64 hi = *r.UpperLimit_.RowIndex_;
            UNIT_ASSERT_VALUES_EQUAL(lo, prevEnd);
            UNIT_ASSERT(hi > lo && hi <= fileSize);
            // The byte just before hi must be '\n' (split on row boundary).
            UNIT_ASSERT_VALUES_EQUAL(fileContent[hi - 1], '\n');
            prevEnd = hi;
        }
        UNIT_ASSERT_VALUES_EQUAL(prevEnd, fileSize);
    }
}

} // namespace NYql::NFmr
