#include "yql_yt_file_coordinator_service.h"

#include <library/cpp/yson/parser.h>
#include <util/stream/file.h>
#include <util/system/fstat.h>
#include <util/system/fs.h>
#include <yt/cpp/mapreduce/common/helpers.h>
#include <yt/cpp/mapreduce/interface/common.h>
#include <yt/yql/providers/yt/gateway/file/yql_yt_file_text_yson.h>
#include <yt/yql/providers/yt/lib/yson_helpers/yson_helpers.h>
#include <yql/essentials/utils/yql_panic.h>

namespace NYql::NFmr {

namespace {

// Split a file into byte ranges aligned on \n boundaries.
// Boundaries are found by seeking to each approximate split point and scanning
// forward to the next newline. The ranges are contiguous and cover the whole file.
// Byte offsets are stored as "row indices" in TReadRange so the job-service reader
// can seek directly without any row counting.
std::vector<std::pair<i64, i64>> ComputeByteRanges(
    const TString& filePath, i64 fileSize, i64 maxBytesPerPart
) {
    const i64 numParts = (fileSize + maxBytesPerPart - 1) / maxBytesPerPart;

    TFile f(filePath, OpenExisting | RdOnly);

    // Returns the byte offset of the first character after the next '\n' at or after
    // fromPos, or fileSize if no '\n' is found. Reads in chunks to avoid per-byte syscalls.
    auto scanToNextRowStart = [&](i64 fromPos) -> i64 {
        f.Seek(fromPos, sSet);
        char buf[4096];
        i64 pos = fromPos;
        while (true) {
            const i64 n = f.Read(buf, sizeof(buf));
            if (n <= 0) {
                return fileSize;
            }
            for (i64 i = 0; i < n; ++i) {
                if (buf[i] == '\n') {
                    return pos + i + 1;
                }
            }
            pos += n;
        }
    };

    std::vector<i64> boundaries;
    boundaries.push_back(0);
    for (i64 part = 1; part < numParts; ++part) {
        const i64 approxPos = part * fileSize / numParts;
        boundaries.push_back(scanToNextRowStart(approxPos));
    }
    boundaries.push_back(fileSize);

    std::vector<std::pair<i64, i64>> ranges;
    for (size_t i = 0; i + 1 < boundaries.size(); ++i) {
        if (boundaries[i] < boundaries[i + 1]) {
            ranges.emplace_back(boundaries[i], boundaries[i + 1]);
        }
    }
    return ranges;
}

class TFileYtCoordinatorService: public IYtCoordinatorService {
public:

    std::pair<std::vector<TYtTableTaskRef>, bool> PartitionYtTables(
        const std::vector<TYtTableRef>& ytTables,
        const std::unordered_map<TFmrTableId, TClusterConnection>& /*clusterConnections*/,
        const TYtPartitionerSettings& settings
    ) override {
        const i64 maxDataWeightPerPart = settings.MaxDataWeightPerPart;
        std::vector<TYtTableTaskRef> ytPartitions;
        TYtTableTaskRef curYtTableTaskRef{};
        i64 curFileLength = 0;

        auto flushCurrent = [&]() {
            if (!curYtTableTaskRef.FilePaths.empty()) {
                ytPartitions.emplace_back(std::exchange(curYtTableTaskRef, TYtTableTaskRef{}));
                curFileLength = 0;
            }
        };

        for (auto& ytTable: ytTables) {
            YQL_ENSURE(ytTable.FilePath);

            // Check if the file was produced by a multi-job sorted upload (splitted=N in attr).
            // Each part becomes its own partition; its number is encoded in FileName so MakeReader
            // can open the specific part file and apply byte ranges.
            {
                const i64 numParts = NFile::ReadSplittedPartsCount(*ytTable.FilePath);
                if (numParts > 0) {
                    flushCurrent();
                    for (i64 p = 0; p < numParts; ++p) {
                        const TString partPath = *ytTable.FilePath + ".part." + ToString(p);
                        const i64 partSize = GetFileLength(partPath);
                        if (partSize == 0) {
                            continue;
                        }
                        if (partSize <= maxDataWeightPerPart) {
                            // One partition for the whole part file; encode part path in FileName.
                            NYT::TRichYPath richPath = ytTable.RichPath;
                            richPath.FileName(ToString(p));
                            TYtTableTaskRef taskRef;
                            taskRef.RichPaths.emplace_back(std::move(richPath));
                            taskRef.FilePaths.emplace_back(*ytTable.FilePath);
                            ytPartitions.emplace_back(std::move(taskRef));
                        } else {
                            // Part file is large — split into byte ranges.
                            const auto byteRanges = ComputeByteRanges(partPath, partSize, maxDataWeightPerPart);
                            for (auto [byteStart, byteEnd] : byteRanges) {
                                NYT::TRichYPath richPath = ytTable.RichPath;
                                richPath.FileName(ToString(p));
                                richPath.AddRange(NYT::TReadRange()
                                    .LowerLimit(NYT::TReadLimit().RowIndex(byteStart))
                                    .UpperLimit(NYT::TReadLimit().RowIndex(byteEnd)));
                                TYtTableTaskRef taskRef;
                                taskRef.RichPaths.emplace_back(std::move(richPath));
                                taskRef.FilePaths.emplace_back(*ytTable.FilePath);
                                ytPartitions.emplace_back(std::move(taskRef));
                                if (ytPartitions.size() > settings.MaxParts) {
                                    return {{}, false};
                                }
                            }
                        }
                        if (ytPartitions.size() > settings.MaxParts) {
                            return {{}, false};
                        }
                    }
                    continue;
                }
            }

            const i64 fileLength = GetFileLength(*ytTable.FilePath);

            if (fileLength <= maxDataWeightPerPart) {
                if (fileLength + curFileLength > maxDataWeightPerPart) {
                    flushCurrent();
                    if (ytPartitions.size() > settings.MaxParts) {
                        return {{}, false};
                    }
                }
                curYtTableTaskRef.RichPaths.emplace_back(ytTable.RichPath);
                curYtTableTaskRef.FilePaths.emplace_back(*ytTable.FilePath);
                curFileLength += fileLength;
            } else {
                // File is too large for one partition — split into byte ranges.
                flushCurrent();

                if (fileLength == 0) {
                    continue;
                }

                const auto byteRanges = ComputeByteRanges(
                    *ytTable.FilePath, fileLength, maxDataWeightPerPart);

                for (auto [byteStart, byteEnd] : byteRanges) {
                    NYT::TRichYPath richPath = ytTable.RichPath;
                    // Byte offsets are stored in the RowIndex fields of TReadRange.
                    richPath.AddRange(NYT::TReadRange()
                        .LowerLimit(NYT::TReadLimit().RowIndex(byteStart))
                        .UpperLimit(NYT::TReadLimit().RowIndex(byteEnd)));
                    TYtTableTaskRef taskRef;
                    taskRef.RichPaths.emplace_back(std::move(richPath));
                    taskRef.FilePaths.emplace_back(*ytTable.FilePath);
                    ytPartitions.emplace_back(std::move(taskRef));
                    if (ytPartitions.size() > settings.MaxParts) {
                        return {{}, false};
                    }
                }
            }
        }
        flushCurrent();
        if (ytPartitions.size() > settings.MaxParts) {
            return {{}, false};
        }
        return {ytPartitions, true};
    }
};

} // namespace

IYtCoordinatorService::TPtr MakeFileYtCoordinatorService() {
    return MakeIntrusive<TFileYtCoordinatorService>();
}

} // namespace NYql::NFmr
