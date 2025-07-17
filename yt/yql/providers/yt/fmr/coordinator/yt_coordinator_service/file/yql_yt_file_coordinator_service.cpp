#include "yql_yt_file_coordinator_service.h"

#include <library/cpp/yson/parser.h>
#include <util/stream/file.h>
#include <util/system/fstat.h>
#include <yt/cpp/mapreduce/common/helpers.h>
#include <yt/yql/providers/yt/gateway/file/yql_yt_file_text_yson.h>
#include <yt/yql/providers/yt/lib/yson_helpers/yson_helpers.h>
#include <yql/essentials/utils/yql_panic.h>

namespace NYql::NFmr {

namespace {

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
        for (auto& ytTable: ytTables) {
            YQL_ENSURE(ytTable.FilePath);
            auto fileLength = GetFileLength(*ytTable.FilePath);
            if (fileLength + curFileLength > maxDataWeightPerPart) {
                ytPartitions.emplace_back(curYtTableTaskRef);
                if (ytPartitions.size() > settings.MaxParts) {
                    return {{}, false};
                }
                curYtTableTaskRef = TYtTableTaskRef{};
                curFileLength = 0;
            }
            TString ytPath = NYT::AddPathPrefix(ytTable.Path, "//");
            auto richPath = NYT::TRichYPath(ytPath).Append(true);
            // append RichPath just in case, TODO - figure out if we actually need to use it somewhere
            curYtTableTaskRef.RichPaths.emplace_back(richPath);
            curYtTableTaskRef.FilePaths.emplace_back(*ytTable.FilePath);
            curFileLength += fileLength;
        }
        ytPartitions.emplace_back(curYtTableTaskRef);
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
