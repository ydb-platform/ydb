#pragma once

#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>

namespace NYql::NFmr {

class IFmrJob: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IFmrJob>;

    virtual ~IFmrJob() = default;

    virtual std::variant<TFmrError, TStatistics> Download(const TDownloadTaskParams& params, const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections = {}, std::shared_ptr<std::atomic<bool>> cancelFlag = nullptr) = 0;

    virtual std::variant<TFmrError, TStatistics> Upload(const TUploadTaskParams& params, const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections = {}, std::shared_ptr<std::atomic<bool>> cancelFlag = nullptr) = 0;

    virtual std::variant<TFmrError, TStatistics> Merge(const TMergeTaskParams& params, const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections = {}, std::shared_ptr<std::atomic<bool>> cancelFlag = nullptr) = 0;

    virtual std::variant<TFmrError, TStatistics> SortedMerge(const TSortedMergeTaskParams& params, const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections = {}, std::shared_ptr<std::atomic<bool>> cancelFlag = nullptr) = 0;

    virtual std::variant<TFmrError, TStatistics> Map(
        const TMapTaskParams& params,
        const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections,
        std::shared_ptr<std::atomic<bool>> cancelFlag,
        const TMaybe<TString>& jobEnvironmentDir = Nothing(),
        const std::vector<TFileInfo>& jobFiles = {},
        const std::vector<TYtResourceInfo>& jobYtResources = {},
        const std::vector<TFmrResourceTaskInfo>& jobFmrResources = {}
    ) = 0;

    virtual std::variant<TFmrError, TStatistics> SortedUpload(const TSortedUploadTaskParams& params, const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections = {}, std::shared_ptr<std::atomic<bool>> cancelFlag = nullptr) = 0;
};

} // namespace NYql
