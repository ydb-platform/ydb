#pragma once

#include <library/cpp/threading/future/core/future.h>
#include <yql/essentials/core/file_storage/storage.h>
#include <yt/cpp/mapreduce/interface/common.h>
#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>

namespace NYql::NFmr {

// Interface for downloading resources to fmr jobs (for example udfs, files, tables for mapJoin).
// In the future can also create containers and and set environment for fmr jobs.

class IFmrJobPreparer: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IFmrJobPreparer>;

    virtual ~IFmrJobPreparer() = default;

    virtual TString GenerateJobEnvironmentDir(const TString& taskId) = 0;

    virtual void InitalizeDistributedCache(const TString& distributedCacheUrl, const TString& distributedCacheToken) = 0;

    virtual NThreading::TFuture<TFileLinkPtr> DownloadFileFromDistributedCache(const TString& md5Key) = 0;

    virtual NThreading::TFuture<TFileLinkPtr> DownloadYtResource(const NYT::TRichYPath& path, const TString& ytServerName, const TString& token) = 0;

    virtual NThreading::TFuture<TFileLinkPtr> DownloadFmrResource(const TFmrResourceTaskInfo& fmrResource) = 0;
};

} // namespace NYql::NFmr
