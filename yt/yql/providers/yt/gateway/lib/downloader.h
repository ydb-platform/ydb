#pragma once

#include <yt/yql/providers/yt/common/yql_yt_settings.h>

#include <yt/cpp/mapreduce/interface/fwd.h>
#include <yt/cpp/mapreduce/interface/common.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/hash.h>

namespace NYql {

struct TSamplingConfig {
    double SamplingPercent;
    ui64 SamplingSeed;
    bool IsSystemSampling;
};

struct TYtTableOptions {
    bool IsTemporary;
    bool IsAnonymous;
    ui32 Epoch;
};

struct TRemoteYtTable {
    NYT::TRichYPath RichPath;
    TYtTableOptions TableOptions;
    NYT::TNode Format;
};

struct TTableDownloaderOptions {
    TVector<TRemoteYtTable> Tables;
    THashMap<TString, ui32> StructColumns;
    TMaybe<TSamplingConfig> SamplingConfig;
    bool ForceLocalTableContent;
    TMaybe<ui32> PublicId;
    TString UniqueId;
    ETableContentDeliveryMode DeliveryMode;
};

struct TTableDownloaderResult {
    TVector<NYT::TRichYPath> RemoteFiles;
    TVector<TString> LocalFiles;
};

using ITableDownloaderFunc = std::function<NThreading::TFuture<TTableDownloaderResult>(const TTableDownloaderOptions&)>;

} // namespace NYql
