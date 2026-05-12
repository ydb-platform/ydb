#pragma once

#include <vector>
#include <yt/yql/providers/yt/fmr/tvm/interface/yql_yt_fmr_tvm_interface.h>

namespace NYql::NFmr {

// settings for tvm client which assume that sidecar with running tvmtool dameon exists in Ya.Deploy.

struct TFmrTvmToolSettings {
    TString SourceTvmAlias;
    TMaybe<ui32> TvmPort;
    TMaybe<TString> TvmSecret;
};

// settings for tvm client in case sidecar doesn't exist, uses tvm api.

struct TFmrTvmApiSettings {
    TTvmId SourceTvmId;
    TString TvmSecret;
    TString TvmDiskCacheDir;
    std::vector<TTvmId> DestinationTvmIds;
};

IFmrTvmClient::TPtr MakeFmrTvmClient(const TFmrTvmToolSettings& tvmClientSettings);

IFmrTvmClient::TPtr MakeFmrTvmClient(const TFmrTvmApiSettings& tvmClientSettings);

} // namespace NYql::NFmr
