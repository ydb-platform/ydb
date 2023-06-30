#pragma once

#include <yt/cpp/mapreduce/common/fwd.h>
#include <yt/cpp/mapreduce/interface/fwd.h>

#include <yt/cpp/mapreduce/http/fwd.h>

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

ui64 RoundUpFileSize(ui64 size);

bool UseLocalModeOptimization(const TClientContext& context, const IClientRetryPolicyPtr& clientRetryPolicy);

TString GetOperationWebInterfaceUrl(TStringBuf serverName, TOperationId operationId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
