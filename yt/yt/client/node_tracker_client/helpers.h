#pragma once

#include "public.h"

#include <yt/yt/core/ypath/public.h>

namespace NYT::NNodeTrackerClient {

////////////////////////////////////////////////////////////////////////////////

NYPath::TYPath GetClusterNodesPath();

NYPath::TYPath GetExecNodesPath();

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

void FormatValue(TStringBuilderBase* builder, const TDiskLocationResources& locationResources, TStringBuf spec);

void FormatValue(TStringBuilderBase* builder, const TDiskResources& diskResources, TStringBuf spec);

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerClient
