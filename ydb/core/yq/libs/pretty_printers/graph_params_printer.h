#pragma once
#include <ydb/core/yq/libs/graph_params/proto/graph_params.pb.h>

#include <util/generic/string.h>

namespace NFq {

TString PrettyPrintGraphParams(const NProto::TGraphParams& graphParams, bool canonical);

} // namespace NFq
