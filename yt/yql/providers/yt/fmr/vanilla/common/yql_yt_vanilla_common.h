#pragma once

#include <yt/cpp/mapreduce/interface/operation.h>
#include <util/generic/string.h>

namespace NYql::NFmr {

// Returns the first backbone MTN IPv6 address from job execution attributes,
// or an empty string if none is found or attributes are absent.
TString ExtractBackboneMtnIp(const NYT::TJobAttributes& job);

} // namespace NYql::NFmr
