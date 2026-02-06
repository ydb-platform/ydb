#pragma once

#include <util/generic/fwd.h>

namespace NKikimrScheme {
    class TEvDescribeSchemeResult;
}

namespace NKikimrKesus {
    class TStreamingQuoterResource;
}

namespace NKikimr::NSchemeShard {

bool BuildScheme(
    const NKikimrScheme::TEvDescribeSchemeResult& describeResult,
    TString& scheme,
    const TString& databaseRoot,
    TString& error);

bool BuildRateLimiterResourceScheme(
    const NKikimrKesus::TStreamingQuoterResource& rateLimiterDesc,
    TString& scheme);

} // namespace NKikimr::NSchemeShard
