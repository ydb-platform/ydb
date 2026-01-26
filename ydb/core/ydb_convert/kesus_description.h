#pragma once

#include <util/generic/fwd.h>

namespace Ydb {
    namespace Coordination {
        class Config;
        class DescribeNodeResult;
    }
    namespace RateLimiter {
        class Resource;
    }
}

namespace NKikimrSchemeOp {
    class TDirEntry;
    class TKesusDescription;
}

namespace NKikimrKesus {
    class TStreamingQuoterResource;
}

namespace NKikimr {

void FillKesusDescription(
    NKikimrSchemeOp::TKesusDescription& out,
    const Ydb::Coordination::Config& inDesc,
    const TString& name);

void FillKesusDescription(
    Ydb::Coordination::DescribeNodeResult& out,
    const NKikimrSchemeOp::TKesusDescription& inDesc,
    const NKikimrSchemeOp::TDirEntry& inDirEntry);

void FillRateLimiterDescription(
    NKikimrKesus::TStreamingQuoterResource& out,
    const Ydb::RateLimiter::Resource& inDesc
);

void FillRateLimiterDescription(
    Ydb::RateLimiter::Resource& out,
    const NKikimrKesus::TStreamingQuoterResource& inDesc
);

} // namespace NKikimr
