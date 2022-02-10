#pragma once

#include <array>
#include <util/generic/strbuf.h>

namespace NYql {

constexpr TStringBuf ConfigProviderName = "config"; 
constexpr TStringBuf KikimrProviderName = "kikimr"; 
constexpr TStringBuf ResultProviderName = "result"; 
constexpr TStringBuf YtProviderName = "yt"; 
constexpr TStringBuf RtmrProviderName = "rtmr"; 
constexpr TStringBuf StatProviderName = "statface"; 
constexpr TStringBuf SolomonProviderName = "solomon"; 
constexpr TStringBuf DqProviderName = "dq"; 
constexpr TStringBuf ClickHouseProviderName = "clickhouse"; 
constexpr TStringBuf YdbProviderName = "ydb"; 
constexpr TStringBuf PqProviderName = "pq"; 
constexpr TStringBuf S3ProviderName = "s3"; 

constexpr std::array<const TStringBuf, 11> Providers = { 
    {ConfigProviderName, YtProviderName, KikimrProviderName, RtmrProviderName, S3ProviderName, 
     StatProviderName, SolomonProviderName, DqProviderName, ClickHouseProviderName, YdbProviderName,
     PqProviderName}
};

} // namespace NYql
