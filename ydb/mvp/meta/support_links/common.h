#pragma once

#include <ydb/mvp/meta/mvp.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>

namespace NMVP::NSupportLinks {

inline constexpr const char* SOURCE_META = "meta";
inline constexpr const char* INVALID_IDENTITY_PARAMS_MESSAGE =
    "Invalid identity parameters. Supported entities: cluster requires 'cluster'; database requires 'cluster' and 'database'.";

struct TResolvedLink {
    TString Title;
    TString Url;
};

struct TSupportError {
    TString Source;
    TMaybe<ui32> Status;
    TString Reason;
    TString Message;
};

struct TLinkResolveContext {
    size_t Place = 0;
    TString SourceName;
    TSupportLinkEntryConfig LinkConfig;
    THashMap<TString, TString> ClusterColumns;
    TVector<std::pair<TString, TString>> QueryParams;
    NActors::TActorId Parent;
    NActors::TActorId HttpProxyId;
};

} // namespace NMVP::NSupportLinks
