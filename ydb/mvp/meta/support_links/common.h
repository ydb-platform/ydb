#pragma once

#include <ydb/mvp/meta/mvp.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>

namespace NMVP::NSupportLinks {

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
