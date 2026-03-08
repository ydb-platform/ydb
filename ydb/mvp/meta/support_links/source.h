#pragma once

#include <ydb/mvp/meta/support_links/types.h>
#include <ydb/mvp/meta/meta_settings.h>

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/http/http.h>

#include <util/generic/hash.h>
#include <memory>

namespace NMVP {

class ILinkSource {
public:
    struct TResolveInput {
        size_t Place = 0;
        const THashMap<TString, TString>& ClusterColumns;
        const NHttp::TUrlParameters& UrlParameters;
        const NActors::TActorId& Parent;
        const NActors::TActorId& HttpProxyId;
    };

    virtual ~ILinkSource() = default;
    virtual const TSupportLinkEntryConfig& Config() const = 0;
    virtual TResolveOutput Resolve(const TResolveInput& input) const = 0;
};

using TLinkSourceFactory = std::shared_ptr<ILinkSource> (*)(TSupportLinkEntryConfig);

void RegisterLinkSource(TString source, TLinkSourceFactory factory);
std::shared_ptr<ILinkSource> MakeLinkSource(TSupportLinkEntryConfig config);

} // namespace NMVP
