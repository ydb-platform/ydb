#pragma once

#include <ydb/mvp/meta/support_links/support_links_resolver.h>
#include <ydb/mvp/meta/mvp.h>

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
    virtual size_t Place() const = 0;
    virtual const TSupportLinkEntryConfig& Config() const = 0;
    virtual TResolveOutput Resolve(const TResolveInput& input) const = 0;
};

std::shared_ptr<ILinkSource> MakeLinkSource(size_t place, TSupportLinkEntryConfig config);

} // namespace NMVP
