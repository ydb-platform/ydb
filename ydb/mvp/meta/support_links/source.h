#pragma once

#include <ydb/mvp/meta/support_links/support_links_resolver.h>

#include <ydb/mvp/meta/meta_settings.h>
#include <ydb/mvp/meta/protos/config.pb.h>

#include <memory>

namespace NMVP {

class ILinkSource {
public:
    struct TResolveInput {
        size_t Place = 0;
        const THashMap<TString, TString>& ClusterColumns;
        const TVector<std::pair<TString, TString>>& QueryParams;
        const NActors::TActorId& Parent;
        const NActors::TActorId& HttpProxyId;
    };

    virtual ~ILinkSource() = default;
    virtual size_t Place() const = 0;
    virtual const TSupportLinkEntry& Config() const = 0;
    virtual TResolveOutput Resolve(const TResolveInput& input) const = 0;
};

std::shared_ptr<ILinkSource> MakeLinkSource(size_t place, TSupportLinkEntry config, const TMetaSettings& metaSettings);
std::shared_ptr<ILinkSource> MakeLinkSource(size_t place, TSupportLinkEntry config);

} // namespace NMVP
