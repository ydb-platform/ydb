#pragma once

#include <ydb/mvp/meta/support_links/entity.h>
#include <ydb/mvp/meta/support_links/types.h>

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/http/http.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <memory>

namespace NMVP::NSupportLinks {

struct TRequestContext {
    TVector<TEntityIdentity> Identities;
    THashMap<TString, TString> ClusterInfo;
    TCgiParameters AdditionalRequestParams;
};

class ILinkSource {
public:
    struct TLinkResolveInput {
        const THashMap<TString, TString>& ClusterInfo;
        const TCgiParameters& AdditionalRequestParams;
        const TEntityIdentity& Identity;
    };

    struct TResolveContext {
        size_t Place = 0;
        NActors::TActorId Owner;
        NActors::TActorId HttpProxyId;
    };

    virtual ~ILinkSource() = default;
    virtual TResolveOutput Resolve(const TLinkResolveInput& input, const TResolveContext& context) const = 0;
};

using TLinkSourceFactory = std::shared_ptr<ILinkSource> (*)(TSupportLinkEntryConfig config, const TMetaSettings& metaSettings);

void ValidateSupportLinksConfig(const TSupportLinksConfig& supportLinks, const TMetaSettings& metaSettings);
void ValidateLinkSourceConfig(const TSupportLinkEntryConfig& config, const TMetaSettings& metaSettings);
std::shared_ptr<ILinkSource> MakeLinkSource(TSupportLinkEntryConfig config, const TMetaSettings& metaSettings);

} // namespace NMVP::NSupportLinks
