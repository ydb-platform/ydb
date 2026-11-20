#include "helpers.h"

#include "client.h"
#include "private.h"
#include "public.h"

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/net/address.h>

#include <util/generic/hash.h>

#include <algorithm>
#include <vector>

namespace NYT::NApi {

using namespace NNet;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ApiLogger;

////////////////////////////////////////////////////////////////////////////////

void ValidateMaintenanceComment(TStringBuf comment)
{
    constexpr int MaxMaintenanceCommentLength = 512;

    if (comment.size() > MaxMaintenanceCommentLength) {
        THROW_ERROR_EXCEPTION("Maintenance comment is too long")
            .With("CommentLength", comment.size())
            .With("MaxCommentLength", MaxMaintenanceCommentLength);
    }
}

void ValidateMaintenanceType(EMaintenanceComponent component, EMaintenanceType type)
{
    static_assert(TEnumTraits<EMaintenanceComponent>::GetDomainSize() == 5);

    static const auto SupportedMaintenanceTypes = [] {
        const auto& domainValues = TEnumTraits<EMaintenanceType>::GetDomainValues();
        const std::vector<EMaintenanceType> allTypes(domainValues.begin(), domainValues.end());
        return THashMap<EMaintenanceComponent, std::vector<EMaintenanceType>>{
            {EMaintenanceComponent::ClusterNode, allTypes},
            {EMaintenanceComponent::Host, allTypes},
            {EMaintenanceComponent::HttpProxy, {
                EMaintenanceType::Ban,
            }},
            {EMaintenanceComponent::RpcProxy, {
                EMaintenanceType::Ban,
            }},
            {EMaintenanceComponent::ChunkLocation, {
                EMaintenanceType::Decommission,
                EMaintenanceType::DisableWriteSessions,
            }},
        };
    }();

    auto it = SupportedMaintenanceTypes.find(component);
    if (it == SupportedMaintenanceTypes.end()) {
        YT_TLOG_ALERT_AND_THROW("Invalid maintenance component")
            .With("Component", component)
            .With("Type", type);
    }

    const auto& supportedTypes = it->second;
    if (std::ranges::find(supportedTypes, type) == supportedTypes.end()) {
        THROW_ERROR_EXCEPTION("Maintenance type is not supported for component")
            .With("Component", component)
            .With("Type", type)
            .With("SupportedTypes", supportedTypes);
    }
}

////////////////////////////////////////////////////////////////////////////////

TFuture<std::optional<std::string>> GetDataCenterByClient(const IClientPtr& client)
{
    TListNodeOptions options;
    options.MaxSize = 1;

    return client->ListNode(RpcProxiesPath, options)
        .Apply(BIND([] (const NYson::TYsonString& items) {
            auto itemsList = ConvertTo<IListNodePtr>(items);
            if (!itemsList->GetChildCount()) {
                return std::optional<std::string>();
            }
            auto host = itemsList->GetChildren()[0];
            return InferYPClusterFromHostName(host->GetValue<std::string>());
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
