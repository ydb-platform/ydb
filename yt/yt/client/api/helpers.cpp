#include "helpers.h"

#include "client.h"

#include "public.h"

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/net/address.h>

namespace NYT::NApi {

using namespace NNet;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void ValidateMaintenanceComment(TStringBuf comment)
{
    constexpr int MaxMaintenanceCommentLength = 512;

    if (comment.size() > MaxMaintenanceCommentLength) {
        THROW_ERROR_EXCEPTION("Maintenance comment is too long")
            << TErrorAttribute("comment_length", comment.size())
            << TErrorAttribute("max_comment_length", MaxMaintenanceCommentLength);
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
