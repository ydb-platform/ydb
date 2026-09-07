#pragma once

#include "billing.h"

#include <ydb/core/persqueue/public/pq_rl_helpers.h>

#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>

namespace NKikimr::NSqsTopic::V1 {

    // Database user-attribute keys that carry the rate-limiter (RU billing)
    // coordination node and topic resource paths. Kept in sync with the gRPC
    // request check actor (ruRlTopicConfig).
    inline constexpr TStringBuf RL_COORDINATION_NODE_ATTR = "serverless_rt_coordination_node_path";
    inline constexpr TStringBuf RL_TOPIC_RESOURCE_ATTR = "serverless_rt_topic_resource_ru";
    inline constexpr TStringBuf CLOUD_ID_ATTR = "cloud_id";
    inline constexpr TStringBuf FOLDER_ID_ATTR = "folder_id";
    inline constexpr TStringBuf DATABASE_ID_ATTR = "database_id";

    inline TMaybe<NPQ::TRlContext> ParseRlContext(
        const THashMap<TString, TString>& attrs,
        const TString& database,
        const TString& token)
    {
        TString coordinationNode;
        TString resourcePath;
        if (const auto* value = attrs.FindPtr(RL_COORDINATION_NODE_ATTR)) {
            coordinationNode = *value;
        }
        if (const auto* value = attrs.FindPtr(RL_TOPIC_RESOURCE_ATTR)) {
            resourcePath = *value;
        }
        if (coordinationNode.empty() || resourcePath.empty()) {
            return Nothing();
        }
        return NPQ::TRlContext(coordinationNode, resourcePath, database, token);
    }

    inline NBilling::TMeteringIds ParseMeteringIds(const THashMap<TString, TString>& attrs) {
        NBilling::TMeteringIds ids;
        if (const auto* value = attrs.FindPtr(CLOUD_ID_ATTR)) {
            ids.CloudId = *value;
        }
        if (const auto* value = attrs.FindPtr(FOLDER_ID_ATTR)) {
            ids.FolderId = *value;
        }
        if (const auto* value = attrs.FindPtr(DATABASE_ID_ATTR)) {
            ids.DatabaseId = *value;
        }
        return ids;
    }

} // namespace NKikimr::NSqsTopic::V1
