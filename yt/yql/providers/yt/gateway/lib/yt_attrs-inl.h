#pragma once

#include "yt_attrs.h"

#include <yt/cpp/mapreduce/common/helpers.h>
#include <yt/yql/providers/yt/common/yql_names.h>
#include <yt/yql/providers/yt/lib/schema/schema.h>
#include <yql/essentials/providers/common/codec/yql_codec_type_flags.h>

namespace NYql {

template <class TExecParamsPtr>
void PrepareCommonAttributes(
    NYT::TNode& attrs,
    const TExecParamsPtr& execCtx,
    const TString& cluster,
    bool createTable)
{
    if (auto compressionCodec = execCtx->Options_.Config()->TemporaryCompressionCodec.Get(cluster)) {
        attrs["compression_codec"] = *compressionCodec;
    }
    if (auto erasureCodec = execCtx->Options_.Config()->TemporaryErasureCodec.Get(cluster)) {
        attrs["erasure_codec"] = ToString(*erasureCodec);
    }
    if (auto optimizeFor = execCtx->Options_.Config()->OptimizeFor.Get(cluster)) {
        attrs["optimize_for"] = ToString(*optimizeFor);
    }
    if (auto ttl = execCtx->Options_.Config()->TempTablesTtl.Get().GetOrElse(TDuration::Zero())) {
        attrs["expiration_timeout"] = ttl.MilliSeconds();
    }

    if (createTable) {
        if (auto replicationFactor = execCtx->Options_.Config()->TemporaryReplicationFactor.Get(cluster)) {
            attrs["replication_factor"] = static_cast<i64>(*replicationFactor);
        }
        if (auto media = execCtx->Options_.Config()->TemporaryMedia.Get(cluster)) {
            attrs["media"] = *media;
        }
        if (auto primaryMedium = execCtx->Options_.Config()->TemporaryPrimaryMedium.Get(cluster)) {
            attrs["primary_medium"] = *primaryMedium;
        }
    }
}

template <class TExecParamsPtr>
void PrepareAttributes(
    NYT::TNode& attrs,
    const TOutputInfo& out,
    const TExecParamsPtr& execCtx,
    const TString& cluster,
    bool createTable,
    const TSet<TString>& securityTags)
{
    PrepareCommonAttributes<TExecParamsPtr>(attrs, execCtx, cluster, createTable);

    NYT::MergeNodes(attrs, out.AttrSpec);

    if (createTable) {
        const auto nativeTypeCompat = execCtx->Options_.Config()->NativeYtTypeCompatibility.Get(cluster).GetOrElse(NTCF_LEGACY);
        attrs["schema"] = RowSpecToYTSchema(out.Spec[YqlRowSpecAttribute], nativeTypeCompat, out.ColumnGroups).ToNode();
    }

    if (!securityTags.empty()) {
        auto tagsAttrNode = NYT::TNode::CreateList();
        for (const auto& tag : securityTags) {
            tagsAttrNode.Add(tag);
        }
        attrs[SecurityTagsName] = std::move(tagsAttrNode);
    }
}

} // namespace NYql


