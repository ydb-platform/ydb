#include "yql_yt_ypath_attrs.h"

#include <yt/cpp/mapreduce/interface/common.h>
#include <yt/cpp/mapreduce/interface/serialize.h>
#include <yt/cpp/mapreduce/common/helpers.h>

#include <library/cpp/yson/node/node.h>
#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/yson/node/node_builder.h>

#include <util/generic/hash_set.h>
#include <util/generic/yexception.h>

namespace NYql {

namespace {

const THashSet<TString> SUPPORTED_RICH_YPATH_ATTRS = {
    "timestamp"
};

}

TMaybe<TString> SerializeRichYPathAttrs(const NYT::TRichYPath& richPath) {
    NYT::TNode pathNode;
    NYT::TNodeBuilder builder(&pathNode);
    NYT::Serialize(richPath, &builder);
    if (!pathNode.HasAttributes() || pathNode.GetAttributes().Empty()) {
        return Nothing();
    }
    auto attrMap = pathNode.GetAttributes().AsMap();
    attrMap.erase("columns");
    attrMap.erase("ranges");
    for (const auto& [attr, _] : attrMap) {
        if (!SUPPORTED_RICH_YPATH_ATTRS.contains(attr)) {
            throw yexception() << "Unsupported YPath attribute: '" << attr << "'";
        }
    }
    pathNode.Attributes() = attrMap;
    return NYT::NodeToYsonString(pathNode.GetAttributes());
}

void DeserializeRichYPathAttrs(const TString& serializedAttrs, NYT::TRichYPath& richPath) {
    NYT::TNode pathNode;
    NYT::TNodeBuilder pathNodeBuilder(&pathNode);
    NYT::Serialize(richPath, &pathNodeBuilder);
    NYT::MergeNodes(pathNode.Attributes(), NYT::NodeFromYsonString(serializedAttrs));
    NYT::Deserialize(richPath, pathNode);
}

} // namespace NYql
