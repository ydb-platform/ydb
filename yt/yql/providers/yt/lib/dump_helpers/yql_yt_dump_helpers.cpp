#include "yql_yt_dump_helpers.h"

#include <yt/yql/providers/yt/lib/hash/yql_hash_builder.h>

#include <library/cpp/yson/node/node_io.h>

#include <util/string/hex.h>

namespace NYql {

TString MakeDumpKey(const TStringBuf& name, const TStringBuf& cluster) {
    return (THashBuilder() << NYT::NodeToCanonicalYsonString(NYT::TNode::CreateList({ name, cluster }))).Finish();
}

TString MakeDumpPath(const TString& srcPath, const TString& cluster, const TYqlOperationOptions& opOptions, const TYtSettings::TConstPtr& configuration, bool out) {
    YQL_ENSURE(opOptions.Id.Defined(), "Operation id is not set");

    auto opIdHash = HexEncode((THashBuilder() << *opOptions.Id).Finish());
    auto tableHash = HexEncode((THashBuilder() << srcPath << cluster).Finish());

    TString root;
    if (!out) {
        auto dumpFolder = configuration->_QueryDumpFolder.Get(cluster);
        YQL_ENSURE(dumpFolder.Defined(), "yt._QueryDumpFolder is not set for cluster " << cluster);
        root = *dumpFolder;
    } else {
        root = "//tmp/yql/_qdump_out";
    }

    return TStringBuilder() << root << "/" << opIdHash << "/" << tableHash;
}

TMaybe<TString> GetDumpPath(const TStringBuf& name, const TStringBuf& cluster, const TString& component, const TQContext& qContext) {
    YQL_ENSURE(qContext.CaptureMode() == EQPlayerCaptureMode::Full);

    auto dumpKey = MakeDumpKey(name, cluster);
    auto item = qContext.GetReader()->Get({ component, dumpKey }).GetValueSync();
    return item.AndThen([] (const TQItem& item) {
        return MakeMaybe(item.Value);
    });
}

} // namespace NYql
