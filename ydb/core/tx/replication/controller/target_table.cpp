#include "target_table.h"

#include <ydb/core/base/path.h>
#include <ydb/core/protos/replication.pb.h>

namespace NKikimr::NReplication::NController {

TTargetTable::TTargetTable(TReplication* replication, ui64 id, const IConfig::TPtr& config)
    : TTargetWithStream(replication, ETargetKind::Table, id, config)
{
}

TString TTargetTable::BuildStreamPath() const {
    return CanonizePath(ChildPath(SplitPath(GetSrcPath()), GetStreamName()));
}

TTargetIndexTable::TTargetIndexTable(TReplication* replication, ui64 id, const IConfig::TPtr& config)
    : TTargetWithStream(replication, ETargetKind::IndexTable, id, config)
{
}

TString TTargetIndexTable::GetSrcIndexPath() const {
    const auto srcParts = SplitPath(GetSrcPath());
    const auto dstParts = SplitPath(GetDstPath());
    if (srcParts.empty() || dstParts.empty() || srcParts.back() == dstParts.back()) {
        // src is the index itself (not ideal, but this is the only way without migration)
        return CanonizePath(TString(ExtractParent(GetSrcPath())));
    }
    return CanonizePath(GetSrcPath());
}

TString TTargetIndexTable::BuildStreamPath() const {
    const auto srcParts = SplitPath(GetSrcPath());
    const auto dstParts = SplitPath(GetDstPath());
    if (srcParts.empty() || dstParts.empty() || srcParts.back() == dstParts.back()) {
        // src is the index itself
        return CanonizePath(ChildPath(srcParts, {GetStreamName()}));
    }
    return CanonizePath(ChildPath(srcParts, {dstParts.back(), GetStreamName()}));
}

}
