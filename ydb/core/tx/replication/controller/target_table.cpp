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
    , SrcIsImpl(config->GetSrcIsImpl())
{
}

TString TTargetIndexTable::GetSrcIndexPath() const {
    const auto srcParts = SplitPath(GetSrcPath());
    const auto dstParts = SplitPath(GetDstPath());
    if (SrcIsImpl) {
        return CanonizePath(TString(ExtractParent(GetSrcPath())));
    }
    // src is the index itself, dst is the impl table
    return CanonizePath(GetSrcPath());
}

TString TTargetIndexTable::BuildStreamPath() const {
    const auto srcParts = SplitPath(GetSrcPath());
    const auto dstParts = SplitPath(GetDstPath());
    if (SrcIsImpl) {
        return CanonizePath(ChildPath(srcParts, {GetStreamName()}));
    }
    // src is the index itself, dst is the impl table
    return CanonizePath(ChildPath(srcParts, {dstParts.back(), GetStreamName()}));
}

}
