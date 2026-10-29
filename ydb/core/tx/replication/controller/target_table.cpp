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

TString TTargetIndexTable::BuildStreamPath() const {
    const auto srcParts = SplitPath(GetSrcPath());
    const auto dstParts = SplitPath(GetDstPath());

    // A replicated index table refers to the source impl table directly, while an index built
    // after the replication was created refers to the source index: its stream lives on the impl
    // table, whose name depends on the index type and is taken from the destination path.
    if (srcParts.empty() || dstParts.empty() || srcParts.back() == dstParts.back()) {
        return CanonizePath(ChildPath(srcParts, {GetStreamName()}));
    }

    return CanonizePath(ChildPath(srcParts, {dstParts.back(), GetStreamName()}));
}

}
